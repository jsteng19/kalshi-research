#!/usr/bin/env python3
"""
College Football Full Game Video Finder

Searches YouTube for full game videos and optionally adds them to a playlist.

Usage:
    # Search only (shows results, doesn't add to playlist)
    python cfb_video_finder.py games.csv --search-only
    
    # Use web scraping instead of API (no quota limits!)
    python cfb_video_finder.py games.csv --scrape
    
    # Add to existing playlist
    python cfb_video_finder.py games.csv --playlist-id PLxxxxxx
    
    # Create new playlist and add videos
    python cfb_video_finder.py games.csv --create-playlist "2024 CFB Games"
    
    # Resume from where you left off (after quota exceeded)
    python cfb_video_finder.py games.csv --resume results.csv --playlist-id PLxxxxx

CSV Format:
    date,home_team,away_team,year
    2024-01-01,Alabama,Michigan,2024
    
Or with a 'teams' column:
    date,teams,year
    2024-01-01,Alabama vs Michigan,2024

OAuth Setup (for playlist features):
    1. Go to Google Cloud Console: https://console.cloud.google.com
    2. Create a project and enable YouTube Data API v3
    3. Create OAuth 2.0 credentials (Desktop app type)
    4. Download the JSON file as 'client_secrets.json' in project root
    5. First run will open browser for authorization
    
API Quota Info:
    - YouTube API has 10,000 units/day limit
    - search.list = 100 units (expensive!)
    - videos.list = 1 unit
    - Use --scrape mode to avoid quota limits entirely
"""

import argparse
import csv
import json
import os
import re
import sys
import time
import random
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus, urlencode

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Load environment variables
load_dotenv()


@dataclass
class Game:
    """Represents a college football game to find."""
    date: str
    home_team: str
    away_team: str
    year: int
    
    @property
    def search_queries(self) -> list[str]:
        """Generate search queries to try."""
        return [
            f"{self.away_team} at {self.home_team} {self.year} full game",
            f"{self.home_team} vs {self.away_team} {self.year} full game",
            f"{self.away_team} vs {self.home_team} {self.year} full game",
            f"{self.home_team} {self.away_team} {self.year} full game",
        ]
    
    @property
    def display_name(self) -> str:
        return f"{self.away_team} at {self.home_team} ({self.year})"


@dataclass  
class VideoResult:
    """Represents a YouTube video search result."""
    video_id: str
    title: str
    channel: str
    duration_seconds: int
    published_at: str
    thumbnail_url: str
    
    @property
    def url(self) -> str:
        return f"https://www.youtube.com/watch?v={self.video_id}"
    
    @property
    def duration_formatted(self) -> str:
        hours = self.duration_seconds // 3600
        minutes = (self.duration_seconds % 3600) // 60
        if hours > 0:
            return f"{hours}h {minutes}m"
        return f"{minutes}m"


class CFBVideoFinder:
    """Finds college football full game videos on YouTube."""
    
    # Minimum duration for a "full game" (90 minutes)
    MIN_DURATION_SECONDS = 90 * 60
    
    # Team name aliases for better matching
    TEAM_ALIASES = {
        'USC': ['USC', 'Southern California', 'Trojans'],
        'LSU': ['LSU', 'Louisiana State', 'Tigers'],
        'UCLA': ['UCLA', 'Bruins'],
        'OSU': ['Ohio State', 'OSU', 'Buckeyes'],
        'OU': ['Oklahoma', 'OU', 'Sooners'],
        'UT': ['Texas', 'UT', 'Longhorns'],
        'FSU': ['Florida State', 'FSU', 'Seminoles'],
        'UGA': ['Georgia', 'UGA', 'Bulldogs'],
        'Bama': ['Alabama', 'Bama', 'Crimson Tide'],
        'Ole Miss': ['Ole Miss', 'Mississippi', 'Rebels'],
        'Miss State': ['Mississippi State', 'Miss State', 'Bulldogs'],
        'A&M': ['Texas A&M', 'A&M', 'Aggies'],
        'Mich': ['Michigan', 'Wolverines'],
        'PSU': ['Penn State', 'PSU', 'Nittany Lions'],
        'ND': ['Notre Dame', 'ND', 'Fighting Irish'],
        'Clemson': ['Clemson', 'Tigers'],
        'Auburn': ['Auburn', 'Tigers'],
        'Miami': ['Miami', 'Hurricanes', 'The U'],
        'Oregon': ['Oregon', 'Ducks'],
        'Washington': ['Washington', 'UW', 'Huskies'],
        'USC': ['USC', 'Southern Cal', 'Trojans'],
        'Florida': ['Florida', 'UF', 'Gators'],
        'Tennessee': ['Tennessee', 'Vols', 'Volunteers'],
    }
    
    # User agents for web scraping
    USER_AGENTS = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    ]
    
    def __init__(self, api_key: str = None, use_scraping: bool = False):
        self.use_scraping = use_scraping
        self.quota_exceeded = False
        
        if api_key and not use_scraping:
            self.youtube = build('youtube', 'v3', developerKey=api_key)
        else:
            self.youtube = None
            
        self.youtube_auth = None  # Set via authenticate_oauth() for playlist operations
        
        # Session for web scraping
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': random.choice(self.USER_AGENTS),
            'Accept-Language': 'en-US,en;q=0.9',
        })
        
    def authenticate_oauth(self, client_secrets_path: str = 'client_secrets_3.json'):
        """
        Authenticate with OAuth for playlist operations.
        Creates/uses a token file for persistent auth.
        """
        try:
            from google_auth_oauthlib.flow import InstalledAppFlow
            from google.auth.transport.requests import Request
            from google.oauth2.credentials import Credentials
        except ImportError:
            print("Error: OAuth libraries not installed.")
            print("Run: pip install google-auth-oauthlib google-auth-httplib2")
            return False
            
        SCOPES = ['https://www.googleapis.com/auth/youtube']
        token_path = Path('youtube_token.json')
        creds = None
        
        # Load existing token
        if token_path.exists():
            creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
            
        # Refresh or get new credentials
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not Path(client_secrets_path).exists():
                    print(f"Error: {client_secrets_path} not found.")
                    print("Download OAuth client secrets from Google Cloud Console.")
                    return False
                    
                flow = InstalledAppFlow.from_client_secrets_file(client_secrets_path, SCOPES)
                creds = flow.run_local_server(port=0)
                
            # Save token for next time
            with open(token_path, 'w') as f:
                f.write(creds.to_json())
                
        self.youtube_auth = build('youtube', 'v3', credentials=creds)
        print("✓ OAuth authentication successful")
        return True
    
    def parse_duration(self, duration: str) -> int:
        """Parse ISO 8601 duration (PT1H30M15S) to seconds."""
        match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration)
        if not match:
            return 0
        hours = int(match.group(1) or 0)
        minutes = int(match.group(2) or 0)
        seconds = int(match.group(3) or 0)
        return hours * 3600 + minutes * 60 + seconds
    
    def get_team_variations(self, team: str) -> list[str]:
        """Get all variations of a team name for matching."""
        team_lower = team.lower()
        variations = [team]
        
        for key, aliases in self.TEAM_ALIASES.items():
            if team_lower in [a.lower() for a in aliases] or key.lower() == team_lower:
                variations.extend(aliases)
                
        return list(set(variations))
    
    def validate_title(self, title: str, game: Game) -> tuple[bool, str]:
        """
        Validate that the video title matches the expected game.
        Returns (is_valid, reason).
        """
        title_lower = title.lower()
        
        # Get all variations of team names
        home_variations = self.get_team_variations(game.home_team)
        away_variations = self.get_team_variations(game.away_team)
        
        # Check if both teams are mentioned
        home_found = any(v.lower() in title_lower for v in home_variations)
        away_found = any(v.lower() in title_lower for v in away_variations)
        
        if not home_found and not away_found:
            return False, "Neither team found in title"
        if not home_found:
            return False, f"Home team '{game.home_team}' not found in title"
        if not away_found:
            return False, f"Away team '{game.away_team}' not found in title"
            
        # Check for year (can be full year or last 2 digits)
        year_str = str(game.year)
        year_short = year_str[-2:]
        year_found = year_str in title or year_short in title
        
        if not year_found:
            # Check for season format (e.g., "2023-24" for 2024)
            prev_year = str(game.year - 1)
            season_format = f"{prev_year[-2:]}-{year_short}"
            if season_format not in title:
                return False, f"Year {game.year} not found in title"
        
        # Check for indicators it's NOT a full game
        bad_indicators = ['highlights', 'recap', 'top plays', 'best moments', 
                         'preview', 'trailer', 'analysis', 'reaction', 'breakdown',
                         'picks', 'predictions', 'betting']
        for indicator in bad_indicators:
            if indicator in title_lower:
                return False, f"Title contains '{indicator}' - likely not full game"
                
        return True, "Title matches expected game"
    
    def search_video(self, game: Game, max_results: int = 5) -> list[VideoResult]:
        """
        Search for full game videos for a given game.
        Returns list of validated video results.
        """
        # Use scraping if configured or if API quota exceeded
        if self.use_scraping or self.quota_exceeded:
            return self.search_video_scrape(game, max_results)
            
        return self.search_video_api(game, max_results)
    
    def search_video_api(self, game: Game, max_results: int = 5) -> list[VideoResult]:
        """Search using YouTube API (costs quota)."""
        all_results = []
        seen_ids = set()
        
        for query in game.search_queries:
            try:
                search_response = self.youtube.search().list(
                    q=query,
                    type='video',
                    part='id,snippet',
                    maxResults=max_results,
                    videoDuration='long',  # >20 minutes
                    order='relevance'
                ).execute()
                
                video_ids = [item['id']['videoId'] for item in search_response.get('items', [])]
                
                if not video_ids:
                    continue
                    
                # Get video details including duration
                details_response = self.youtube.videos().list(
                    id=','.join(video_ids),
                    part='contentDetails,snippet'
                ).execute()
                
                for item in details_response.get('items', []):
                    video_id = item['id']
                    if video_id in seen_ids:
                        continue
                    seen_ids.add(video_id)
                    
                    duration_seconds = self.parse_duration(
                        item['contentDetails']['duration']
                    )
                    
                    # Skip if too short
                    if duration_seconds < self.MIN_DURATION_SECONDS:
                        continue
                        
                    snippet = item['snippet']
                    title = snippet['title']
                    
                    # Validate title
                    is_valid, reason = self.validate_title(title, game)
                    if not is_valid:
                        continue
                        
                    result = VideoResult(
                        video_id=video_id,
                        title=title,
                        channel=snippet['channelTitle'],
                        duration_seconds=duration_seconds,
                        published_at=snippet['publishedAt'][:10],
                        thumbnail_url=snippet['thumbnails']['default']['url']
                    )
                    all_results.append(result)
                    
            except HttpError as e:
                error_str = str(e).lower()
                if 'quota' in error_str:
                    print(f"  ⚠ API quota exceeded! Switching to scraping mode...")
                    self.quota_exceeded = True
                    # Try scraping for this game
                    return self.search_video_scrape(game, max_results)
                print(f"  ⚠ Search error for '{query}': {e}")
                # Stop trying more queries for this game on error
                break
                
            # Small delay between queries
            time.sleep(0.5)
            
            # If we found a good result, stop searching
            if all_results:
                break
                
        return all_results
    
    def search_video_scrape(self, game: Game, max_results: int = 5) -> list[VideoResult]:
        """
        Search using web scraping (no API quota!).
        Scrapes YouTube search results page directly.
        """
        all_results = []
        seen_ids = set()
        
        # Only try first query to be efficient
        query = game.search_queries[0]
        
        try:
            # Build search URL with duration filter (long videos)
            search_url = f"https://www.youtube.com/results?search_query={quote_plus(query)}&sp=EgIYAg%253D%253D"
            
            # Rotate user agent
            self.session.headers['User-Agent'] = random.choice(self.USER_AGENTS)
            
            response = self.session.get(search_url, timeout=15)
            if response.status_code != 200:
                print(f"  ⚠ Scrape failed: HTTP {response.status_code}")
                return []
            
            # Extract video data from the page's JavaScript
            # YouTube embeds data in ytInitialData variable
            match = re.search(r'var ytInitialData = ({.*?});</script>', response.text)
            if not match:
                # Try alternative pattern
                match = re.search(r'ytInitialData\s*=\s*({.*?});</script>', response.text)
            
            if not match:
                print(f"  ⚠ Could not parse YouTube response")
                return []
                
            data = json.loads(match.group(1))
            
            # Navigate the JSON structure to find video results
            contents = (data.get('contents', {})
                       .get('twoColumnSearchResultsRenderer', {})
                       .get('primaryContents', {})
                       .get('sectionListRenderer', {})
                       .get('contents', []))
            
            for section in contents:
                items = (section.get('itemSectionRenderer', {})
                        .get('contents', []))
                
                for item in items:
                    video_data = item.get('videoRenderer', {})
                    if not video_data:
                        continue
                        
                    video_id = video_data.get('videoId')
                    if not video_id or video_id in seen_ids:
                        continue
                    seen_ids.add(video_id)
                    
                    title = ''
                    title_runs = video_data.get('title', {}).get('runs', [])
                    if title_runs:
                        title = title_runs[0].get('text', '')
                    
                    # Get duration
                    duration_text = video_data.get('lengthText', {}).get('simpleText', '0:00')
                    duration_seconds = self.parse_duration_text(duration_text)
                    
                    # Skip if too short
                    if duration_seconds < self.MIN_DURATION_SECONDS:
                        continue
                    
                    # Validate title
                    is_valid, reason = self.validate_title(title, game)
                    if not is_valid:
                        continue
                    
                    channel = ''
                    channel_runs = video_data.get('ownerText', {}).get('runs', [])
                    if channel_runs:
                        channel = channel_runs[0].get('text', '')
                    
                    published = video_data.get('publishedTimeText', {}).get('simpleText', '')
                    
                    result = VideoResult(
                        video_id=video_id,
                        title=title,
                        channel=channel,
                        duration_seconds=duration_seconds,
                        published_at=published,
                        thumbnail_url=f"https://i.ytimg.com/vi/{video_id}/default.jpg"
                    )
                    all_results.append(result)
                    
                    if len(all_results) >= max_results:
                        break
                        
                if all_results:
                    break
                    
        except Exception as e:
            print(f"  ⚠ Scrape error: {e}")
            
        # Rate limit scraping
        time.sleep(random.uniform(2, 4))
        
        return all_results
    
    def parse_duration_text(self, duration_text: str) -> int:
        """Parse duration like '2:15:30' or '1:30:00' to seconds."""
        parts = duration_text.split(':')
        try:
            if len(parts) == 3:
                return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
            elif len(parts) == 2:
                return int(parts[0]) * 60 + int(parts[1])
            else:
                return 0
        except ValueError:
            return 0
    
    def create_playlist(self, title: str, description: str = "") -> Optional[str]:
        """Create a new playlist and return its ID."""
        if not self.youtube_auth:
            print("Error: OAuth not authenticated. Call authenticate_oauth() first.")
            return None
            
        try:
            response = self.youtube_auth.playlists().insert(
                part='snippet,status',
                body={
                    'snippet': {
                        'title': title,
                        'description': description
                    },
                    'status': {
                        'privacyStatus': 'private'  # Start as private
                    }
                }
            ).execute()
            
            playlist_id = response['id']
            print(f"✓ Created playlist: {title}")
            print(f"  ID: {playlist_id}")
            print(f"  URL: https://www.youtube.com/playlist?list={playlist_id}")
            return playlist_id
            
        except HttpError as e:
            print(f"Error creating playlist: {e}")
            return None
    
    def add_to_playlist(self, playlist_id: str, video_id: str) -> bool:
        """Add a video to a playlist."""
        if not self.youtube_auth:
            print("Error: OAuth not authenticated. Call authenticate_oauth() first.")
            return False
            
        try:
            self.youtube_auth.playlistItems().insert(
                part='snippet',
                body={
                    'snippet': {
                        'playlistId': playlist_id,
                        'resourceId': {
                            'kind': 'youtube#video',
                            'videoId': video_id
                        }
                    }
                }
            ).execute()
            return True
            
        except HttpError as e:
            if 'duplicate' in str(e).lower():
                print(f"  ⏭ Already in playlist")
                return True
            print(f"  ⚠ Error adding to playlist: {e}")
            return False


def load_games_from_csv(csv_path: str) -> list[Game]:
    """Load games from a CSV file."""
    games = []
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = [h.lower().strip() for h in reader.fieldnames or []]
        
        for row in reader:
            # Normalize keys to lowercase
            row = {k.lower().strip(): v.strip() if v else '' for k, v in row.items()}
            
            # Skip empty rows
            if not any(row.values()):
                continue
                
            # Extract date
            date = row.get('date', '')
            
            # Extract teams
            if 'home_team' in row and 'away_team' in row:
                home_team = row['home_team']
                away_team = row['away_team']
            elif 'teams' in row:
                # Parse "Team1 vs Team2" or "Team1 at Team2"
                teams_str = row['teams']
                if ' at ' in teams_str:
                    parts = teams_str.split(' at ')
                    away_team = parts[0].strip()
                    home_team = parts[1].strip()
                elif ' vs ' in teams_str.lower():
                    parts = re.split(r'\s+vs\.?\s+', teams_str, flags=re.IGNORECASE)
                    home_team = parts[0].strip()
                    away_team = parts[1].strip() if len(parts) > 1 else ''
                else:
                    print(f"Warning: Could not parse teams from '{teams_str}'")
                    continue
            elif 'home' in row and 'away' in row:
                home_team = row['home']
                away_team = row['away']
            else:
                print(f"Warning: Could not find team columns in row: {row}")
                continue
                
            # Extract year
            if 'year' in row:
                year = int(row['year'])
            elif date:
                # Try to extract year from date
                year_match = re.search(r'20\d{2}', date)
                if year_match:
                    year = int(year_match.group())
                else:
                    print(f"Warning: Could not determine year for {home_team} vs {away_team}")
                    continue
            else:
                print(f"Warning: No year specified for {home_team} vs {away_team}")
                continue
                
            if home_team and away_team:
                games.append(Game(
                    date=date,
                    home_team=home_team,
                    away_team=away_team,
                    year=year
                ))
                
    return games


def load_previous_results(csv_path: str) -> dict[str, dict]:
    """Load previous results from CSV to enable resume."""
    results = {}
    if not os.path.exists(csv_path):
        return results
        
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            game_key = row.get('game', '')
            if game_key and row.get('status') in ('found', 'added_to_playlist'):
                results[game_key] = row
    return results


def main():
    parser = argparse.ArgumentParser(
        description='Find college football full game videos on YouTube',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
CSV Format Options:
  Option 1: date,home_team,away_team,year
  Option 2: date,teams,year  (where teams = "Away at Home" or "Home vs Away")
  Option 3: date,home,away,year

Examples:
  python cfb_video_finder.py games.csv --search-only
  python cfb_video_finder.py games.csv --scrape  # No API quota!
  python cfb_video_finder.py games.csv --playlist-id PLxxxxx
  python cfb_video_finder.py games.csv --create-playlist "My CFB Games"
  python cfb_video_finder.py games.csv --resume results.csv --playlist-id PLxxxxx

API Quota:
  YouTube API has 10,000 units/day. search.list = 100 units each!
  Use --scrape to avoid API quota limits entirely.
        """
    )
    parser.add_argument('csv_file', help='CSV file with game list')
    parser.add_argument('--search-only', action='store_true', 
                       help='Only search and display results, do not add to playlist')
    parser.add_argument('--scrape', action='store_true',
                       help='Use web scraping instead of API (no quota limits!)')
    parser.add_argument('--playlist-id', help='Existing playlist ID to add videos to')
    parser.add_argument('--create-playlist', metavar='TITLE',
                       help='Create a new playlist with this title')
    parser.add_argument('--output', '-o', metavar='FILE',
                       help='Output CSV file with found video URLs')
    parser.add_argument('--resume', metavar='PREV_RESULTS',
                       help='Resume from previous results CSV (skip already found)')
    parser.add_argument('--min-duration', type=int, default=90,
                       help='Minimum video duration in minutes (default: 90)')
    parser.add_argument('--max-results', type=int, default=10,
                       help='Maximum search results per query (default: 3)')
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.search_only and not args.playlist_id and not args.create_playlist:
        args.search_only = True
        print("Note: Running in search-only mode. Use --playlist-id or --create-playlist to add to playlist.\n")
    
    # Check for API key (not required for scrape mode)
    api_key = os.getenv('YOUTUBE_API_KEY')
    if not api_key and not args.scrape:
        print("Warning: YOUTUBE_API_KEY not set. Using scrape mode.")
        args.scrape = True
        
    # Load games
    if not os.path.exists(args.csv_file):
        print(f"Error: CSV file not found: {args.csv_file}")
        return 1
        
    games = load_games_from_csv(args.csv_file)
    if not games:
        print("Error: No games found in CSV file.")
        return 1
    
    # Load previous results if resuming
    previous_results = {}
    if args.resume:
        previous_results = load_previous_results(args.resume)
        if previous_results:
            print(f"📂 Loaded {len(previous_results)} previous results from {args.resume}")
        
    print(f"🏈 College Football Video Finder")
    print(f"   Games to find: {len(games)}")
    print(f"   Min duration: {args.min_duration} minutes")
    print(f"   Mode: {'Web Scraping (no quota)' if args.scrape else 'YouTube API'}")
    print()
    
    # Initialize finder
    finder = CFBVideoFinder(api_key, use_scraping=args.scrape)
    finder.MIN_DURATION_SECONDS = args.min_duration * 60
    
    # Handle playlist setup
    playlist_id = None
    if args.playlist_id:
        playlist_id = args.playlist_id
        print(f"📋 Using playlist: {playlist_id}")
        if not finder.authenticate_oauth():
            print("Warning: OAuth failed, continuing in search-only mode")
            playlist_id = None
    elif args.create_playlist:
        if finder.authenticate_oauth():
            playlist_id = finder.create_playlist(
                args.create_playlist,
                f"College football full game videos - Created {datetime.now().strftime('%Y-%m-%d')}"
            )
            if not playlist_id:
                print("Warning: Could not create playlist, continuing in search-only mode")
        else:
            print("Warning: OAuth failed, continuing in search-only mode")
    print()
    
    # Search for videos
    results = []
    found_count = 0
    added_count = 0
    skipped_count = 0
    
    for i, game in enumerate(games, 1):
        print(f"[{i}/{len(games)}] {game.display_name}")
        
        # Check if already found in previous results
        if game.display_name in previous_results:
            prev = previous_results[game.display_name]
            print(f"  ⏭ Already found (from resume): {prev.get('video_title', '')[:50]}...")
            results.append(prev)
            found_count += 1
            skipped_count += 1
            
            # Still add to playlist if needed and not already added
            if playlist_id and prev.get('status') != 'added_to_playlist':
                video_id = prev.get('video_url', '').split('v=')[-1].split('&')[0]
                if video_id and finder.add_to_playlist(playlist_id, video_id):
                    print(f"  ➕ Added to playlist")
                    added_count += 1
                    prev['status'] = 'added_to_playlist'
            elif prev.get('status') == 'added_to_playlist':
                added_count += 1
            continue
        
        videos = finder.search_video(game, max_results=args.max_results)
        
        if not videos:
            print(f"  ❌ No full game videos found")
            results.append({
                'game': game.display_name,
                'date': game.date,
                'status': 'not_found',
                'video_url': '',
                'video_title': '',
                'duration': '',
                'channel': ''
            })
            continue
            
        # Use best result (first one that passed validation)
        best = videos[0]
        found_count += 1
        
        print(f"  ✅ Found: {best.title[:60]}...")
        print(f"     Duration: {best.duration_formatted} | Channel: {best.channel}")
        print(f"     URL: {best.url}")
        
        result_data = {
            'game': game.display_name,
            'date': game.date,
            'status': 'found',
            'video_url': best.url,
            'video_title': best.title,
            'duration': best.duration_formatted,
            'channel': best.channel
        }
        
        # Add to playlist if configured
        if playlist_id:
            if finder.add_to_playlist(playlist_id, best.video_id):
                print(f"  ➕ Added to playlist")
                added_count += 1
                result_data['status'] = 'added_to_playlist'
            else:
                result_data['status'] = 'found_but_not_added'
                
        results.append(result_data)
        
        # Rate limiting (less needed for API, more for scraping)
        if finder.use_scraping or finder.quota_exceeded:
            time.sleep(random.uniform(1.5, 3))
        else:
            time.sleep(0.5)
        
    # Summary
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total games: {len(games)}")
    print(f"Videos found: {found_count}")
    if skipped_count:
        print(f"  (from resume: {skipped_count})")
    print(f"Not found: {len(games) - found_count}")
    if finder.quota_exceeded:
        print(f"⚠ API quota was exceeded - switched to scraping mode")
    if playlist_id:
        print(f"Added to playlist: {added_count}")
        print(f"Playlist URL: https://www.youtube.com/playlist?list={playlist_id}")
    
    # Save output CSV if specified
    if args.output:
        with open(args.output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'game', 'date', 'status', 'video_url', 'video_title', 'duration', 'channel'
            ])
            writer.writeheader()
            writer.writerows(results)
        print(f"\nResults saved to: {args.output}")
        
    return 0


if __name__ == '__main__':
    sys.exit(main())

