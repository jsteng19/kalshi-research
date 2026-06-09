"""
Direct YouTube Search Scraper for Mention Markets

Scrapes YouTube search results directly without API (no quota limits).
Adapted from src/college-football/cfb_video_finder.py.
"""

import json
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Any
from urllib.parse import quote_plus

import requests


@dataclass
class VideoResult:
    """YouTube video search result."""
    video_id: str
    title: str
    channel: str
    duration_seconds: int
    published_text: str  # e.g., "2 days ago"
    description: str = ""
    
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


class YouTubeSearchScraper:
    """
    Direct YouTube search scraper - no API needed.
    
    Usage:
        scraper = YouTubeSearchScraper()
        videos = scraper.search("Bernie Sanders Colbert interview January 2026")
        for v in videos:
            print(v.url, v.title)
    """
    
    USER_AGENTS = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    ]
    
    def __init__(self, min_delay: float = 1.5, max_delay: float = 3.0):
        self.min_delay = min_delay
        self.max_delay = max_delay
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': random.choice(self.USER_AGENTS),
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        })
    
    def search(
        self,
        query: str,
        max_results: int = 10,
        min_duration_minutes: int = 0,
        filter_long: bool = False,
    ) -> List[VideoResult]:
        """
        Search YouTube and return video results.
        
        Args:
            query: Search query
            max_results: Maximum number of results
            min_duration_minutes: Minimum video duration (0 = no filter)
            filter_long: If True, filter to videos >20 minutes
            
        Returns:
            List of VideoResult objects
        """
        # Rotate user agent
        self.session.headers['User-Agent'] = random.choice(self.USER_AGENTS)
        
        # Build search URL
        # sp=EgIYAg%253D%253D is the filter for "long" videos (>20 min)
        if filter_long:
            search_url = f"https://www.youtube.com/results?search_query={quote_plus(query)}&sp=EgIYAg%253D%253D"
        else:
            search_url = f"https://www.youtube.com/results?search_query={quote_plus(query)}"
        
        try:
            response = self.session.get(search_url, timeout=15)
            if response.status_code != 200:
                print(f"  ⚠ Search failed: HTTP {response.status_code}")
                return []
            
            # Extract video data from ytInitialData JavaScript variable
            match = re.search(r'var ytInitialData = ({.*?});</script>', response.text)
            if not match:
                match = re.search(r'ytInitialData\s*=\s*({.*?});</script>', response.text)
            
            if not match:
                print("  ⚠ Could not parse YouTube response")
                return []
            
            data = json.loads(match.group(1))
            videos = self._parse_search_results(data, max_results, min_duration_minutes * 60)
            
            # Rate limiting
            time.sleep(random.uniform(self.min_delay, self.max_delay))
            
            return videos
            
        except Exception as e:
            print(f"  ⚠ Search error: {e}")
            return []
    
    def _parse_search_results(
        self,
        data: Dict,
        max_results: int,
        min_duration_seconds: int
    ) -> List[VideoResult]:
        """Parse YouTube search response JSON."""
        results = []
        seen_ids = set()
        
        # Navigate the nested JSON structure
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
                
                # Get title
                title = ''
                title_runs = video_data.get('title', {}).get('runs', [])
                if title_runs:
                    title = title_runs[0].get('text', '')
                
                # Get duration
                duration_text = video_data.get('lengthText', {}).get('simpleText', '0:00')
                duration_seconds = self._parse_duration(duration_text)
                
                # Skip if too short
                if min_duration_seconds > 0 and duration_seconds < min_duration_seconds:
                    continue
                
                # Get channel
                channel = ''
                channel_runs = video_data.get('ownerText', {}).get('runs', [])
                if channel_runs:
                    channel = channel_runs[0].get('text', '')
                
                # Get published time
                published = video_data.get('publishedTimeText', {}).get('simpleText', '')
                
                # Get description snippet
                description = ''
                desc_runs = video_data.get('detailedMetadataSnippets', [])
                if desc_runs:
                    snippets = desc_runs[0].get('snippetText', {}).get('runs', [])
                    description = ''.join(s.get('text', '') for s in snippets)
                
                results.append(VideoResult(
                    video_id=video_id,
                    title=title,
                    channel=channel,
                    duration_seconds=duration_seconds,
                    published_text=published,
                    description=description,
                ))
                
                if len(results) >= max_results:
                    return results
        
        return results
    
    def _parse_duration(self, duration_text: str) -> int:
        """Parse duration like '2:15:30' or '1:30' to seconds."""
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


def search_youtube_videos(
    speaker: str,
    event_description: str,
    target_date: Optional[str] = None,
    max_results: int = 10,
    min_duration_minutes: int = 0,
    verbose: bool = True,
) -> List[VideoResult]:
    """
    Search YouTube for videos of a speaker at an event.
    
    Args:
        speaker: Speaker name (e.g., "Bernie Sanders")
        event_description: Event description (e.g., "Late Show Colbert")
        target_date: Optional date string (e.g., "January 2026")
        max_results: Maximum results to return
        min_duration_minutes: Minimum duration filter
        verbose: Print progress
        
    Returns:
        List of VideoResult objects
    """
    # Build search query
    # For fight events, event_description may already be complete (e.g., "Gaethje vs Pimblett full fight")
    # Check if event_description already contains "full" or "fight" to avoid duplication
    query_parts = []
    
    # If event_description is already a complete query (contains fight/interview/press keywords), use it as-is
    content_keywords = [
        'full fight', 'full interview', 'full match', 'fight', 'vs',
        'interview', 'press', 'briefing', 'conference', 'podcast',
        'speech', 'town hall', 'full', 'remarks', 'hearing', 'debate',
    ]
    if any(phrase in event_description.lower() for phrase in content_keywords):
        query_parts.append(event_description)
        if speaker and speaker.lower() not in event_description.lower():
            # Add speaker if not already in description
            query_parts.insert(0, speaker)
    else:
        # Traditional format: speaker + event_description + "full interview"
        query_parts = [speaker, event_description]
        if target_date:
            query_parts.append(target_date)
        query_parts.append("full interview")
    
    query = " ".join(query_parts)
    
    if verbose:
        print(f"🔍 Searching YouTube: {query}")
    
    scraper = YouTubeSearchScraper()
    videos = scraper.search(
        query,
        max_results=max_results,
        min_duration_minutes=min_duration_minutes,
    )
    
    if verbose:
        print(f"✅ Found {len(videos)} video(s)")
    
    return videos


def discover_and_transcribe(
    speaker: str,
    event_description: str,
    target_date: Optional[str] = None,
    output_dir: str = "data/mentions/transcripts",
    max_videos: int = 3,
    verbose: bool = True,
) -> List[str]:
    """
    Two-phase collection: scrape YouTube for videos, then transcribe.
    
    Args:
        speaker: Speaker name
        event_description: Event/show description
        target_date: Target date
        output_dir: Where to save transcripts
        max_videos: Max videos to transcribe
        verbose: Print progress
        
    Returns:
        List of saved transcript file paths
    """
    import sys
    from pathlib import Path
    
    # Phase 1: Find videos
    if verbose:
        print("=" * 50)
        print("Phase 1: Video Discovery (YouTube Scraping)")
        print("=" * 50)
    
    videos = search_youtube_videos(
        speaker=speaker,
        event_description=event_description,
        target_date=target_date,
        max_results=max_videos * 2,  # Get extras in case some fail
        verbose=verbose,
    )
    
    if not videos:
        if verbose:
            print("No videos found.")
        return []
    
    # Phase 2: Transcribe
    if verbose:
        print()
        print("=" * 50)
        print("Phase 2: Transcription")
        print("=" * 50)
    
    saved_files = []
    
    try:
        # Import our YouTube transcript downloader
        project_root = Path(__file__).parent.parent.parent.parent
        sys.path.insert(0, str(project_root))
        
        from src.scrapers.youtube.youtube_transcript_downloader import YouTubeTranscriptDownloader
        
        downloader = YouTubeTranscriptDownloader(use_proxy=True)
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        transcribed = 0
        for video in videos:
            if transcribed >= max_videos:
                break
            
            if verbose:
                print(f"  📝 {video.title[:50]}...")
            
            try:
                transcript = downloader.download_transcript(video.video_id)
                if transcript:
                    # Save transcript
                    safe_title = "".join(c if c.isalnum() or c in ' -_' else '' for c in video.title)[:50]
                    filename = f"{datetime.now().strftime('%Y-%m-%d')}_{safe_title}.txt"
                    filepath = output_path / filename
                    
                    full_text = " ".join([s.text for s in transcript])
                    
                    with open(filepath, 'w') as f:
                        f.write(f"Source: {video.url}\n")
                        f.write(f"Title: {video.title}\n")
                        f.write(f"Channel: {video.channel}\n")
                        f.write(f"Duration: {video.duration_formatted}\n")
                        f.write(f"Speaker: {speaker}\n")
                        f.write("=" * 80 + "\n\n")
                        f.write(full_text)
                    
                    saved_files.append(str(filepath))
                    transcribed += 1
                    if verbose:
                        print(f"    ✅ Saved ({len(full_text)} chars)")
                else:
                    if verbose:
                        print(f"    ❌ No transcript available")
                        
            except Exception as e:
                if verbose:
                    print(f"    ❌ Error: {e}")
    
    except ImportError as e:
        if verbose:
            print(f"YouTube downloader not available: {e}")
            print("Video URLs found:")
            for v in videos[:max_videos]:
                print(f"  • {v.url}")
    
    if verbose:
        print()
        print(f"✅ Saved {len(saved_files)} transcript(s) to {output_dir}")
    
    return saved_files


if __name__ == '__main__':
    # Quick test
    videos = search_youtube_videos(
        speaker="Will Smith",
        event_description="Tonight Show Jimmy Fallon",
        target_date="January 2026",
        verbose=True,
    )
    
    print()
    for i, v in enumerate(videos[:5], 1):
        print(f"{i}. {v.title[:60]}...")
        print(f"   {v.url}")
        print(f"   Duration: {v.duration_formatted} | Channel: {v.channel}")
        print()
