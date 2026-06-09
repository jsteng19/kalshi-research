#!/usr/bin/env python3
"""
NCAAB Full Game Video Finder

Searches YouTube for full college basketball game broadcasts and saves
results to data/ncaab/video_links.csv.

Handles incremental updates naturally: games already found (status='found')
are skipped; 'not_found' games can be retried with --retry.

Usage:
    # Search for games with top 10 announcers (by Kalshi game count)
    python src/ncaab/run_video_pipeline.py

    # Top 20 announcers, use web scraping (no API quota)
    python src/ncaab/run_video_pipeline.py --top 20 --scrape

    # Specific announcers only
    python src/ncaab/run_video_pipeline.py --announcers "Dan Shulman,Sean McDonough"

    # Update: add only new games, skip already-found
    python src/ncaab/run_video_pipeline.py --update

    # Retry games that weren't found last time
    python src/ncaab/run_video_pipeline.py --retry

YouTube Setup:
    Set YOUTUBE_API_KEY in .env to use the YouTube Data API v3 (10,000 units/day).
    search.list costs 100 units each — use --scrape to avoid quota entirely.
"""

import json
import os
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class NCAABGame:
    """Represents one college basketball game to find on YouTube."""
    date: str               # YYYY-MM-DD
    away_team: str
    home_team: str
    network: str = ''
    play_by_play: str = ''
    color_commentator: str = ''
    ticker: str = ''        # Kalshi ticker if available

    @property
    def year(self) -> int:
        try:
            return int(self.date[:4])
        except (ValueError, IndexError):
            return datetime.now().year

    @property
    def display_name(self) -> str:
        return f"{self.away_team} at {self.home_team} ({self.date})"

    @property
    def search_queries(self) -> list[str]:
        away, home, yr = self.away_team, self.home_team, self.year
        return [
            f"{away} at {home} {yr} college basketball full game",
            f"{home} vs {away} {yr} men's basketball full game",
            f"{away} vs {home} {yr} basketball full game replay",
            f"{away} {home} {yr} ncaab full game",
        ]


@dataclass
class VideoResult:
    """One YouTube search result."""
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


# ---------------------------------------------------------------------------
# Team aliases for title matching
# ---------------------------------------------------------------------------
# Maps the canonical 506-Sports name -> all reasonable YouTube title forms.
# Only entries that actually differ significantly from the canonical name.

TEAM_ALIASES: dict[str, list[str]] = {
    # ACC
    'Duke':             ['Duke', 'Duke Blue Devils'],
    'North Carolina':   ['North Carolina', 'UNC', 'Tar Heels', 'Carolina'],
    'UNC':              ['North Carolina', 'UNC', 'Tar Heels', 'Carolina'],
    'Virginia':         ['Virginia', 'Virginia Cavaliers', 'UVA', 'Cavaliers'],
    'NC State':         ['NC State', 'North Carolina State', 'Wolfpack'],
    'Florida State':    ['Florida State', 'FSU', 'Seminoles'],
    'Notre Dame':       ['Notre Dame', 'Fighting Irish'],
    'Pittsburgh':       ['Pittsburgh', 'Pitt', 'Panthers'],
    'Virginia Tech':    ['Virginia Tech', 'Hokies'],
    'Georgia Tech':     ['Georgia Tech', 'Yellow Jackets'],
    'Boston College':   ['Boston College', 'Eagles'],
    'California':       ['California', 'Cal', 'Golden Bears'],
    # Big Ten
    'Michigan':         ['Michigan', 'Michigan Wolverines', 'Wolverines'],
    'Michigan State':   ['Michigan State', 'Michigan State Spartans', 'Spartans'],
    'Ohio State':       ['Ohio State', 'Ohio State Buckeyes', 'Buckeyes'],
    'Indiana':          ['Indiana', 'Indiana Hoosiers', 'Hoosiers'],
    'Purdue':           ['Purdue', 'Purdue Boilermakers', 'Boilermakers'],
    'Wisconsin':        ['Wisconsin', 'Wisconsin Badgers', 'Badgers'],
    'Iowa':             ['Iowa', 'Iowa Hawkeyes', 'Hawkeyes'],
    'Illinois':         ['Illinois', 'Illinois Fighting Illini', 'Fighting Illini'],
    'Minnesota':        ['Minnesota', 'Minnesota Golden Gophers', 'Gophers'],
    'Penn State':       ['Penn State', 'Penn State Nittany Lions', 'Nittany Lions'],
    'Maryland':         ['Maryland', 'Maryland Terrapins', 'Terps'],
    'Nebraska':         ['Nebraska', 'Nebraska Cornhuskers', 'Cornhuskers'],
    'Northwestern':     ['Northwestern', 'Northwestern Wildcats'],
    'Rutgers':          ['Rutgers', 'Scarlet Knights'],
    'USC':              ['USC', 'Southern California', 'Trojans'],
    'UCLA':             ['UCLA', 'Bruins'],
    'Washington':       ['Washington', 'Washington Huskies', 'UW'],
    # SEC
    'Kentucky':         ['Kentucky', 'Kentucky Wildcats', 'UK', 'Wildcats'],
    'Tennessee':        ['Tennessee', 'Tennessee Volunteers', 'Vols'],
    'Alabama':          ['Alabama', 'Alabama Crimson Tide', 'Bama', 'Crimson Tide'],
    'Auburn':           ['Auburn', 'Auburn Tigers'],
    'Florida':          ['Florida', 'Florida Gators', 'Gators'],
    'LSU':              ['LSU', 'LSU Tigers'],
    'Arkansas':         ['Arkansas', 'Arkansas Razorbacks', 'Razorbacks'],
    'Mississippi State':['Mississippi State', 'Miss State'],
    'Ole Miss':         ['Ole Miss', 'Mississippi', 'Rebels'],
    'Georgia':          ['Georgia', 'Georgia Bulldogs'],
    'South Carolina':   ['South Carolina', 'Gamecocks'],
    'Missouri':         ['Missouri', 'Mizzou'],
    'Vanderbilt':       ['Vanderbilt', 'Commodores'],
    'Texas A&M':        ['Texas A&M', 'Texas A and M', 'Aggies'],
    'Texas':            ['Texas', 'Texas Longhorns', 'Longhorns'],
    'Oklahoma':         ['Oklahoma', 'Oklahoma Sooners', 'Sooners'],
    # Big 12
    'Kansas':           ['Kansas', 'Kansas Jayhawks', 'Jayhawks'],
    'Kansas State':     ['Kansas State', 'Kansas State Wildcats', 'K-State'],
    'Iowa State':       ['Iowa State', 'Iowa State Cyclones', 'Cyclones'],
    'Baylor':           ['Baylor', 'Baylor Bears'],
    'TCU':              ['TCU', 'TCU Horned Frogs', 'Horned Frogs'],
    'Texas Tech':       ['Texas Tech', 'Red Raiders'],
    'West Virginia':    ['West Virginia', 'WVU', 'Mountaineers'],
    'Oklahoma State':   ['Oklahoma State', 'Oklahoma State Cowboys'],
    'BYU':              ['BYU', 'Brigham Young'],
    'Cincinnati':       ['Cincinnati', 'Cincinnati Bearcats', 'Bearcats'],
    'UCF':              ['UCF', 'Central Florida'],
    'Houston':          ['Houston', 'Houston Cougars'],
    'Colorado':         ['Colorado', 'Colorado Buffaloes', 'Buffs'],
    'Arizona':          ['Arizona', 'Arizona Wildcats'],
    'Arizona State':    ['Arizona State', 'Sun Devils', 'ASU'],
    'Utah':             ['Utah', 'Utah Utes'],
    # Big East
    'UConn':            ['UConn', 'Connecticut', 'Connecticut Huskies'],
    "St. John's":       ["St. John's", 'St Johns', 'Red Storm'],
    'Marquette':        ['Marquette', 'Marquette Golden Eagles'],
    'Villanova':        ['Villanova', 'Nova'],
    'Georgetown':       ['Georgetown', 'Georgetown Hoyas'],
    'Xavier':           ['Xavier', 'Xavier Musketeers'],
    'Seton Hall':       ['Seton Hall', 'Pirates'],
    # Other high-majors
    'Gonzaga':          ['Gonzaga', 'Gonzaga Bulldogs', 'Zags'],
    'Memphis':          ['Memphis', 'Memphis Tigers'],
    'San Diego State':  ['San Diego State', 'SDSU', 'Aztecs'],
    'VCU':              ['VCU', 'Virginia Commonwealth', 'Rams'],
    'Dayton':           ['Dayton', 'Dayton Flyers', 'Flyers'],
}


# ---------------------------------------------------------------------------
# Main finder class
# ---------------------------------------------------------------------------

class NCAABVideoFinder:
    """Searches YouTube for NCAAB full game broadcast videos."""

    # College basketball full game is at least 50 minutes (condensed edits cut
    # commercials/timeouts; raw game clock is 40 min so 50 min is safe floor)
    MIN_DURATION_SECONDS = 50 * 60

    # Real broadcasts are <3 hours; longer = fake live stream archive
    MAX_DURATION_SECONDS = 3 * 3600

    # Title must contain at least one of these to confirm it's men's basketball
    # (not football, wrestling, volleyball, etc. that share team names)
    REQUIRED_SPORT_TOKENS = [
        'basketball', 'ncaab', "men's basketball", 'mens basketball',
        'ncaa basketball', 'college basketball', 'hoops',
    ]

    # Exact wrong-sport indicators — immediately disqualify
    BAD_TITLE_TOKENS = [
        # Content type
        'highlights', 'highlight', 'recap', 'top plays', 'best moments',
        'top 10', 'preview', 'trailer', 'analysis', 'reaction', 'breakdown',
        'picks', 'predictions', 'betting', 'bracketology', 'halftime',
        'interview', 'press conference', 'first look', 'watch party',
        # Video game simulations
        'rosters', 'simulation', 'dynasty mode', 'my career', 'nba 2k',
        # Radio / audio-only streams (no actual video of the game)
        'game cast', '& audio', 'radio play', 'game score', 'audio only',
        'game audio', '- stream',
        'basketball in 60', 'b1g basketball in 60', 'encore',
        # Fake live-stream scoreboards / score overlays
        'scoreboard', 'live score', 'score update', 'score board',
        'live results', 'live now', 'crew cam',
        '🔴', 'score live',
        # Wrong sports
        'football', 'ncaaf', 'cfb', 'cfp', 'bowl game', 'bowl',
        'wrestling', 'duals', 'dual meet',
        'softball', 'volleyball', 'lacrosse', 'soccer', 'baseball',
        'hockey', 'swimming', 'tennis', 'golf', 'track and field',
        # Football venue/format giveaways
        'wrigley', 'rose bowl', 'sugar bowl', 'orange bowl', 'fiesta bowl',
        'cotton bowl', 'peach bowl', 'alamo bowl',
        'cfp semifinal', 'playoff semifinal',
        # Football week/season patterns handled in validate_title via regex
        # Women's sports
        "women's", 'ncaaw', 'womens', 'lady ',
        # High school
        'ihsaa', 'high school', 'nfhs',
    ]

    # Channels that reliably produce radio casts, score overlays, or fake streams.
    # Comparison is done after stripping whitespace from the channel name.
    KNOWN_BAD_CHANNELS = {
        "CHICAGO PLAY'S",
        'EL DOMO SPORTS',
        'World Football Live',
        'Truth Tell',
        'Elite Media Network',
        'BearcastMedia',        # audio-only streams
        'Indiana SRN',          # high school / audio
        'Wrestling tournament',   # misnamed channel streaming basketball
        'PS2SPORTSINFOSTATION',  # PS2 video game simulations
    }

    USER_AGENTS = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    ]

    def __init__(self, api_key: str = None, use_scraping: bool = False):
        self.use_scraping = use_scraping
        self.quota_exceeded = False

        if api_key and not use_scraping:
            from googleapiclient.discovery import build
            self.youtube = build('youtube', 'v3', developerKey=api_key)
        else:
            self.youtube = None

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': random.choice(self.USER_AGENTS),
            'Accept-Language': 'en-US,en;q=0.9',
        })

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def search_video(self, game: NCAABGame, max_results: int = 5) -> list[VideoResult]:
        if self.use_scraping or self.quota_exceeded:
            return self._search_scrape(game, max_results)
        return self._search_api(game, max_results)

    def get_team_variations(self, team: str) -> list[str]:
        """Return all known YouTube-title forms for a team name."""
        if team in TEAM_ALIASES:
            return TEAM_ALIASES[team]
        # Try case-insensitive lookup
        low = team.lower()
        for key, aliases in TEAM_ALIASES.items():
            if key.lower() == low:
                return aliases
        # No alias: just use the name itself
        return [team]

    def validate_title(self, title: str, game: NCAABGame, duration_seconds: int = 0) -> tuple[bool, str]:
        """
        Check that a video title (and duration) plausibly match an NCAAB men's
        basketball full-game broadcast.  Returns (is_valid, reason).
        """
        tl = title.lower()

        # --- 1. Duration gates --------------------------------------------------
        if duration_seconds and duration_seconds > self.MAX_DURATION_SECONDS:
            return False, f"duration too long ({duration_seconds//3600}h) — likely fake stream"

        # --- 2. Forbidden tokens -------------------------------------------------
        for token in self.BAD_TITLE_TOKENS:
            if token in tl:
                return False, f"bad token '{token}'"

        # Football week patterns ("Week 4", "Wk 6", etc.)
        if re.search(r'\bwee?k\s*\d{1,2}\b', tl):
            return False, "football week pattern"

        # Date in football season months (Aug–Oct) suggests wrong sport
        # e.g. "(09-06-25)" or "10/05/2025"
        if re.search(r'\b(0[89]|10)[/\-]\d{1,2}[/\-]\d{2,4}\b', title):
            return False, "date in football-season month (Aug–Oct)"

        # --- 2b. Explicit date mismatch check -----------------------------------
        date_reason = self._title_date_reason(title, game)
        if date_reason:
            return False, date_reason

        # --- 3. Wrong-year check -------------------------------------------------
        # If a 4-digit year appears and it's NOT from the expected season, reject.
        valid_years = {str(game.year), str(game.year - 1)}
        years_found = re.findall(r'\b(20\d{2})\b', title)
        for y in years_found:
            if y not in valid_years:
                return False, f"wrong year {y} (expected {game.year} season)"

        # --- 4. Both teams must be identifiable ----------------------------------
        home_found = self._team_present(tl, game.home_team, game.away_team)
        away_found = self._team_present(tl, game.away_team, game.home_team)

        if not home_found:
            return False, f"home '{game.home_team}' not in title"
        if not away_found:
            return False, f"away '{game.away_team}' not in title"

        # --- 5. Sport confirmation -----------------------------------------------
        # Title must explicitly mention basketball (or ncaab), OR contain a
        # phrase like "full game" / "game replay" that implies it, but NOT if it
        # passed only because two team names share a name with another sport.
        # We require at least one positive basketball indicator.
        has_sport = any(tok in tl for tok in self.REQUIRED_SPORT_TOKENS)
        has_full_game = 'full game' in tl or 'game replay' in tl or 'full replay' in tl

        if not has_sport and not has_full_game:
            return False, "no basketball/full-game indicator in title"

        return True, "ok"

    def _title_date_reason(self, title: str, game: NCAABGame) -> str | None:
        try:
            game_dt = datetime.strptime(game.date, "%Y-%m-%d")
        except ValueError:
            return None

        title_lower = title.lower()
        expected_month = game_dt.month
        expected_day = game_dt.day
        expected_year = game_dt.year

        month_map = {
            "jan": 1, "january": 1,
            "feb": 2, "february": 2,
            "mar": 3, "march": 3,
            "apr": 4, "april": 4,
            "may": 5,
            "jun": 6, "june": 6,
            "jul": 7, "july": 7,
            "aug": 8, "august": 8,
            "sep": 9, "sept": 9, "september": 9,
            "oct": 10, "october": 10,
            "nov": 11, "november": 11,
            "dec": 12, "december": 12,
        }

        # Numeric dates like 2/18/23 or 03-14-2024
        numeric_dates = re.findall(r"\b(\d{1,2})[/-](\d{1,2})(?:[/-](\d{2,4}))?\b", title_lower)
        for month_str, day_str, year_str in numeric_dates:
            month = int(month_str)
            day = int(day_str)
            if year_str:
                year = int(year_str)
                if year < 100:
                    year += 2000
                if (month, day, year) != (expected_month, expected_day, expected_year):
                    return f"title date mismatch ({month}/{day}/{year})"
            elif (month, day) != (expected_month, expected_day):
                return f"title date mismatch ({month}/{day})"

        # Month-name dates like March 16, 2024 or Mar 7 2026
        month_name_dates = re.findall(
            r"\b([a-z]{3,9})\.?\s+(\d{1,2})(?:,?\s*(\d{2,4}))?\b",
            title_lower,
        )
        for month_name, day_str, year_str in month_name_dates:
            month = month_map.get(month_name)
            if not month:
                continue
            day = int(day_str)
            if year_str:
                year = int(year_str)
                if year < 100:
                    year += 2000
                if (month, day, year) != (expected_month, expected_day, expected_year):
                    return f"title date mismatch ({month_name} {day}, {year})"
            elif (month, day) != (expected_month, expected_day):
                return f"title date mismatch ({month_name} {day})"

        return None

    def _team_present(self, tl: str, team_name: str, other_team_name: str) -> bool:
        """
        Check if team_name is genuinely mentioned in tl, even when another team
        name (other_team_name) contains team_name as a substring.

        Example: team_name="Iowa", other_team_name="Iowa State"
        → "iowa" found in "fdu vs iowa state" should return False because the
          only occurrence of "iowa" is part of "iowa state".
        """
        team_vars = self.get_team_variations(team_name)
        other_vars = [v.lower() for v in self.get_team_variations(other_team_name)]

        for v in team_vars:
            vl = v.lower()
            if vl not in tl:
                continue
            # Found the alias somewhere. Check every occurrence to ensure it's
            # not just a prefix of a longer other-team alias.
            # (e.g. "iowa" found in "iowa state" — only flag if other alias is longer)
            for m in re.finditer(re.escape(vl), tl):
                pos = m.start()
                # Is this occurrence actually the start of an other-team alias
                # that is LONGER than our alias? (Avoids "iowa state" being
                # mis-flagged because other_var "iowa" shares its prefix.)
                is_part_of_other = any(
                    len(ov) > len(vl) and tl[pos:pos + len(ov)] == ov
                    for ov in other_vars
                )
                if not is_part_of_other:
                    return True  # Genuine standalone occurrence
        return False

    # ------------------------------------------------------------------
    # Internal search implementations
    # ------------------------------------------------------------------

    def _search_api(self, game: NCAABGame, max_results: int) -> list[VideoResult]:
        from googleapiclient.errors import HttpError

        all_results = []
        seen_ids: set[str] = set()

        for query in game.search_queries:
            try:
                search_resp = self.youtube.search().list(
                    q=query,
                    type='video',
                    part='id,snippet',
                    maxResults=max_results,
                    videoDuration='long',   # >20 min
                    order='relevance',
                ).execute()

                video_ids = [item['id']['videoId']
                             for item in search_resp.get('items', [])]
                if not video_ids:
                    continue

                details_resp = self.youtube.videos().list(
                    id=','.join(video_ids),
                    part='contentDetails,snippet',
                ).execute()

                for item in details_resp.get('items', []):
                    vid_id = item['id']
                    if vid_id in seen_ids:
                        continue
                    seen_ids.add(vid_id)

                    dur = self._parse_iso_duration(
                        item['contentDetails']['duration']
                    )
                    if dur < self.MIN_DURATION_SECONDS:
                        continue

                    snippet = item['snippet']
                    title = snippet['title']
                    channel = snippet['channelTitle'].strip()

                    if channel in self.KNOWN_BAD_CHANNELS:
                        continue

                    ok, reason = self.validate_title(title, game, duration_seconds=dur)
                    if not ok:
                        continue

                    all_results.append(VideoResult(
                        video_id=vid_id,
                        title=title,
                        channel=channel,
                        duration_seconds=dur,
                        published_at=snippet['publishedAt'][:10],
                        thumbnail_url=snippet['thumbnails']['default']['url'],
                    ))

            except HttpError as e:
                if 'quota' in str(e).lower():
                    print("  ⚠ API quota exceeded — switching to scrape mode")
                    self.quota_exceeded = True
                    return self._search_scrape(game, max_results)
                print(f"  ⚠ API error for '{query}': {e}")
                break

            time.sleep(0.5)
            if all_results:
                break

        return all_results

    def _search_scrape(self, game: NCAABGame, max_results: int) -> list[VideoResult]:
        """Scrape YouTube search results (no quota cost)."""
        all_results = []
        seen_ids: set[str] = set()

        # Use the first (most targeted) query
        query = game.search_queries[0]

        try:
            # &sp=EgIYAg%3D%3D filters for "long" videos (>20 min)
            url = (
                f"https://www.youtube.com/results"
                f"?search_query={quote_plus(query)}&sp=EgIYAg%253D%253D"
            )
            self.session.headers['User-Agent'] = random.choice(self.USER_AGENTS)
            resp = self.session.get(url, timeout=15)

            if resp.status_code != 200:
                print(f"  ⚠ Scrape HTTP {resp.status_code}")
                return []

            # YouTube embeds search data in ytInitialData JS variable
            match = re.search(r'var ytInitialData = ({.*?});</script>',
                              resp.text, re.DOTALL)
            if not match:
                match = re.search(r'ytInitialData\s*=\s*({.*?});</script>',
                                  resp.text, re.DOTALL)
            if not match:
                print("  ⚠ Could not parse YouTube response")
                return []

            data = json.loads(match.group(1))
            contents = (
                data.get('contents', {})
                    .get('twoColumnSearchResultsRenderer', {})
                    .get('primaryContents', {})
                    .get('sectionListRenderer', {})
                    .get('contents', [])
            )

            for section in contents:
                items = section.get('itemSectionRenderer', {}).get('contents', [])
                for item in items:
                    vd = item.get('videoRenderer', {})
                    if not vd:
                        continue

                    vid_id = vd.get('videoId')
                    if not vid_id or vid_id in seen_ids:
                        continue
                    seen_ids.add(vid_id)

                    title_runs = vd.get('title', {}).get('runs', [])
                    title = title_runs[0].get('text', '') if title_runs else ''

                    dur_text = vd.get('lengthText', {}).get('simpleText', '0:00')
                    dur = self._parse_duration_text(dur_text)
                    if dur < self.MIN_DURATION_SECONDS:
                        continue

                    channel_runs = vd.get('ownerText', {}).get('runs', [])
                    channel = (channel_runs[0].get('text', '') if channel_runs else '').strip()

                    if channel in self.KNOWN_BAD_CHANNELS:
                        continue

                    ok, reason = self.validate_title(title, game, duration_seconds=dur)
                    if not ok:
                        continue

                    published = vd.get('publishedTimeText', {}).get('simpleText', '')

                    all_results.append(VideoResult(
                        video_id=vid_id,
                        title=title,
                        channel=channel,
                        duration_seconds=dur,
                        published_at=published,
                        thumbnail_url=f"https://i.ytimg.com/vi/{vid_id}/default.jpg",
                    ))

                    if len(all_results) >= max_results:
                        break

                if all_results:
                    break

        except Exception as e:
            print(f"  ⚠ Scrape error: {e}")

        time.sleep(random.uniform(2, 4))
        return all_results

    # ------------------------------------------------------------------
    # Duration parsers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_iso_duration(s: str) -> int:
        """Parse ISO 8601 like PT1H30M15S -> seconds."""
        m = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', s)
        if not m:
            return 0
        h = int(m.group(1) or 0)
        mn = int(m.group(2) or 0)
        sc = int(m.group(3) or 0)
        return h * 3600 + mn * 60 + sc

    @staticmethod
    def _parse_duration_text(s: str) -> int:
        """Parse '2:15:30' or '1:30' -> seconds."""
        parts = s.strip().split(':')
        try:
            if len(parts) == 3:
                return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
            if len(parts) == 2:
                return int(parts[0]) * 60 + int(parts[1])
        except ValueError:
            pass
        return 0


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

RESULTS_COLUMNS = [
    'ticker', 'date', 'away_team', 'home_team', 'network',
    'play_by_play', 'color_commentator',
    'youtube_url', 'youtube_title', 'duration_min', 'channel',
    'status', 'searched_at',
]


def load_existing_results(csv_path: str | Path) -> pd.DataFrame:
    """Load existing video_links.csv, or return an empty frame."""
    csv_path = Path(csv_path)
    if csv_path.exists():
        df = pd.read_csv(csv_path, dtype=str).fillna('')
        return df
    return pd.DataFrame(columns=RESULTS_COLUMNS)


def save_results(df: pd.DataFrame, csv_path: str | Path) -> None:
    """Write results CSV, ensuring all expected columns exist."""
    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    for col in RESULTS_COLUMNS:
        if col not in df.columns:
            df[col] = ''
    df[RESULTS_COLUMNS].to_csv(csv_path, index=False)


def game_key(g: NCAABGame) -> str:
    """Unique string identifier for a game (used to detect duplicates)."""
    return f"{g.date}|{g.away_team}|{g.home_team}"


def df_game_key(row) -> str:
    return f"{row.get('date','')}|{row.get('away_team','')}|{row.get('home_team','')}"


def games_from_announcers_df(
    announcers_df: pd.DataFrame,
    pbp_list: list[str],
) -> list[NCAABGame]:
    """
    Extract all games from announcers_df for the given play-by-play announcers.
    Returns a deduplicated list of NCAABGame objects.
    """
    subset = announcers_df[
        announcers_df['play_by_play'].isin(pbp_list)
    ].copy()

    seen_keys: set[str] = set()
    games: list[NCAABGame] = []

    for _, row in subset.iterrows():
        date = str(row.get('date', ''))[:10]
        away = str(row.get('away_team', ''))
        home = str(row.get('home_team', ''))
        if not date or not away or not home:
            continue
        key = f"{date}|{away}|{home}"
        if key in seen_keys:
            continue
        seen_keys.add(key)
        games.append(NCAABGame(
            date=date,
            away_team=away,
            home_team=home,
            network=str(row.get('network', '')),
            play_by_play=str(row.get('play_by_play', '')),
            color_commentator=str(row.get('color_commentator', '')),
            ticker=str(row.get('ticker', '')),
        ))

    games.sort(key=lambda g: g.date)
    return games
