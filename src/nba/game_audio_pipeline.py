#!/usr/bin/env python3
"""
NBA Game Audio Pipeline

Downloads and extracts audio from NBA game replays for transcription.

Supports multiple video sources:
- OK.ru embeds (via guidedesgemmes.com for newer games)
- Filemoon embeds (direct HLS extraction for older games)

Pipeline:
1. Generate basketball-video.com URL from game info
2. Detect video source type (OK.ru or Filemoon)
3. Extract using appropriate method
4. Convert to transcription-optimized audio
"""

import os
import re
import sys
import time
import requests
from bs4 import BeautifulSoup
from typing import Optional, Tuple, Dict
from datetime import datetime
import pandas as pd

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from nba.okru_extractor import find_okru_embeds
from nba.fast_audio_extractor import extract_audio_fast
from nba.filemoon_extractor import extract_audio_from_hls, extract_hls_url_with_browser


# Team name variations for URL generation
TEAM_URL_NAMES = {
    'Hawks': 'atlanta-hawks',
    'Nets': 'brooklyn-nets',
    'Celtics': 'boston-celtics',
    'Hornets': 'charlotte-hornets',
    'Bulls': 'chicago-bulls',
    'Cavaliers': 'cleveland-cavaliers',
    'Mavericks': 'dallas-mavericks',
    'Nuggets': 'denver-nuggets',
    'Pistons': 'detroit-pistons',
    'Warriors': 'golden-state-warriors',
    'Rockets': 'houston-rockets',
    'Pacers': 'indiana-pacers',
    'Clippers': 'la-clippers',
    'Lakers': 'los-angeles-lakers',
    'Grizzlies': 'memphis-grizzlies',
    'Heat': 'miami-heat',
    'Bucks': 'milwaukee-bucks',
    'Timberwolves': 'minnesota-timberwolves',
    'Pelicans': 'new-orleans-pelicans',
    'Knicks': 'new-york-knicks',
    'Thunder': 'oklahoma-city-thunder',
    'Magic': 'orlando-magic',
    '76ers': 'philadelphia-76ers',
    'Suns': 'phoenix-suns',
    'Trail Blazers': 'portland-trail-blazers',
    'Kings': 'sacramento-kings',
    'Spurs': 'san-antonio-spurs',
    'Raptors': 'toronto-raptors',
    'Jazz': 'utah-jazz',
    'Wizards': 'washington-wizards',
}


# Filemoon domain variations
FILEMOON_DOMAINS = ['filemoon.to', 'filemoon.sx', 'luluvdo', 'fmembed.cc']


def detect_video_source(game_url: str, prefer_filemoon: bool = True, verbose: bool = True) -> Dict:
    """
    Detect what type of video source is on a basketball-video.com page.
    
    Checks for video sources and returns info about all available options.
    When prefer_filemoon=True, prioritizes Filemoon (faster parallel downloads)
    over OK.ru (often throttled).
    
    Returns:
        Dict with:
        - 'type': 'direct_filemoon', 'direct_okru', 'okru', or 'unknown'
        - 'okru_url': Direct OK.ru URL if available
        - 'filemoon_url': Direct Filemoon URL if available
        - 'embed_url': URL to guidedesgemmes embed page
        - 'has_filemoon': Whether Filemoon is available as backup
        - 'has_okru': Whether OK.ru is available
        - 'page_exists': Whether the game page exists (not 404)
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    }
    
    result = {
        'type': 'unknown', 
        'okru_url': None, 
        'filemoon_url': None, 
        'embed_url': None,
        'has_filemoon': False,
        'has_okru': False,
        'page_exists': False
    }
    
    try:
        resp = requests.get(game_url, headers=headers, timeout=30)
        if resp.status_code == 404:
            if verbose:
                print(f"❌ Page not found (404): {game_url}")
            return result
        resp.raise_for_status()
        result['page_exists'] = True
    except requests.RequestException as e:
        if verbose:
            print(f"❌ Failed to fetch game page: {e}")
        return result
    
    soup = BeautifulSoup(resp.text, 'html.parser')
    
    # Check all iframes for video sources - collect ALL sources
    iframes = soup.find_all('iframe')
    for iframe in iframes:
        src = iframe.get('src', '')
        if not src:
            continue
            
        # Collect OK.ru/VK embeds
        if 'ok.ru' in src.lower() or 'vk.com' in src.lower():
            result['okru_url'] = src
            result['has_okru'] = True
        
        # Collect Filemoon embeds (various domains)
        if any(domain in src.lower() for domain in FILEMOON_DOMAINS):
            result['filemoon_url'] = src
            result['has_filemoon'] = True
    
    # Check for link-based video sources (older games use direct links, not iframes)
    # Search ALL links on the page, not just in content div
    all_links = soup.find_all('a')
    for link in all_links:
        href = link.get('href', '')
        if not href:
            continue
        
        # OK.ru/VK direct links
        if 'ok.ru' in href.lower() or 'vk.com' in href.lower():
            if not result['okru_url']:  # Don't override iframe source
                result['okru_url'] = href
            result['has_okru'] = True
        
        # Filemoon-like services (various domains)
        if any(domain in href.lower() for domain in FILEMOON_DOMAINS):
            if not result['filemoon_url']:  # Don't override iframe source
                result['filemoon_url'] = href
            result['has_filemoon'] = True
        
        # guidedesgemmes (leads to OK.ru)
        if 'guidedesgemmes' in href.lower():
            result['embed_url'] = href
            result['has_okru'] = True
    
    # Determine primary type based on preference
    if prefer_filemoon and result['has_filemoon']:
        result['type'] = 'direct_filemoon'
        if verbose:
            print(f"✅ Using Filemoon (fast parallel): {result['filemoon_url'][:50]}...")
            if result['has_okru']:
                print(f"   (OK.ru also available as backup)")
    elif result['has_okru']:
        if result['okru_url']:
            result['type'] = 'direct_okru'
            if verbose:
                print(f"✅ Found direct OK.ru embed: {result['okru_url'][:60]}...")
        elif result['embed_url']:
            result['type'] = 'okru'
            if verbose:
                print(f"✅ Found guidedesgemmes watch button: {result['embed_url'][:60]}...")
        if verbose and result['has_filemoon']:
            print(f"   (Filemoon also available as backup)")
    elif result['has_filemoon']:
        result['type'] = 'direct_filemoon'
        if verbose:
            print(f"✅ Found direct Filemoon embed: {result['filemoon_url'][:60]}...")
    else:
        if verbose:
            print("⚠️  Could not detect video source type")
    
    return result


def find_working_game_url(away_team: str, home_team: str, date: str, verbose: bool = True) -> Tuple[Optional[str], Dict]:
    """
    Try multiple URL formats and return the first one with a working video source.
    
    Args:
        away_team: Away team name
        home_team: Home team name
        date: Date in YYYY-MM-DD format
        verbose: Print progress
        
    Returns:
        Tuple of (working_url or None, source_info dict)
    """
    urls = generate_game_urls(away_team, home_team, date)
    
    for i, url in enumerate(urls):
        if verbose:
            print(f"   Trying URL format {i+1}/{len(urls)}...")
        
        source_info = detect_video_source(url, prefer_filemoon=True, verbose=False)
        
        if source_info['page_exists'] and source_info['has_filemoon']:
            if verbose:
                print(f"✅ Found working URL with Filemoon: {url}")
            return url, source_info
        elif source_info['page_exists'] and source_info['has_okru']:
            if verbose:
                print(f"✅ Found working URL with OK.ru: {url}")
            return url, source_info
    
    # If no URL worked, return the first that at least exists
    for url in urls:
        source_info = detect_video_source(url, prefer_filemoon=True, verbose=False)
        if source_info['page_exists']:
            if verbose:
                print(f"⚠️  Found page but no video source: {url}")
            return url, source_info
    
    if verbose:
        print(f"❌ No working URL found for {away_team} @ {home_team} on {date}")
    return None, {'type': 'unknown', 'page_exists': False, 'has_filemoon': False, 'has_okru': False}


# Month abbreviations for old URL format
MONTH_ABBREV = {1:'jan', 2:'feb', 3:'mar', 4:'apr', 5:'may', 6:'jun', 
                7:'jul', 8:'aug', 9:'sep', 10:'oct', 11:'nov', 12:'dec'}


def generate_game_urls(away_team: str, home_team: str, date: str) -> list:
    """
    Generate multiple URL formats for basketball-video.com.
    
    Returns list of URLs to try, in order of preference.
    Different date ranges use different URL formats.
    
    Args:
        away_team: Away team name (e.g., "Knicks", "Warriors", or full names)
        home_team: Home team name
        date: Date string in YYYY-MM-DD format
        
    Returns:
        List of URLs to try
    """
    # Convert team names to URL format
    away_url = TEAM_URL_NAMES.get(away_team, away_team.lower().replace(' ', '-'))
    home_url = TEAM_URL_NAMES.get(home_team, home_team.lower().replace(' ', '-'))
    
    # Parse date
    dt = datetime.strptime(date, '%Y-%m-%d')
    month_full = dt.strftime('%B').lower()
    month_abbrev = MONTH_ABBREV[dt.month]
    day = dt.day
    year = dt.year
    
    urls = []
    
    # Format 1 (newer, 2024-25 season onwards):
    # {away}-vs-{home}-full-game-replay-{month}-{day}-{year}-nba
    urls.append(f"https://basketball-video.com/{away_url}-vs-{home_url}-full-game-replay-{month_full}-{day}-{year}-nba")
    
    # Format 2 (older, pre-2024-25 season):
    # {away}-vs-{home}-{mon}-{day}-{year}-nba-full-game-replay
    urls.append(f"https://basketball-video.com/{away_url}-vs-{home_url}-{month_abbrev}-{day}-{year}-nba-full-game-replay")
    
    # Format 3 (alternative): try with 76ers special case
    if '76ers' in away_url or '76ers' in home_url:
        away_alt = away_url.replace('76ers', 'philadelphia-76ers')
        home_alt = home_url.replace('76ers', 'philadelphia-76ers')
        urls.append(f"https://basketball-video.com/{away_alt}-vs-{home_alt}-{month_abbrev}-{day}-{year}-nba-full-game-replay")
    
    return urls


def generate_game_url(away_team: str, home_team: str, date: str) -> str:
    """
    Generate basketball-video.com URL for a game.
    
    Tries multiple URL formats and returns the first one that works.
    
    Args:
        away_team: Away team name (e.g., "Knicks", "Warriors")
        home_team: Home team name
        date: Date string in YYYY-MM-DD format
        
    Returns:
        Full URL to the game replay page (first working format)
    """
    urls = generate_game_urls(away_team, home_team, date)
    
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
    
    for url in urls:
        try:
            resp = requests.head(url, headers=headers, timeout=10, allow_redirects=True)
            if resp.status_code == 200:
                return url
        except:
            continue
    
    # If none work, return the first (new format) as default
    return urls[0]


def extract_okru_link(game_url: str) -> Optional[str]:
    """
    Extract the OK.ru embed page URL from a basketball-video.com game page.
    
    DEPRECATED: Use detect_video_source() instead for better source detection.
    
    Args:
        game_url: URL to the basketball-video.com game page
        
    Returns:
        URL to the page containing the OK.ru embed, or None if not found
    """
    source_info = detect_video_source(game_url)
    if source_info['type'] == 'okru':
        return source_info['embed_url']
    return None


def get_okru_video_url(embed_page_url: str) -> Optional[str]:
    """
    Extract the OK.ru video URL from an embed page.
    
    Args:
        embed_page_url: URL to the page containing the OK.ru embed
        
    Returns:
        OK.ru video URL, or None if not found
    """
    okru_embeds = find_okru_embeds(embed_page_url)
    
    if not okru_embeds:
        print("❌ No OK.ru embeds found on page")
        return None
    
    # Return the first OK.ru embed
    okru_url = okru_embeds[0]
    print(f"✅ Found OK.ru video: {okru_url}")
    return okru_url


TEAM_ABBREVS = {
    'Atlanta Hawks': 'atl', 'Boston Celtics': 'bos', 'Brooklyn Nets': 'bkn',
    'Charlotte Hornets': 'cha', 'Chicago Bulls': 'chi', 'Cleveland Cavaliers': 'cle',
    'Dallas Mavericks': 'dal', 'Denver Nuggets': 'den', 'Detroit Pistons': 'det',
    'Golden State Warriors': 'gsw', 'Houston Rockets': 'hou', 'Indiana Pacers': 'ind',
    'Los Angeles Clippers': 'lac', 'Los Angeles Lakers': 'lal', 'Memphis Grizzlies': 'mem',
    'Miami Heat': 'mia', 'Milwaukee Bucks': 'mil', 'Minnesota Timberwolves': 'min',
    'New Orleans Pelicans': 'nop', 'New York Knicks': 'nyk', 'Oklahoma City Thunder': 'okc',
    'Orlando Magic': 'orl', 'Philadelphia 76ers': 'phi', 'Phoenix Suns': 'phx',
    'Portland Trail Blazers': 'por', 'Sacramento Kings': 'sac', 'San Antonio Spurs': 'sas',
    'Toronto Raptors': 'tor', 'Utah Jazz': 'uta', 'Washington Wizards': 'was',
    'LA Clippers': 'lac'
}

def get_team_abbrev(team: str) -> str:
    """Get 3-letter team abbreviation."""
    return TEAM_ABBREVS.get(team, team[:3].lower())

def generate_filename(date: str, away_team: str, home_team: str) -> str:
    """
    Generate filename in format: yyyy-mm-dd_away-at-home using 3-letter abbreviations.
    
    Args:
        date: Date string in YYYY-MM-DD format
        away_team: Away team name
        home_team: Home team name
        
    Returns:
        Filename (without extension)
    """
    away_abbr = get_team_abbrev(away_team)
    home_abbr = get_team_abbrev(home_team)
    
    return f"{date}_{away_abbr}-at-{home_abbr}"


def capture_hls_url_playwright(filemoon_url: str, timeout: int = 30) -> Optional[str]:
    """
    Use Playwright to navigate to Filemoon and capture HLS URL from network requests.
    
    Args:
        filemoon_url: The filemoon.sx/e/... or luluvdo.com/e/... URL
        timeout: Max time to wait for HLS URL
        
    Returns:
        HLS master.m3u8 URL or None
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("❌ Playwright not installed")
        return None
    
    hls_url = None
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                viewport={'width': 1280, 'height': 720}
            )
            page = context.new_page()
            
            # Capture network requests
            captured_urls = []
            def handle_request(request):
                if 'master.m3u8' in request.url or 'index-v1-a1' in request.url:
                    captured_urls.append(request.url)
            
            page.on('request', handle_request)
            
            # Navigate to Filemoon
            page.goto(filemoon_url, wait_until='domcontentloaded')
            
            # Wait for video to potentially load
            page.wait_for_timeout(3000)
            
            # Try clicking play button if present
            try:
                play_btn = page.locator('button, .play-button, .vjs-big-play-button').first
                if play_btn.is_visible(timeout=2000):
                    play_btn.click()
                    page.wait_for_timeout(3000)
            except:
                pass
            
            # Wait a bit more for network requests
            page.wait_for_timeout(2000)
            
            browser.close()
            
            # Find the master.m3u8 URL
            for url in captured_urls:
                if 'master.m3u8' in url:
                    hls_url = url
                    break
            
            if not hls_url and captured_urls:
                # Use first captured URL as fallback
                hls_url = captured_urls[0]
    
    except Exception as e:
        print(f"❌ Playwright error: {e}")
        return None
    
    return hls_url


def download_filemoon_audio(
    filemoon_url: str,
    output_path: str,
    use_browser: bool = True
) -> bool:
    """
    Download audio from a Filemoon embed.
    
    Uses browser automation to capture the HLS URL, then ffmpeg to extract audio.
    
    Args:
        filemoon_url: The filemoon.sx/e/... URL
        output_path: Where to save the audio file
        use_browser: Whether to use Playwright for HLS URL extraction
        
    Returns:
        True if successful, False otherwise
    """
    print(f"🎬 Extracting HLS URL from Filemoon...")
    
    if use_browser:
        # Try automated Playwright first
        hls_url = capture_hls_url_playwright(filemoon_url)
        if not hls_url:
            # Fallback to existing method
            hls_url = extract_hls_url_with_browser(filemoon_url)
        if not hls_url:
            print("❌ Failed to extract HLS URL via browser")
            return False
    else:
        # Fallback: API-based (less reliable)
        print("⚠️  Browser extraction disabled, trying API method...")
        from nba.filemoon_extractor import extract_hls_url_from_filemoon
        hls_url = extract_hls_url_from_filemoon(filemoon_url)
        if not hls_url:
            return False
    
    print(f"✅ Got HLS URL, starting audio extraction...")
    return extract_audio_from_hls(
        hls_url=hls_url,
        output_path=output_path,
        sample_rate=16000,
        channels=1,
        bitrate='48k'
    )


def download_game_audio(
    away_team: str,
    home_team: str,
    date: str,
    output_dir: str = 'data/nba/audio',
    skip_existing: bool = True,
    hls_url: str = None,  # Optional: pre-captured HLS URL for Filemoon
    prefer_filemoon: bool = True,  # Prefer Filemoon (faster) over OK.ru (throttled)
    fast_only: bool = False  # Skip if only slow sources (OK.ru) available
) -> Tuple[bool, str]:
    """
    Complete pipeline to download audio for a single game.
    
    Automatically detects video source type (OK.ru or Filemoon) and
    uses the appropriate extraction method. Tries multiple URL formats.
    
    Args:
        away_team: Away team name (e.g., "Knicks")
        home_team: Home team name (e.g., "Kings")
        date: Date in YYYY-MM-DD format
        output_dir: Directory to save audio files
        skip_existing: Skip if audio file already exists
        hls_url: Optional pre-captured HLS URL (for Filemoon games)
        
    Returns:
        Tuple of (success: bool, message: str)
    """
    import sys
    from nba.filemoon_extractor import extract_audio_from_hls_parallel
    
    filename = generate_filename(date, away_team, home_team)
    output_path = os.path.join(output_dir, f"{filename}.mp3")
    
    print(f"\n{'='*60}")
    print(f"📅 {date} | {away_team} @ {home_team}")
    print(f"{'='*60}")
    sys.stdout.flush()
    
    # Check if already downloaded
    if skip_existing and os.path.exists(output_path):
        print(f"⏭️  Already exists: {output_path}")
        return True, "Already exists"
    
    os.makedirs(output_dir, exist_ok=True)
    
    # If HLS URL provided, use it directly (for Filemoon)
    if hls_url:
        print(f"🔗 Using provided HLS URL...")
        sys.stdout.flush()
        success = extract_audio_from_hls_parallel(
            hls_url=hls_url,
            output_path=output_path,
            sample_rate=16000,
            channels=1,
            bitrate='48k'
        )
        if success:
            return True, f"Saved to {output_path}"
        else:
            return False, "HLS extraction failed"
    
    # Step 1: Find working game URL (tries multiple formats)
    print(f"🔗 Step 1/4: Finding game URL (trying multiple formats)...")
    sys.stdout.flush()
    game_url, source_info = find_working_game_url(away_team, home_team, date)
    
    if not game_url or not source_info['page_exists']:
        return False, "Game page not found (404)"
    
    print(f"   URL: {game_url}")
    
    # Fast-only mode: skip if only slow sources available
    if fast_only and not source_info['has_filemoon']:
        print(f"⏭️  Skipping (fast-only mode, no Filemoon source)")
        return False, "Skipped: no fast source available"
    
    # Step 2: Detect video source type (already done in find_working_game_url)
    print(f"🔍 Step 2/4: Video source: {source_info['type']}")
    sys.stdout.flush()
    
    if source_info['type'] == 'direct_filemoon':
        # Filemoon: use browser to extract HLS and parallel download
        print(f"🎬 Step 3/4: Extracting HLS from Filemoon...")
        sys.stdout.flush()
        
        filemoon_url = source_info['filemoon_url']
        hls_url = capture_hls_url_playwright(filemoon_url)
        
        if not hls_url:
            # Fallback to existing method
            hls_url = extract_hls_url_with_browser(filemoon_url)
        
        if not hls_url:
            # Check if video is 404
            return False, "Failed to extract HLS URL (video may be expired)"
        
        print(f"⬇️  Step 4/4: Downloading audio (parallel HLS)...")
        sys.stdout.flush()
        
        success = extract_audio_from_hls_parallel(
            hls_url=hls_url,
            output_path=output_path,
            sample_rate=16000,
            channels=1,
            bitrate='48k',
            max_workers=16
        )
        if success:
            return True, f"Saved to {output_path}"
        else:
            return False, "Filemoon extraction failed"
    
    elif source_info['type'] == 'direct_okru':
        # Direct OK.ru embed - use fast extraction directly
        okru_url = source_info['okru_url']
        
        print(f"🎬 Step 3/4: Direct OK.ru detected...")
        print(f"⬇️  Step 4/4: Downloading audio (fast streaming)...")
        sys.stdout.flush()
        
        success = extract_audio_fast(
            okru_url=okru_url,
            output_path=output_path,
            sample_rate=16000,
            channels=1,
            bitrate='48k'
        )
        
        if success:
            return True, f"Saved to {output_path}"
        else:
            return False, "Audio extraction failed"
    
    elif source_info['type'] == 'okru':
        # OK.ru via guidedesgemmes - need to extract link first
        embed_url = source_info['embed_url']
        
        # Step 3: Get OK.ru video URL
        print(f"🎬 Step 3/4: Extracting OK.ru video link...")
        sys.stdout.flush()
        okru_url = get_okru_video_url(embed_url)
        if not okru_url:
            return False, "Could not find OK.ru video"
        
        # Step 4: Download audio (fast streaming method)
        print(f"⬇️  Step 4/4: Downloading audio (fast streaming)...")
        sys.stdout.flush()
        
        success = extract_audio_fast(
            okru_url=okru_url,
            output_path=output_path,
            sample_rate=16000,
            channels=1,
            bitrate='48k'
        )
        
        if success:
            return True, f"Saved to {output_path}"
        else:
            return False, "Audio extraction failed"
    
    else:
        return False, f"No video source found on page"


def download_game_worker(args: tuple) -> tuple:
    """
    Worker function for parallel downloads.
    
    Args:
        args: Tuple of (idx, row, output_dir, prefer_filemoon, fast_only)
        
    Returns:
        Tuple of (idx, success, message, filename)
    """
    idx, row, output_dir, prefer_filemoon, fast_only = args
    
    try:
        success, message = download_game_audio(
            away_team=row['away_team'],
            home_team=row['home_team'],
            date=row['date'],
            output_dir=output_dir,
            prefer_filemoon=prefer_filemoon,
            fast_only=fast_only
        )
        filename = generate_filename(row['date'], row['away_team'], row['home_team']) + '.mp3' if success else ''
        return (idx, success, message, filename)
    except Exception as e:
        return (idx, False, f"Exception: {str(e)}", '')


def process_download_list(
    csv_path: str,
    output_dir: str = 'data/nba/audio',
    limit: int = None,
    delay_seconds: float = 2.0,
    parallel: int = 1,
    prefer_filemoon: bool = True,
    fast_only: bool = False
) -> pd.DataFrame:
    """
    Process a download list CSV and download audio for each game.
    
    Args:
        csv_path: Path to the download list CSV
        output_dir: Directory to save audio files
        limit: Max number of games to process (None for all)
        delay_seconds: Delay between downloads (only for sequential)
        parallel: Number of parallel downloads (1 = sequential)
        prefer_filemoon: Prefer Filemoon over OK.ru (faster)
        
    Returns:
        Updated DataFrame with status
    """
    import sys
    import concurrent.futures
    
    df = pd.read_csv(csv_path)
    
    # Filter to pending games
    pending = df[df['status'] == 'pending']
    if limit:
        pending = pending.head(limit)
    
    total = len(pending)
    print(f"\n{'='*60}")
    print(f"🏀 NBA GAME AUDIO DOWNLOAD PIPELINE")
    print(f"{'='*60}")
    print(f"📋 Processing {total} games from {csv_path}")
    print(f"💾 Output directory: {output_dir}")
    print(f"🚀 Parallel workers: {parallel}")
    print(f"🎯 Prefer Filemoon: {prefer_filemoon}")
    print(f"⚡ Fast only: {fast_only}")
    print(f"{'='*60}\n")
    sys.stdout.flush()
    
    results = []
    
    if parallel > 1:
        # Parallel processing
        print(f"⚡ Running {parallel} downloads in parallel...")
        sys.stdout.flush()
        
        # Prepare work items
        work_items = [
            (idx, row, output_dir, prefer_filemoon, fast_only) 
            for idx, row in pending.iterrows()
        ]
        
        completed = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=parallel) as executor:
            future_to_idx = {
                executor.submit(download_game_worker, item): item[0] 
                for item in work_items
            }
            
            for future in concurrent.futures.as_completed(future_to_idx):
                idx, success, message, filename = future.result()
                completed += 1
                
                # Update DataFrame
                if success:
                    df.loc[idx, 'status'] = 'downloaded'
                    df.loc[idx, 'audio_file'] = filename
                else:
                    df.loc[idx, 'status'] = f'failed: {message}'
                
                # Get game info for results
                row = df.loc[idx]
                results.append((row['date'], row.get('matchup', f"{row['away_team']} @ {row['home_team']}"), success, message))
                
                # Save progress
                df.to_csv(csv_path, index=False)
                
                # Progress update
                succeeded_so_far = sum(1 for r in results if r[2])
                failed_so_far = sum(1 for r in results if not r[2])
                print(f"\n📊 Progress: {completed}/{total} complete ({succeeded_so_far} ✅, {failed_so_far} ❌)")
                sys.stdout.flush()
    else:
        # Sequential processing
        for i, (idx, row) in enumerate(pending.iterrows(), 1):
            print(f"\n[{i}/{total}] Starting game {i} of {total}...")
            sys.stdout.flush()
            
            success, message = download_game_audio(
                away_team=row['away_team'],
                home_team=row['home_team'],
                date=row['date'],
                output_dir=output_dir,
                prefer_filemoon=prefer_filemoon,
                fast_only=fast_only
            )
            
            # Update status
            if success:
                df.loc[idx, 'status'] = 'downloaded'
                df.loc[idx, 'audio_file'] = generate_filename(row['date'], row['away_team'], row['home_team']) + '.mp3'
            else:
                df.loc[idx, 'status'] = f'failed: {message}'
            
            results.append((row['date'], row.get('matchup', f"{row['away_team']} @ {row['home_team']}"), success, message))
            
            # Save after each game (in case of interruption)
            df.to_csv(csv_path, index=False)
            
            # Progress update
            succeeded_so_far = sum(1 for r in results if r[2])
            failed_so_far = sum(1 for r in results if not r[2])
            print(f"\n📊 Progress: {i}/{total} complete ({succeeded_so_far} ✅, {failed_so_far} ❌)")
            sys.stdout.flush()
            
            # Rate limiting
            if delay_seconds > 0 and i < total:
                print(f"⏳ Waiting {delay_seconds}s before next game...")
                sys.stdout.flush()
                time.sleep(delay_seconds)
    
    # Final summary
    print(f"\n{'='*60}")
    print("📊 FINAL SUMMARY")
    print(f"{'='*60}")
    succeeded = sum(1 for r in results if r[2])
    failed = sum(1 for r in results if not r[2])
    print(f"✅ Succeeded: {succeeded}")
    print(f"❌ Failed: {failed}")
    print(f"💾 Results saved to: {csv_path}")
    
    if failed > 0:
        print("\n❌ Failed games:")
        for date, matchup, success, msg in results:
            if not success:
                print(f"  - {date} {matchup}: {msg}")
    
    return df


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Download NBA game audio for transcription')
    parser.add_argument('--csv', type=str, help='Path to download list CSV')
    parser.add_argument('--away', type=str, help='Away team name for single game')
    parser.add_argument('--home', type=str, help='Home team name for single game')
    parser.add_argument('--date', type=str, help='Game date (YYYY-MM-DD) for single game')
    parser.add_argument('-o', '--output', type=str, default='data/nba/audio', help='Output directory')
    parser.add_argument('--limit', type=int, help='Max games to process from CSV')
    parser.add_argument('--delay', type=float, default=2.0, help='Delay between downloads (seconds)')
    parser.add_argument('--parallel', '-p', type=int, default=1, help='Number of parallel downloads (default: 1)')
    parser.add_argument('--prefer-filemoon', action='store_true', default=True, 
                        help='Prefer Filemoon over OK.ru (faster, default: True)')
    parser.add_argument('--prefer-okru', action='store_true',
                        help='Prefer OK.ru over Filemoon')
    parser.add_argument('--fast-only', '-f', action='store_true',
                        help='Only download games with fast sources (Filemoon), skip OK.ru-only games')
    
    args = parser.parse_args()
    
    # Determine source preference
    prefer_filemoon = not args.prefer_okru
    
    if args.csv:
        # Process CSV file
        process_download_list(
            csv_path=args.csv,
            output_dir=args.output,
            limit=args.limit,
            delay_seconds=args.delay,
            parallel=args.parallel,
            prefer_filemoon=prefer_filemoon,
            fast_only=args.fast_only
        )
    elif args.away and args.home and args.date:
        # Single game
        success, msg = download_game_audio(
            away_team=args.away,
            home_team=args.home,
            date=args.date,
            output_dir=args.output,
            prefer_filemoon=prefer_filemoon,
            fast_only=args.fast_only
        )
        print(f"\nResult: {'✅ Success' if success else '❌ Failed'} - {msg}")
    else:
        parser.print_help()
        print("\nExamples:")
        print("  # Single game:")
        print("  python game_audio_pipeline.py --away Knicks --home Kings --date 2026-01-14")
        print()
        print("  # Process CSV - fast only (recommended, ~2 min/game):")
        print("  python game_audio_pipeline.py --csv data/nba/dave_pasch_games.csv --fast-only")
        print()
        print("  # Process CSV with parallel downloads:")
        print("  python game_audio_pipeline.py --csv data/nba/dave_pasch_games.csv -p 2 --fast-only")


if __name__ == '__main__':
    main()
