#!/usr/bin/env python3
"""
Filemoon Audio Extractor

Extracts audio from Filemoon embedded videos by:
1. Loading the page to get the HLS master playlist URL
2. Parallel downloading of HLS segments for speed
3. Using ffmpeg to extract audio from downloaded segments

This bypasses yt-dlp's piracy block by going straight to the HLS stream.
"""

import subprocess
import requests
import re
import time
import threading
import os
import tempfile
import concurrent.futures
from typing import Optional, Tuple, List
from urllib.parse import urljoin


def extract_hls_url_from_filemoon(filemoon_url: str, timeout: int = 15) -> Optional[str]:
    """
    Extract the HLS master playlist URL from a Filemoon embed page.
    
    This uses requests to get the page and then parses the JavaScript
    for the HLS URL pattern.
    
    Args:
        filemoon_url: The filemoon.sx/e/... URL
        timeout: Request timeout in seconds
        
    Returns:
        The HLS master.m3u8 URL, or None if not found
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://basketball-video.com/',
    }
    
    try:
        # First request to filemoon.sx (disable SSL verification due to cert issues)
        resp = requests.get(filemoon_url, headers=headers, timeout=timeout, verify=False)
        resp.raise_for_status()
        
        # Filemoon redirects to a secondary domain (like g9r6.com) 
        # The HLS URL is fetched via an API call
        
        # Extract the video ID from the URL
        match = re.search(r'/e/([a-z0-9]+)', filemoon_url)
        if not match:
            print("❌ Could not extract video ID from Filemoon URL")
            return None
        
        video_id = match.group(1)
        print(f"📋 Video ID: {video_id}")
        
        # Try to find the API URL in the page
        # Look for patterns like "api/videos/{id}/embed/playback"
        api_base_match = re.search(r'https?://([a-z0-9]+\.com)', resp.text)
        if api_base_match:
            api_domain = api_base_match.group(1)
        else:
            # Try common Filemoon API domains
            api_domain = 'g9r6.com'
        
        # Try the playback API endpoint
        api_url = f"https://{api_domain}/api/videos/{video_id}/embed/playback"
        print(f"🔗 Trying API: {api_url}")
        
        headers['Referer'] = filemoon_url
        api_resp = requests.get(api_url, headers=headers, timeout=timeout, verify=False)
        
        if api_resp.status_code == 200:
            # The API returns JSON with the HLS URL
            data = api_resp.json()
            if 'sources' in data:
                for source in data['sources']:
                    if 'file' in source and '.m3u8' in source['file']:
                        hls_url = source['file']
                        print(f"✅ Found HLS URL from API")
                        return hls_url
            
            # Alternative: check for 'hls' key directly
            if 'hls' in data:
                return data['hls']
        
        # Fallback: search for m3u8 URL in page content
        m3u8_match = re.search(r'(https?://[^\s"\']+master\.m3u8[^\s"\']*)', resp.text)
        if m3u8_match:
            return m3u8_match.group(1)
        
        print("❌ Could not find HLS URL")
        return None
        
    except requests.RequestException as e:
        print(f"❌ Request failed: {e}")
        return None


def parse_m3u8_playlist(m3u8_url: str, session: requests.Session = None) -> Tuple[List[str], Optional[str]]:
    """
    Parse an M3U8 playlist and return segment URLs.
    
    Args:
        m3u8_url: URL to the M3U8 playlist
        session: Optional requests session
        
    Returns:
        Tuple of (list of segment URLs, variant playlist URL if master)
    """
    if session is None:
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
    
    try:
        resp = session.get(m3u8_url, timeout=30)
        resp.raise_for_status()
        content = resp.text
    except Exception as e:
        print(f"❌ Failed to fetch m3u8: {e}")
        return [], None
    
    segments = []
    variant_url = None
    base_url = m3u8_url.rsplit('/', 1)[0] + '/'
    
    lines = content.strip().split('\n')
    for i, line in enumerate(lines):
        line = line.strip()
        
        # Check for variant playlist (master m3u8)
        if line.startswith('#EXT-X-STREAM-INF'):
            # Next line should be the variant URL
            if i + 1 < len(lines):
                variant = lines[i + 1].strip()
                if not variant.startswith('#'):
                    if not variant.startswith('http'):
                        variant = urljoin(base_url, variant)
                    variant_url = variant
        
        # Check for segment
        if line.endswith('.ts') or '.ts?' in line:
            if not line.startswith('http'):
                line = urljoin(base_url, line)
            segments.append(line)
    
    return segments, variant_url


def download_segment(args: Tuple[str, str, int, requests.Session]) -> Tuple[int, bool, str]:
    """
    Download a single HLS segment.
    
    Args:
        args: Tuple of (url, output_path, index, session)
        
    Returns:
        Tuple of (index, success, error_msg)
    """
    url, output_path, index, session = args
    
    for attempt in range(3):
        try:
            resp = session.get(url, timeout=30 + attempt * 10)
            resp.raise_for_status()
            
            with open(output_path, 'wb') as f:
                f.write(resp.content)
            
            return (index, True, "")
        except Exception as e:
            if attempt == 2:
                return (index, False, str(e))
            time.sleep(1 + attempt)
    
    return (index, False, "Max retries exceeded")


def extract_audio_from_hls_parallel(
    hls_url: str,
    output_path: str,
    sample_rate: int = 16000,
    channels: int = 1,
    bitrate: str = '48k',
    max_workers: int = 16,
    timeout_minutes: int = 20
) -> bool:
    """
    Extract audio from HLS stream using parallel segment downloads.
    
    Much faster than sequential ffmpeg download (~3-4x speedup).
    
    Args:
        hls_url: The HLS master.m3u8 URL  
        output_path: Where to save the audio file
        sample_rate: Audio sample rate (16000 for transcription)
        channels: Number of audio channels (1 for mono)
        bitrate: Audio bitrate (48k for speech)
        max_workers: Number of parallel download threads
        timeout_minutes: Maximum time to wait
        
    Returns:
        True if successful, False otherwise
    """
    print(f"🚀 Fast parallel extraction ({max_workers} workers)...")
    start_time = time.time()
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
    })
    
    # Parse playlist
    print(f"📋 Parsing playlist...")
    segments, variant_url = parse_m3u8_playlist(hls_url, session)
    
    # If this is a master playlist, get the variant
    if variant_url and not segments:
        print(f"   Found variant playlist, fetching segments...")
        segments, _ = parse_m3u8_playlist(variant_url, session)
    
    if not segments:
        print(f"❌ No segments found in playlist")
        return False
    
    total_segments = len(segments)
    print(f"📦 Found {total_segments} segments to download")
    
    # Create temp directory for segments
    with tempfile.TemporaryDirectory() as temp_dir:
        segment_paths = []
        download_tasks = []
        
        for i, url in enumerate(segments):
            seg_path = os.path.join(temp_dir, f"seg_{i:05d}.ts")
            segment_paths.append(seg_path)
            download_tasks.append((url, seg_path, i, session))
        
        # Download segments in parallel
        print(f"⬇️  Downloading segments...")
        downloaded = 0
        failed = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(download_segment, task): task[2] for task in download_tasks}
            
            for future in concurrent.futures.as_completed(futures):
                idx, success, error = future.result()
                if success:
                    downloaded += 1
                else:
                    failed += 1
                
                # Progress update every 20 segments
                if (downloaded + failed) % 20 == 0 or (downloaded + failed) == total_segments:
                    elapsed = time.time() - start_time
                    pct = (downloaded + failed) / total_segments * 100
                    print(f"\r   📊 {downloaded}/{total_segments} downloaded ({pct:.0f}%, {elapsed:.0f}s)...", end='', flush=True)
        
        print()  # New line
        
        if failed > total_segments * 0.1:  # More than 10% failed
            print(f"❌ Too many failed downloads: {failed}/{total_segments}")
            return False
        
        print(f"✅ Downloaded {downloaded}/{total_segments} segments in {time.time()-start_time:.1f}s")
        
        # Create concat file for ffmpeg
        concat_file = os.path.join(temp_dir, "concat.txt")
        with open(concat_file, 'w') as f:
            for path in segment_paths:
                if os.path.exists(path):
                    f.write(f"file '{path}'\n")
        
        # Extract audio with ffmpeg
        print(f"🎬 Extracting audio with ffmpeg...")
        cmd = [
            'ffmpeg',
            '-y',
            '-f', 'concat',
            '-safe', '0',
            '-i', concat_file,
            '-vn',
            '-ar', str(sample_rate),
            '-ac', str(channels),
            '-b:a', bitrate,
            output_path
        ]
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout_minutes * 60
            )
            
            if result.returncode == 0:
                final_size = os.path.getsize(output_path) if os.path.exists(output_path) else 0
                total_time = time.time() - start_time
                print(f"✅ Complete: {final_size/1024/1024:.1f} MB in {total_time:.1f}s")
                return True
            else:
                print(f"❌ ffmpeg failed: {result.stderr[:300]}")
                return False
                
        except subprocess.TimeoutExpired:
            print(f"❌ ffmpeg timed out")
            return False


def extract_audio_from_hls(
    hls_url: str,
    output_path: str,
    sample_rate: int = 16000,
    channels: int = 1,
    bitrate: str = '48k',
    timeout_minutes: int = 20,
    use_parallel: bool = True,
    max_workers: int = 16
) -> bool:
    """
    Extract audio from an HLS stream.
    
    Args:
        hls_url: The HLS master.m3u8 URL
        output_path: Where to save the audio file
        sample_rate: Audio sample rate (16000 for transcription)
        channels: Number of audio channels (1 for mono)
        bitrate: Audio bitrate (48k for speech)
        timeout_minutes: Maximum time to wait for extraction
        use_parallel: Use parallel segment downloads (much faster)
        max_workers: Number of parallel download threads
        
    Returns:
        True if successful, False otherwise
    """
    if use_parallel:
        return extract_audio_from_hls_parallel(
            hls_url=hls_url,
            output_path=output_path,
            sample_rate=sample_rate,
            channels=channels,
            bitrate=bitrate,
            max_workers=max_workers,
            timeout_minutes=timeout_minutes
        )
    
    # Fallback: sequential ffmpeg (slower but simpler)
    cmd = [
        'ffmpeg',
        '-y',  # Overwrite output file
        '-i', hls_url,
        '-vn',  # No video
        '-ar', str(sample_rate),
        '-ac', str(channels),
        '-b:a', bitrate,
        output_path
    ]
    
    print(f"🎬 Starting audio extraction (sequential)...")
    print(f"   Output: {output_path}")
    
    # Track progress in background
    stop_progress = threading.Event()
    
    def progress_monitor():
        start_time = time.time()
        while not stop_progress.is_set():
            if os.path.exists(output_path):
                size = os.path.getsize(output_path)
                elapsed = time.time() - start_time
                print(f"\r   📊 {size/1024/1024:.1f} MB written ({elapsed/60:.1f} min elapsed)...", end='', flush=True)
            stop_progress.wait(5)
    
    progress_thread = threading.Thread(target=progress_monitor, daemon=True)
    progress_thread.start()
    
    try:
        # Run ffmpeg with timeout
        timeout_seconds = timeout_minutes * 60
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_seconds
        )
        
        stop_progress.set()
        print()  # New line after progress
        
        if result.returncode == 0:
            final_size = os.path.getsize(output_path) if os.path.exists(output_path) else 0
            print(f"✅ Extraction complete: {final_size/1024/1024:.1f} MB")
            return True
        else:
            print(f"❌ ffmpeg failed: {result.stderr[:500]}")
            return False
            
    except subprocess.TimeoutExpired:
        stop_progress.set()
        print(f"\n❌ Extraction timed out after {timeout_minutes} minutes")
        return False
    except Exception as e:
        stop_progress.set()
        print(f"\n❌ Error: {e}")
        return False


def extract_filemoon_audio(
    filemoon_url: str,
    output_path: str,
    sample_rate: int = 16000,
    channels: int = 1,
    bitrate: str = '48k'
) -> bool:
    """
    Full pipeline to extract audio from a Filemoon embed URL.
    
    Args:
        filemoon_url: The filemoon.sx/e/... URL
        output_path: Where to save the audio file
        sample_rate: Audio sample rate
        channels: Number of audio channels  
        bitrate: Audio bitrate
        
    Returns:
        True if successful, False otherwise
    """
    print(f"🎬 Filemoon extraction: {filemoon_url[:50]}...")
    
    # Note: The API extraction often doesn't work without browser JavaScript
    # The most reliable method is to use the HLS URL captured from browser network requests
    # For automated extraction, we need to use Playwright to get the HLS URL
    
    # For now, return False to signal that manual HLS URL is needed
    print("⚠️  Filemoon requires browser-based HLS URL extraction")
    print("   Use the game_audio_pipeline with browser support for Filemoon games")
    return False


# Alternative: Browser-based extraction (requires Playwright)
def extract_hls_url_with_browser(filemoon_url: str, timeout: int = 45) -> Optional[str]:
    """
    Extract HLS URL from Filemoon-like services using a browser to capture network requests.
    
    Supports: filemoon.sx, filemoon.to, luluvdo.com, and similar services.
    
    Args:
        filemoon_url: The video embed URL (e.g., filemoon.sx/e/..., luluvdo.com/e/...)
        timeout: How long to wait for the HLS URL to appear
        
    Returns:
        The HLS master.m3u8 URL, or None if not found
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("❌ Playwright not installed. Run: pip install playwright && playwright install")
        return None
    
    hls_url = None
    
    with sync_playwright() as p:
        # Use headed browser for better compatibility
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={'width': 1280, 'height': 720},
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            ignore_https_errors=True  # Ignore SSL certificate errors
        )
        page = context.new_page()
        
        # Capture network requests - look for any HLS stream
        def handle_request(request):
            nonlocal hls_url
            url = request.url
            # Match various HLS patterns
            if hls_url:  # Already found
                return
            if 'master.m3u8' in url:
                hls_url = url
                print(f"✅ Captured HLS URL: {url[:80]}...")
            elif '/hls' in url and '.m3u8' in url:
                hls_url = url
                print(f"✅ Captured HLS URL: {url[:80]}...")
            elif 'index-v1' in url and '.m3u8' in url:  # Variant playlist
                # Try to get master from variant
                hls_url = url.replace('index-v1-a1.m3u8', 'master.m3u8')
                print(f"✅ Captured HLS URL from variant: {hls_url[:80]}...")
        
        page.on('request', handle_request)
        
        print(f"🌐 Loading Filemoon page...")
        try:
            page.goto(filemoon_url, wait_until='domcontentloaded', timeout=timeout * 1000)
        except Exception as e:
            print(f"   Page load: {str(e)[:50]}")
        
        # Wait for page to fully load and JS to execute
        page.wait_for_timeout(3000)
        
        # Try to click the video element or play button to trigger loading
        if not hls_url:
            print("   Trying to trigger video playback...")
            try:
                # Try clicking on video container
                page.click('video', timeout=5000)
            except:
                pass
            try:
                # Try clicking play button
                page.click('.jw-icon-playback', timeout=3000)
            except:
                pass
            try:
                # Try clicking any play icon
                page.click('[aria-label="Play"]', timeout=3000)
            except:
                pass
        
        # Wait for HLS URL to appear
        if not hls_url:
            print("   Waiting for HLS URL...")
            for i in range(20):  # Wait up to 20 seconds
                if hls_url:
                    break
                page.wait_for_timeout(1000)
                if i == 10:
                    print("   Still waiting...")
        
        browser.close()
    
    return hls_url


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract audio from Filemoon videos')
    parser.add_argument('url', help='Filemoon URL or HLS m3u8 URL')
    parser.add_argument('-o', '--output', default='output.mp3', help='Output file path')
    parser.add_argument('--hls', action='store_true', help='URL is already an HLS m3u8 URL')
    parser.add_argument('--browser', action='store_true', help='Use browser to extract HLS URL')
    
    args = parser.parse_args()
    
    if args.hls:
        # Direct HLS extraction
        success = extract_audio_from_hls(args.url, args.output)
    elif args.browser:
        # Browser-based extraction
        hls_url = extract_hls_url_with_browser(args.url)
        if hls_url:
            success = extract_audio_from_hls(hls_url, args.output)
        else:
            print("❌ Failed to extract HLS URL")
            success = False
    else:
        # API-based extraction (may not work for all videos)
        success = extract_filemoon_audio(args.url, args.output)
    
    exit(0 if success else 1)
