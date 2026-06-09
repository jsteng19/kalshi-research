#!/usr/bin/env python3
"""
OK.ru NBA Game Audio Extractor

Extracts audio from NBA game videos hosted on OK.ru (Odnoklassniki).
Uses yt-dlp which has native support for OK.ru streaming.

Usage:
    # Single video
    python okru_extractor.py https://ok.ru/videoembed/11430202378924
    
    # From a page containing OK.ru embeds
    python okru_extractor.py --from-page https://some-site.com/nba-game-page
    
    # Batch from file
    python okru_extractor.py --batch urls.txt
    
    # With custom output
    python okru_extractor.py -o data/nba/audio/ https://ok.ru/...

Requirements:
    pip install yt-dlp beautifulsoup4 requests
"""

import argparse
import os
import re
import subprocess
import sys
import shutil
from pathlib import Path
from typing import List, Optional
from datetime import datetime

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing dependencies. Run: pip install requests beautifulsoup4")
    sys.exit(1)


def _yt_dlp_cmd() -> str:
    """Resolve a usable yt-dlp executable for the current environment."""
    candidates = [
        os.environ.get("YTDLP_BIN", "").strip(),
        shutil.which("yt-dlp"),
        str(Path(sys.executable).with_name("yt-dlp")),
    ]
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return candidate
    return "yt-dlp"


def normalize_okru_url(url: str) -> str:
    """Normalize OK.ru URL to full https URL."""
    url = url.strip()
    # Handle protocol-relative URLs (//ok.ru/...)
    if url.startswith('//'):
        url = 'https:' + url
    # Handle missing protocol
    elif url.startswith('ok.ru'):
        url = 'https://' + url
    return url


def find_okru_embeds(url: str) -> List[str]:
    """Find all OK.ru video embeds on a page."""
    try:
        response = requests.get(url, timeout=30, headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        okru_urls = []
        
        # Find iframes with ok.ru embeds
        for iframe in soup.find_all('iframe'):
            src = iframe.get('src', '')
            if 'ok.ru' in src and 'video' in src:
                normalized = normalize_okru_url(src)
                if normalized not in okru_urls:
                    okru_urls.append(normalized)
        
        # Also check for direct links in page content
        # Match both full URLs and protocol-relative URLs
        okru_pattern = r'(?:https?:)?//(?:www\.)?ok\.ru/(?:videoembed|video)/(\d+)'
        for match in re.finditer(okru_pattern, response.text):
            url_match = match.group(0)
            normalized = normalize_okru_url(url_match)
            if normalized not in okru_urls:
                okru_urls.append(normalized)
        
        return okru_urls
        
    except Exception as e:
        print(f"Error fetching page: {e}")
        return []


def get_video_info(url: str) -> Optional[dict]:
    """Get video information using yt-dlp."""
    try:
        result = subprocess.run(
            [_yt_dlp_cmd(), '--dump-json', url],
            capture_output=True, text=True, timeout=60
        )
        
        if result.returncode == 0:
            import json
            return json.loads(result.stdout)
        else:
            print(f"Error getting video info: {result.stderr}")
            return None
            
    except Exception as e:
        print(f"Error: {e}")
        return None


def extract_audio(
    url: str, 
    output_dir: str = '.', 
    format_id: str = 'hls-333',
    audio_format: str = 'mp3',
    filename: str = None,
    sample_rate: int = 16000,
    channels: int = 1,
    bitrate: str = '48k',
    for_transcription: bool = True
) -> bool:
    """
    Extract audio from OK.ru video.
    
    Args:
        url: OK.ru video URL
        output_dir: Output directory for audio files
        format_id: yt-dlp format ID (hls-333 = lowest quality, fastest)
        audio_format: Output audio format (mp3, wav, etc.)
        filename: Custom output filename (without extension)
        sample_rate: Audio sample rate in Hz (16000 for transcription)
        channels: Number of audio channels (1 = mono for transcription)
        bitrate: Audio bitrate (48k is good for speech)
        for_transcription: If True, uses optimized settings for speech recognition
    
    Returns:
        True if successful, False otherwise
    """
    os.makedirs(output_dir, exist_ok=True)
    
    if filename:
        output_template = os.path.join(output_dir, f"{filename}.%(ext)s")
    else:
        output_template = os.path.join(output_dir, "%(title)s.%(ext)s")
    
    # First, check available formats to find the smallest one
    print(f"🔍 Checking available formats...")
    sys.stdout.flush()
    
    try:
        check_result = subprocess.run(
            [_yt_dlp_cmd(), '-F', '--no-warnings', url],
            capture_output=True, text=True, timeout=60
        )
        
        # Parse available formats
        has_hls = 'hls-' in check_result.stdout
        has_mobile_only = 'mobile' in check_result.stdout and not has_hls
        
        if has_mobile_only:
            print(f"⚠️  WARNING: Only 'mobile' format available (large file ~2-4GB)")
            print(f"   This video doesn't have smaller HLS streams.")
            print(f"   Skipping to avoid huge download.")
            return False
        
        # Choose format: prefer lowest HLS if available
        if has_hls:
            chosen_format = 'hls-333/hls-535/worst'  # Lowest HLS, fallback to next lowest
            print(f"✅ HLS streams available - using lowest quality")
        else:
            chosen_format = 'worst'
            print(f"📦 Using smallest available format")
            
    except Exception as e:
        print(f"⚠️  Could not check formats: {e}")
        chosen_format = 'worst'
    
    # Build command with speed optimizations
    cmd = [
        _yt_dlp_cmd(),
        '-f', chosen_format,
        '-x',  # Extract audio
        '--audio-format', audio_format,
        '--force-overwrites',
        '--progress',  # Show progress
        '--newline',   # Progress on new lines for better output
        '--concurrent-fragments', '4',  # Download 4 HLS segments in parallel
        '--retries', '3',  # Retry failed downloads
        '--fragment-retries', '3',  # Retry failed fragments
        '-o', output_template,
    ]
    
    # Add FFmpeg post-processor args for transcription-optimized audio
    if for_transcription:
        # 16kHz mono at low bitrate - perfect for Whisper/speech recognition
        ffmpeg_args = f'-ar {sample_rate} -ac {channels} -b:a {bitrate}'
        cmd.extend(['--postprocessor-args', f'ExtractAudio:{ffmpeg_args}'])
    else:
        cmd.extend(['--audio-quality', '2'])
    
    # Add URL last
    cmd.append(url)
    
    print(f"🎬 Extracting audio from: {url}")
    print(f"💾 Output: {output_template}")
    if for_transcription:
        print(f"📝 Transcription mode: {sample_rate}Hz, {channels}ch, {bitrate}")
    print(f"⏳ Starting download...")
    sys.stdout.flush()
    
    try:
        # Use Popen to stream output in real-time
        process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        
        # Stream output line by line
        for line in process.stdout:
            line = line.strip()
            if line:
                # Filter to show only progress-relevant lines
                if any(x in line.lower() for x in ['download', '%', 'extract', 'destination', 'deleting']):
                    print(f"  {line}")
                    sys.stdout.flush()
        
        process.wait(timeout=3600)
        
        if process.returncode == 0:
            print("✅ Audio extraction successful!")
            return True
        else:
            print("❌ Extraction failed")
            return False
            
    except subprocess.TimeoutExpired:
        process.kill()
        print("❌ Extraction timed out (>1 hour)")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def batch_extract(
    urls: List[str],
    output_dir: str = '.',
    format_id: str = 'hls-333',
    audio_format: str = 'mp3',
    sample_rate: int = 16000,
    channels: int = 1,
    bitrate: str = '48k',
    for_transcription: bool = True
) -> dict:
    """
    Batch extract audio from multiple OK.ru videos.
    
    Returns:
        Dict with 'success', 'failed' counts and 'results' list
    """
    results = {
        'success': 0,
        'failed': 0,
        'results': []
    }
    
    for i, url in enumerate(urls, 1):
        print(f"\n[{i}/{len(urls)}] Processing: {url}")
        
        success = extract_audio(
            url, output_dir, format_id, audio_format,
            sample_rate=sample_rate, channels=channels,
            bitrate=bitrate, for_transcription=for_transcription
        )
        
        results['results'].append({
            'url': url,
            'success': success
        })
        
        if success:
            results['success'] += 1
        else:
            results['failed'] += 1
    
    print(f"\n📊 Batch complete: {results['success']} success, {results['failed']} failed")
    return results


def main():
    parser = argparse.ArgumentParser(description='Extract audio from OK.ru NBA game videos')
    parser.add_argument('url', nargs='?', help='OK.ru video URL or page URL with --from-page')
    parser.add_argument('-o', '--output-dir', default='data/nba/audio', help='Output directory')
    parser.add_argument('-f', '--format', default='hls-333', 
                        help='yt-dlp format ID (hls-333=144p fastest, hls-973=360p good quality)')
    parser.add_argument('--audio-format', default='mp3', help='Output audio format')
    parser.add_argument('--from-page', action='store_true', 
                        help='Extract OK.ru embeds from the given page URL')
    parser.add_argument('--batch', type=str, help='File containing URLs (one per line)')
    parser.add_argument('--info', action='store_true', help='Just show video info')
    parser.add_argument('--list-formats', action='store_true', help='List available formats')
    
    # Transcription optimization options
    parser.add_argument('--sample-rate', '-r', type=int, default=16000,
                        help='Audio sample rate in Hz (default: 16000 for transcription)')
    parser.add_argument('--channels', '-c', type=int, default=1,
                        help='Audio channels (default: 1 = mono)')
    parser.add_argument('--bitrate', '-b', default='48k',
                        help='Audio bitrate (default: 48k for speech)')
    parser.add_argument('--high-quality', action='store_true',
                        help='Skip transcription optimization, use high quality audio')
    
    args = parser.parse_args()
    
    if args.batch:
        # Batch mode from file
        with open(args.batch, 'r') as f:
            urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        
        if not urls:
            print("No URLs found in batch file")
            return 1
        
        print(f"📦 Processing {len(urls)} URLs from batch file")
        batch_extract(
            urls, args.output_dir, args.format, args.audio_format,
            sample_rate=args.sample_rate, channels=args.channels,
            bitrate=args.bitrate, for_transcription=not args.high_quality
        )
        return 0
    
    if not args.url:
        parser.print_help()
        return 1
    
    if args.from_page:
        # Find OK.ru embeds on the page
        print(f"🔍 Searching for OK.ru embeds on: {args.url}")
        okru_urls = find_okru_embeds(args.url)
        
        if not okru_urls:
            print("No OK.ru embeds found on the page")
            return 1
        
        print(f"Found {len(okru_urls)} OK.ru embed(s):")
        for url in okru_urls:
            print(f"  - {url}")
        
        # Extract from all found embeds
        batch_extract(
            okru_urls, args.output_dir, args.format, args.audio_format,
            sample_rate=args.sample_rate, channels=args.channels,
            bitrate=args.bitrate, for_transcription=not args.high_quality
        )
        return 0
    
    if args.list_formats:
        # List available formats
        cmd = [_yt_dlp_cmd(), '-F', args.url]
        subprocess.run(cmd)
        return 0
    
    if args.info:
        # Just show info
        info = get_video_info(args.url)
        if info:
            print(f"Title: {info.get('title', 'N/A')}")
            print(f"Duration: {info.get('duration', 0) / 60:.1f} minutes")
            print(f"Uploader: {info.get('uploader', 'N/A')}")
        return 0
    
    # Single URL extraction
    success = extract_audio(
        args.url, args.output_dir, args.format, args.audio_format,
        sample_rate=args.sample_rate, channels=args.channels,
        bitrate=args.bitrate, for_transcription=not args.high_quality
    )
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
