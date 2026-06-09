#!/usr/bin/env python3
"""
Fast Audio Extractor - Streams audio directly via yt-dlp -> ffmpeg pipe.

This is ~3x faster than downloading the full video first because:
1. yt-dlp streams the HLS segments
2. FFmpeg converts on-the-fly (no intermediate files)
3. Only audio is processed, video is discarded immediately
"""

import os
import subprocess
import sys
import time
import threading
from typing import Optional


def extract_audio_fast(
    okru_url: str,
    output_path: str,
    sample_rate: int = 16000,
    channels: int = 1,
    bitrate: str = '48k',
    show_progress: bool = True
) -> bool:
    """
    Extract audio by streaming yt-dlp output directly to FFmpeg.
    
    Args:
        okru_url: OK.ru video URL
        output_path: Output MP3 file path
        sample_rate: Audio sample rate (16000 for transcription)
        channels: Number of channels (1 = mono for transcription)
        bitrate: Audio bitrate (48k for speech)
        show_progress: Show progress during download
    
    Returns:
        True if successful
    """
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
    
    print(f"🎵 Fast audio extraction: {okru_url}")
    print(f"💾 Output: {output_path}")
    print(f"📝 Settings: {sample_rate}Hz, {channels}ch, {bitrate}")
    sys.stdout.flush()
    
    start = time.time()
    
    # yt-dlp streams to stdout
    ytdlp_cmd = [
        'yt-dlp',
        '-f', 'hls-333/worst',  # Lowest quality (we only need audio)
        '--no-warnings',
        '-o', '-',  # Output to stdout
        okru_url
    ]
    
    # FFmpeg reads from stdin and converts to audio
    ffmpeg_cmd = [
        'ffmpeg', '-y',
        '-i', 'pipe:0',  # Read from stdin
        '-vn',  # No video - discard immediately
        '-ar', str(sample_rate),
        '-ac', str(channels),
        '-b:a', bitrate,
        output_path
    ]
    
    try:
        # Start yt-dlp
        ytdlp = subprocess.Popen(
            ytdlp_cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.DEVNULL
        )
        
        # Start FFmpeg, reading from yt-dlp's output
        ffmpeg = subprocess.Popen(
            ffmpeg_cmd, 
            stdin=ytdlp.stdout, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE
        )
        
        # Allow yt-dlp to receive SIGPIPE if ffmpeg exits
        ytdlp.stdout.close()
        
        # Progress monitor
        if show_progress:
            def monitor():
                while ffmpeg.poll() is None:
                    if os.path.exists(output_path):
                        size = os.path.getsize(output_path)
                        elapsed = time.time() - start
                        print(f"  ⏱️  {elapsed:.0f}s: {size/1024/1024:.1f} MB", end='\r')
                        sys.stdout.flush()
                    time.sleep(2)
            
            t = threading.Thread(target=monitor, daemon=True)
            t.start()
        
        # Wait for completion
        stdout, stderr = ffmpeg.communicate(timeout=1800)  # 30 min timeout
        
        elapsed = time.time() - start
        print()  # New line after progress
        
        if ffmpeg.returncode == 0 and os.path.exists(output_path):
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            print(f"✅ Done in {elapsed:.1f}s ({elapsed/60:.1f} min)")
            print(f"📦 Output: {size_mb:.1f} MB")
            return True
        else:
            print(f"❌ FFmpeg failed (code {ffmpeg.returncode})")
            if stderr:
                print(f"   Error: {stderr.decode()[-200:]}")
            return False
            
    except subprocess.TimeoutExpired:
        ytdlp.kill()
        ffmpeg.kill()
        print("❌ Timed out (>30 min)")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def batch_extract(
    urls: list,
    output_dir: str,
    delay: float = 2.0
) -> dict:
    """
    Process multiple URLs.
    
    Args:
        urls: List of (url, filename) tuples
        output_dir: Output directory
        delay: Delay between downloads (avoid rate limiting)
    
    Returns:
        dict with success/failed counts
    """
    os.makedirs(output_dir, exist_ok=True)
    
    results = {'success': 0, 'failed': 0, 'files': []}
    
    for i, (url, filename) in enumerate(urls, 1):
        print(f"\n{'='*60}")
        print(f"📥 [{i}/{len(urls)}] {filename}")
        print(f"{'='*60}")
        
        output_path = os.path.join(output_dir, filename)
        
        if extract_audio_fast(url, output_path):
            results['success'] += 1
            results['files'].append(output_path)
        else:
            results['failed'] += 1
        
        if i < len(urls) and delay > 0:
            print(f"⏳ Waiting {delay}s before next download...")
            time.sleep(delay)
    
    print(f"\n{'='*60}")
    print(f"📊 BATCH COMPLETE: {results['success']}/{len(urls)} successful")
    print(f"{'='*60}")
    
    return results


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Fast audio extraction from OK.ru (streaming)')
    parser.add_argument('url', help='OK.ru video URL')
    parser.add_argument('-o', '--output', required=True, help='Output MP3 file path')
    parser.add_argument('-r', '--sample-rate', type=int, default=16000, help='Sample rate (default: 16000)')
    parser.add_argument('-c', '--channels', type=int, default=1, help='Channels (default: 1 = mono)')
    parser.add_argument('-b', '--bitrate', default='48k', help='Bitrate (default: 48k)')
    
    args = parser.parse_args()
    
    success = extract_audio_fast(
        args.url,
        args.output,
        sample_rate=args.sample_rate,
        channels=args.channels,
        bitrate=args.bitrate
    )
    
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
