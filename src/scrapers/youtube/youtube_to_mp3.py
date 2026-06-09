#!/usr/bin/env python3
"""Simple YouTube to MP3 downloader using yt-dlp."""

import subprocess
import sys

def download_mp3(url: str, output_dir: str = "."):
    """Download YouTube video as MP3."""
    cmd = [
        "yt-dlp",
        "-x",  # Extract audio
        "--audio-format", "mp3",
        "--audio-quality", "0",  # Best quality
        "-o", f"{output_dir}/%(title)s.%(ext)s",
        "--no-playlist",  # Download only the video, not the playlist
        url
    ]
    
    print(f"Downloading: {url}")
    subprocess.run(cmd, check=True)
    print("Download complete!")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python youtube_to_mp3.py <youtube_url> [output_dir]")
        sys.exit(1)
    
    url = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "."
    download_mp3(url, output_dir)

