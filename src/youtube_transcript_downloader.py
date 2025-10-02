#!/usr/bin/env python3
"""
Simple YouTube Transcript Downloader with built-in proxy support.
Downloads transcripts with proper filenames (yyyy-mm-dd_title.txt) and skips existing files.
"""

import argparse
import csv
import json
import os
import re
import time
import random
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.proxies import WebshareProxyConfig
from tqdm import tqdm
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


class YouTubeTranscriptDownloader:
    def __init__(self, use_proxy=True):
        self.session = requests.Session()
        
        # Set up user agents for metadata scraping
        self.user_agents = [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15'
        ]
        self.set_random_user_agent()
        
        # Set up YouTube Transcript API with proxy if configured
        self.ytt_api = self.setup_transcript_api(use_proxy)
        
        # Rate limiting
        self.min_delay = 3.0
        self.max_delay = 7.0

    def setup_transcript_api(self, use_proxy=True):
        """Set up YouTube Transcript API with optional proxy configuration."""
        if use_proxy:
            proxy_username = os.getenv('WEBSHARE_PROXY_USERNAME')
            proxy_password = os.getenv('WEBSHARE_PROXY_PASSWORD')
            
            if proxy_username and proxy_password:
                logger.info("Using Webshare proxy configuration")
                proxy_config = WebshareProxyConfig(
                    proxy_username=proxy_username,
                    proxy_password=proxy_password,
                )
                return YouTubeTranscriptApi(proxy_config=proxy_config)
            else:
                logger.warning("Proxy credentials not found in .env file, running without proxy")
                logger.warning("Add WEBSHARE_PROXY_USERNAME and WEBSHARE_PROXY_PASSWORD to .env file")
        
        logger.info("Using YouTube Transcript API without proxy")
        return YouTubeTranscriptApi()

    def set_random_user_agent(self):
        """Set a random user agent for metadata scraping."""
        ua = random.choice(self.user_agents)
        self.session.headers.update({'User-Agent': ua})

    def extract_video_id(self, url):
        """Extract video ID from YouTube URL."""
        patterns = [
            r'(?:youtube\.com\/watch\?v=|youtu\.be\/|youtube\.com\/embed\/)([^&\n?#]+)',
            r'youtube\.com\/watch\?.*v=([^&\n?#]+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        
        try:
            parsed_url = urlparse(url)
            if parsed_url.hostname in ['www.youtube.com', 'youtube.com']:
                if parsed_url.path == '/watch':
                    return parse_qs(parsed_url.query).get('v', [None])[0]
            elif parsed_url.hostname in ['youtu.be']:
                return parsed_url.path[1:]
        except:
            pass
        
        return None

    def get_video_metadata(self, video_id):
        """Get video metadata by scraping YouTube page."""
        url = f"https://www.youtube.com/watch?v={video_id}"
        
        for attempt in range(3):
            try:
                self.set_random_user_agent()
                response = self.session.get(url, timeout=15)
                
                if response.status_code != 200:
                    time.sleep(2)
                    continue
                    
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract title
                title = None
                title_tag = soup.find('meta', property='og:title')
                if title_tag:
                    title = title_tag.get('content', '')
                
                # Try alternative title extraction
                if not title:
                    title_match = re.search(r'"title":"([^"]+)"', response.text)
                    if title_match:
                        title = title_match.group(1)
                
                # Extract upload date
                upload_date = None
                
                # Try JSON-LD structured data
                scripts = soup.find_all('script', type='application/ld+json')
                for script in scripts:
                    try:
                        data = json.loads(script.string)
                        if isinstance(data, list):
                            data = data[0]
                        if data.get('@type') == 'VideoObject':
                            upload_date_str = data.get('uploadDate', '')
                            if upload_date_str:
                                upload_date = upload_date_str[:10]
                                break
                    except:
                        continue
                
                # Try regex patterns for date
                if not upload_date:
                    date_patterns = [
                        r'"publishDate":"(\d{4}-\d{2}-\d{2})',
                        r'"datePublished":"(\d{4}-\d{2}-\d{2})',
                        r'"uploadDate":"(\d{4}-\d{2}-\d{2})'
                    ]
                    for pattern in date_patterns:
                        match = re.search(pattern, response.text)
                        if match:
                            upload_date = match.group(1)
                            break
                
                if title:
                    return title, upload_date
                    
            except Exception as e:
                logger.warning(f"Metadata extraction attempt {attempt + 1} failed for {video_id}: {e}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
        
        return None, None

    def sanitize_filename(self, filename):
        """Sanitize filename by removing invalid characters."""
        filename = re.sub(r'[<>:"/\\|?*]', '', filename)
        filename = re.sub(r'[\n\r\t]', ' ', filename)
        filename = re.sub(r'\s+', ' ', filename.strip())
        
        if len(filename) > 180:
            filename = filename[:180] + '...'
        
        return filename

    def create_filename(self, video_id, title, upload_date):
        """Create formatted filename."""
        if not upload_date:
            upload_date = datetime.now().strftime('%Y-%m-%d')
        
        if not title:
            title = f"Video_{video_id}"
        
        clean_title = self.sanitize_filename(title)
        filename = f"{upload_date}_{clean_title}.txt"
        
        return filename

    def download_transcript(self, video_id):
        """Download transcript using the configured API."""
        try:
            # Try different language preferences
            for lang_codes in [['en'], ['en-US'], ['en-GB'], ['English - DTVCC1'], None]:  # None = auto-detect
                try:
                    if lang_codes:
                        transcript = self.ytt_api.fetch(video_id, language_codes=lang_codes, time_stamps=False)
                    else:
                        transcript = self.ytt_api.fetch(video_id)
                    return transcript
                except Exception as e:
                    if 'transcript' in str(e).lower() or 'language' in str(e).lower():
                        continue  # Try next language
                    else:
                        raise e  # Re-raise non-language related errors
        except Exception as e:
            logger.error(f"Failed to download transcript for {video_id}: {e}")
            return None

    def save_transcript(self, transcript, video_id, title, upload_date, output_dir):
        """Save transcript to file."""
        filename = self.create_filename(video_id, title, upload_date)
        output_path = Path(output_dir) / filename
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            # Write metadata header
            f.write(f"Video ID: {video_id}\n")
            f.write(f"Title: {title or 'N/A'}\n")
            f.write(f"Upload Date: {upload_date or 'N/A'}\n")
            f.write(f"Language: {transcript.language}\n")
            f.write(f"Language Code: {transcript.language_code}\n")
            f.write(f"Is Generated: {transcript.is_generated}\n")
            f.write("=" * 80 + "\n\n")
            
            # Write clean transcript
            full_text = []
            for snippet in transcript:
                full_text.append(snippet.text)
            
            f.write(" ".join(full_text))
        
        return output_path

    def file_exists(self, video_id, output_dir):
        """Check if transcript file already exists for this video by reading the Video ID from file content."""
        output_path = Path(output_dir)
        if not output_path.exists():
            return False
        
        # Look through all .txt files and check their Video ID in the content
        for file_path in output_path.glob("*.txt"):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    first_line = f.readline().strip()
                    # Check if first line contains the Video ID
                    if first_line.startswith("Video ID: ") and video_id in first_line:
                        return True
            except Exception:
                # If we can't read the file, continue checking other files
                continue
        
        return False

    def process_csv(self, csv_file, output_dir):
        """Process all URLs in CSV file."""
        # Read URLs
        urls = []
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                urls.append(row['url'])
        
        logger.info(f"Found {len(urls)} URLs to process")
        
        successful = 0
        failed = 0
        skipped = 0
        
        for i, url in enumerate(tqdm(urls, desc="Processing videos")):
            video_id = self.extract_video_id(url)
            if not video_id:
                logger.warning(f"Could not extract video ID from: {url}")
                failed += 1
                continue
            
            # Check if already exists
            if self.file_exists(video_id, output_dir):
                logger.info(f"Skipping {video_id} (already exists)")
                skipped += 1
                continue
            
            try:
                logger.info(f"Processing {i+1}/{len(urls)}: {video_id}")
                
                # Get metadata
                title, upload_date = self.get_video_metadata(video_id)
                logger.info(f"Title: {title[:50]}..." if title else "Title: N/A")
                logger.info(f"Date: {upload_date}" if upload_date else "Date: N/A")
                
                # Download transcript
                transcript = self.download_transcript(video_id)
                if not transcript:
                    logger.error(f"Failed to download transcript for {video_id}")
                    failed += 1
                    continue
                
                # Save transcript
                output_path = self.save_transcript(transcript, video_id, title, upload_date, output_dir)
                logger.info(f"✓ Saved: {output_path.name}")
                successful += 1
                
            except Exception as e:
                logger.error(f"✗ Failed {video_id}: {e}")
                failed += 1
            
            # Add delay between requests
            if i < len(urls) - 1:
                delay = random.uniform(self.min_delay, self.max_delay)
                time.sleep(delay)
        
        # Print summary
        print(f"\n{'='*60}")
        print("DOWNLOAD SUMMARY")
        print(f"{'='*60}")
        print(f"Total videos: {len(urls)}")
        print(f"✓ Successful: {successful}")
        print(f"✗ Failed: {failed}")
        print(f"⏭ Skipped (existing): {skipped}")
        print(f"Success rate: {successful/(len(urls)-skipped)*100:.1f}%" if (len(urls)-skipped) > 0 else "N/A")


def main():
    parser = argparse.ArgumentParser(description='Download YouTube transcripts with built-in proxy support')
    parser.add_argument('input', help='Input CSV file with YouTube URLs')
    parser.add_argument('output', help='Output directory for transcripts')
    parser.add_argument('--delay-min', type=float, default=3.0, help='Minimum delay between requests (seconds)')
    parser.add_argument('--delay-max', type=float, default=7.0, help='Maximum delay between requests (seconds)')
    parser.add_argument('--no-proxy', action='store_true', help='Disable proxy usage')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input):
        print(f"Error: Input file '{args.input}' not found")
        return 1
    
    print(f"Input: {args.input}")
    print(f"Output: {args.output}")
    print(f"Delay: {args.delay_min}-{args.delay_max}s between requests")
    
    # Check proxy configuration
    use_proxy = not args.no_proxy
    if use_proxy:
        proxy_username = os.getenv('WEBSHARE_PROXY_USERNAME')
        proxy_password = os.getenv('WEBSHARE_PROXY_PASSWORD')
        if proxy_username and proxy_password:
            print("✓ Webshare proxy configured")
        else:
            print("⚠ No proxy credentials found in .env file")
            print("  Add WEBSHARE_PROXY_USERNAME and WEBSHARE_PROXY_PASSWORD to .env")
    else:
        print("⚠ Proxy disabled by --no-proxy flag")
    
    print("Features: Skip existing files, metadata extraction, built-in proxy support")
    print()
    
    downloader = YouTubeTranscriptDownloader(use_proxy)
    downloader.min_delay = args.delay_min
    downloader.max_delay = args.delay_max
    
    try:
        downloader.process_csv(args.input, args.output)
        print("\n🎉 Download completed!")
        return 0
    except KeyboardInterrupt:
        print("\n⏸ Download interrupted by user")
        return 1
    except Exception as e:
        print(f"\n💥 Error: {e}")
        return 1


if __name__ == "__main__":
    exit(main())