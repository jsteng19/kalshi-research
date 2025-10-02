#!/usr/bin/env python3
"""
Manual NFL M3U8 Downloader

Opens the game page and lets you manually click play, then captures M3U8 files.
"""

import os
import re
import time
import json
import requests
from typing import List
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import argparse


class ManualNFLDownloader:
    """Manual NFL+ M3U8 downloader with user interaction."""
    
    def __init__(self, download_dir: str = "nfl_games"):
        self.download_dir = download_dir
        self.driver = None
        
        # Create download directory
        os.makedirs(download_dir, exist_ok=True)
    
    def setup_driver(self):
        """Setup Chrome driver with network logging."""
        chrome_options = Options()
        
        # Keep browser visible and use existing profile
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Enable performance logging to capture network requests
        chrome_options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
        
        self.driver = webdriver.Chrome(options=chrome_options)
        return self.driver
    
    def get_network_logs(self) -> List[str]:
        """Extract M3U8 URLs from Chrome network logs."""
        logs = self.driver.get_log('performance')
        m3u8_urls = []
        
        for log in logs:
            try:
                message = json.loads(log['message'])
                if message['message']['method'] == 'Network.responseReceived':
                    url = message['message']['params']['response']['url']
                    if '.m3u8' in url and 'subs' in url.lower():
                        m3u8_urls.append(url)
            except:
                continue
        
        return list(set(m3u8_urls))  # Remove duplicates
    
    def download_m3u8_from_url(self, game_url: str) -> List[str]:
        """Download M3U8 files from a specific NFL+ game URL with manual interaction."""
        print(f"📺 Processing: {game_url}")
        
        try:
            # Navigate to the game page
            self.driver.get(game_url)
            print("   🌐 Game page loaded")
            
            print("👤 Manual steps:")
            print("   1. If you see a subscription wall, please log in to NFL+")
            print("   2. Click the PLAY button to start the video")
            print("   3. Let the video play for at least 10-15 seconds")
            print("   4. Press Enter here when the video is playing...")
            
            # Wait for user to manually start the video
            try:
                input()
            except EOFError:
                print("   ⏳ Waiting 30 seconds for manual video start...")
                time.sleep(30)
            
            print("   🔍 Checking for M3U8 caption files...")
            
            # Get M3U8 URLs from network logs
            m3u8_urls = self.get_network_logs()
            
            if not m3u8_urls:
                print("   ⚠️  No M3U8 files found yet, waiting a bit more...")
                time.sleep(10)
                m3u8_urls = self.get_network_logs()
            
            if not m3u8_urls:
                print("   ❌ No M3U8 caption files found")
                print("   💡 Try:")
                print("      - Make sure captions/subtitles are enabled")
                print("      - Let the video play longer")
                print("      - Check if this game has captions available")
                return []
            
            print(f"   ✅ Found {len(m3u8_urls)} M3U8 files!")
            
            # Download each M3U8 file
            downloaded_files = []
            for i, m3u8_url in enumerate(m3u8_urls):
                print(f"   📥 Downloading M3U8 {i+1}/{len(m3u8_urls)}...")
                
                # Extract game info from URL for filename
                game_match = re.search(r'/games/([^/]+)', game_url)
                if game_match:
                    game_name = game_match.group(1)
                else:
                    game_name = "unknown_game"
                
                filename = f"{game_name}_{i}.m3u8" if len(m3u8_urls) > 1 else f"{game_name}.m3u8"
                filepath = os.path.join(self.download_dir, filename)
                
                if self.download_m3u8_file(m3u8_url, filepath):
                    print(f"      ✅ Downloaded: {filename}")
                    downloaded_files.append(filepath)
                else:
                    print(f"      ❌ Failed to download: {filename}")
            
            return downloaded_files
            
        except Exception as e:
            print(f"   ❌ Error processing {game_url}: {e}")
            return []
    
    def download_m3u8_file(self, url: str, filepath: str) -> bool:
        """Download a single M3U8 file."""
        try:
            # Use the same session as the browser for authentication
            cookies = self.driver.get_cookies()
            session = requests.Session()
            
            for cookie in cookies:
                session.cookies.set(cookie['name'], cookie['value'])
            
            response = session.get(url, timeout=30)
            response.raise_for_status()
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            return True
            
        except Exception as e:
            print(f"      Error downloading {url}: {e}")
            return False
    
    def process_urls(self, urls: List[str]) -> List[str]:
        """Process a list of NFL+ game URLs."""
        print(f"🏈 Manual NFL M3U8 Downloader Starting...")
        print(f"📋 Processing {len(urls)} URLs")
        
        all_downloaded_files = []
        
        try:
            # Setup browser
            self.setup_driver()
            
            # Process each URL
            for i, url in enumerate(urls, 1):
                print(f"\n{'='*60}")
                print(f"[{i}/{len(urls)}] Processing Game {i}")
                print(f"{'='*60}")
                
                downloaded = self.download_m3u8_from_url(url)
                all_downloaded_files.extend(downloaded)
                
                if i < len(urls):
                    print(f"\n⏳ Moving to next game in 5 seconds...")
                    time.sleep(5)
            
            print(f"\n🎉 Complete! Downloaded {len(all_downloaded_files)} M3U8 files")
            print(f"📁 Files saved to: {self.download_dir}")
            
            if all_downloaded_files:
                print(f"\n🔄 Next step: Extract captions with:")
                print(f"python src/nfl_caption_extractor.py {self.download_dir}/")
            
            return all_downloaded_files
            
        finally:
            if self.driver:
                print("\n🔄 Keeping browser open for 10 seconds...")
                time.sleep(10)
                self.driver.quit()


def main():
    parser = argparse.ArgumentParser(description='Manual NFL M3U8 downloader')
    parser.add_argument('urls', nargs='*', help='NFL+ game URLs to process')
    parser.add_argument('--file', help='File containing URLs (one per line)')
    parser.add_argument('--output-dir', default='nfl_games', help='Output directory')
    
    args = parser.parse_args()
    
    # Collect URLs
    urls = []
    
    # From command line arguments
    if args.urls:
        urls.extend(args.urls)
    
    # From file
    if args.file:
        try:
            with open(args.file, 'r') as f:
                file_urls = [line.strip() for line in f if line.strip()]
                urls.extend(file_urls)
        except FileNotFoundError:
            print(f"❌ File not found: {args.file}")
            return
    
    if not urls:
        print("❌ No URLs provided. Use --help for usage information.")
        print("\nExample usage:")
        print("  python src/manual_nfl_downloader.py 'https://www.nfl.com/plus/games/lions-at-ravens-2025-reg-3'")
        print("  python src/manual_nfl_downloader.py --file data-football/monday_night_games.csv")
        return
    
    # Create downloader and process URLs
    downloader = ManualNFLDownloader(download_dir=args.output_dir)
    downloader.process_urls(urls)


if __name__ == '__main__':
    main()
