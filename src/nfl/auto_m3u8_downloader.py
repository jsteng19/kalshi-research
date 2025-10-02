#!/usr/bin/env python3
"""
Automated NFL M3U8 Downloader

Manual auth, then fully automated extraction with smart waits and retries.
"""

import os
import re
import time
import json
import requests
from typing import List, Dict, Set
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import argparse


class AutoNFLDownloader:
    """Fully automated NFL+ M3U8 downloader with smart waiting and retries."""
    
    def __init__(self, download_dir: str = "data-football", wait_time: int = 30):
        self.download_dir = download_dir
        self.wait_time = wait_time
        self.driver = None
        self.logged_in_confirmed = False
        
        # Create download directory
        os.makedirs(download_dir, exist_ok=True)
    
    def setup_driver(self):
        """Setup Chrome driver with network logging and optimal settings."""
        chrome_options = Options()
        
        # Keep browser visible but optimized
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Optimize for media loading
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--disable-features=VizDisplayCompositor")
        chrome_options.add_argument("--autoplay-policy=no-user-gesture-required")
        
        # Enable performance logging to capture network requests
        chrome_options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.set_page_load_timeout(60)
        
        return self.driver
    
    def confirm_nfl_plus_access(self) -> bool:
        """Confirm NFL+ access with manual login if needed."""
        print("🔐 Checking NFL+ access...")
        
        try:
            self.driver.get("https://www.nfl.com/plus")
            time.sleep(5)
            
            # Check if we need to log in - look for more specific indicators
            page_source = self.driver.page_source.lower()
            
            # Look for sign-in buttons or login forms specifically
            needs_login = False
            login_indicators = [
                'sign in to nfl+',
                'login to nfl+', 
                'subscribe to nfl+',
                'get nfl+'
            ]
            
            for indicator in login_indicators:
                if indicator in page_source:
                    needs_login = True
                    break
            
            # Also check for actual sign-in elements
            try:
                signin_elements = self.driver.find_elements(By.CSS_SELECTOR, 
                    "a[href*='sign-in'], [data-testid*='sign-in'], [class*='sign-in']")
                if signin_elements:
                    needs_login = True
            except:
                pass
            
            if needs_login:
                print("👤 Please log in to NFL+ manually in the browser window...")
                print("   1. Click 'Sign In' or 'Subscribe'")
                print("   2. Enter your NFL+ credentials") 
                print("   3. Complete login process")
                print("   4. Press Enter here when ready to continue...")
                print("   (Or just press Enter to skip login check and try anyway)")
                
                try:
                    input()
                except EOFError:
                    print("   ⏳ Waiting 30 seconds...")
                    time.sleep(30)
            
            # Always proceed - we'll check for subscription walls on individual games
            print("   ✅ Proceeding to game URLs...")
            self.logged_in_confirmed = True
            return True
            
        except Exception as e:
            print(f"   ❌ Error checking NFL+ access: {e}")
            print("   ⚠️  Proceeding anyway - will check access on individual games")
            return True
    
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
                elif message['message']['method'] == 'Network.requestWillBeSent':
                    url = message['message']['params']['request']['url']
                    if '.m3u8' in url and 'subs' in url.lower():
                        m3u8_urls.append(url)
            except:
                continue
        
        return list(set(m3u8_urls))  # Remove duplicates
    
    def wait_for_video_load(self) -> bool:
        """Wait for video player to load and start making requests."""
        print("   ⏳ Waiting for video player to load...")
        
        # Wait for video player elements to appear
        video_selectors = [
            "video", 
            "[data-testid*='video']", 
            "[class*='video']",
            "[class*='player']",
            ".nfl-o-videoPlayer"
        ]
        
        for selector in video_selectors:
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                print(f"   ✅ Video player found: {selector}")
                return True
            except TimeoutException:
                continue
        
        print("   ⚠️  No video player found, but continuing...")
        return False
    
    def try_autoplay_video(self) -> bool:
        """Try to trigger video autoplay through various methods."""
        print("   🎬 Attempting to trigger video playback...")
        
        # Method 1: Look for and click play buttons
        play_selectors = [
            "[data-testid='play-button']",
            "[data-testid*='play']",
            ".play-button",
            "button[aria-label*='play' i]",
            "button[aria-label*='Play' i]",
            "[class*='play'][class*='button']",
            ".nfl-o-videoPlayer__playButton"
        ]
        
        for selector in play_selectors:
            try:
                play_button = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                )
                self.driver.execute_script("arguments[0].click();", play_button)
                print(f"   ▶️  Clicked play button: {selector}")
                time.sleep(3)
                return True
            except TimeoutException:
                continue
        
        # Method 2: Try to trigger play on video elements directly
        try:
            video_elements = self.driver.find_elements(By.TAG_NAME, "video")
            for video in video_elements:
                self.driver.execute_script("arguments[0].play();", video)
                print("   ▶️  Triggered video.play() directly")
                time.sleep(3)
                return True
        except:
            pass
        
        # Method 3: Simulate user interaction to allow autoplay
        try:
            # Click somewhere on the page to enable autoplay
            body = self.driver.find_element(By.TAG_NAME, "body")
            self.driver.execute_script("arguments[0].click();", body)
            print("   👆 Simulated user interaction for autoplay")
            time.sleep(3)
        except:
            pass
        
        print("   ⚠️  No play button found, relying on autoplay...")
        return False
    
    def smart_wait_for_m3u8(self, max_wait: int = None) -> List[str]:
        """Smart waiting strategy for M3U8 files - stops as soon as files are found."""
        if max_wait is None:
            max_wait = self.wait_time
            
        print(f"   🕐 Smart waiting for M3U8 files (up to {max_wait}s)...")
        
        found_urls: Set[str] = set()
        check_intervals = [3, 5, 8, 12, 15]  # Quick initial checks
        total_waited = 0
        
        for interval in check_intervals:
            if total_waited >= max_wait:
                break
                
            wait_time = min(interval, max_wait - total_waited)
            print(f"   ⏳ Checking after {wait_time}s...")
            time.sleep(wait_time)
            total_waited += wait_time
            
            # Get current M3U8 URLs
            current_urls = self.get_network_logs()
            new_urls = set(current_urls) - found_urls
            
            if new_urls:
                found_urls.update(new_urls)
                print(f"   ✅ Found {len(new_urls)} M3U8 URLs! Stopping wait.")
                
                # Quick final check for any additional files (2 seconds max)
                print(f"   ⏳ Quick final check...")
                time.sleep(2)
                final_urls = self.get_network_logs()
                final_new = set(final_urls) - found_urls
                if final_new:
                    found_urls.update(final_new)
                    print(f"   ✅ Found {len(final_new)} additional M3U8 URLs")
                
                break
        
        return list(found_urls)
    
    def download_m3u8_from_url(self, game_url: str, game_date: str = None) -> List[str]:
        """Fully automated M3U8 download from a game URL."""
        print(f"📺 Processing: {game_url}")
        
        try:
            # Navigate to game page
            self.driver.get(game_url)
            time.sleep(5)
            
            # Check for subscription wall
            page_source = self.driver.page_source.lower()
            if 'subscription' in page_source and ('required' in page_source or 'wall' in page_source):
                print("   🔒 Subscription wall detected")
                if not self.logged_in_confirmed:
                    print("   ❌ Please ensure you're logged in to NFL+")
                    return []
            
            # Wait for video player to load
            self.wait_for_video_load()
            
            # Try to trigger video playback
            self.try_autoplay_video()
            
            # Smart wait for M3U8 files
            m3u8_urls = self.smart_wait_for_m3u8()
            
            if not m3u8_urls:
                print("   ❌ No M3U8 caption files found")
                print("   💡 Possible reasons:")
                print("      - Game may not have captions available")
                print("      - Video didn't start playing")
                print("      - Network requests not captured properly")
                return []
            
            print(f"   ✅ Found {len(m3u8_urls)} M3U8 files!")
            
            # Download each M3U8 file
            downloaded_files = []
            for i, m3u8_url in enumerate(m3u8_urls):
                print(f"   📥 Downloading M3U8 {i+1}/{len(m3u8_urls)}...")
                
                # Generate filename: yyyy-mm-dd_team1-at-team2.m3u8
                if game_date:
                    # Extract team names from URL
                    game_match = re.search(r'/games/([^/]+)', game_url)
                    if game_match:
                        team_part = game_match.group(1)
                        # Remove year and week info, keep just teams
                        teams_only = re.sub(r'-\d{4}-reg-\d+$', '', team_part)
                        filename = f"{game_date}_{teams_only}.m3u8"
                    else:
                        filename = f"{game_date}_unknown_game.m3u8"
                else:
                    # Fallback to old naming
                    game_match = re.search(r'/games/([^/]+)', game_url)
                    if game_match:
                        game_name = game_match.group(1)
                    else:
                        game_name = "unknown_game"
                    filename = f"{game_name}_{i}.m3u8" if len(m3u8_urls) > 1 else f"{game_name}.m3u8"
                
                # Add suffix if multiple M3U8 files
                if len(m3u8_urls) > 1:
                    name_parts = filename.rsplit('.', 1)
                    filename = f"{name_parts[0]}_{i}.{name_parts[1]}"
                
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
        """Download a single M3U8 file using browser session."""
        try:
            # Use browser cookies for authentication
            cookies = self.driver.get_cookies()
            session = requests.Session()
            
            for cookie in cookies:
                session.cookies.set(cookie['name'], cookie['value'])
            
            # Add headers to mimic browser
            session.headers.update({
                'User-Agent': self.driver.execute_script("return navigator.userAgent;"),
                'Referer': self.driver.current_url,
                'Accept': '*/*'
            })
            
            response = session.get(url, timeout=30)
            response.raise_for_status()
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            return True
            
        except Exception as e:
            print(f"      Error downloading {url}: {e}")
            return False
    
    def process_urls(self, urls: List[str], url_dates: Dict[str, str] = None) -> List[str]:
        """Process multiple URLs with full automation."""
        print(f"🏈 Automated NFL M3U8 Downloader Starting...")
        print(f"📋 Processing {len(urls)} URLs")
        print(f"⏰ Max wait time per game: {self.wait_time}s")
        
        all_downloaded_files = []
        
        try:
            # Setup browser
            self.setup_driver()
            
            # Confirm NFL+ access (manual login if needed)
            if not self.confirm_nfl_plus_access():
                print("❌ Cannot proceed without NFL+ access")
                return []
            
            # Process each URL
            success_count = 0
            for i, url in enumerate(urls, 1):
                print(f"\n{'='*70}")
                print(f"[{i}/{len(urls)}] Processing Game {i}")
                print(f"{'='*70}")
                
                # Get date for this URL if available
                game_date = url_dates.get(url) if url_dates else None
                downloaded = self.download_m3u8_from_url(url, game_date)
                all_downloaded_files.extend(downloaded)
                
                if downloaded:
                    success_count += 1
                    print(f"   ✅ Successfully downloaded {len(downloaded)} files")
                else:
                    print(f"   ❌ No files downloaded for this game")
                
                # Brief pause between games
                if i < len(urls):
                    print(f"   ⏳ Brief pause before next game...")
                    time.sleep(5)
            
            print(f"\n🎉 Batch Complete!")
            print(f"📊 Results: {success_count}/{len(urls)} games successful")
            print(f"📁 Downloaded {len(all_downloaded_files)} M3U8 files to: {self.download_dir}")
            
            if all_downloaded_files:
                print(f"\n🔄 Next step: Extract captions with:")
                print(f"python src/nfl_caption_extractor.py {self.download_dir}/")
            
            return all_downloaded_files
            
        finally:
            if self.driver:
                print("\n🔄 Keeping browser open for 10 seconds for inspection...")
                time.sleep(10)
                self.driver.quit()


def main():
    parser = argparse.ArgumentParser(description='Automated NFL M3U8 downloader')
    parser.add_argument('urls', nargs='*', help='NFL+ game URLs to process')
    parser.add_argument('--file', help='File containing URLs (one per line)')
    parser.add_argument('--output-dir', default='data-football', help='Output directory')
    parser.add_argument('--wait-time', type=int, default=30, 
                       help='Max wait time per game for M3U8 detection (seconds)')
    
    args = parser.parse_args()
    
    # Collect URLs and dates
    urls = []
    url_dates = {}
    
    if args.urls:
        urls.extend(args.urls)
    
    if args.file:
        try:
            with open(args.file, 'r') as f:
                lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]
                
                # Check if this is a CSV with date,url format
                if lines and ',' in lines[0] and lines[0].startswith('date,url'):
                    # Skip header and parse CSV
                    for line in lines[1:]:
                        if ',' in line:
                            parts = line.split(',', 1)
                            if len(parts) == 2:
                                date_str = parts[0].strip()
                                url_str = parts[1].strip()
                                urls.append(url_str)
                                url_dates[url_str] = date_str
                else:
                    # Plain URL list format
                    urls.extend(lines)
        except FileNotFoundError:
            print(f"❌ File not found: {args.file}")
            return
    
    if not urls:
        print("❌ No URLs provided. Use --help for usage information.")
        print("\nExample usage:")
        print("  python src/auto_nfl_downloader.py 'https://www.nfl.com/plus/games/lions-at-ravens-2025-reg-3'")
        print("  python src/auto_nfl_downloader.py --file data-football/monday_night_games.csv")
        print("  python src/auto_nfl_downloader.py --file data-football/all_games.csv --wait-time 45")
        return
    
    # Create downloader and process URLs
    downloader = AutoNFLDownloader(
        download_dir=args.output_dir,
        wait_time=args.wait_time
    )
    downloader.process_urls(urls, url_dates)


if __name__ == '__main__':
    main()
