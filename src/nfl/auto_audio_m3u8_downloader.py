#!/usr/bin/env python3
"""
Automated NFL Audio M3U8 Downloader

Downloads M3U8 files specifically for audio extraction and transcription.
Optimized for getting master.m3u8 and prog.m3u8 files from NFL+ streams.
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


class AutoAudioM3U8Downloader:
    """Automated NFL+ master.m3u8 downloader for transcription workflows."""
    
    def __init__(self, download_dir: str = "data-football", wait_time: int = 30, force: bool = False, max_retries: int = 3):
        self.download_dir = download_dir
        self.wait_time = wait_time
        self.force = force
        self.max_retries = max_retries
        self.driver = None
        self.logged_in_confirmed = False
        
        # Create download directory
        os.makedirs(download_dir, exist_ok=True)
    
    def setup_driver(self):
        """Setup Chrome driver optimized for audio stream detection."""
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
            
            # Check if we need to log in
            page_source = self.driver.page_source.lower()
            
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
            
            print("   ✅ Proceeding to game URLs...")
            self.logged_in_confirmed = True
            return True
            
        except Exception as e:
            print(f"   ❌ Error checking NFL+ access: {e}")
            print("   ⚠️  Proceeding anyway - will check access on individual games")
            return True
    
    def get_audio_m3u8_urls(self) -> List[str]:
        """Extract master.m3u8 URLs from Chrome network logs."""
        logs = self.driver.get_log('performance')
        m3u8_urls = []
        
        for log in logs:
            try:
                message = json.loads(log['message'])
                if message['message']['method'] == 'Network.responseReceived':
                    url = message['message']['params']['response']['url']
                    # Look for master.m3u8 files only (needed for audio extraction)
                    if '.m3u8' in url and 'master' in url.lower():
                        m3u8_urls.append(url)
                elif message['message']['method'] == 'Network.requestWillBeSent':
                    url = message['message']['params']['request']['url']
                    if '.m3u8' in url and 'master' in url.lower():
                        m3u8_urls.append(url)
            except:
                continue
        
        return list(set(m3u8_urls))  # Remove duplicates
    
    def check_m3u8_content(self, url: str) -> bool:
        """Check if M3U8 content is valid (not an API error)."""
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
            
            response = session.get(url, timeout=10)
            content = response.text.strip()
            
            # Check for API error patterns
            error_patterns = [
                '{"error":"Invalid API Key","success":false}',
                '{"error":',
                '"success":false',
                'invalid api key',
                'unauthorized',
                'forbidden'
            ]
            
            for pattern in error_patterns:
                if pattern.lower() in content.lower():
                    print(f"      ❌ API error detected in M3U8: {pattern}")
                    return False
            
            # Check if it's a valid M3U8 playlist
            if content.startswith('#EXTM3U') or '#EXT-X-STREAM-INF:' in content:
                print(f"      ✅ Valid M3U8 content detected")
                return True
            else:
                print(f"      ❌ Invalid M3U8 content: {content[:100]}...")
                return False
                
        except Exception as e:
            print(f"      ❌ Error checking M3U8 content: {e}")
            return False
    
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
                WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                print(f"   ✅ Video player found: {selector}")
                return True
            except TimeoutException:
                continue
        
        print("   ⚠️  No video player found, but continuing...")
        return False
    
    def try_autoplay_video(self) -> bool:
        """Video autoplays on opening, no need to trigger playback."""
        print("   🎬 Video should autoplay automatically...")
        return True
    
    def smart_wait_for_audio_m3u8(self, max_wait: int = None, max_retries: int = 3) -> List[str]:
        """Smart waiting strategy for master.m3u8 files with retry logic."""
        if max_wait is None:
            max_wait = self.wait_time
            
        print(f"   🕐 Smart waiting for master.m3u8 files (up to {max_wait}s, max {max_retries} retries)...")
        
        for retry in range(max_retries):
            if retry > 0:
                print(f"   🔄 Retry attempt {retry}/{max_retries}")
                print(f"   🔄 Refreshing page to get fresh M3U8 URLs...")
                self.driver.refresh()
                time.sleep(3)  # Wait for page to reload
            
            found_urls: Set[str] = set()
            check_intervals = [2, 3, 5, 8]  # Faster initial checks
            total_waited = 0
            
            for interval in check_intervals:
                if total_waited >= max_wait:
                    break
                    
                wait_time = min(interval, max_wait - total_waited)
                print(f"   ⏳ Checking after {wait_time}s...")
                time.sleep(wait_time)
                total_waited += wait_time
                
                # Get current master.m3u8 URLs
                current_urls = self.get_audio_m3u8_urls()
                new_urls = set(current_urls) - found_urls
                
                if new_urls:
                    found_urls.update(new_urls)
                    print(f"   ✅ Found {len(new_urls)} master.m3U8 URLs!")
                    
                    # Validate the M3U8 content
                    valid_urls = []
                    for url in found_urls:
                        print(f"   🔍 Validating M3U8: {url}")
                        if self.check_m3u8_content(url):
                            valid_urls.append(url)
                        else:
                            print(f"      ⚠️  Invalid content, trying next URL...")
                    
                    if valid_urls:
                        print(f"   ✅ Found {len(valid_urls)} valid master.m3U8 URLs!")
                        return valid_urls
                    else:
                        print(f"   ❌ All M3U8 URLs have invalid content, will retry...")
                        break
            
            if retry < max_retries - 1:
                print(f"   ⏳ No valid M3U8 URLs found, retrying...")
        
        print(f"   ❌ Failed to get valid M3U8 URLs after {max_retries} retries")
        return []
    
    def extract_team_names_from_url(self, game_url: str) -> str:
        """Extract team names from NFL game URL."""
        # Extract team names from URL like: lions-at-ravens-2025-reg-3
        game_match = re.search(r'/games/([^/]+)', game_url)
        if game_match:
            team_part = game_match.group(1)
            # Remove year and week info, keep just teams
            teams_only = re.sub(r'-\d{4}-reg-\d+$', '', team_part)
            return teams_only
        return "unknown-teams"
    
    def check_existing_file(self, game_url: str, game_date: str = None) -> str:
        """Check if output file already exists before processing URL."""
        team_names = self.extract_team_names_from_url(game_url)
        if game_date:
            filename = f"{game_date}_{team_names}.m3u8"
        else:
            filename = f"{team_names}.m3u8"
        
        filepath = os.path.join(self.download_dir, filename)
        return filepath if os.path.exists(filepath) else None
    
    def download_audio_m3u8_from_url(self, game_url: str, game_date: str = None) -> List[str]:
        """Download master.m3u8 files from a game URL."""
        print(f"🎵 Processing audio streams: {game_url}")
        
        # Check if output file already exists BEFORE visiting URL
        team_names = self.extract_team_names_from_url(game_url)
        if game_date:
            filename = f"{game_date}_{team_names}.m3u8"
        else:
            filename = f"{team_names}.m3u8"
        
        filepath = os.path.join(self.download_dir, filename)
        
        if os.path.exists(filepath) and not self.force:
            file_size = os.path.getsize(filepath)
            print(f"   ⏭️  M3U8 file already exists: {filename}")
            print(f"      File size: {file_size / 1024:.1f} KB")
            print(f"      Skipping URL visit to avoid overwriting")
            return [filepath]  # Return existing file path
        elif os.path.exists(filepath) and self.force:
            print(f"   🔄 Overwriting existing file: {filename}")
        
        try:
            # Navigate to game page
            self.driver.get(game_url)
            time.sleep(3)  # Reduced wait time
            
            # Check for subscription wall
            page_source = self.driver.page_source.lower()
            if 'subscription' in page_source and ('required' in page_source or 'wall' in page_source):
                print("   🔒 Subscription wall detected")
                if not self.logged_in_confirmed:
                    print("   ❌ Please ensure you're logged in to NFL+")
                    return []
            
            # Wait for video player to load
            self.wait_for_video_load()
            
            # Video autoplays, no need to trigger
            self.try_autoplay_video()
            
            # Smart wait for master.m3u8 files with retry logic
            m3u8_urls = self.smart_wait_for_audio_m3u8(max_retries=self.max_retries)
            
            if not m3u8_urls:
                print("   ❌ No valid master.m3u8 files found after retries")
                print("   💡 Possible reasons:")
                print("      - Game may not have audio streams available")
                print("      - API key issues (temporary or permanent)")
                print("      - Video didn't start playing")
                print("      - Network requests not captured properly")
                return []
            
            print(f"   ✅ Found {len(m3u8_urls)} valid master.m3u8 files!")
            
            # Extract team names for filename
            team_names = self.extract_team_names_from_url(game_url)
            
            # Download each master.m3u8 file
            downloaded_files = []
            if m3u8_urls:
                m3u8_url = m3u8_urls[0]
                print(f"   📥 Downloading master.m3u8 1/1...")

                # Generate filename: date_<team1>-at-<team2>.m3u8
                if game_date:
                    filename = f"{game_date}_{team_names}.m3u8"
                else:
                    filename = f"{team_names}.m3u8"

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
        """Process multiple URLs for master.m3u8 files."""
        print(f"🎵 Automated NFL Master M3U8 Downloader Starting...")
        print(f"📋 Processing {len(urls)} URLs for master.m3u8 files")
        print(f"⏰ Max wait time per game: {self.wait_time}s")
        
        # Pre-check for existing files to avoid unnecessary browser setup
        existing_files = []
        urls_to_process = []
        
        print(f"\n🔍 Pre-checking for existing files...")
        for i, url in enumerate(urls, 1):
            game_date = url_dates.get(url) if url_dates else None
            existing_file = self.check_existing_file(url, game_date)
            
            if existing_file and not self.force:
                existing_files.append(existing_file)
                print(f"   [{i}/{len(urls)}] ⏭️  File exists: {os.path.basename(existing_file)}")
            else:
                urls_to_process.append((i, url, game_date))
                if existing_file and self.force:
                    print(f"   [{i}/{len(urls)}] 🔄 Will overwrite: {os.path.basename(existing_file)}")
                else:
                    print(f"   [{i}/{len(urls)}] 📥 Will download: {self.extract_team_names_from_url(url)}")
        
        print(f"\n📊 Pre-check results:")
        print(f"   ⏭️  Files already exist: {len(existing_files)}")
        print(f"   📥 URLs to process: {len(urls_to_process)}")
        
        if not urls_to_process:
            print(f"\n✅ All files already exist! No browser needed.")
            return existing_files
        
        all_downloaded_files = existing_files.copy()
        
        try:
            # Setup browser only if we have URLs to process
            self.setup_driver()
            
            # Confirm NFL+ access (manual login if needed)
            if not self.confirm_nfl_plus_access():
                print("❌ Cannot proceed without NFL+ access")
                return all_downloaded_files
            
            # Process only URLs that need downloading
            success_count = 0
            skipped_count = len(existing_files)
            
            for url_index, (original_index, url, game_date) in enumerate(urls_to_process, 1):
                print(f"\n{'='*70}")
                print(f"[{url_index}/{len(urls_to_process)}] Processing Game {original_index} for Audio Streams")
                print(f"{'='*70}")
                
                downloaded = self.download_audio_m3u8_from_url(url, game_date)
                all_downloaded_files.extend(downloaded)
                
                if downloaded:
                    # Check if this was a skip (file already existed)
                    team_names = self.extract_team_names_from_url(url)
                    if game_date:
                        filename = f"{game_date}_{team_names}.m3u8"
                    else:
                        filename = f"{team_names}.m3u8"
                    filepath = os.path.join(self.download_dir, filename)
                    
                    if os.path.exists(filepath) and len(downloaded) == 1 and downloaded[0] == filepath:
                        skipped_count += 1
                        print(f"   ⏭️  Skipped (file already exists)")
                    else:
                        success_count += 1
                        print(f"   ✅ Successfully downloaded {len(downloaded)} master.m3u8 files")
                else:
                    print(f"   ❌ No master.m3u8 files downloaded for this game")
                
                # Brief pause between games
                if url_index < len(urls_to_process):
                    time.sleep(2)  # Reduced pause time
            
            print(f"\n🎉 Master M3U8 Download Complete!")
            print(f"📊 Results: {success_count}/{len(urls)} games successful")
            print(f"⏭️  Skipped: {skipped_count} games (files already exist)")
            print(f"📁 Total files available: {len(all_downloaded_files)} master.m3u8 files in: {self.download_dir}")
            
            if all_downloaded_files:
                print(f"\n🔄 Next steps for transcription:")
                print(f"1. Extract audio: python src/nfl/ts_audio_extractor.py {self.download_dir}/<date>_<teams>.m3u8 --stream 0 -r 16000 -c 1")
                print(f"2. Transcribe: python src/nfl/whisper_transcriber.py audio.wav -f transcript")
                print(f"3. Complete workflow: python src/nfl/extract_and_transcribe.py {self.download_dir}/<date>_<teams>.m3u8 --stream 0 -r 16000 -c 1")
            
            return all_downloaded_files
            
        finally:
            if self.driver:
                print("\n🔄 Keeping browser open for 10 seconds for inspection...")
                time.sleep(10)
                self.driver.quit()


def main():
    parser = argparse.ArgumentParser(description='Automated NFL Master M3U8 downloader for transcription')
    parser.add_argument('urls', nargs='*', help='NFL+ game URLs to process')
    parser.add_argument('--file', help='CSV file containing URLs (date,url format)')
    parser.add_argument('--output-dir', default='data-football', help='Output directory for M3U8 files')
    parser.add_argument('--wait-time', type=int, default=20, 
                       help='Max wait time per game for M3U8 detection (seconds)')
    parser.add_argument('--force', action='store_true', 
                       help='Overwrite existing M3U8 files')
    parser.add_argument('--max-retries', type=int, default=3,
                       help='Maximum retries for API error recovery (default: 3)')
    
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
        print("  python src/nfl/auto_audio_m3u8_downloader.py 'https://www.nfl.com/plus/games/lions-at-ravens-2025-reg-3'")
        print("  python src/nfl/auto_audio_m3u8_downloader.py --file data-football/monday_night_games.csv")
        print("  python src/nfl/auto_audio_m3u8_downloader.py --file data-football/all_games.csv --wait-time 45")
        print("  python src/nfl/auto_audio_m3u8_downloader.py --file data-football/all_games.csv --force")
        print("  python src/nfl/auto_audio_m3u8_downloader.py --file data-football/all_games.csv --max-retries 5")
        return
    
    # Create downloader and process URLs
    downloader = AutoAudioM3U8Downloader(
        download_dir=args.output_dir,
        wait_time=args.wait_time,
        force=args.force,
        max_retries=args.max_retries
    )
    downloader.process_urls(urls, url_dates)


if __name__ == '__main__':
    main()
