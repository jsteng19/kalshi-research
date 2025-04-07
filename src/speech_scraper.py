import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import threading
from ratelimit import limits, sleep_and_retry

# Rate limit: 10 requests per second
@sleep_and_retry
@limits(calls=10, period=1)
def rate_limited_request(url, headers):
    return requests.get(url, headers=headers)

class TrumpSpeechScraper:
    def __init__(self, url, save_path, max_workers=12):
        self.speeches = []
        self.base_url = url
        self.save_path = save_path
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.max_workers = max_workers
        self._lock = threading.Lock()
        
    def collect_transcript_urls(self, target_date=None):
        """
        Collects transcript URLs by scrolling through pages
        Args:
            target_date (datetime, optional): Date to scroll back to. Defaults to None for all transcripts.
        Returns:
            list: List of transcript URLs
        """
        if target_date is None:
            target_date = datetime.strptime('June 15, 2015', '%B %d, %Y')
            
        print("Starting URL collection...")
        print(f"Will only collect transcripts after {target_date.strftime('%B %d, %Y')}")
        
        # Setup Chrome with WebDriver manager
        print("Setting up Chrome WebDriver...")
        options = webdriver.ChromeOptions()
        options.add_argument('--headless=new')  # Updated headless mode
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--start-maximized')
        options.add_argument('--disable-blink-features=AutomationControlled')
        
        # Add additional options to bypass anti-bot measures
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-infobars')
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--ignore-ssl-errors')
        options.add_argument('--disable-popup-blocking')
        
        # Add user agent to make the request more like a regular browser
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36')
        
        # Force Chrome to log all console messages, to help diagnose issues
        options.add_argument('--enable-logging')
        options.add_argument('--v=1')
        
        driver = None
        service = None
        
        try:
            # Try using ChromeDriverManager
            try:
                print("Attempting to use ChromeDriverManager...")
                service = Service(ChromeDriverManager().install())
            except Exception as e:
                print(f"ChromeDriverManager failed: {str(e)}")
                print("Trying alternative methods...")
                
                # Try looking for chromedriver in current directory or PATH
                import shutil
                chromedriver_path = shutil.which('chromedriver.exe') or './chromedriver.exe'
                if os.path.exists(chromedriver_path):
                    print(f"Found chromedriver at: {chromedriver_path}")
                    service = Service(chromedriver_path)
                else:
                    print("No chromedriver found. Please download manually or install with: pip install webdriver-manager")
                    return []
            
            max_init_retries = 3
            for init_attempt in range(max_init_retries):
                try:
                    print(f"Initializing Chrome WebDriver (attempt {init_attempt + 1}/{max_init_retries})...")
                    driver = webdriver.Chrome(service=service, options=options)
                    
                    # Set page load timeout
                    driver.set_page_load_timeout(60)
                    
                    print(f"Loading URL: {self.base_url}")
                    driver.get(self.base_url)
                    
                    # Print page title to help diagnose issues
                    print(f"Page title: {driver.title}")
                    
                    # Wait for Vue.js app to initialize and content to load
                    print("Waiting for content to load...")
                    wait = WebDriverWait(driver, 45)  # Increased timeout to 45 seconds
                    
                    # First wait for the document to be ready
                    wait.until(lambda d: d.execute_script('return document.readyState') == 'complete')
                    
                    # Check if we've been blocked or redirected
                    current_url = driver.current_url
                    if "rollcall.com/factbase" not in current_url.lower():
                        print(f"Warning: Redirected to {current_url} - possible security measure")
                        if "captcha" in driver.page_source.lower() or "robot" in driver.page_source.lower():
                            print("Detected Captcha or bot detection page")
                            raise Exception("Bot detection triggered - manual verification needed")
                    
                    # Then wait for the specific content
                    try:
                        content = wait.until(
                            EC.presence_of_element_located((By.ID, "factbase-content"))
                        )
                        print("Content loaded successfully")
                    except Exception as content_error:
                        print(f"Content element not found: {str(content_error)}")
                        print("Looking for alternative elements...")
                        
                        # Try to find any links matching our expected pattern
                        try:
                            transcript_links = driver.find_elements(By.CSS_SELECTOR, 
                                'a[href*="/factbase/trump/transcript/"]')
                            if transcript_links:
                                print(f"Found {len(transcript_links)} transcript links directly")
                            else:
                                # Save page source for debugging
                                with open("page_source.html", "w", encoding="utf-8") as f:
                                    f.write(driver.page_source)
                                print("Page source saved to page_source.html for debugging")
                                raise Exception("No transcript links found and no factbase-content element")
                        except Exception as e:
                            print(f"Alternative element search failed: {str(e)}")
                            raise
                    
                    # Wait additional time for dynamic content
                    time.sleep(15)  # Increased initial wait time
                    
                    break  # If we get here, initialization was successful
                    
                except Exception as e:
                    print(f"Initialization attempt {init_attempt + 1} failed: {str(e)}")
                    if init_attempt < max_init_retries - 1:
                        print("Retrying initialization...")
                        time.sleep(10)  # Increased wait time between retries
                        if driver:
                            driver.quit()
                            driver = None
                    else:
                        if driver:
                            # Save screenshot and page source for debugging
                            try:
                                driver.save_screenshot("error_screenshot.png")
                                with open("error_page.html", "w", encoding="utf-8") as f:
                                    f.write(driver.page_source)
                                print("Error screenshot and page saved for debugging")
                            except:
                                pass
                        raise Exception("Failed to initialize WebDriver after multiple attempts")
            
            if driver is None:
                return []
        
            # Keep track of URLs and their dates
            transcript_urls = []  # Maintain chronological order
            url_dates = {}  # Track dates for each URL
            seen_urls = set()  # For deduplication
            page = 1
            consecutive_no_new = 0  # Counter for consecutive attempts with no new content
            retry_count = 0
            max_retries = 3
            
            while consecutive_no_new < 3 and retry_count < max_retries:
                try:
                    print(f"\nChecking page {page}...")
                    
                    # Wait for any existing content to stabilize
                    time.sleep(2)
                    
                    # Find all transcript links on current page with retry
                    for attempt in range(3):
                        try:
                            # Try more general selector first
                            transcript_links = WebDriverWait(driver, 10).until(
                                EC.presence_of_all_elements_located((
                                    By.CSS_SELECTOR, 
                                    'a[href*="/factbase/trump/transcript/"]'
                                ))
                            )
                            print(f"Found {len(transcript_links)} links with general selector")
                            
                            # If found with general selector, we can try to filter for title if needed
                            title_links = driver.find_elements(By.CSS_SELECTOR, 
                                'a[href*="/factbase/trump/transcript/"][title="View Transcript"]')
                            
                            if title_links:
                                print(f"Found {len(title_links)} links with title 'View Transcript'")
                                transcript_links = title_links
                                
                            break
                        except Exception as e:
                            if attempt == 2:  # Last attempt
                                raise
                            print(f"Retry {attempt + 1}/3 finding transcript links: {str(e)}")
                            time.sleep(2)
                    
                    current_transcript_count = len(transcript_links)
                    print(f"Found {current_transcript_count} total transcripts on page")
                    
                    if not transcript_links:
                        print("No transcript links found on this page")
                        break
                    
                    # Extract all URLs from current page with retry for stale elements
                    page_urls = []  # Store URLs in order for this page
                    page_dates = []  # Store dates for range display
                    
                    for link in transcript_links:
                        for attempt in range(3):
                            try:
                                url = link.get_attribute('href')
                                if url:
                                    # Extract date from URL
                                    date_match = re.search(r'(?:january|february|march|april|may|june|july|august|september|october|november|december)-(\d{1,2})-(\d{4})', url, re.IGNORECASE)
                                    if date_match:
                                        try:
                                            month_str, day_str, year_str = date_match.group(0).split('-')
                                            date = datetime(int(year_str), datetime.strptime(month_str, '%B').month, int(day_str))
                                            if date >= target_date:
                                                page_urls.append((url, date))
                                                page_dates.append(date)
                                        except ValueError:
                                            pass  # Skip URLs with invalid dates
                                break
                            except Exception as e:
                                if attempt == 2:  # Last attempt
                                    print(f"Failed to get URL after 3 attempts: {str(e)}")
                                    continue
                                time.sleep(1)
                    
                    if page_dates:
                        earliest_date = min(page_dates)
                        latest_date = max(page_dates)
                        print(f"Page date range: {latest_date.strftime('%B %d, %Y')} to {earliest_date.strftime('%B %d, %Y')}")
                    
                    # Process new URLs in chronological order
                    new_urls = [(url, date) for url, date in page_urls if url not in seen_urls]
                    if new_urls:
                        print(f"Found {len(new_urls)} new transcripts after {target_date.strftime('%B %d, %Y')}")
                        # Sort by date (newest first) and add to list
                        new_urls.sort(key=lambda x: x[1], reverse=True)
                        for url, date in new_urls:
                            transcript_urls.append(url)
                            url_dates[url] = date
                            seen_urls.add(url)
                        consecutive_no_new = 0  # Reset counter since we found new content
                        retry_count = 0  # Reset retry count on success
                    else:
                        consecutive_no_new += 1
                        print(f"No new transcripts found. Attempt {consecutive_no_new}/3")
                    
                    # Set up a mutation observer to watch for content changes
                    setup_observer = """
                        if (window._factbaseObserver) {
                            window._factbaseObserver.disconnect();
                        }
                        window.contentChanged = false;
                        window._factbaseObserver = new MutationObserver(() => {
                            window.contentChanged = true;
                        });
                        
                        try {
                            const content = document.getElementById('factbase-content');
                            if (content) {
                                window._factbaseObserver.observe(
                                    content,
                                    { childList: true, subtree: true }
                                );
                                console.log("Observer attached successfully");
                            } else {
                                console.log("factbase-content element not found for observer");
                                // Try to observe the body instead
                                window._factbaseObserver.observe(
                                    document.body,
                                    { childList: true, subtree: true }
                                );
                                console.log("Observer attached to body instead");
                            }
                        } catch (e) {
                            console.error("Error setting up observer:", e);
                        }
                    """
                    driver.execute_script(setup_observer)
                    
                    # Get current scroll position
                    current_scroll = driver.execute_script("return window.pageYOffset;")
                    
                    # Scroll window aggressively
                    driver.execute_script("""
                        window.scrollTo({
                            top: document.body.scrollHeight + 1000,
                            behavior: 'auto'
                        });
                    """)
                    
                    print("Scrolled window to bottom")
                    
                    # Wait for potential content changes
                    wait_start = time.time()
                    content_changed = False
                    new_scroll = current_scroll
                    
                    while time.time() - wait_start < 15:  # Increased wait time to 15 seconds
                        content_changed = driver.execute_script("return window.contentChanged;")
                        new_scroll = driver.execute_script("return window.pageYOffset;")
                        
                        if content_changed or new_scroll > current_scroll:
                            print("Detected content change or scroll position change")
                            break
                        time.sleep(0.5)
                    
                    if not content_changed and new_scroll <= current_scroll:
                        print("No content or scroll position changes detected")
                        
                    # Clean up observer
                    cleanup_observer = """
                        if (window._factbaseObserver) {
                            window._factbaseObserver.disconnect();
                            delete window._factbaseObserver;
                        }
                        delete window.contentChanged;
                    """
                    driver.execute_script(cleanup_observer)
                    
                    # Additional wait for any animations
                    time.sleep(2)
                    
                    page += 1
                    
                except Exception as e:
                    print(f"Error during page processing: {str(e)}")
                    retry_count += 1
                    if retry_count < max_retries:
                        print(f"Retrying... (attempt {retry_count + 1}/{max_retries})")
                        time.sleep(5)  # Wait before retry
                        continue
                    else:
                        print("Max retries reached")
                        break
            
            print(f"\nFinished collecting URLs. Found {len(transcript_urls)} unique transcripts after {target_date.strftime('%B %d, %Y')}.")
            
            # Save URLs to file in chronological order (newest first)
            urls_file = self.save_path
            os.makedirs(os.path.dirname(urls_file), exist_ok=True)
            with open(urls_file, 'w') as f:
                for url in transcript_urls:  # Already in chronological order
                    f.write(f"{url}\n")
            print(f"Saved URLs to {urls_file}")
            
            return transcript_urls
            
        except Exception as e:
            print(f"Error collecting URLs: {str(e)}")
            return []
        finally:
            if driver:
                driver.quit()
                print("WebDriver closed")

    def process_transcripts(self, url_path="data/transcript_urls.txt"):
        """
        Process a list of transcript URLs and save them to files in parallel
        Args:
            urls (list, optional): List of URLs to process. If None, reads from url_path
            url_path (str, optional): Path to file containing URLs. Defaults to "data/transcript_urls.txt"
        """

        try:
            with open(url_path, 'r') as f:
                urls = [line.strip() for line in f if line.strip()]
            print(f"Loaded {len(urls)} URLs from {url_path}")
        except FileNotFoundError:
            print(f"No {url_path} file found. Please run collect_transcript_urls first.")
            return
        
        # Create raw-transcripts directory
        os.makedirs('data/raw-transcripts', exist_ok=True)
        
        # Process URLs in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks and create a mapping of futures to URLs
            future_to_url = {
                executor.submit(self._process_single_transcript, url, i, len(urls)): url 
                for i, url in enumerate(urls, 1)
            }
            
            # Process completed tasks with a progress bar
            with tqdm(total=len(urls), desc="Processing transcripts") as pbar:
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        metadata = future.result()
                        if metadata:
                            with self._lock:
                                self.speeches.append(metadata)
                    except Exception as e:
                        print(f"\nError processing {url}: {str(e)}")
                    pbar.update(1)
        
        print(f"\nFinished processing. Collected {len(self.speeches)} speeches.")

    def _process_single_transcript(self, url, index, total):
        """
        Process a single transcript URL
        Args:
            url (str): URL to process
            index (int): Current transcript index
            total (int): Total number of transcripts
        Returns:
            dict: Metadata for the processed transcript
        """
        try:
            # First check if this is a press briefing by fetching just the title
            response = rate_limited_request(url, self.headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            title = soup.find('title').text if soup.find('title') else ''
            clean_title = title.replace('Roll Call Factba.se - ', '')
            
            # Check category from title
            if ': ' in clean_title:
                category = clean_title.split(': ', 1)[0]
            else:
                category = 'Press Briefing' if clean_title.startswith('Press Briefing') else 'Uncategorized'
            
            # # Skip press briefings early
            # if category.lower() == 'press briefing':
            #     return None
            
            # Extract date from title
            date_match = re.search(r'-\s*([A-Za-z]+\s+\d+,\s+\d{4})', title)
            date = date_match.group(1).strip() if date_match else None
            
            # Try to determine the filename this transcript would have
            if date:
                try:
                    parsed_date = datetime.strptime(date, '%B %d, %Y')
                    date_str = parsed_date.strftime('%Y-%m-%d')
                except:
                    date_str = date.replace(' ', '-')
                
                # Clean title for filename
                if date in clean_title:
                    clean_title = clean_title.replace(f" - {date}", "")
                if ': ' in clean_title:
                    clean_title = clean_title.split(': ', 1)[1]
                
                filename = re.sub(r'[^\w\s-]', '', clean_title)
                filename = re.sub(r'[\s]+', '_', filename).lower()
                potential_path = f"data/raw-transcripts/{category.lower()}/{date_str}_{filename}.txt"
                
                # Check if file already exists
                if os.path.exists(potential_path):
                    print(f"\nSkipping existing transcript: {potential_path}")
                    return None
            
            # If we get here, we need to process the transcript
            transcript_text, date, title, category = self._fetch_transcript(url)
            
            if transcript_text and title and category:
                # Create category directory if it doesn't exist
                category_dir = f"data/raw-transcripts/{category.lower()}"
                os.makedirs(category_dir, exist_ok=True)
                
                # Convert date to YYYY-MM-DD format
                try:
                    parsed_date = datetime.strptime(date, '%B %d, %Y')
                    date_str = parsed_date.strftime('%Y-%m-%d')
                except:
                    date_str = date.replace(' ', '-')
                
                # Remove the date from the end of the title if it exists
                if date in title:
                    title = title.replace(f" - {date}", "")
                
                # Create filename from title
                filename = re.sub(r'[^\w\s-]', '', title)
                filename = re.sub(r'[\s]+', '_', filename).lower()
                filename = f"{category_dir}/{date_str}_{filename}.txt"
                
                # Save transcript
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(transcript_text)
                
                # Return metadata
                return {
                    'date': date,
                    'title': title,
                    'category': category,
                    'url': url,
                    'filename': filename
                }
            
            return None
            
        except Exception as e:
            print(f"\nError parsing speech from {url}: {str(e)}")
            return None

    def scrape_factbase(self, scroll_pages=None):
        """
        Main method to scrape transcripts - collects URLs then processes them
        Args:
            scroll_pages (int, optional): Number of times to scroll for more content
        """
        urls = self.collect_transcript_urls(scroll_pages)
        if urls:
            self.process_transcripts(urls)

    def _fetch_transcript(self, url):
        """
        Fetches and parses the full transcript from a speech page
        Args:
            url (str): URL of the transcript page
        Returns:
            tuple: (transcript_text, date, title, category)
        """
        try:
            response = rate_limited_request(url, self.headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract title and date
            title = soup.find('title').text if soup.find('title') else ''
            date_match = re.search(r'-\s*([A-Za-z]+\s+\d+,\s+\d{4})', title)
            date = date_match.group(1).strip() if date_match else None
            
            # Clean title - remove prefix
            clean_title = title.replace('Roll Call Factba.se - ', '')
            
            # Extract category and title
            if ': ' in clean_title:
                category, title_part = clean_title.split(': ', 1)
            else:
                # Handle cases where title starts with "Press Briefing" without colon
                if clean_title.startswith('Press Briefing'):
                    category = 'Press Briefing'
                    title_part = clean_title
                else:
                    category = 'Uncategorized'
                    title_part = clean_title
            
            # # Skip press briefings
            # if category.lower() == 'press briefing':
            #     return None, None, None, None
            
            # Find all text chunks with the specific classes
            text_chunks = []
            chunks = soup.find_all('div', class_=lambda x: x and all(c in str(x) for c in ['flex-auto', 'text-md', 'text-gray-600', 'leading-loose']))
            
            for chunk in chunks:
                # Get the speaker label from the h2 tag within the parent's parent div
                parent_div = chunk.find_parent('div', class_='w-full')
                if parent_div:
                    speaker_elem = parent_div.find('h2', class_='text-md inline')
                    speaker = speaker_elem.get_text(strip=True) if speaker_elem else "SPEAKER"
                else:
                    speaker = "SPEAKER"
                
                # Get the text content
                text = chunk.get_text(strip=True)
                text = re.sub(r'\[\d{2}:\d{2}:\d{2}\]', '', text)
                
                if text:
                    text_chunks.append(f"{speaker}: {text}")
            
            # Combine all chunks with proper spacing
            if text_chunks:
                full_text = f"Source: {url}\n\n" + "\n\n".join(text_chunks)
                return full_text, date, title_part, category
            
            return None, None, None, None
            
        except Exception as e:
            print(f"Error fetching transcript from {url}: {str(e)}")
            return None, None, None, None