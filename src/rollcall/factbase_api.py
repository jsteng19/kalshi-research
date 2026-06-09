"""
Factbase API Client

This module provides a clean API-based approach to fetching transcripts from
the Roll Call / Factbase service without requiring Selenium web scraping.

The key discovery: Roll Call exposes a WordPress REST API endpoint at:
    https://rollcall.com/wp-json/factbase/v1/search

This endpoint returns JSON data with transcript metadata including:
- URLs to transcript pages
- Dates, titles, categories
- Speaker information
- Document metadata

The transcript content itself can be fetched via simple HTTP requests and
parsed with BeautifulSoup - no JavaScript rendering needed.
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import os
import re
from typing import Optional, List, Dict, Any, Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time
from ratelimit import limits, sleep_and_retry


# Rate limit: 10 requests per second
@sleep_and_retry
@limits(calls=10, period=1)
def rate_limited_request(url: str, headers: dict) -> requests.Response:
    """Make a rate-limited HTTP request."""
    return requests.get(url, headers=headers, timeout=30)


class FactbaseAPI:
    """
    Client for the Factbase/Roll Call API.
    
    This class provides methods to:
    1. Search and list transcripts via the API
    2. Fetch individual transcript content
    3. Process and save transcripts locally
    
    Example usage:
        api = FactbaseAPI(person="trump")
        
        # Get new transcripts since a date
        transcripts = api.get_transcripts_since(datetime(2026, 1, 1))
        
        # Process and save them
        api.process_transcripts(transcripts, data_dir="data")
    """
    
    BASE_URL = "https://rollcall.com/wp-json/factbase/v1/search"
    
    def __init__(self, person: str = "trump", max_workers: int = 12):
        """
        Initialize the Factbase API client.
        
        Args:
            person: The person to search for (e.g., "trump", "vance")
            max_workers: Number of parallel workers for fetching transcripts
        """
        self.person = person.lower()
        self.max_workers = max_workers
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
    
    def search(
        self,
        page: int = 1,
        sort: str = "date",
        media: str = "",
        record_type: str = "",
        location: str = "all",
        place: str = "all"
    ) -> Dict[str, Any]:
        """
        Search for transcripts using the Factbase API.
        
        Args:
            page: Page number (1-indexed)
            sort: Sort order ("date" for newest first)
            media: Media type filter
            record_type: Record type filter (e.g., "transcript")
            location: Location filter
            place: Place filter
            
        Returns:
            Dict with 'meta' and 'data' keys containing search results
        """
        params = {
            "page": page,
            "sort": sort,
            "media": media,
            "type": record_type,
            "location": location,
            "place": place,
            "format": "json",
            "person": self.person
        }
        
        response = rate_limited_request(
            self.BASE_URL,
            self.headers
        )
        
        # Actually need to pass params
        response = requests.get(
            self.BASE_URL,
            params=params,
            headers=self.headers,
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    
    def iter_all_transcripts(
        self,
        since_date: Optional[datetime] = None,
        max_pages: Optional[int] = None
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Iterate through all transcripts, optionally filtered by date.
        
        Args:
            since_date: Only return transcripts on or after this date
            max_pages: Maximum number of pages to fetch (None for all)
            
        Yields:
            Dict containing transcript metadata for each transcript
        """
        page = 1
        total_pages = None
        
        while True:
            if max_pages and page > max_pages:
                break
                
            result = self.search(page=page)
            meta = result.get("meta", {})
            data = result.get("data", [])
            
            if total_pages is None:
                total_pages = meta.get("total_pages", 1)
                print(f"Found {meta.get('records_matched', 0)} total transcripts across {total_pages} pages")
            
            if not data:
                break
            
            for item in data:
                # Parse the date
                date_str = item.get("date", "")
                try:
                    item_date = datetime.strptime(date_str, "%Y-%m-%d")
                except ValueError:
                    item_date = None
                
                # Filter by date if specified
                if since_date and item_date and item_date < since_date:
                    # Since results are sorted by date (newest first),
                    # once we hit an older date, we can stop
                    return
                
                yield item
            
            # Check if we've reached the last page
            if page >= total_pages:
                break
                
            page += 1
    
    def get_transcripts_since(
        self,
        since_date: datetime,
        max_results: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all transcripts since a given date.
        
        Args:
            since_date: Only return transcripts on or after this date
            max_results: Maximum number of results to return
            
        Returns:
            List of transcript metadata dicts
        """
        transcripts = []
        for item in self.iter_all_transcripts(since_date=since_date):
            transcripts.append(item)
            if max_results and len(transcripts) >= max_results:
                break
        return transcripts
    
    def get_all_transcript_urls(
        self,
        since_date: Optional[datetime] = None
    ) -> List[str]:
        """
        Get all transcript URLs since a given date.
        
        Args:
            since_date: Only return transcripts on or after this date
            
        Returns:
            List of transcript URLs
        """
        return [
            item.get("factbase_url", "")
            for item in self.iter_all_transcripts(since_date=since_date)
            if item.get("factbase_url")
        ]
    
    def fetch_transcript_content(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Fetch the full transcript content from a transcript page URL.
        
        Args:
            url: URL to the transcript page
            
        Returns:
            Dict with transcript text, date, title, and category, or None on error
        """
        try:
            response = rate_limited_request(url, self.headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract title and date from page title
            title = soup.find('title')
            title_text = title.text if title else ''
            date_match = re.search(r'-\s*([A-Za-z]+\s+\d+,\s+\d{4})', title_text)
            date = date_match.group(1).strip() if date_match else None
            
            # Clean title - remove prefix
            clean_title = title_text.replace('Roll Call Factba.se - ', '')
            
            # Extract category and title
            if ': ' in clean_title:
                category, title_part = clean_title.split(': ', 1)
            else:
                if clean_title.startswith('Press Briefing'):
                    category = 'Press Briefing'
                    title_part = clean_title
                else:
                    category = 'Uncategorized'
                    title_part = clean_title
            
            # Find all text chunks with the specific classes
            text_chunks = []
            chunks = soup.find_all('div', class_=lambda x: x and all(
                c in str(x) for c in ['flex-auto', 'text-md', 'text-gray-600', 'leading-loose']
            ))
            
            for chunk in chunks:
                # Get the speaker label from the h2 tag within the parent's parent div
                parent_div = chunk.find_parent('div', class_='w-full')
                if parent_div:
                    speaker_elem = parent_div.find('h2', class_='text-md inline')
                    speaker = speaker_elem.get_text(strip=True) if speaker_elem else "SPEAKER"
                else:
                    speaker = "SPEAKER"
                
                # Get the text content and clean timestamps
                text = chunk.get_text(strip=True)
                text = re.sub(r'\[\d{2}:\d{2}:\d{2}\]', '', text)
                
                if text:
                    text_chunks.append(f"{speaker}: {text}")
            
            if text_chunks:
                full_text = f"Source: {url}\n\n" + "\n\n".join(text_chunks)
                return {
                    "text": full_text,
                    "date": date,
                    "title": title_part,
                    "category": category,
                    "url": url
                }
            
            return None
            
        except Exception as e:
            print(f"Error fetching transcript from {url}: {str(e)}")
            return None
    
    def process_transcripts(
        self,
        transcripts: List[Dict[str, Any]],
        data_dir: str = "data",
        skip_existing: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Process and save a list of transcripts.
        
        Args:
            transcripts: List of transcript metadata from the API
            data_dir: Base directory for saving transcripts
            skip_existing: If True, skip transcripts that already exist
            
        Returns:
            List of processed transcript metadata
        """
        processed = []
        
        # Create raw-transcripts directory
        os.makedirs(f'{data_dir}/raw-transcripts', exist_ok=True)
        
        def process_single(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            """Process a single transcript."""
            url = item.get("factbase_url", "")
            if not url:
                return None
            
            # Determine expected file path to check for existing
            record_type = item.get("record_type", "Uncategorized").lower()
            date_str = item.get("date", "")
            title = item.get("record_title", "")
            
            if date_str and title:
                # Clean title for filename
                clean_title = re.sub(r'[^\w\s-]', '', title)
                clean_title = re.sub(r'[\s]+', '_', clean_title).lower()
                potential_path = f"{data_dir}/raw-transcripts/{record_type}/{date_str}_{clean_title}.txt"
                
                if skip_existing and os.path.exists(potential_path):
                    return None
            
            # Fetch the full transcript content
            content = self.fetch_transcript_content(url)
            if not content:
                return None
            
            # Use API metadata for category and date (more reliable than HTML parsing)
            category = record_type or content.get("category", "uncategorized").lower()
            category_dir = f"{data_dir}/raw-transcripts/{category}"
            os.makedirs(category_dir, exist_ok=True)

            # Use API date (already YYYY-MM-DD), fall back to HTML-parsed date
            if date_str:
                date = date_str  # already set from item.get("date") above
            else:
                date = content.get("date") or ""
                if date:
                    try:
                        parsed_date = datetime.strptime(date, '%B %d, %Y')
                        date_str = parsed_date.strftime('%Y-%m-%d')
                    except ValueError:
                        date_str = date.replace(' ', '-')
                else:
                    date_str = "unknown"

            # Use API title (more reliable than HTML parsing)
            title = title or content.get("title", "untitled")
            
            filename = re.sub(r'[^\w\s-]', '', title)
            filename = re.sub(r'[\s]+', '_', filename).lower()
            filepath = f"{category_dir}/{date_str}_{filename}.txt"
            
            # Save transcript
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content["text"])
            
            return {
                "date": date,
                "title": title,
                "category": category,
                "url": url,
                "filename": filepath
            }
        
        # Process in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(process_single, item): item
                for item in transcripts
            }
            
            with tqdm(total=len(transcripts), desc="Processing transcripts") as pbar:
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            processed.append(result)
                    except Exception as e:
                        item = futures[future]
                        print(f"\nError processing {item.get('factbase_url', 'unknown')}: {str(e)}")
                    pbar.update(1)
        
        print(f"\nProcessed {len(processed)} new transcripts")
        return processed


def get_new_transcripts(
    since_date: datetime,
    person: str = "trump",
    data_dir: str = "data",
    max_workers: int = 12
) -> List[Dict[str, Any]]:
    """
    Convenience function to get and process all new transcripts since a date.
    
    Args:
        since_date: Only process transcripts on or after this date
        person: Person to get transcripts for (e.g., "trump")
        data_dir: Directory to save transcripts
        max_workers: Number of parallel workers
        
    Returns:
        List of processed transcript metadata
        
    Example:
        from datetime import datetime
        from src.rollcall.factbase_api import get_new_transcripts
        
        # Get all Trump transcripts from 2026
        results = get_new_transcripts(
            since_date=datetime(2026, 1, 1),
            person="trump",
            data_dir="/path/to/data"
        )
    """
    api = FactbaseAPI(person=person, max_workers=max_workers)
    
    print(f"Fetching transcript list for {person} since {since_date.strftime('%B %d, %Y')}...")
    transcripts = api.get_transcripts_since(since_date)
    print(f"Found {len(transcripts)} transcripts")
    
    if transcripts:
        return api.process_transcripts(transcripts, data_dir=data_dir)
    return []


def save_transcript_urls(
    output_path: str,
    since_date: Optional[datetime] = None,
    person: str = "trump"
) -> List[str]:
    """
    Save transcript URLs to a file (compatible with existing pipeline).
    
    Args:
        output_path: Path to save the URLs file
        since_date: Only include transcripts on or after this date
        person: Person to get transcripts for
        
    Returns:
        List of URLs that were saved
    """
    api = FactbaseAPI(person=person)
    urls = api.get_all_transcript_urls(since_date=since_date)
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        for url in urls:
            f.write(f"{url}\n")
    
    print(f"Saved {len(urls)} URLs to {output_path}")
    return urls


if __name__ == "__main__":
    # Example usage: get transcripts from the last week
    from datetime import timedelta
    
    since = datetime.now() - timedelta(days=7)
    results = get_new_transcripts(since_date=since, data_dir="data")
    
    print(f"\nFetched {len(results)} transcripts:")
    for r in results[:5]:
        print(f"  - {r.get('title', 'Unknown')}")
