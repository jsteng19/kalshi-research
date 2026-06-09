"""
ICDB Basketball Scraper

Scrapes basketball.icdb.tv for commentator data including:
- Match listings for a specific commentator
- Co-commentator information for each match
- Game details (date, teams, competition, channel)
"""

import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re
import time
from typing import Optional, List, Dict
from dataclasses import dataclass
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class CommentatorInfo:
    """Information about a commentator entry"""
    name: str
    role: str  # Main Commentator, Co-commentator, Pundit, etc.
    language: str
    person_id: Optional[int] = None


@dataclass 
class MatchInfo:
    """Information about a match"""
    match_id: int
    match_url: str
    teams: str  # "Team A @ Team B"
    home_team: str
    away_team: str
    competition: str
    channel: str
    timestamp: int
    date: datetime
    date_str: str
    commentators: List[CommentatorInfo] = None


class ICDBScraper:
    """Scraper for basketball.icdb.tv"""
    
    BASE_URL = "https://basketball.icdb.tv"
    
    def __init__(self, delay: float = 0.5):
        """
        Initialize the scraper.
        
        Args:
            delay: Delay between requests in seconds (be nice to the server)
        """
        self.scraper = cloudscraper.create_scraper()
        self.delay = delay
        self._last_request_time = 0
    
    def _request(self, url: str, method: str = "GET", data: dict = None) -> BeautifulSoup:
        """Make a request with rate limiting."""
        # Rate limiting
        elapsed = time.time() - self._last_request_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        
        if method == "GET":
            response = self.scraper.get(url)
        else:
            response = self.scraper.post(url, data=data)
        
        self._last_request_time = time.time()
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")
    
    def search_commentator(self, name: str) -> List[Dict]:
        """
        Search for a commentator by name.
        
        Args:
            name: Name to search for
            
        Returns:
            List of matching commentators with id and label
        """
        url = f"{self.BASE_URL}/fetchcomms.php"
        response = self.scraper.post(url, data={"search": name})
        return response.json()
    
    def search_competition(self, name: str) -> List[Dict]:
        """
        Search for a competition by name.
        
        Args:
            name: Competition name to search for
            
        Returns:
            List of matching competitions with id and label
        """
        url = f"{self.BASE_URL}/fetchcompsearch.php"
        response = self.scraper.post(url, data={"search": name})
        return response.json()
    
    def get_commentator_matches(
        self,
        commentator_id: int,
        competition_id: Optional[int] = None,
        limit: Optional[int] = None,
        max_pages: int = 20
    ) -> List[MatchInfo]:
        """
        Get all matches for a commentator.
        
        Note: ICDB pagination via URL params doesn't seem to work consistently.
        This method fetches the first page and deduplicates results.
        
        Args:
            commentator_id: The commentator's ID from search
            competition_id: Optional competition ID to filter by
            limit: Maximum number of matches to return
            max_pages: Maximum number of pages to attempt (will stop early if duplicates detected)
            
        Returns:
            List of MatchInfo objects (deduplicated)
        """
        matches = []
        seen_match_ids = set()
        page = 1
        
        while page <= max_pages:
            # Build URL with filters and pagination
            url = f"{self.BASE_URL}/person/{commentator_id}"
            params = []
            if competition_id:
                params.append(f"comp={competition_id}")
            if page > 1:
                params.append(f"page={page}")
            if params:
                url += "?" + "&".join(params)
            
            logger.info(f"Fetching page {page}: {url}")
            soup = self._request(url)
            
            # Find the results list
            results_body = soup.find("div", class_="results-list-body")
            if not results_body:
                logger.warning(f"No results found on page {page}")
                break
            
            match_rows = results_body.find_all("div", class_="match-row")
            
            if not match_rows:
                logger.info(f"No more matches found on page {page}")
                break
            
            new_matches_count = 0
            duplicate_count = 0
            
            for row in match_rows:
                if limit and len(matches) >= limit:
                    logger.info(f"Reached limit of {limit} matches")
                    return matches
                    
                match_info = self._parse_match_row(row)
                if match_info:
                    # Check for duplicates by match_id
                    if match_info.match_id in seen_match_ids:
                        duplicate_count += 1
                        continue
                    
                    seen_match_ids.add(match_info.match_id)
                    matches.append(match_info)
                    new_matches_count += 1
            
            logger.info(f"Page {page}: {new_matches_count} new matches, {duplicate_count} duplicates (total unique: {len(matches)})")
            
            # If all matches on this page were duplicates, pagination isn't working
            if new_matches_count == 0 and duplicate_count > 0:
                logger.info(f"Stopping: page {page} returned only duplicates (pagination not supported)")
                break
            
            # If we got fewer than expected, we're probably on the last page
            if new_matches_count < 50 and duplicate_count == 0:
                logger.info(f"Last page reached (got {new_matches_count} < 50 new matches)")
                break
            
            page += 1
        
        logger.info(f"Total unique matches fetched: {len(matches)}")
        return matches
    
    def _parse_match_row(self, row) -> Optional[MatchInfo]:
        """Parse a match row from the results list."""
        try:
            # Get match link
            match_link = row.find("a", href=re.compile(r"/match/"))
            if not match_link:
                return None
            
            href = match_link.get("href", "")
            match_id_match = re.search(r"/match/(\d+)", href)
            if not match_id_match:
                return None
            
            match_id = int(match_id_match.group(1))
            teams = match_link.text.strip()
            
            # Parse teams
            if "@" in teams:
                parts = teams.split("@")
                away_team = parts[0].strip()
                home_team = parts[1].strip()
            else:
                away_team = teams
                home_team = ""
            
            # Get timestamp
            time_span = row.find("span", class_="local-time")
            timestamp = 0
            date = None
            date_str = ""
            if time_span and time_span.get("data-timestamp"):
                timestamp = int(time_span.get("data-timestamp"))
                date = datetime.fromtimestamp(timestamp)
                date_str = date.strftime("%Y-%m-%d %H:%M")
            
            # Get competition
            comp_div = row.find("div", class_="col-comp")
            competition = comp_div.text.strip() if comp_div else ""
            
            # Get channel
            chan_div = row.find("div", class_="col-chan")
            channel = chan_div.text.strip() if chan_div else ""
            
            # Build full URL
            match_url = f"{self.BASE_URL}/match/{match_id}"
            
            return MatchInfo(
                match_id=match_id,
                match_url=match_url,
                teams=teams,
                home_team=home_team,
                away_team=away_team,
                competition=competition,
                channel=channel,
                timestamp=timestamp,
                date=date,
                date_str=date_str
            )
        except Exception as e:
            logger.error(f"Error parsing match row: {e}")
            return None
    
    def get_match_commentators(self, match_id: int, deduplicate: bool = True) -> List[CommentatorInfo]:
        """
        Get all commentators for a specific match.
        
        Args:
            match_id: The match ID
            deduplicate: Whether to remove duplicate entries (default: True)
            
        Returns:
            List of CommentatorInfo objects
        """
        url = f"{self.BASE_URL}/match/{match_id}"
        soup = self._request(url)
        
        commentators = []
        seen = set()  # For deduplication
        
        # Find all role rows
        role_rows = soup.find_all("div", class_="role-row")
        
        for row in role_rows:
            try:
                # Get role
                role_div = row.find("div", class_="role-col-role")
                if not role_div:
                    continue
                role = role_div.text.strip()
                
                # Get name and person ID
                name_div = row.find("div", class_="role-col-name")
                if not name_div:
                    continue
                
                name_link = name_div.find("a")
                if name_link:
                    name = name_link.text.strip()
                    href = name_link.get("href", "")
                    person_id_match = re.search(r"/(\d+)/", href)
                    person_id = int(person_id_match.group(1)) if person_id_match else None
                else:
                    name = name_div.text.strip()
                    person_id = None
                
                # Get language
                lang_div = row.find("div", class_="role-col-lang")
                language = lang_div.text.strip() if lang_div else "Unknown"
                
                # Deduplicate by (name, role, language)
                key = (name, role, language)
                if deduplicate and key in seen:
                    continue
                seen.add(key)
                
                commentators.append(CommentatorInfo(
                    name=name,
                    role=role,
                    language=language,
                    person_id=person_id
                ))
            except Exception as e:
                logger.error(f"Error parsing commentator row: {e}")
                continue
        
        return commentators
    
    def get_commentator_games_with_details(
        self,
        commentator_name: str,
        competition_name: str = "NBA Regular Season",
        language: str = "English",
        limit: Optional[int] = None,
        include_co_commentators: bool = True,
        max_pages: int = 20
    ) -> pd.DataFrame:
        """
        Get a full DataFrame of games for a commentator with all details.
        
        This is the main convenience method that combines searching,
        fetching matches, and optionally fetching co-commentator data.
        Now supports pagination to fetch all games (not just first 50).
        
        Args:
            commentator_name: Name of the commentator to search for
            competition_name: Competition to filter by (default: NBA Regular Season)
            language: Language to filter by (default: English)
            limit: Maximum number of matches to return (None = all)
            include_co_commentators: Whether to fetch co-commentator data (slower)
            max_pages: Maximum pages to fetch (default 20, ~1000 games max)
            
        Returns:
            DataFrame with columns: date, teams, home_team, away_team, competition,
            channel, main_commentator, co_commentator, analysts, etc.
        """
        # Search for commentator
        logger.info(f"Searching for commentator: {commentator_name}")
        comms = self.search_commentator(commentator_name)
        if not comms:
            raise ValueError(f"No commentator found matching '{commentator_name}'")
        
        # Use first match
        comm = comms[0]
        comm_id = int(comm["value"])
        comm_label = comm["label"]
        logger.info(f"Found commentator: {comm_label} (ID: {comm_id})")
        
        # Search for competition
        comp_id = None
        if competition_name:
            logger.info(f"Searching for competition: {competition_name}")
            comps = self.search_competition(competition_name)
            if comps:
                # Find exact match first
                for c in comps:
                    if c["label"].lower() == competition_name.lower():
                        comp_id = int(c["value"])
                        break
                if not comp_id:
                    comp_id = int(comps[0]["value"])
                logger.info(f"Using competition ID: {comp_id}")
        
        # Get matches (with pagination support)
        logger.info("Fetching match list...")
        matches = self.get_commentator_matches(comm_id, comp_id, limit, max_pages=max_pages)
        logger.info(f"Found {len(matches)} matches total")
        
        # Build result data
        rows = []
        
        for i, match in enumerate(matches):
            logger.info(f"Processing match {i+1}/{len(matches)}: {match.teams}")
            
            row = {
                "date": match.date_str,
                "timestamp": match.timestamp,
                "teams": match.teams,
                "home_team": match.home_team,
                "away_team": match.away_team,
                "competition": match.competition,
                "channel": match.channel,
                "match_id": match.match_id,
                "match_url": match.match_url,
            }
            
            if include_co_commentators:
                # Fetch commentator details (deduplicated)
                commentators = self.get_match_commentators(match.match_id, deduplicate=True)
                
                # Filter by language
                lang_comms = [c for c in commentators if c.language.lower() == language.lower()]
                
                # Find main commentator and co-commentator
                main_comm = next((c for c in lang_comms if c.role == "Main Commentator"), None)
                co_comms = [c for c in lang_comms if c.role == "Co-commentator"]
                analysts = [c for c in lang_comms if c.role in ("Pundit", "Analyst")]
                presenters = [c for c in lang_comms if c.role == "Presenter"]
                reporters = [c for c in lang_comms if c.role == "Reporter"]
                
                row["main_commentator"] = main_comm.name if main_comm else ""
                row["co_commentator"] = co_comms[0].name if co_comms else ""
                row["co_commentators"] = ", ".join([c.name for c in co_comms]) if co_comms else ""
                row["analysts"] = ", ".join([a.name for a in analysts]) if analysts else ""
                row["presenters"] = ", ".join([p.name for p in presenters]) if presenters else ""
                row["reporters"] = ", ".join([r.name for r in reporters]) if reporters else ""
                row["all_commentators"] = ", ".join([f"{c.name} ({c.role})" for c in lang_comms])
            
            rows.append(row)
        
        df = pd.DataFrame(rows)
        
        # Sort by date descending
        if "timestamp" in df.columns:
            df = df.sort_values("timestamp", ascending=False)
        
        return df


class ICDBBrowserScraper:
    """
    Browser-based scraper for basketball.icdb.tv using Playwright.
    
    This scraper uses browser automation to scroll and load more results,
    allowing it to fetch more than the 50 games returned by the basic HTTP scraper.
    """
    
    def __init__(self, headless: bool = True):
        """
        Initialize the browser scraper.
        
        Args:
            headless: Whether to run browser in headless mode (default True)
        """
        self.headless = headless
        self._http_scraper = ICDBScraper(delay=0.3)  # For fetching match details
    
    def get_commentator_matches_browser(
        self,
        commentator_id: int,
        competition_id: Optional[int] = None,
        max_games: int = 500,
        scroll_pause: float = 1.0
    ) -> List[MatchInfo]:
        """
        Get matches for a commentator using browser automation to scroll and load more.
        
        Args:
            commentator_id: The commentator's ID
            competition_id: Optional competition ID to filter by
            max_games: Maximum number of games to fetch (default 500)
            scroll_pause: Seconds to wait after each scroll (default 1.0)
            
        Returns:
            List of MatchInfo objects
        """
        from playwright.sync_api import sync_playwright
        
        # Build URL
        url = f"https://basketball.icdb.tv/person/{commentator_id}"
        if competition_id:
            url += f"?comp={competition_id}"
        
        logger.info(f"Opening browser for: {url}")
        
        matches = []
        seen_match_ids = set()
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            page = context.new_page()
            
            # Go to the page with longer timeout
            page.goto(url, wait_until='domcontentloaded', timeout=60000)
            
            # Wait for Cloudflare challenge if present, then wait for results
            time.sleep(3)  # Give time for any JS challenges
            
            # Wait for results to load
            try:
                page.wait_for_selector('.match-row', timeout=30000)
            except:
                # If still blocked, try waiting more
                logger.warning("Waiting longer for page to load...")
                time.sleep(5)
                page.wait_for_selector('.match-row', timeout=30000)
            
            page_num = 1
            
            while len(matches) < max_games:
                # Get current match rows
                match_rows = page.query_selector_all('.match-row')
                
                # Parse matches from current page
                new_on_page = 0
                for row in match_rows:
                    match_info = self._parse_match_row_element(row)
                    if match_info and match_info.match_id not in seen_match_ids:
                        seen_match_ids.add(match_info.match_id)
                        matches.append(match_info)
                        new_on_page += 1
                        
                        if len(matches) >= max_games:
                            break
                
                logger.info(f"Page {page_num}: {new_on_page} new matches (total: {len(matches)})")
                
                if len(matches) >= max_games:
                    break
                
                # Look for "Next" button to go to next page
                next_button = page.query_selector('button:has-text("Next"), a:has-text("Next")')
                if not next_button:
                    logger.info("No 'Next' button found, reached last page")
                    break
                
                # Check if Next button is disabled or hidden
                is_disabled = next_button.get_attribute('disabled')
                is_hidden = 'hidden' in (next_button.get_attribute('class') or '')
                
                if is_disabled or is_hidden:
                    logger.info("'Next' button is disabled, reached last page")
                    break
                
                # Click Next button
                try:
                    next_button.click()
                    time.sleep(scroll_pause)
                    page.wait_for_selector('.match-row', timeout=10000)
                    page_num += 1
                except Exception as e:
                    logger.error(f"Error clicking Next: {e}")
                    break
            
            browser.close()
        
        logger.info(f"Total unique matches fetched: {len(matches)}")
        return matches
    
    def _parse_match_row_element(self, row) -> Optional[MatchInfo]:
        """Parse a match row from a Playwright element."""
        try:
            # Get match link
            match_link = row.query_selector('a[href*="/match/"]')
            if not match_link:
                return None
            
            href = match_link.get_attribute('href') or ''
            match_id_match = re.search(r'/match/(\d+)', href)
            if not match_id_match:
                return None
            
            match_id = int(match_id_match.group(1))
            teams = match_link.inner_text().strip()
            
            # Parse teams
            if '@' in teams:
                parts = teams.split('@')
                away_team = parts[0].strip()
                home_team = parts[1].strip()
            else:
                away_team = teams
                home_team = ''
            
            # Get timestamp
            time_span = row.query_selector('.local-time')
            timestamp = 0
            date = None
            date_str = ''
            if time_span:
                ts = time_span.get_attribute('data-timestamp')
                if ts:
                    timestamp = int(ts)
                    date = datetime.fromtimestamp(timestamp)
                    date_str = date.strftime('%Y-%m-%d %H:%M')
            
            # Get competition
            comp_div = row.query_selector('.col-comp')
            competition = comp_div.inner_text().strip() if comp_div else ''
            
            # Get channel
            chan_div = row.query_selector('.col-chan')
            channel = chan_div.inner_text().strip() if chan_div else ''
            
            return MatchInfo(
                match_id=match_id,
                match_url=f"https://basketball.icdb.tv/match/{match_id}",
                teams=teams,
                home_team=home_team,
                away_team=away_team,
                competition=competition,
                channel=channel,
                timestamp=timestamp,
                date=date,
                date_str=date_str
            )
        except Exception as e:
            logger.error(f"Error parsing match row: {e}")
            return None
    
    def get_commentator_games_with_details(
        self,
        commentator_name: str,
        competition_name: str = "NBA Regular Season",
        language: str = "English",
        max_games: int = 500,
        include_co_commentators: bool = True
    ) -> pd.DataFrame:
        """
        Get games for a commentator using browser automation.
        
        Args:
            commentator_name: Name of the commentator
            competition_name: Competition to filter by
            language: Language to filter by
            max_games: Maximum games to fetch (default 500)
            include_co_commentators: Whether to fetch co-commentator details
            
        Returns:
            DataFrame with game and commentator details
        """
        # Search for commentator using HTTP scraper
        logger.info(f"Searching for commentator: {commentator_name}")
        comms = self._http_scraper.search_commentator(commentator_name)
        if not comms:
            raise ValueError(f"No commentator found matching '{commentator_name}'")
        
        comm = comms[0]
        comm_id = int(comm["value"])
        logger.info(f"Found commentator: {comm['label']} (ID: {comm_id})")
        
        # Search for competition
        comp_id = None
        if competition_name:
            logger.info(f"Searching for competition: {competition_name}")
            comps = self._http_scraper.search_competition(competition_name)
            if comps:
                for c in comps:
                    if c["label"].lower() == competition_name.lower():
                        comp_id = int(c["value"])
                        break
                if not comp_id:
                    comp_id = int(comps[0]["value"])
                logger.info(f"Using competition ID: {comp_id}")
        
        # Get matches using browser
        logger.info(f"Fetching matches with browser (max {max_games})...")
        matches = self.get_commentator_matches_browser(comm_id, comp_id, max_games)
        logger.info(f"Found {len(matches)} matches")
        
        # Build result data
        rows = []
        
        for i, match in enumerate(matches):
            logger.info(f"Processing match {i+1}/{len(matches)}: {match.teams}")
            
            row = {
                "date": match.date_str,
                "timestamp": match.timestamp,
                "teams": match.teams,
                "home_team": match.home_team,
                "away_team": match.away_team,
                "competition": match.competition,
                "channel": match.channel,
                "match_id": match.match_id,
                "match_url": match.match_url,
            }
            
            if include_co_commentators:
                # Use HTTP scraper for match details (faster than browser)
                commentators = self._http_scraper.get_match_commentators(match.match_id, deduplicate=True)
                
                # Filter by language
                lang_comms = [c for c in commentators if c.language.lower() == language.lower()]
                
                # Find main commentator and co-commentator
                main_comm = next((c for c in lang_comms if c.role == "Main Commentator"), None)
                co_comms = [c for c in lang_comms if c.role == "Co-commentator"]
                analysts = [c for c in lang_comms if c.role in ("Pundit", "Analyst")]
                presenters = [c for c in lang_comms if c.role == "Presenter"]
                reporters = [c for c in lang_comms if c.role == "Reporter"]
                
                row["main_commentator"] = main_comm.name if main_comm else ""
                row["co_commentator"] = co_comms[0].name if co_comms else ""
                row["co_commentators"] = ", ".join([c.name for c in co_comms]) if co_comms else ""
                row["analysts"] = ", ".join([a.name for a in analysts]) if analysts else ""
                row["presenters"] = ", ".join([p.name for p in presenters]) if presenters else ""
                row["reporters"] = ", ".join([r.name for r in reporters]) if reporters else ""
                row["all_commentators"] = ", ".join([f"{c.name} ({c.role})" for c in lang_comms])
            
            rows.append(row)
        
        df = pd.DataFrame(rows)
        
        # Sort by date descending
        if "timestamp" in df.columns and len(df) > 0:
            df = df.sort_values("timestamp", ascending=False)
        
        return df


def main():
    """Example usage"""
    scraper = ICDBScraper(delay=0.5)
    
    # Example: Get Mike Breen's NBA Regular Season games
    print("=" * 60)
    print("ICDB Basketball Scraper Demo")
    print("=" * 60)
    
    # Quick demo - just get match list without co-commentator details
    print("\n1. Searching for commentator 'Mike Breen'...")
    comms = scraper.search_commentator("Mike Breen")
    print(f"   Found: {comms}")
    
    print("\n2. Searching for competition 'NBA Regular'...")
    comps = scraper.search_competition("NBA Regular")
    print(f"   Found: {comps}")
    
    # Get matches
    print("\n3. Getting match list (first 5 games)...")
    matches = scraper.get_commentator_matches(
        commentator_id=4,  # Mike Breen
        competition_id=1,  # NBA Regular Season
        limit=5
    )
    
    print(f"   Found {len(matches)} matches:")
    for m in matches:
        print(f"     {m.date_str}: {m.teams}")
        print(f"       Channel: {m.channel}")
    
    # Get detailed commentator info for first match
    if matches:
        print(f"\n4. Getting commentator details for: {matches[0].teams}")
        commentators = scraper.get_match_commentators(matches[0].match_id)
        english_comms = [c for c in commentators if c.language == "English"]
        for c in english_comms:
            print(f"     {c.role}: {c.name}")
    
    # Full example with DataFrame
    print("\n5. Building full DataFrame with co-commentators (first 5 games)...")
    df = scraper.get_commentator_games_with_details(
        commentator_name="Mike Breen",
        competition_name="NBA Regular Season",
        language="English",
        limit=5,
        include_co_commentators=True
    )
    
    # Show key columns
    print("\n   Result DataFrame:")
    key_cols = ["date", "teams", "main_commentator", "co_commentator", "channel"]
    print(df[key_cols].to_string(index=False))
    
    print("\n" + "=" * 60)
    print("Demo complete! Full DataFrame columns available:")
    print(f"   {list(df.columns)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
