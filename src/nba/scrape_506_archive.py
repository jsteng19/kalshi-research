"""
506 Sports Scraper for NBA Games

Scrapes 506sports.com week pages (nba.php?yr=YYYYYYY&wk=N) directly using
requests + BeautifulSoup. This avoids the archived wiki, which is CAPTCHA-
blocked for programmatic access.
"""

import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd

from parse_506_archive import (
    parse_date_line,
    games_to_dataframe,
    get_team_code,
)

_SESSION = None


def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
        _SESSION.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })
    return _SESSION


def _current_season() -> str:
    """Return the season string for the current or most recent NBA season, e.g. '202526'."""
    now = datetime.now()
    # NBA season starts in October; season YYYY-YY+1 is labelled as YYYY(YY+1)
    start_year = now.year if now.month >= 10 else now.year - 1
    return f"{start_year}{str(start_year + 1)[-2:]}"


def _parse_week_page(html: str, year: int) -> List[Dict]:
    """Parse games from a 506sports.com week page."""
    soup = BeautifulSoup(html, "html.parser")
    games = []
    current_date = None

    for el in soup.find_all(["h2", "h3"]):
        text = el.get_text(strip=True)
        date = parse_date_line(text, year)
        if date:
            current_date = date

        # Games come after the date h2/h3, in sibling div#cgame elements.
        # Walk the next siblings of this element.
        for sib in el.next_siblings:
            if not hasattr(sib, "get"):
                continue
            tag = getattr(sib, "name", None)
            if tag in ("h2", "h3"):
                break
            if sib.get("id") != "cgame":
                continue
            if current_date is None:
                continue

            matchup_el = sib.find(id="cmatchup")
            time_el = sib.find(id="ctime")
            network_el = sib.find(id="cntwk")
            anncrs_el = sib.find(id="canncrs")

            matchup = matchup_el.get_text(strip=True) if matchup_el else ""
            network = network_el.get_text(strip=True) if network_el else ""
            time_str = time_el.get_text(strip=True) if time_el else ""
            anncrs_str = anncrs_el.get_text(strip=True) if anncrs_el else ""

            # Strip "play-in:" / "Game N:" prefixes
            matchup = re.sub(r"^(?:play-in|game\s+\d+):\s*", "", matchup, flags=re.I).strip()

            # Parse "Away @ Home"
            m = re.match(r"^(.+?)\s*@\s*(.+)$", matchup)
            if not m:
                continue
            away_name = m.group(1).strip()
            home_name = m.group(2).strip()
            away_code = get_team_code(away_name)
            home_code = get_team_code(home_name)
            if not away_code or not home_code:
                continue

            # Skip games with no announced crew
            anncrs_str = anncrs_str.strip()
            if not anncrs_str or anncrs_str.startswith("(local"):
                continue

            announcers = [a.strip() for a in anncrs_str.split(",") if a.strip()]

            games.append({
                "date": current_date,
                "away_team": away_name,
                "home_team": home_name,
                "away_code": away_code,
                "home_code": home_code,
                "network": network,
                "time": time_str,
                "play_by_play": announcers[0] if announcers else None,
                "color_commentator": announcers[1] if len(announcers) > 1 else None,
                "other_announcers": announcers[2:],
            })

    return games


def scrape_506_nba_archive(
    season: Optional[str] = None,
    since_date: Optional[datetime] = None,
    start_year: int = 2025,
) -> pd.DataFrame:
    """
    Scrape all NBA week pages from 506sports.com for the given season.

    Args:
        season:     Season string like "202526". Auto-detected if None.
        since_date: Only return games on or after this date (speeds up incremental updates).
        start_year: The year the season starts (October).

    Returns:
        DataFrame with ticker, date, teams, announcers.
    """
    if season is None:
        season = _current_season()

    session = _get_session()
    all_games: List[Dict] = []

    # Regular season weeks (stop early when pages become empty)
    for wk in [str(i) for i in range(1, 31)]:
        url = f"https://506sports.com/nba.php?yr={season}&wk={wk}"
        try:
            resp = session.get(url, timeout=20)
            if resp.status_code != 200:
                break
        except Exception as e:
            print(f"  Warning: failed to fetch week {wk}: {e}")
            break
        games = _parse_week_page(resp.text, start_year)
        if not games and int(wk) > 5:
            break
        all_games.extend(games)

    # Playoff weeks — always attempt all four rounds
    for wk in ["p1", "p2", "p3", "p4"]:
        url = f"https://506sports.com/nba.php?yr={season}&wk={wk}"
        try:
            resp = session.get(url, timeout=20)
            if resp.status_code != 200:
                break
        except Exception as e:
            print(f"  Warning: failed to fetch playoff week {wk}: {e}")
            break
        games = _parse_week_page(resp.text, start_year)
        if not games:
            break
        all_games.extend(games)

    df = games_to_dataframe(all_games)
    if since_date is not None and not df.empty:
        df = df[pd.to_datetime(df["date"]) >= pd.Timestamp(since_date)]
    return df


def save_games_csv(df: pd.DataFrame, filepath: str) -> None:
    """Save games DataFrame to CSV."""
    df.to_csv(filepath, index=False)
    print(f"Saved {len(df)} games to {filepath}")


if __name__ == "__main__":
    import sys

    output_path = "data/nba/game_announcers.csv"
    if len(sys.argv) > 1:
        output_path = sys.argv[1]

    print("Scraping 506sports.com week pages...")
    df = scrape_506_nba_archive()

    print(f"\nFound {len(df)} games")
    if not df.empty:
        print("\nSample data:")
        print(df.head(10).to_string())
        print(f"\nUnique play-by-play announcers:")
        print(df["play_by_play"].value_counts())
        save_games_csv(df, output_path)
