#!/usr/bin/env python3
"""
506 Sports Parser for NCAAB Games

Parses 506 Sports live page (mcbb.php) and archive wiki to extract
college basketball broadcast information and map to Kalshi tickers.

Archive format (MediaWiki paragraphs):
    Saturday, November 8
    Duke-North Carolina, ESPN 7:00 p.m. - Dan Shulman, Jay Bilas, ...

Live page format (structured divs):
    <h3>SATURDAY, FEBRUARY 15</h3>
    <div id="cgame">
        <div id="cmatchup">Duke @ North Carolina</div>
        <div id="ctime">7:00 PM ET</div>
        <div id="cntwk">ESPN</div>
        <div id="canncrs">Dan Shulman, Jay Bilas</div>
    </div>
"""

import re
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.ncaab.ncaab_config import SERIES_TICKER, get_team_code, _strip_ranking, TEAM_CODES


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

def parse_date_line(line: str, start_year: int = 2025) -> Optional[datetime]:
    """
    Parse a date line like "Saturday, November 8" or "SATURDAY, FEBRUARY 15".
    NCAAB season starts in November, so months Nov-Dec use start_year,
    Jan-Apr use start_year+1.
    """
    month_map = {
        'january': 1, 'jan': 1,
        'february': 2, 'feb': 2,
        'march': 3, 'mar': 3,
        'april': 4, 'apr': 4,
        'may': 5,
        'june': 6, 'jun': 6,
        'july': 7, 'jul': 7,
        'august': 8, 'aug': 8,
        'september': 9, 'sep': 9, 'sept': 9,
        'october': 10, 'oct': 10,
        'november': 11, 'nov': 11,
        'december': 12, 'dec': 12,
    }

    # Formats like:
    # - Monday, November 8
    # - Tue. 11/11 (Veterans Day)
    # - Saturday 11/8
    text = line.strip()
    match = re.search(
        r'(?:Mon(?:day)?|Tue(?:s|sday)?|Wed(?:nesday)?|Thu(?:rs|rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)\.?,?\s+'
        r'([A-Za-z]+)\s+(\d{1,2})',
        text,
        re.IGNORECASE,
    )
    if match:
        month_token = match.group(1).lower()
        day = int(match.group(2))
        month = month_map.get(month_token)
        if month is not None:
            year = start_year if month >= 10 else start_year + 1
            return datetime(year, month, day)

    match = re.search(
        r'(?:Mon(?:day)?|Tue(?:s|sday)?|Wed(?:nesday)?|Thu(?:rs|rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)\.?,?\s+'
        r'(\d{1,2})/(\d{1,2})',
        text,
        re.IGNORECASE,
    )
    if not match:
        return None
    month = int(match.group(1))
    day = int(match.group(2))
    # NCAAB: Nov-Dec = start_year, Jan-May = start_year+1
    year = start_year if month >= 10 else start_year + 1
    return datetime(year, month, day)


# ---------------------------------------------------------------------------
# Game line parsing
# ---------------------------------------------------------------------------

def _clean_team_name(name: str) -> str:
    """Normalize team display names from 506 lines for code lookup."""
    s = name.strip()
    s = re.sub(r'^\[\d+\]\s*', '', s)         # bracketed rank seed prefix, e.g. [1]
    s = re.sub(r'^\d+\s+', '', s)             # numeric rank prefix, e.g. 5
    s = re.sub(r'\s*\[[^\]]+\]\s*', ' ', s)   # bracket notes, e.g. [IN], [OH]
    s = re.sub(r'\s+', ' ', s).strip()
    return _strip_ranking(s)


def _parse_matchup(matchup: str) -> Optional[tuple[str, str]]:
    """
    Parse matchup text into (away_team, home_team).
    Supports formats:
    - Team A @ Team B
    - Team A at Team B
    - Team A vs. Team B
    - TeamA-TeamB
    """
    text = matchup.strip()
    text = re.sub(r'^\[\d+\]\s*', '', text)

    for sep_pattern in [r'\s+@\s+', r'\s+at\s+', r'\s+vs\.?\s+', r'\s*-\s*']:
        parts = re.split(sep_pattern, text, maxsplit=1, flags=re.IGNORECASE)
        if len(parts) == 2:
            away = _clean_team_name(parts[0])
            home = _clean_team_name(parts[1])
            if away and home:
                return away, home
    return None


def parse_game_line(line: str) -> Optional[Dict]:
    """
    Parse a game line like:
    "Duke-North Carolina, ESPN 7:00 p.m. - Dan Shulman, Jay Bilas"

    Returns dict with away_team, home_team, network, time, play_by_play,
    color_commentator, other_announcers.
    """
    line = " ".join(line.strip().split())
    line = re.sub(r'^\[\d+\]\s*', '', line)

    # Newer archive format:
    # "Away @ Home, 7:00, ESPN - Announcer1, Announcer2"
    modern_pattern = (
        r'^(?P<matchup>.+?),\s*'
        r'(?P<time>\d{1,2}:\d{2}(?:\s*[APap]\.?M\.?)?(?:\s*ET)?)\s*,\s*'
        r'(?P<network>[^-]+?)'
        r'(?:\s*-\s*(?P<announcers>.+))?$'
    )
    m = re.match(modern_pattern, line, re.IGNORECASE)
    if not m:
        # Alternate ordering seen on some lines:
        # "Away @ Home, Network, 8:00 - Announcer1, Announcer2"
        modern_pattern_alt = (
            r'^(?P<matchup>.+?),\s*'
            r'(?P<network>[^,]+?)\s*,\s*'
            r'(?P<time>\d{1,2}:\d{2}(?:\s*[APap]\.?M\.?)?(?:\s*ET)?)'
            r'(?:\s*-\s*(?P<announcers>.+))?$'
        )
        m = re.match(modern_pattern_alt, line, re.IGNORECASE)
    away_team, home_team, network, time_str, announcers_str = None, None, "", "", ""
    if m:
        teams = _parse_matchup(m.group("matchup"))
        if not teams:
            return None
        away_team, home_team = teams
        network = (m.group("network") or "").strip()
        time_str = (m.group("time") or "").strip()
        announcers_str = (m.group("announcers") or "").strip()
    else:
        # Older archive format:
        # "Away-Home, ESPN 7:00 p.m. - Announcer1, Announcer2"
        legacy_pattern = (
            r'^([A-Za-z0-9&\'. ]+)-([A-Za-z0-9&\'. ]+?)(?:\s*\([^)]+\))?,\s*'
            r'(.+?)\s+(\d{1,2}:\d{2}\s*[pa]\.?m\.?)\s*-\s*(.+)$'
        )
        match = re.match(legacy_pattern, line, re.IGNORECASE)
        if not match:
            return None
        away_team = _clean_team_name(match.group(1))
        home_team = _clean_team_name(match.group(2))
        network = match.group(3).strip()
        time_str = match.group(4).strip()
        announcers_str = match.group(5).strip()

    # Clean announcer notes
    announcers_str = re.sub(r'\([^)]*\)', '', announcers_str)
    announcers = [a.strip() for a in announcers_str.split(',') if a.strip()]

    return {
        'away_team': away_team,
        'home_team': home_team,
        'away_code': get_team_code(away_team),
        'home_code': get_team_code(home_team),
        'network': network,
        'time': time_str,
        'play_by_play': announcers[0] if len(announcers) > 0 else None,
        'color_commentator': announcers[1] if len(announcers) > 1 else None,
        'other_announcers': announcers[2:] if len(announcers) > 2 else [],
    }


# ---------------------------------------------------------------------------
# Ticker generation
# ---------------------------------------------------------------------------

_MONTH_NAMES = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
                'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']


def generate_kalshi_ticker(date: datetime, away_code: str, home_code: str) -> str:
    """
    Generate a Kalshi event ticker.
    Format: KXNCAABMENTION-{YY}{MON}{DD}{AWAY}{HOME}
    """
    date_str = date.strftime('%y%b%d').upper()
    return f"{SERIES_TICKER}-{date_str}{away_code}{home_code}"


def games_to_dataframe(games: List[Dict]) -> pd.DataFrame:
    """Convert list of games to DataFrame with Kalshi tickers."""
    records = []
    for game in games:
        if game.get('away_code') and game.get('home_code'):
            ticker = generate_kalshi_ticker(
                game['date'], game['away_code'], game['home_code']
            )
            records.append({
                'ticker': ticker,
                'date': game['date'],
                'away_team': game['away_team'],
                'home_team': game['home_team'],
                'away_code': game['away_code'],
                'home_code': game['home_code'],
                'network': game['network'],
                'time': game['time'],
                'play_by_play': game['play_by_play'],
                'color_commentator': game['color_commentator'],
            })
    df = pd.DataFrame(records)
    if not df.empty:
        df = df.sort_values('date')
    return df


# ---------------------------------------------------------------------------
# Archive scraper (MediaWiki format)
# ---------------------------------------------------------------------------

def scrape_506_archive(
    url: str = "https://archive.506sports.com/wiki/2025-26_College_Basketball_Season",
    start_year: int = 2025,
) -> pd.DataFrame:
    """
    Scrape the 506 Sports Archive NCAAB page.
    Returns DataFrame with ticker, date, teams, and announcers.
    """
    response = requests.get(url, timeout=30, headers={'User-Agent': 'Mozilla/5.0'})
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')
    content = soup.find('div', class_='mw-parser-output')
    if not content:
        content = soup.find('main') or soup.body

    games = []
    current_date = None

    for elem in content.find_all(['p', 'h2', 'h3']):
        text = elem.get_text(" ", strip=True)
        if not text:
            continue

        date = parse_date_line(text, start_year)
        if date:
            current_date = date
            continue

        if current_date and ',' in text and any(sep in text.lower() for sep in [' @ ', ' at ', ' vs', '-']):
            game = parse_game_line(text)
            if game:
                game['date'] = current_date
                games.append(game)

    return games_to_dataframe(games)


# ---------------------------------------------------------------------------
# Live page scraper (Playwright for JS-rendered content)
# ---------------------------------------------------------------------------

def scrape_506_live(
    url: str = "https://506sports.com/mcbb.php",
    start_year: int = 2025,
) -> pd.DataFrame:
    """
    Scrape the live 506sports.com/mcbb.php page using Playwright.
    Falls back gracefully if Playwright is not installed.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("  Playwright not installed; skipping live 506 scrape.")
        return pd.DataFrame()

    print(f"Scraping 506 Sports live page ({url})...")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            )
            page = context.new_page()
            page.goto(url, timeout=30000)
            page.wait_for_load_state("domcontentloaded")

            data = page.evaluate("""() => {
                const results = [];
                const children = document.querySelectorAll('h2, h3, [id="cgame"]');
                for (const el of children) {
                    if (el.tagName === 'H2' || el.tagName === 'H3') {
                        results.push({type: 'date', text: el.innerText.trim()});
                    } else if (el.id === 'cgame') {
                        const matchup = el.querySelector('[id="cmatchup"]');
                        const time = el.querySelector('[id="ctime"]');
                        const network = el.querySelector('[id="cntwk"]');
                        const announcers = el.querySelector('[id="canncrs"]');
                        results.push({
                            type: 'game',
                            matchup: matchup ? matchup.innerText.trim() : '',
                            time: time ? time.innerText.trim() : '',
                            network: network ? network.innerText.trim() : '',
                            announcers: announcers ? announcers.innerText.trim() : '',
                        });
                    }
                }
                return results;
            }""")
            browser.close()
    except Exception as e:
        print(f"  Live scrape failed: {e}")
        return pd.DataFrame()

    games = []
    current_date = None
    for item in data:
        if item["type"] == "date":
            date = parse_date_line(item["text"], start_year)
            if date:
                current_date = date
            continue

        if item["type"] != "game" or current_date is None:
            continue

        matchup = item.get("matchup", "")
        m = re.match(r'^(.+?)\s*@\s*(.+)$', matchup)
        if not m:
            continue
        away_team = _strip_ranking(m.group(1))
        home_team = _strip_ranking(m.group(2))
        away_code = get_team_code(away_team)
        home_code = get_team_code(home_team)
        if not away_code or not home_code:
            # Store with team names even if we can't resolve codes yet
            pass

        announcers_str = item.get("announcers", "")
        announcers = [a.strip() for a in announcers_str.split(",") if a.strip()]

        game = {
            "date": current_date,
            "away_team": away_team,
            "home_team": home_team,
            "away_code": away_code,
            "home_code": home_code,
            "network": item.get("network", "").strip(),
            "time": item.get("time", "").strip(),
            "play_by_play": announcers[0] if len(announcers) > 0 else None,
            "color_commentator": announcers[1] if len(announcers) > 1 else None,
            "other_announcers": announcers[2:] if len(announcers) > 2 else [],
        }
        games.append(game)

    df = games_to_dataframe(games)
    print(f"  Found {len(df)} games from live page")
    return df


# ---------------------------------------------------------------------------
# Text-based parser (for manual pasting / snapshots)
# ---------------------------------------------------------------------------

def parse_506_text(text: str, start_year: int = 2025) -> pd.DataFrame:
    """Parse raw text from the 506 archive page."""
    games = []
    current_date = None

    for line in text.strip().split('\n'):
        line = line.strip()
        if not line:
            continue

        date = parse_date_line(line, start_year)
        if date:
            current_date = date
            continue

        if current_date and '-' in line and ',' in line:
            game = parse_game_line(line)
            if game:
                game['date'] = current_date
                games.append(game)

    return games_to_dataframe(games)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == '--live':
        df = scrape_506_live()
    elif len(sys.argv) > 1:
        with open(sys.argv[1], 'r') as f:
            df = parse_506_text(f.read())
    else:
        print("Scraping 506 archive...")
        df = scrape_506_archive()

    print(f"\nFound {len(df)} games")
    if not df.empty:
        print(df.to_string())
        print(f"\nUnique play-by-play announcers:")
        print(df['play_by_play'].value_counts())
