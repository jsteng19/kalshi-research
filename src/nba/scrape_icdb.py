#!/usr/bin/env python3
"""
Scrape Internet Commentator Database (ICDB) for NBA announcer info.

https://basketball.icdb.tv

Can update existing CSVs in data/nba/icdb with new games since the latest
date in each file (--update or --update-dir).

Uses ICDBScraper from src/icdb_scraper.py for the update path so the output
CSV format matches the original full-detail format (match_id, commentator
details, etc.).
"""

import argparse
import os
import sys
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import time
import logging

# Make src/ importable so we can reuse ICDBScraper
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

logger = logging.getLogger(__name__)


def get_session() -> requests.Session:
    """Create a session with proper headers."""
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    })
    return session


def search_commentator_id(name: str, session: requests.Session = None) -> Optional[int]:
    """
    Resolve commentator ID by name via ICDB search API.

    Args:
        name: Commentator name (e.g., "Ian Eagle", "Michael Grady")
        session: Optional requests session

    Returns:
        Commentator ID if found, None otherwise
    """
    if session is None:
        session = get_session()
    try:
        resp = session.post(
            'https://basketball.icdb.tv/fetchcomms.php',
            data={'search': name},
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        if isinstance(data, list) and len(data) > 0:
            first = data[0]
            if isinstance(first, dict) and 'id' in first:
                return int(first['id'])
            if isinstance(first, dict) and 'value' in first:
                return int(first['value'])
        return None
    except Exception:
        return None


def find_commentator_id(name: str, session: requests.Session = None) -> Optional[int]:
    """
    Search for a commentator's ID by name.
    
    Args:
        name: Commentator name (e.g., "Michael Grady")
        session: Optional requests session
        
    Returns:
        Commentator ID if found, None otherwise
    """
    if session is None:
        session = get_session()
    
    # Search by trying IDs (could be improved with actual search API)
    name_slug = name.replace(' ', '-')
    
    # Check common ranges
    for test_id in range(1, 1000):
        try:
            url = f'https://basketball.icdb.tv/stats/{test_id}/x'
            resp = session.get(url, timeout=10)
            
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'html.parser')
                title = soup.find('title')
                if title and name.lower() in title.text.lower():
                    return test_id
                    
        except Exception as e:
            continue
            
        if test_id % 100 == 0:
            print(f"  Searched {test_id} IDs...")
    
    return None


def scrape_commentator_stats(commentator_id: int, session: requests.Session = None) -> Dict:
    """
    Scrape commentator statistics from ICDB.
    
    Args:
        commentator_id: ICDB commentator ID
        session: Optional requests session
        
    Returns:
        Dictionary with name, teams, channels, co-commentators, recent matches
    """
    if session is None:
        session = get_session()
    
    url = f'https://basketball.icdb.tv/stats/{commentator_id}/x'
    resp = session.get(url, timeout=30)
    
    if resp.status_code != 200:
        raise ValueError(f"Failed to fetch stats: {resp.status_code}")
    
    soup = BeautifulSoup(resp.text, 'html.parser')
    
    # Get name from title
    title = soup.find('title')
    name = title.text.split(' - ')[0] if title else f"ID {commentator_id}"
    
    result = {
        'id': commentator_id,
        'name': name,
        'teams': [],
        'channels': [],
        'co_commentators': [],
        'recent_matches': []
    }
    
    # Parse data cards
    data_cards = soup.find_all('div', class_='data-card')
    
    for card in data_cards:
        rows = card.find_all('div', class_='data-row')
        actual_rows = [r for r in rows if 'data-card-header' not in r.get('class', [])]
        
        if not actual_rows:
            continue
            
        # Determine card type from first row structure
        first_row = actual_rows[0]
        cols = first_row.find_all('div', class_=re.compile(r'^col-'))
        
        if any('col-date' in str(c.get('class', [])) for c in cols):
            # Matches card
            for row in actual_rows:
                date = row.find('div', class_='col-date')
                match = row.find('div', class_='col-match')
                comp = row.find('div', class_='col-comp')
                channel = row.find('div', class_='col-chan')
                role = row.find('div', class_='col-role')
                
                if date and match:
                    match_text = match.get_text(strip=True)
                    # Parse away @ home
                    if '@' in match_text:
                        parts = match_text.split('@')
                        away = parts[0].strip()
                        home = parts[1].strip()
                    else:
                        away, home = match_text, ''
                    
                    result['recent_matches'].append({
                        'date': date.get_text(strip=True),
                        'away_team': away,
                        'home_team': home,
                        'competition': comp.get_text(strip=True) if comp else '',
                        'channel': channel.get_text(strip=True) if channel else '',
                        'role': role.get_text(strip=True) if role else ''
                    })
                    
        elif len(cols) == 2:
            # Stats card (Team/Channel/Person stats)
            # Cards use col-name and col-count classes
            # Determine type from header row
            header_row = rows[0] if rows else None
            if header_row:
                header_name = header_row.find('div', class_='col-name')
                header_text = header_name.get_text(strip=True).lower() if header_name else ''
            else:
                header_text = ''
            
            for row in actual_rows:
                name_col = row.find('div', class_='col-name')
                count_col = row.find('div', class_='col-count')
                
                if name_col and count_col:
                    name_text = name_col.get_text(strip=True)
                    try:
                        count_val = int(count_col.get_text(strip=True))
                    except ValueError:
                        continue
                    
                    if 'team' in header_text:
                        result['teams'].append({
                            'team': name_text,
                            'count': count_val
                        })
                    elif 'channel' in header_text:
                        result['channels'].append({
                            'channel': name_text,
                            'count': count_val
                        })
                    elif 'person' in header_text:
                        result['co_commentators'].append({
                            'name': name_text,
                            'count': count_val
                        })
    
    return result


def get_nba_team_stats(stats: Dict) -> pd.DataFrame:
    """
    Extract NBA team statistics from commentator stats.
    
    Filters out WNBA and other leagues.
    """
    nba_teams = [
        'Hawks', 'Celtics', 'Nets', 'Hornets', 'Bulls', 'Cavaliers',
        'Mavericks', 'Nuggets', 'Pistons', 'Warriors', 'Rockets', 'Pacers',
        'Clippers', 'Lakers', 'Grizzlies', 'Heat', 'Bucks', 'Timberwolves',
        'Pelicans', 'Knicks', 'Thunder', 'Magic', '76ers', 'Suns',
        'Trail Blazers', 'Kings', 'Spurs', 'Raptors', 'Jazz', 'Wizards'
    ]
    
    nba_data = []
    for team_stat in stats.get('teams', []):
        team_name = team_stat['team']
        # Check if it's an NBA team
        if any(t in team_name for t in nba_teams):
            nba_data.append(team_stat)
    
    return pd.DataFrame(nba_data)


def get_latest_date_from_csv(csv_path: str) -> Optional[str]:
    """
    Read an ICDB-style CSV and return the latest game date (YYYY-MM-DD).
    Returns None if file is missing, empty, or has no parseable date column.
    """
    path = Path(csv_path)
    if not path.exists():
        return None
    try:
        df = pd.read_csv(csv_path)
        if df.empty or 'date' not in df.columns:
            return None
        df['date_parsed'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['date_parsed'])
        if df.empty:
            return None
        latest = df['date_parsed'].max()
        return latest.strftime('%Y-%m-%d')
    except Exception:
        return None


def get_existing_match_ids(csv_path: str) -> set:
    """Return set of match_ids already in a CSV (for dedup)."""
    try:
        df = pd.read_csv(csv_path)
        if 'match_id' in df.columns:
            return set(df['match_id'].dropna().astype(int).tolist())
    except Exception:
        pass
    return set()


def _name_from_csv_path(csv_path: str) -> str:
    """Derive commentator name from CSV filename: dave_pasch_icdb.csv -> Dave Pasch."""
    stem = Path(csv_path).stem
    if stem.endswith('_icdb'):
        stem = stem[:-5]
    return stem.replace('_', ' ').title()


def update_csv_from_scrape(
    csv_path: str,
    commentator_name: Optional[str] = None,
    include_co_commentators: bool = True,
) -> int:
    """
    Update an existing ICDB CSV with new games using ICDBScraper.
    Fetches the first page of matches, keeps only those newer than
    the latest date (and not already present by match_id), fetches
    full commentator details for the new ones, and appends to the CSV.

    Args:
        csv_path: Path to the CSV to update.
        commentator_name: Announcer name. Derived from filename if None.
        include_co_commentators: Whether to fetch commentator details for new games.

    Returns:
        Number of new games appended.
    """
    from src.scrapers.icdb_scraper import ICDBScraper

    path = Path(csv_path)
    if commentator_name is None:
        commentator_name = _name_from_csv_path(csv_path)

    latest_date_str = get_latest_date_from_csv(csv_path)
    existing_ids = get_existing_match_ids(csv_path)

    scraper = ICDBScraper(delay=0.3)

    # Resolve commentator ID
    comms = scraper.search_commentator(commentator_name)
    if not comms:
        raise ValueError(f"No ICDB results for: {commentator_name}")
    comm_id = int(comms[0]['value'])
    logger.info(f"{commentator_name} -> ID {comm_id}")

    # We don't filter by competition here; the CSVs include NBA Cup, Playoffs, etc.
    # Fetch first page (up to 50 most recent matches)
    matches = scraper.get_commentator_matches(comm_id, competition_id=None, limit=50, max_pages=1)
    if not matches:
        return 0

    # Filter to NBA-only competitions
    nba_comps = {'NBA Regular Season', 'NBA Playoffs', 'NBA Cup', 'NBA Pre-Season',
                 'NBA Finals', 'NBA All-Star Game', 'NBA Summer League'}
    matches = [m for m in matches if m.competition in nba_comps]

    # Filter to only matches newer than latest and not already in CSV
    new_matches = []
    for m in matches:
        if m.match_id in existing_ids:
            continue
        # Compare date (strip time for comparison)
        match_date = m.date.strftime('%Y-%m-%d') if m.date else ''
        if latest_date_str and match_date <= latest_date_str:
            continue
        new_matches.append(m)

    if not new_matches:
        return 0

    # Build rows in the same format as the existing CSV
    rows = []
    for i, match in enumerate(new_matches):
        logger.info(f"  Fetching details {i+1}/{len(new_matches)}: {match.teams}")
        row = {
            'date': match.date_str,
            'timestamp': match.timestamp,
            'teams': match.teams,
            'home_team': match.home_team,
            'away_team': match.away_team,
            'competition': match.competition,
            'channel': match.channel,
            'match_id': match.match_id,
            'match_url': match.match_url,
        }

        if include_co_commentators:
            try:
                commentators = scraper.get_match_commentators(match.match_id, deduplicate=True)
                lang_comms = [c for c in commentators if c.language.lower() == 'english']
                main_comm = next((c for c in lang_comms if c.role == 'Main Commentator'), None)
                co_comms = [c for c in lang_comms if c.role == 'Co-commentator']
                analysts = [c for c in lang_comms if c.role in ('Pundit', 'Analyst')]
                presenters = [c for c in lang_comms if c.role == 'Presenter']
                reporters = [c for c in lang_comms if c.role == 'Reporter']
                row['main_commentator'] = main_comm.name if main_comm else ''
                row['co_commentator'] = co_comms[0].name if co_comms else ''
                row['co_commentators'] = ', '.join(c.name for c in co_comms) if co_comms else ''
                row['analysts'] = ', '.join(a.name for a in analysts) if analysts else ''
                row['presenters'] = ', '.join(p.name for p in presenters) if presenters else ''
                row['reporters'] = ', '.join(r.name for r in reporters) if reporters else ''
                row['all_commentators'] = ', '.join(f'{c.name} ({c.role})' for c in lang_comms)
            except Exception as e:
                logger.warning(f"  Could not fetch commentators for match {match.match_id}: {e}")
                row.update({k: '' for k in ('main_commentator', 'co_commentator', 'co_commentators',
                                              'analysts', 'presenters', 'reporters', 'all_commentators')})
        row['matchup'] = f"{match.away_team} @ {match.home_team}"
        row['status'] = 'pending'
        row['audio_file'] = ''
        rows.append(row)

    new_df = pd.DataFrame(rows)

    # Load existing and merge
    if path.exists():
        existing = pd.read_csv(csv_path)
        combined = pd.concat([new_df, existing], ignore_index=True)
    else:
        combined = new_df

    # Dedupe by match_id (prefer existing), then by (date, away_team, home_team) as fallback
    if 'match_id' in combined.columns:
        combined = combined.drop_duplicates(subset=['match_id'], keep='last')
    combined = combined.drop_duplicates(subset=['away_team', 'home_team', 'date'], keep='first')

    # Sort by date descending
    combined['_sort'] = pd.to_datetime(combined['date'], errors='coerce')
    combined = combined.sort_values('_sort', ascending=False).drop(columns=['_sort']).reset_index(drop=True)
    combined.to_csv(csv_path, index=False)
    return len(new_df)


# Known commentator IDs (add more as discovered)
KNOWN_COMMENTATORS = {
    'Michael Grady': 95,
    'Mike Breen': 4,
    'Doris Burke': None,  # ID to be found
    'Ryan Ruocco': 2,
}


def main():
    parser = argparse.ArgumentParser(
        description='Scrape ICDB for NBA announcer info; optionally update existing CSVs with new games.',
    )
    parser.add_argument(
        '--update',
        metavar='CSV',
        help='Update this CSV with new games since its latest date (e.g. data/nba/icdb/ian_eagle_icdb.csv)',
    )
    parser.add_argument(
        '--update-dir',
        metavar='DIR',
        default=None,
        help='Update all *_icdb.csv files in this directory (default: data/nba/icdb if --update not set)',
    )
    args = parser.parse_args()

    # Update mode: one CSV or a directory of CSVs
    if args.update or args.update_dir is not None:
        csvs_to_update = []
        if args.update:
            csvs_to_update.append(Path(args.update))
        if args.update_dir is not None:
            dir_path = Path(args.update_dir)
            if not dir_path.is_dir():
                print(f"Error: not a directory: {dir_path}")
                return 1
            csvs_to_update.extend(sorted(dir_path.glob('*_icdb.csv')))
        if not csvs_to_update:
            print("No CSV files to update. Use --update <path> or --update-dir <dir>.")
            return 1
        total_new = 0
        for csv_path in csvs_to_update:
            latest = get_latest_date_from_csv(str(csv_path))
            print(f"  {csv_path.name} (latest: {latest or 'empty'})...", end=' ', flush=True)
            try:
                n = update_csv_from_scrape(str(csv_path))
                total_new += n
                print(f"+{n} new games")
            except Exception as e:
                print(f"error - {e}")
        print(f"\nTotal new games added: {total_new}")
        return 0

    # Demo usage when no --update
    session = get_session()
    session.get('https://basketball.icdb.tv/', timeout=30)
    commentator_id = KNOWN_COMMENTATORS.get('Michael Grady', 95)
    print(f"Fetching stats for commentator ID {commentator_id}...")
    stats = scrape_commentator_stats(commentator_id, session)
    print(f"\n=== {stats['name']} ===")
    print("\nTop Teams:")
    for team in stats['teams'][:10]:
        print(f"  {team['team']}: {team['count']} games")
    print("\nTop Co-Commentators:")
    for person in stats['co_commentators'][:5]:
        print(f"  {person['name']}: {person['count']} games")
    print("\nRecent Matches:")
    for match in stats['recent_matches'][:5]:
        print(f"  {match['date']}: {match['away_team']} @ {match['home_team']} ({match['channel']})")
    nba_teams = get_nba_team_stats(stats)
    print(f"\nNBA Teams: {len(nba_teams)}")
    print(nba_teams.to_string(index=False))
    return 0


if __name__ == '__main__':
    exit(main() or 0)
