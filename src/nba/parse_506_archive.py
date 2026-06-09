"""
506 Sports Archive Parser for NBA Games

Parses the 506 Sports Archive Wiki format to extract game broadcast information
and map to Kalshi NBA Mentions event tickers.
"""

import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import pandas as pd


# Team name to 3-letter code mapping (Kalshi uses these codes)
TEAM_CODES = {
    # Full official names (e.g. from ICDB)
    'Atlanta Hawks': 'ATL',
    'Boston Celtics': 'BOS',
    'Brooklyn Nets': 'BKN',
    'Charlotte Hornets': 'CHA',
    'Chicago Bulls': 'CHI',
    'Cleveland Cavaliers': 'CLE',
    'Dallas Mavericks': 'DAL',
    'Denver Nuggets': 'DEN',
    'Detroit Pistons': 'DET',
    'Golden State Warriors': 'GSW',
    'Houston Rockets': 'HOU',
    'Indiana Pacers': 'IND',
    'Los Angeles Clippers': 'LAC',
    'Los Angeles Lakers': 'LAL',
    'Memphis Grizzlies': 'MEM',
    'Miami Heat': 'MIA',
    'Milwaukee Bucks': 'MIL',
    'Minnesota Timberwolves': 'MIN',
    'New Orleans Pelicans': 'NOP',
    'New York Knicks': 'NYK',
    'Oklahoma City Thunder': 'OKC',
    'Orlando Magic': 'ORL',
    'Philadelphia 76ers': 'PHI',
    'Phoenix Suns': 'PHX',
    'Portland Trail Blazers': 'POR',
    'Sacramento Kings': 'SAC',
    'San Antonio Spurs': 'SAS',
    'Toronto Raptors': 'TOR',
    'Utah Jazz': 'UTA',
    'Washington Wizards': 'WAS',
    # Short/city names (506 Sports format)
    'Hawks': 'ATL', 'Atlanta': 'ATL',
    'Celtics': 'BOS', 'Boston': 'BOS',
    'Nets': 'BKN', 'Brooklyn': 'BKN',
    'Hornets': 'CHA', 'Charlotte': 'CHA',
    'Bulls': 'CHI', 'Chicago': 'CHI',
    'Cavaliers': 'CLE', 'Cleveland': 'CLE', 'Cavs': 'CLE',
    'Mavericks': 'DAL', 'Dallas': 'DAL', 'Mavs': 'DAL',
    'Nuggets': 'DEN', 'Denver': 'DEN',
    'Pistons': 'DET', 'Detroit': 'DET',
    'Warriors': 'GSW', 'Golden State': 'GSW',
    'Rockets': 'HOU', 'Houston': 'HOU',
    'Pacers': 'IND', 'Indiana': 'IND',
    'Clippers': 'LAC', 'LA Clippers': 'LAC',
    'Lakers': 'LAL', 'LA Lakers': 'LAL',
    'Grizzlies': 'MEM', 'Memphis': 'MEM',
    'Heat': 'MIA', 'Miami': 'MIA',
    'Bucks': 'MIL', 'Milwaukee': 'MIL',
    'Timberwolves': 'MIN', 'Minnesota': 'MIN',
    'Pelicans': 'NOP', 'New Orleans': 'NOP',
    'Knicks': 'NYK', 'New York': 'NYK',
    'Thunder': 'OKC', 'Oklahoma City': 'OKC', 'Oklahoma Thunder': 'OKC',
    'Magic': 'ORL', 'Orlando': 'ORL',
    '76ers': 'PHI', 'Philadelphia': 'PHI', 'Sixers': 'PHI',
    'Suns': 'PHX', 'Phoenix': 'PHX',
    'Trail Blazers': 'POR', 'Portland': 'POR', 'Blazers': 'POR',
    'Kings': 'SAC', 'Sacramento': 'SAC',
    'Spurs': 'SAS', 'San Antonio': 'SAS',
    'Raptors': 'TOR', 'Toronto': 'TOR',
    'Jazz': 'UTA', 'Utah': 'UTA',
    'Wizards': 'WAS', 'Washington': 'WAS',
}


def get_team_code(team_name: str) -> Optional[str]:
    """Convert team name to 3-letter code."""
    # Clean up team name
    team_name = team_name.strip()
    
    # Direct lookup
    if team_name in TEAM_CODES:
        return TEAM_CODES[team_name]
    
    # Try case-insensitive
    for name, code in TEAM_CODES.items():
        if name.lower() == team_name.lower():
            return code
    
    return None


def parse_date_line(line: str, current_year: int = 2025) -> Optional[datetime]:
    """
    Parse a date line like "Tuesday, October 21 - NBC DH" or "Friday, October 24"
    
    Args:
        line: The line containing the date
        current_year: The year to use for dates (defaults to 2025)
        
    Returns:
        datetime object or None if not a date line
    """
    # Pattern for date lines
    date_patterns = [
        # "Tuesday, October 21 - NBC DH" or "Friday, October 24"
        r'(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+'
        r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+'
        r'(\d{1,2})',
    ]
    
    for pattern in date_patterns:
        match = re.search(pattern, line, re.IGNORECASE)
        if match:
            month_str = match.group(1).title()
            day = int(match.group(2))

            month_map = {
                'January': 1, 'February': 2, 'March': 3, 'April': 4,
                'May': 5, 'June': 6, 'July': 7, 'August': 8,
                'September': 9, 'October': 10, 'November': 11, 'December': 12
            }
            month = month_map[month_str]
            
            # Adjust year for Jan+ (2026 season)
            year = current_year if month >= 10 else current_year + 1
            
            return datetime(year, month, day)
    
    return None


def parse_game_line(line: str) -> Optional[Dict]:
    """
    Parse a game line like:
    "Lakers-Warriors, ESPN 8:30 p.m. - Mike Breen, Richard Jefferson, Tim Legler, Jorge Sedano"
    
    Returns dict with:
    - away_team, home_team, network, time
    - play_by_play, color_commentator
    - other_announcers (list)
    """
    # Pattern: Away-Home, Network Time - Announcer1, Announcer2, ...
    # Handle special cases like "(Mexico City)" or "(Las Vegas)"
    # Network can have spaces (e.g., "Prime Video"), so use non-greedy match up to time
    game_pattern = r'^([A-Za-z0-9 ]+)-([A-Za-z0-9 ]+?)(?:\s*\([^)]+\))?,\s*(.+?)\s+(\d{1,2}:\d{2}\s*[pa]\.?m\.?)\s*-\s*(.+)$'
    
    match = re.match(game_pattern, line.strip(), re.IGNORECASE)
    if not match:
        return None
    
    away_team = match.group(1).strip()
    home_team = match.group(2).strip()
    network = match.group(3).strip()
    time = match.group(4).strip()
    announcers_str = match.group(5).strip()
    
    # Parse announcers - split by comma but handle edge cases
    # Some entries have notes like "(DET Bench)" we should strip
    announcers_str = re.sub(r'\([^)]*Bench[^)]*\)', '', announcers_str)
    announcers_str = re.sub(r'\([^)]*Second Half[^)]*\)', '', announcers_str)
    
    announcers = [a.strip() for a in announcers_str.split(',') if a.strip()]
    
    # First announcer is play-by-play, second is color
    play_by_play = announcers[0] if len(announcers) > 0 else None
    color_commentator = announcers[1] if len(announcers) > 1 else None
    other_announcers = announcers[2:] if len(announcers) > 2 else []
    
    return {
        'away_team': away_team,
        'home_team': home_team,
        'away_code': get_team_code(away_team),
        'home_code': get_team_code(home_team),
        'network': network,
        'time': time,
        'play_by_play': play_by_play,
        'color_commentator': color_commentator,
        'other_announcers': other_announcers,
    }


def parse_506_archive(text: str, start_year: int = 2025) -> List[Dict]:
    """
    Parse the full 506 Sports Archive text.
    
    Args:
        text: The raw text from the 506 Sports Archive
        start_year: The year the season starts (October)
        
    Returns:
        List of game dictionaries with date, teams, and announcers
    """
    games = []
    current_date = None
    
    lines = text.strip().split('\n')
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Check if this is a date line
        date = parse_date_line(line, start_year)
        if date:
            current_date = date
            continue
        
        # Check if this is a game line
        if current_date and '-' in line and ',' in line:
            game = parse_game_line(line)
            if game:
                game['date'] = current_date
                games.append(game)
    
    return games


def generate_kalshi_ticker(date: datetime, away_code: str, home_code: str) -> str:
    """
    Generate a Kalshi event ticker from game info.
    
    Format: KXNBAMENTION-{YY}{MON}{DD}{AWAY}{HOME}
    Example: KXNBAMENTION-25DEC05LALBOS (Dec 5, 2025, LAL @ BOS)
    """
    date_str = date.strftime('%y%b%d').upper()  # e.g., "25DEC05"
    return f"KXNBAMENTION-{date_str}{away_code}{home_code}"


def games_to_dataframe(games: List[Dict]) -> pd.DataFrame:
    """
    Convert list of games to a DataFrame with Kalshi tickers.
    
    Returns DataFrame with columns:
    - ticker, date, away_team, home_team, network, time
    - play_by_play, color_commentator
    """
    records = []
    for game in games:
        if game['away_code'] and game['home_code']:
            ticker = generate_kalshi_ticker(
                game['date'], 
                game['away_code'], 
                game['home_code']
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


def parse_506_archive_file(filepath: str, start_year: int = 2025) -> pd.DataFrame:
    """
    Parse a 506 Sports Archive file and return a DataFrame.
    
    Args:
        filepath: Path to the text file
        start_year: The year the season starts
        
    Returns:
        DataFrame with ticker, date, teams, and announcers
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        text = f.read()
    
    games = parse_506_archive(text, start_year)
    return games_to_dataframe(games)


def parse_506_snapshot(snapshot_text: str, start_year: int = 2025) -> pd.DataFrame:
    """
    Parse a browser snapshot from the 506 Sports Archive page.
    
    The snapshot has lines like:
    - paragraph [ref=e57]: Sunday, October 5
    - paragraph [ref=e58]: Lakers-Warriors, ESPN 8:30 p.m. - Mike Breen, Richard Jefferson, Tim Legler, Jorge Sedano
    
    Args:
        snapshot_text: The browser snapshot text
        start_year: The year the season starts
        
    Returns:
        DataFrame with ticker, date, teams, and announcers
    """
    games = []
    current_date = None
    
    lines = snapshot_text.strip().split('\n')
    
    for line in lines:
        # Extract content from paragraph lines
        # Format: "- paragraph [ref=e57]: Content here"
        para_match = re.search(r'paragraph\s+\[ref=\w+\]:\s*(.+)$', line)
        if not para_match:
            continue
        
        content = para_match.group(1).strip()
        
        # Check if this is a date line
        date = parse_date_line(content, start_year)
        if date:
            current_date = date
            continue
        
        # Check if this is a game line
        if current_date and '-' in content and ',' in content:
            game = parse_game_line(content)
            if game:
                game['date'] = current_date
                games.append(game)
    
    return games_to_dataframe(games)


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python parse_506_archive.py <filepath>")
        print("       python parse_506_archive.py --snapshot <snapshot_filepath>")
        sys.exit(1)
    
    if sys.argv[1] == '--snapshot':
        filepath = sys.argv[2]
        with open(filepath, 'r', encoding='utf-8') as f:
            text = f.read()
        df = parse_506_snapshot(text)
    else:
        filepath = sys.argv[1]
        df = parse_506_archive_file(filepath)
    
    print(f"Parsed {len(df)} games")
    print(df.to_string())

