#!/usr/bin/env python3
"""
NFL Configuration Data

Centralized configuration for:
- Announcer crews and their names
- Team mappings (ticker symbols to full names)
- Stadium information (turf/grass, indoor/outdoor)
- Data directories and paths
"""

from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import re


# =============================================================================
# ANNOUNCER CONFIGURATIONS
# =============================================================================

@dataclass
class AnnouncerCrew:
    """Configuration for an announcer crew."""
    name: str                           # Human-readable name
    folder: str                         # data/football/<folder> subfolder
    csv_path: Optional[str]             # Path to CSV with games (optional)
    play_by_play: List[str]             # Play-by-play announcer name variations
    color: List[str]                    # Color commentator name variations  
    sideline: Optional[List[str]] = None  # Sideline reporter names (optional)
    validation_threshold: float = 5.0   # Min avg announcer mentions per game


# Announcer crew configurations
ANNOUNCER_CREWS: Dict[str, AnnouncerCrew] = {
    # Prime-time shows
    'mnf': AnnouncerCrew(
        name='Monday Night Football',
        folder='nfl/mnf',
        csv_path='data/football/csvs/monday-night.csv',
        play_by_play=['Joe', 'Buck'],
        color=['Troy', 'Aikman'],
        sideline=['Lisa', 'Salters'],
    ),
    'snf': AnnouncerCrew(
        name='Sunday Night Football',
        folder='nfl/snf',
        csv_path='data/football/csvs/sunday-night.csv',
        play_by_play=['Mike', 'Tirico'],
        color=['Cris', 'Collinsworth'],
        sideline=['Melissa', 'Stark'],
    ),
    'tnf': AnnouncerCrew(
        name='Thursday Night Football',
        folder='nfl/tnf',
        csv_path='data/football/csvs/thursday-night.csv',
        play_by_play=['Al', 'Michaels'],
        color=['Kirk', 'Herbstreit'],
    ),
    
    # CBS crews
    'cbs': AnnouncerCrew(
        name='CBS (Nantz/Romo)',
        folder='nfl/cbs',
        csv_path=None,
        play_by_play=['Jim', 'Nantz'],
        color=['Tony', 'Romo'],
    ),
    'cbs-eagle': AnnouncerCrew(
        name='CBS (Eagle/Watt)',
        folder='nfl/cbs-eagle',
        csv_path='data/football/csvs/cbs-eagle.csv',
        play_by_play=['Ian', 'Eagle'],
        color=['JJ', 'Watt'],
    ),
    'cbs-spero': AnnouncerCrew(
        name='CBS (Spero/Archuleta)',
        folder='nfl/cbs-spero',
        csv_path='data/football/csvs/cbs-spero.csv',
        play_by_play=['Spero', 'Dedes'],
        color=['Adam', 'Archuleta'],
    ),
    
    # FOX crews
    'fox': AnnouncerCrew(
        name='FOX (Burkhardt/Brady)',
        folder='nfl/fox',
        csv_path='data/football/csvs/fox.csv',
        play_by_play=['Kevin', 'Burkhardt'],
        color=['Tom', 'Brady'],
    ),
    'fox-2': AnnouncerCrew(
        name='FOX2 (Davis/Olsen)',
        folder='nfl/fox-2',
        csv_path='data/football/csvs/fox-2.csv',
        play_by_play=['Joe', 'Davis'],
        color=['Greg', 'Olsen'],
    ),
    
    # Individual announcer-focused datasets
    'kevin-kugler': AnnouncerCrew(
        name='Kevin Kugler',
        folder='nfl/kevin-kugler',
        csv_path='data/football/csvs/kevin-kugler.csv',
        play_by_play=['Kevin', 'Kugler'],
        color=['Daryl', 'Johnston'],  # Most common pairing
    ),
    'adam-amin': AnnouncerCrew(
        name='Adam Amin/Brees',
        folder='nfl/adam-amin',
        csv_path='data/football/csvs/adam-amin.csv',
        play_by_play=['Adam', 'Amin'],
        color=['Drew', 'Brees'],
    ),
    'noah-eagle': AnnouncerCrew(
        name='Noah Eagle',
        folder='nfl/noah-eagle',
        csv_path='data/football/csvs/noah-eagle.csv',
        play_by_play=['Noah', 'Eagle'],
        color=['Todd', 'Blackledge'],
    ),
    'rich-eisen': AnnouncerCrew(
        name='Rich Eisen',
        folder='nfl/rich-eisen',
        csv_path='data/football/csvs/rich-eisen.csv',
        play_by_play=['Rich', 'Eisen'],
        color=[],  # Variable
    ),
    'chris-myers': AnnouncerCrew(
        name='Chris Myers',
        folder='nfl/chris-myers',
        csv_path='data/football/csvs/chris-myers.csv',
        play_by_play=['Chris', 'Myers'],
        color=[],  # Variable
    ),
    'kevin-harlan': AnnouncerCrew(
        name='Kevin Harlan/Green',
        folder='nfl/kevin-harlan',
        csv_path='data/football/csvs/kevin-harlan.csv',
        play_by_play=['Kevin', 'Harlan'],
        color=['Trent', 'Green'],
    ),
    'roy-philpott': AnnouncerCrew(
        name='Roy Philpott',
        folder='roy-philpott',
        csv_path='data/football/college-csvs/roy-philpott.csv',
        play_by_play=['Roy', 'Philpott'],
        color=['Sam', 'Acho'],
    ),
    'brad-nessler': AnnouncerCrew(
        name='Brad Nessler',
        folder='college/brad-nessler',
        csv_path='data/football/college-csvs/brad-nessler.csv',
        play_by_play=['Brad', 'Nessler'],
        color=['Gary', 'Danielson'],
    ),
    'mark-jones': AnnouncerCrew(
        name='Mark Jones',
        folder='college/mark-jones',
        csv_path='data/football/college-csvs/mark_jones.csv',
        play_by_play=['Mark', 'Jones'],
        color=['Roddy', 'Jones'],
    ),
    'dave-flemming': AnnouncerCrew(
        name='Dave Flemming',
        folder='college/dave-flemming',
        csv_path='data/football/college-csvs/flemming.csv',
        play_by_play=['Dave', 'Flemming'],
        color=['Brock', 'Osweiler'],
    ),
    'chris-fowler': AnnouncerCrew(
        name='Chris Fowler',
        folder='college/dave-flemming',
        csv_path='data/football/college-csvs/fowler.csv',
        play_by_play=['Chris', 'Fowler'],
        color=['Kirk', 'Herbstreit'],
    ),
    'joe-tessitore': AnnouncerCrew(
        name='Joe Tessitore',
        folder='college/joe-tessitore',
        csv_path='data/football/college-csvs/tessitore_palmer_games.csv.csv',
        play_by_play=['Joe', 'Tessitore'],
        color=['Jesse', 'Palmer'],
    ),
    'dave-pasch': AnnouncerCrew(
        name='Dave Pasch',
        folder='college/dave-pasch',
        csv_path='data/football/college-csvs/pasch.csv',
        play_by_play=['Dave', 'Pasch'],
        color=['Dusty', 'Dvoracek'],
    ),
    'andrew-catalon': AnnouncerCrew(
        name='Andrew Catalon',
        folder='nfl/catalon',
        csv_path='data/football/csvs/catalon.csv',
        play_by_play=['Andrew', 'Catalon'],
        color=['Charles', 'Davis'],
    ),
}


def get_announcer_crew(crew_name: str) -> Optional[AnnouncerCrew]:
    """Get announcer crew configuration by name (case-insensitive)."""
    return ANNOUNCER_CREWS.get(crew_name.lower())


def get_all_announcer_names(crew: AnnouncerCrew) -> List[str]:
    """Get all announcer names for a crew (for validation phrases)."""
    names = list(crew.play_by_play) + list(crew.color)
    if crew.sideline:
        names.extend(crew.sideline)
    return names


# =============================================================================
# TEAM CONFIGURATIONS
# =============================================================================

# Team ticker symbols to full names
TEAM_NAMES: Dict[str, str] = {
    # AFC East
    'buf': 'Bills', 'bills': 'Bills',
    'mia': 'Dolphins', 'dolphins': 'Dolphins',
    'ne': 'Patriots', 'patriots': 'Patriots', 'pats': 'Patriots',
    'nyj': 'Jets', 'jets': 'Jets',
    
    # AFC North
    'bal': 'Ravens', 'ravens': 'Ravens',
    'cin': 'Bengals', 'bengals': 'Bengals',
    'cle': 'Browns', 'browns': 'Browns',
    'pit': 'Steelers', 'steelers': 'Steelers',
    
    # AFC South
    'hou': 'Texans', 'texans': 'Texans',
    'ind': 'Colts', 'colts': 'Colts',
    'jac': 'Jaguars', 'jaguars': 'Jaguars', 'jags': 'Jaguars',
    'ten': 'Titans', 'titans': 'Titans',
    
    # AFC West
    'den': 'Broncos', 'broncos': 'Broncos',
    'kc': 'Chiefs', 'chiefs': 'Chiefs',
    'lv': 'Raiders', 'raiders': 'Raiders', 'oak': 'Raiders',
    'lac': 'Chargers', 'chargers': 'Chargers', 'sd': 'Chargers',
    
    # NFC East
    'dal': 'Cowboys', 'cowboys': 'Cowboys',
    'nyg': 'Giants', 'giants': 'Giants',
    'phi': 'Eagles', 'eagles': 'Eagles',
    'was': 'Commanders', 'commanders': 'Commanders', 
    'washington': 'Commanders', 'football-team': 'Commanders',
    'redskins': 'Commanders',
    
    # NFC North
    'chi': 'Bears', 'bears': 'Bears',
    'det': 'Lions', 'lions': 'Lions',
    'gb': 'Packers', 'packers': 'Packers',
    'min': 'Vikings', 'vikings': 'Vikings',
    
    # NFC South
    'atl': 'Falcons', 'falcons': 'Falcons',
    'car': 'Panthers', 'panthers': 'Panthers',
    'no': 'Saints', 'saints': 'Saints',
    'tb': 'Buccaneers', 'buccaneers': 'Buccaneers', 'bucs': 'Buccaneers',
    
    # NFC West
    'ari': 'Cardinals', 'cardinals': 'Cardinals', 'az': 'Cardinals',
    'la': 'Rams', 'rams': 'Rams', 'lar': 'Rams',
    'sf': '49ers', '49ers': '49ers', 'niners': '49ers',
    'sea': 'Seahawks', 'seahawks': 'Seahawks',
}


def get_team_name(ticker: str) -> str:
    """Get full team name from ticker/abbreviation."""
    return TEAM_NAMES.get(ticker.lower(), ticker.title())


# =============================================================================
# STADIUM CONFIGURATIONS
# =============================================================================

@dataclass 
class StadiumInfo:
    """Stadium information for a team."""
    team: str
    stadium_name: str
    surface: str           # 'grass' or 'turf'
    roof_type: str         # 'outdoor', 'indoor', or 'retractable'
    city: str
    state: str


# Stadium information by team
STADIUMS: Dict[str, StadiumInfo] = {
    # AFC East
    'bills': StadiumInfo('Bills', 'Highmark Stadium', 'turf', 'outdoor', 'Orchard Park', 'NY'),
    'dolphins': StadiumInfo('Dolphins', 'Hard Rock Stadium', 'grass', 'outdoor', 'Miami Gardens', 'FL'),
    'patriots': StadiumInfo('Patriots', 'Gillette Stadium', 'turf', 'outdoor', 'Foxborough', 'MA'),
    'jets': StadiumInfo('Jets', 'MetLife Stadium', 'turf', 'outdoor', 'East Rutherford', 'NJ'),
    
    # AFC North
    'ravens': StadiumInfo('Ravens', 'M&T Bank Stadium', 'grass', 'outdoor', 'Baltimore', 'MD'),
    'bengals': StadiumInfo('Bengals', 'Paycor Stadium', 'turf', 'outdoor', 'Cincinnati', 'OH'),
    'browns': StadiumInfo('Browns', 'Cleveland Browns Stadium', 'grass', 'outdoor', 'Cleveland', 'OH'),
    'steelers': StadiumInfo('Steelers', 'Acrisure Stadium', 'grass', 'outdoor', 'Pittsburgh', 'PA'),
    
    # AFC South
    'texans': StadiumInfo('Texans', 'NRG Stadium', 'turf', 'retractable', 'Houston', 'TX'),
    'colts': StadiumInfo('Colts', 'Lucas Oil Stadium', 'turf', 'retractable', 'Indianapolis', 'IN'),
    'jaguars': StadiumInfo('Jaguars', 'EverBank Stadium', 'grass', 'outdoor', 'Jacksonville', 'FL'),
    'titans': StadiumInfo('Titans', 'Nissan Stadium', 'turf', 'outdoor', 'Nashville', 'TN'),
    
    # AFC West
    'broncos': StadiumInfo('Broncos', 'Empower Field', 'grass', 'outdoor', 'Denver', 'CO'),
    'chiefs': StadiumInfo('Chiefs', 'Arrowhead Stadium', 'grass', 'outdoor', 'Kansas City', 'MO'),
    'raiders': StadiumInfo('Raiders', 'Allegiant Stadium', 'grass', 'indoor', 'Las Vegas', 'NV'),
    'chargers': StadiumInfo('Chargers', 'SoFi Stadium', 'turf', 'indoor', 'Inglewood', 'CA'),
    
    # NFC East
    'cowboys': StadiumInfo('Cowboys', 'AT&T Stadium', 'turf', 'retractable', 'Arlington', 'TX'),
    'giants': StadiumInfo('Giants', 'MetLife Stadium', 'turf', 'outdoor', 'East Rutherford', 'NJ'),
    'eagles': StadiumInfo('Eagles', 'Lincoln Financial Field', 'grass', 'outdoor', 'Philadelphia', 'PA'),
    'commanders': StadiumInfo('Commanders', 'Northwest Stadium', 'grass', 'outdoor', 'Landover', 'MD'),
    
    # NFC North
    'bears': StadiumInfo('Bears', 'Soldier Field', 'grass', 'outdoor', 'Chicago', 'IL'),
    'lions': StadiumInfo('Lions', 'Ford Field', 'turf', 'indoor', 'Detroit', 'MI'),
    'packers': StadiumInfo('Packers', 'Lambeau Field', 'grass', 'outdoor', 'Green Bay', 'WI'),
    'vikings': StadiumInfo('Vikings', 'U.S. Bank Stadium', 'turf', 'indoor', 'Minneapolis', 'MN'),
    
    # NFC South
    'falcons': StadiumInfo('Falcons', 'Mercedes-Benz Stadium', 'turf', 'retractable', 'Atlanta', 'GA'),
    'panthers': StadiumInfo('Panthers', 'Bank of America Stadium', 'turf', 'outdoor', 'Charlotte', 'NC'),
    'saints': StadiumInfo('Saints', 'Caesars Superdome', 'turf', 'indoor', 'New Orleans', 'LA'),
    'buccaneers': StadiumInfo('Buccaneers', 'Raymond James Stadium', 'grass', 'outdoor', 'Tampa', 'FL'),
    
    # NFC West
    'cardinals': StadiumInfo('Cardinals', 'State Farm Stadium', 'grass', 'retractable', 'Glendale', 'AZ'),
    'rams': StadiumInfo('Rams', 'SoFi Stadium', 'turf', 'indoor', 'Inglewood', 'CA'),
    '49ers': StadiumInfo('49ers', "Levi's Stadium", 'grass', 'outdoor', 'Santa Clara', 'CA'),
    'seahawks': StadiumInfo('Seahawks', 'Lumen Field', 'turf', 'outdoor', 'Seattle', 'WA'),
}


def get_stadium(team: str) -> Optional[StadiumInfo]:
    """Get stadium info for a team."""
    team_lower = team.lower()
    # Try direct lookup first
    if team_lower in STADIUMS:
        return STADIUMS[team_lower]
    # Try to resolve through team names
    team_name = get_team_name(team_lower)
    if team_name.lower() in STADIUMS:
        return STADIUMS[team_name.lower()]
    return None


# Precomputed regex patterns for stadium classification
GRASS_TEAMS_PATTERN = '|'.join([
    f'at-{team}|{team}.txt' 
    for team, info in STADIUMS.items() 
    if info.surface == 'grass'
])

TURF_TEAMS_PATTERN = '|'.join([
    f'at-{team}|{team}.txt'
    for team, info in STADIUMS.items()
    if info.surface == 'turf'
])

INDOOR_TEAMS_PATTERN = '|'.join([
    f'at-{team}|{team}.txt'
    for team, info in STADIUMS.items()
    if info.roof_type == 'indoor'
])

OUTDOOR_TEAMS_PATTERN = '|'.join([
    f'at-{team}|{team}.txt'
    for team, info in STADIUMS.items()
    if info.roof_type == 'outdoor'
])

RETRACTABLE_TEAMS_PATTERN = '|'.join([
    f'at-{team}|{team}.txt'
    for team, info in STADIUMS.items()
    if info.roof_type == 'retractable'
])


# =============================================================================
# URL AND FILENAME PARSING
# =============================================================================

def parse_game_url(url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Parse an NFL game URL to extract teams and year.
    
    Args:
        url: NFL+ game URL
        
    Returns:
        Tuple of (away_team, home_team, year) or (None, None, None) if parsing fails
    """
    # Pattern 1: /games/lions-at-ravens-2025-reg-3
    m = re.search(r'/games/([a-z0-9\-]+)-at-([a-z0-9\-]+)-(\d{4})-', url)
    if m:
        away = m.group(1)
        home = m.group(2)
        year = m.group(3)
        return away, home, year
    
    # Pattern 2: /games/2024/01/01/lions-at-ravens
    m = re.search(r'/games/(\d{4})/\d{2}/\d{2}/([a-z0-9\-]+)-at-([a-z0-9\-]+)', url)
    if m:
        year = m.group(1)
        away = m.group(2)
        home = m.group(3)
        return away, home, year
    
    return None, None, None


def parse_filename(filename: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Parse a transcript filename to extract date and teams.
    
    Args:
        filename: Transcript filename like '2024-01-01_lions-at-ravens.txt'
        
    Returns:
        Tuple of (date_str, away_team, home_team, year) or Nones if parsing fails
    """
    m = re.match(r'(\d{4})-(\d{2})-(\d{2})_([a-z0-9\-]+)-at-([a-z0-9\-]+)\.txt', filename, re.IGNORECASE)
    if m:
        date_str = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        year = m.group(1)
        away = m.group(4)
        home = m.group(5)
        return date_str, away, home, year
    return None, None, None, None


# Valid ticker abbreviations (uppercase) - standard NFL team codes used in Kalshi tickers
TICKER_TEAM_CODES = {
    'BUF', 'MIA', 'NE', 'NYJ',  # AFC East
    'BAL', 'CIN', 'CLE', 'PIT',  # AFC North
    'HOU', 'IND', 'JAC', 'JAX', 'TEN',  # AFC South
    'DEN', 'KC', 'LV', 'LAC',  # AFC West
    'DAL', 'NYG', 'PHI', 'WAS', 'WSH',  # NFC East
    'CHI', 'DET', 'GB', 'MIN',  # NFC North
    'ATL', 'CAR', 'NO', 'TB',  # NFC South
    'ARI', 'AZ', 'LA', 'LAR', 'SF', 'SEA',  # NFC West
}


def extract_teams_from_ticker(ticker: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract teams from a Kalshi ticker like 'KXNFLMENTION-25DEC28PHIBUF'.
    
    Args:
        ticker: Kalshi event ticker
        
    Returns:
        Tuple of (away_team, home_team) or (None, None) if parsing fails
    """
    # Extract the teams portion after the date (e.g., "26JAN10GBCHI" -> "GBCHI")
    m = re.search(r'\d{2}[A-Z]{3}\d{2}([A-Z]{4,6})$', ticker)
    if not m:
        return None, None
    
    teams_str = m.group(1)
    
    # Try different split positions to find valid team combinations
    # Split position determines where away team ends and home team begins
    for split_pos in range(2, len(teams_str) - 1):  # At least 2 chars on each side
        away = teams_str[:split_pos]
        home = teams_str[split_pos:]
        
        if away in TICKER_TEAM_CODES and home in TICKER_TEAM_CODES:
            return away.lower(), home.lower()
    
    return None, None


# =============================================================================
# KALSHI MARKET INTEGRATION
# =============================================================================

def get_market_phrases(event_ticker: str) -> List[str]:
    """
    Get phrases for a given Kalshi event ticker.
    
    Args:
        event_ticker: Kalshi event ticker like 'KXNFLMENTION-25DEC28PHIBUF'
        
    Returns:
        List of phrases from the market's yes_sub_title fields
    """
    from kalshi import constants
    constants.use_prod()
    from kalshi.rest import market
    markets = market.GetMarkets(event_ticker=event_ticker)
    return [m['yes_sub_title'] for m in markets['markets']]

