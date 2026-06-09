#!/usr/bin/env python3
"""
NBA Configuration Data

Centralized configuration for:
- Announcer crews and their names
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
    folder: str                         # data/nba/transcripts/<folder> subfolder
    play_by_play: List[str]             # Play-by-play announcer name variations
    color: List[str]                    # Color commentator name variations  
    validation_threshold: float = 5.0   # Min avg announcer mentions per game


# Announcer crew configurations
ANNOUNCER_CREWS: Dict[str, AnnouncerCrew] = {
    'michael-grady': AnnouncerCrew(
        name='Michael Grady',
        folder='michael-grady',
        play_by_play=['Michael', 'Grady'],
        color=['Brent', 'Barry'],  # Common pairing
    ),
    'noah-eagle': AnnouncerCrew(
        name='Noah Eagle',
        folder='noah-eagle',
        play_by_play=['Noah', 'Eagle'],
        color=['Richard', 'Jefferson'],
    ),
    'ian-eagle': AnnouncerCrew(
        name='Ian Eagle',
        folder='ian-eagle',
        play_by_play=['Ian', 'Eagle'],
        color=['Stan', 'Van Gundy'],
    ),
    'mike-breen': AnnouncerCrew(
        name='Mike Breen',
        folder='mike-breen',
        play_by_play=['Mike', 'Breen'],
        color=['Doris', 'Burke'],
    ),
    'dave-pasch': AnnouncerCrew(
        name='Dave Pasch',
        folder='dave-pasch',
        play_by_play=['Dave', 'Pasch'],
        color=['Jamal', 'Crawford'],
    ),
    'terry-gannon': AnnouncerCrew(
        name='Terry Gannon',
        folder='terry-gannon',
        play_by_play=['Terry', 'Gannon'],
        color=['Robbie', 'Hummel'],
    ),
    'mark-jones': AnnouncerCrew(
        name='Mark Jones',
        folder='mark-jones',
        play_by_play=['Mark', 'Jones'],
        color=['Grant', 'Hill'],
    ),
    'mike-tirico': AnnouncerCrew(
        name='Mike Tirico',
        folder='mike-tirico',
        play_by_play=['Mike', 'Tirico'],
        color=['Reggie', 'Miller'],
    ),
    'ryan-ruocco': AnnouncerCrew(
        name='Ryan Ruocco',
        folder='ryan-ruocco',
        play_by_play=['Ryan', 'Ruocco'],
        color=[],  # Variable
    ),
    'kevin-harlan': AnnouncerCrew(
        name='Kevin Harlan',
        folder='kevin-harlan',
        play_by_play=['Kevin', 'Harlan'],
        color=['Trent', 'Green'],
    ),
    'john-michael': AnnouncerCrew(
        name='John Michael',
        folder='john-michael',
        play_by_play=['John', 'Michael'],
        color=['Kenny', 'Smith'],
    ),
    'mark-follohill': AnnouncerCrew(
        name='Mark Followill',
        folder='mark-followill',
        play_by_play=['Mark', 'Followill'],
        color=['Jamal', 'Crawford'],
    ),
}


def get_announcer_crew(crew_name: str) -> Optional[AnnouncerCrew]:
    """Get announcer crew configuration by name (case-insensitive)."""
    return ANNOUNCER_CREWS.get(crew_name.lower())


def get_all_announcer_names(crew: AnnouncerCrew) -> List[str]:
    """Get all announcer names for a crew (for validation phrases)."""
    names = list(crew.play_by_play) + list(crew.color)
    return names


def list_crews() -> Dict[str, AnnouncerCrew]:
    """Return all available crews."""
    return ANNOUNCER_CREWS


# =============================================================================
# TEAM CONFIGURATIONS
# =============================================================================

# NBA team ticker symbols to full names
TEAM_NAMES: Dict[str, str] = {
    # Eastern Conference - Atlantic
    'bos': 'Celtics', 'celtics': 'Celtics',
    'bkn': 'Nets', 'nets': 'Nets', 'brk': 'Nets',
    'nyk': 'Knicks', 'knicks': 'Knicks', 'ny': 'Knicks',
    'phi': 'Sixers', '76ers': 'Sixers', 'sixers': 'Sixers',
    'tor': 'Raptors', 'raptors': 'Raptors',
    
    # Eastern Conference - Central
    'chi': 'Bulls', 'bulls': 'Bulls',
    'cle': 'Cavaliers', 'cavaliers': 'Cavaliers', 'cavs': 'Cavaliers',
    'det': 'Pistons', 'pistons': 'Pistons',
    'ind': 'Pacers', 'pacers': 'Pacers',
    'mil': 'Bucks', 'bucks': 'Bucks',
    
    # Eastern Conference - Southeast
    'atl': 'Hawks', 'hawks': 'Hawks',
    'cha': 'Hornets', 'hornets': 'Hornets',
    'mia': 'Heat', 'heat': 'Heat',
    'orl': 'Magic', 'magic': 'Magic',
    'was': 'Wizards', 'wizards': 'Wizards',
    
    # Western Conference - Northwest
    'den': 'Nuggets', 'nuggets': 'Nuggets',
    'min': 'Timberwolves', 'timberwolves': 'Timberwolves', 'wolves': 'Timberwolves',
    'okc': 'Thunder', 'thunder': 'Thunder',
    'por': 'Trail Blazers', 'blazers': 'Trail Blazers',
    'uta': 'Jazz', 'jazz': 'Jazz',
    
    # Western Conference - Pacific
    'gsw': 'Warriors', 'warriors': 'Warriors', 'gs': 'Warriors',
    'lac': 'Clippers', 'clippers': 'Clippers',
    'lal': 'Lakers', 'lakers': 'Lakers',
    'phx': 'Suns', 'suns': 'Suns', 'pho': 'Suns',
    'sac': 'Kings', 'kings': 'Kings',
    
    # Western Conference - Southwest
    'dal': 'Mavericks', 'mavericks': 'Mavericks', 'mavs': 'Mavericks',
    'hou': 'Rockets', 'rockets': 'Rockets',
    'mem': 'Grizzlies', 'grizzlies': 'Grizzlies',
    'nop': 'Pelicans', 'pelicans': 'Pelicans', 'no': 'Pelicans',
    'sas': 'Spurs', 'spurs': 'Spurs', 'sa': 'Spurs',
}


def get_team_name(ticker: str) -> str:
    """Get full team name from ticker/abbreviation."""
    return TEAM_NAMES.get(ticker.lower(), ticker.title())


# =============================================================================
# URL AND FILENAME PARSING
# =============================================================================

def parse_filename(filename: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Parse a transcript filename to extract date and teams.
    
    Args:
        filename: Transcript filename like '2025-11-04_magic-at-hawks.txt'
        
    Returns:
        Tuple of (date_str, away_team, home_team, year) or Nones if parsing fails
    """
    m = re.match(r'(\d{4})-(\d{1,2})-(\d{1,2})_([a-z0-9\-]+)-at-([a-z0-9\-]+)\.txt', filename, re.IGNORECASE)
    if m:
        date_str = f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"
        year = m.group(1)
        away = m.group(4)
        home = m.group(5)
        return date_str, away, home, year
    return None, None, None, None


# Valid ticker abbreviations (uppercase) - standard NBA team codes used in Kalshi tickers
TICKER_TEAM_CODES = {
    'BOS', 'BKN', 'NYK', 'PHI', 'TOR',  # Atlantic
    'CHI', 'CLE', 'DET', 'IND', 'MIL',  # Central
    'ATL', 'CHA', 'MIA', 'ORL', 'WAS',  # Southeast
    'DEN', 'MIN', 'OKC', 'POR', 'UTA',  # Northwest
    'GSW', 'GS', 'LAC', 'LAL', 'PHX', 'SAC',  # Pacific
    'DAL', 'HOU', 'MEM', 'NOP', 'NO', 'SAS', 'SA',  # Southwest
}


def extract_teams_from_ticker(ticker: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract teams from a Kalshi ticker like 'KXNBAMENTION-26JAN16MINHOU'.
    
    Args:
        ticker: Kalshi event ticker
        
    Returns:
        Tuple of (away_team, home_team) or (None, None) if parsing fails
    """
    # Extract the teams portion after the date (e.g., "26JAN16MINHOU" -> "MINHOU")
    m = re.search(r'\d{2}[A-Z]{3}\d{2}([A-Z]{4,6})$', ticker)
    if not m:
        return None, None
    
    teams_str = m.group(1)
    
    # Try different split positions to find valid team combinations
    for split_pos in range(2, len(teams_str) - 1):
        away = teams_str[:split_pos]
        home = teams_str[split_pos:]
        
        if away in TICKER_TEAM_CODES and home in TICKER_TEAM_CODES:
            return away.lower(), home.lower()
    
    return None, None


# =============================================================================
# TRANSCRIPTION VARIANT MAPPING (KALSHI PHRASE -> SEARCH ALTERNATIVES)
# =============================================================================
# Kalshi strike phrases may be transcribed as different spellings (e.g. "Air ball",
# "Air-ball", "Airball"). Slash-separated alternatives are passed to the regex
# pattern generator so all forms match. Regex already expands hyphenated words
# (e.g. "Alley-oop" -> "Alley oop", "Alleyoop"); we add mappings for phrases
# that come from Kalshi without hyphens or with different spacing.

# Canonical search strings (one per concept) so we can add many key variants
_AIRBALL_SEARCH = "Airball/Air ball/Air-ball"
_AIRBALLS_SEARCH = "Airballs/Air balls/Air-balls"
_AIRBALLED_SEARCH = "Airballed/Air balled/Air-balled"
_ALLEY_OOP_SEARCH = "Alley-oop/Alley oop/Alleyoop"
_PLAYOFF_SEARCH = "Playoff/Play off/Play-off"
_OVERTIME_SEARCH = "Overtime/Over time/Over-time"

# Single combined phrase Kalshi may return (spaces around slashes)
_AIRBALL_COMBO_SEARCH = "Airball/Air ball/Air-ball/Airballs/Air balls/Air-balls/Airballed/Air balled/Air-balled"

NBA_TRANSCRIPTION_VARIANTS: Dict[str, str] = {
    # Airball / Air balls / Air-ball(s/ed) — all spellings Kalshi might return
    "Airball": _AIRBALL_SEARCH,
    "Air ball": _AIRBALL_SEARCH,
    "Air-ball": _AIRBALL_SEARCH,
    "airball": _AIRBALL_SEARCH,
    "Airballs": _AIRBALLS_SEARCH,
    "Air balls": _AIRBALLS_SEARCH,
    "Air-balls": _AIRBALLS_SEARCH,
    "airballs": _AIRBALLS_SEARCH,
    "Airballed": _AIRBALLED_SEARCH,
    "Air balled": _AIRBALLED_SEARCH,
    "Air-balled": _AIRBALLED_SEARCH,
    "airballed": _AIRBALLED_SEARCH,
    # Kalshi combined phrase (exact yes_sub_title: "Airball / Airballs / Airballed")
    "Airball / Airballs / Airballed": _AIRBALL_COMBO_SEARCH,
    "Airball/Airballs/Airballed": _AIRBALL_COMBO_SEARCH,
    # Alley-oop
    "Alley-oop": _ALLEY_OOP_SEARCH,
    "Alley oop": _ALLEY_OOP_SEARCH,
    "Alleyoop": _ALLEY_OOP_SEARCH,
    "alley-oop": _ALLEY_OOP_SEARCH,
    "alley oop": _ALLEY_OOP_SEARCH,
    "alleyoop": _ALLEY_OOP_SEARCH,
    # Playoff / Play off
    "Playoff": _PLAYOFF_SEARCH,
    "Play off": _PLAYOFF_SEARCH,
    "Play-off": _PLAYOFF_SEARCH,
    "playoff": _PLAYOFF_SEARCH,
    # Overtime / Over time
    "Overtime": _OVERTIME_SEARCH,
    "Over time": _OVERTIME_SEARCH,
    "Over-time": _OVERTIME_SEARCH,
    "overtime": _OVERTIME_SEARCH,
}


def get_search_value_for_phrase(phrase: str) -> str:
    """
    Return the search string (slash-separated alternatives) for a Kalshi/market
    phrase so transcriptions like "Air ball", "alley oop" still match.
    Tries exact key, then stripped, then case-insensitive lookup.
    """
    if not phrase:
        return phrase
    s = phrase.strip()
    if s in NBA_TRANSCRIPTION_VARIANTS:
        return NBA_TRANSCRIPTION_VARIANTS[s]
    # Case-insensitive fallback
    lower = s.lower()
    for key, value in NBA_TRANSCRIPTION_VARIANTS.items():
        if key.lower() == lower:
            return value
    return phrase


# =============================================================================
# CREW LOOKUP HELPERS
# =============================================================================

def find_crew_by_announcer_name(name: str) -> Optional[str]:
    """
    Match a play-by-play announcer name to an ANNOUNCER_CREWS key.

    Compares *name* (case-insensitive) against each crew's ``name`` field.

    Args:
        name: e.g. "Mike Breen", "Noah Eagle"

    Returns:
        The crew key (e.g. "mike-breen") or None if no match.
    """
    if not name:
        return None
    lower = name.strip().lower()
    for key, crew in ANNOUNCER_CREWS.items():
        if crew.name.lower() == lower:
            return key
    return None
