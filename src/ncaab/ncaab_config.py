#!/usr/bin/env python3
"""
NCAAB Configuration Data

Centralized configuration for:
- Team name/code mappings for Kalshi tickers
- Announcer crews and their names
- Transcription variant mappings
"""

import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


SERIES_TICKER = "KXNCAABMENTION"

# =============================================================================
# ANNOUNCER CONFIGURATIONS
# =============================================================================

@dataclass
class AnnouncerCrew:
    """Configuration for an announcer crew."""
    name: str
    folder: str
    play_by_play: List[str]
    color: List[str]
    validation_threshold: float = 5.0


# Common NCAAB broadcast announcers (populated as we discover them from 506)
ANNOUNCER_CREWS: Dict[str, AnnouncerCrew] = {
    'dan-shulman': AnnouncerCrew(
        name='Dan Shulman', folder='dan-shulman',
        play_by_play=['Dan', 'Shulman'], color=['Jay', 'Bilas'],
    ),
    'sean-mcdonough': AnnouncerCrew(
        name='Sean McDonough', folder='sean-mcdonough',
        play_by_play=['Sean', 'McDonough'], color=['Jay', 'Bilas'],
    ),
    'dave-flemming': AnnouncerCrew(
        name='Dave Flemming', folder='dave-flemming',
        play_by_play=['Dave', 'Flemming'], color=[],
    ),
    'jason-benetti': AnnouncerCrew(
        name='Jason Benetti', folder='jason-benetti',
        play_by_play=['Jason', 'Benetti'], color=[],
    ),
    'kevin-harlan': AnnouncerCrew(
        name='Kevin Harlan', folder='kevin-harlan',
        play_by_play=['Kevin', 'Harlan'], color=[],
    ),
    'ian-eagle': AnnouncerCrew(
        name='Ian Eagle', folder='ian-eagle',
        play_by_play=['Ian', 'Eagle'], color=[],
    ),
    'brad-nessler': AnnouncerCrew(
        name='Brad Nessler', folder='brad-nessler',
        play_by_play=['Brad', 'Nessler'], color=[],
    ),
    'brian-anderson': AnnouncerCrew(
        name='Brian Anderson', folder='brian-anderson',
        play_by_play=['Brian', 'Anderson'], color=[],
    ),
    'andrew-catalon': AnnouncerCrew(
        name='Andrew Catalon', folder='andrew-catalon',
        play_by_play=['Andrew', 'Catalon'], color=[],
    ),
    'bob-wischusen': AnnouncerCrew(
        name='Bob Wischusen', folder='bob-wischusen',
        play_by_play=['Bob', 'Wischusen'], color=[],
    ),
    'dave-pasch': AnnouncerCrew(
        name='Dave Pasch', folder='dave-pasch',
        play_by_play=['Dave', 'Pasch'], color=[],
    ),
    'rece-davis': AnnouncerCrew(
        name='Rece Davis', folder='rece-davis',
        play_by_play=['Rece', 'Davis'], color=[],
    ),
    'carter-blackburn': AnnouncerCrew(
        name='Carter Blackburn', folder='carter-blackburn',
        play_by_play=['Carter', 'Blackburn'], color=[],
    ),
    'mark-jones': AnnouncerCrew(
        name='Mark Jones', folder='mark-jones',
        play_by_play=['Mark', 'Jones'], color=[],
    ),
    'ryan-ruocco': AnnouncerCrew(
        name='Ryan Ruocco', folder='ryan-ruocco',
        play_by_play=['Ryan', 'Ruocco'], color=[],
    ),
    'spero-dedes': AnnouncerCrew(
        name='Spero Dedes', folder='spero-dedes',
        play_by_play=['Spero', 'Dedes'], color=[],
    ),
    'tom-mccarthy': AnnouncerCrew(
        name='Tom McCarthy', folder='tom-mccarthy',
        play_by_play=['Tom', 'McCarthy'], color=[],
    ),
}


def get_announcer_crew(crew_name: str) -> Optional[AnnouncerCrew]:
    return ANNOUNCER_CREWS.get(crew_name.lower())


def get_all_announcer_names(crew: AnnouncerCrew) -> List[str]:
    return list(crew.play_by_play) + list(crew.color)


def find_crew_by_announcer_name(name: str) -> Optional[str]:
    if not name:
        return None
    lower = name.strip().lower()
    for key, crew in ANNOUNCER_CREWS.items():
        if crew.name.lower() == lower:
            return key
    return None


# =============================================================================
# TEAM CONFIGURATIONS
# =============================================================================
# Kalshi NCAAB tickers use abbreviated school codes like:
#   KXNCAABMENTION-26FEB13DUKEUNC
# This maps common school names/aliases to Kalshi ticker codes.
# We'll build this dynamically from 506 scraping + Kalshi event data.

TEAM_CODES: Dict[str, str] = {
    # Power conference teams (most likely to be on Kalshi)
    # ACC
    'Duke': 'DUKE', 'Blue Devils': 'DUKE',
    'North Carolina': 'UNC', 'UNC': 'UNC', 'Tar Heels': 'UNC',
    'Virginia': 'UVA', 'Cavaliers': 'UVA', 'UVA': 'UVA',
    'Louisville': 'LOU', 'Cardinals': 'LOU',
    'Clemson': 'CLEM', 'Tigers': 'CLEM',
    'Pittsburgh': 'PITT', 'Pitt': 'PITT', 'Panthers': 'PITT',
    'Syracuse': 'SYR', 'Orange': 'SYR',
    'Wake Forest': 'WAKE', 'Demon Deacons': 'WAKE',
    'Notre Dame': 'ND', 'Fighting Irish': 'ND',
    'Georgia Tech': 'GT', 'Yellow Jackets': 'GT',
    'Miami': 'MIA', 'Hurricanes': 'MIA',
    'Florida State': 'FSU', 'Seminoles': 'FSU',
    'Virginia Tech': 'VT', 'Hokies': 'VT',
    'Boston College': 'BC', 'Eagles': 'BC',
    'NC State': 'NCST', 'Wolfpack': 'NCST',
    'Stanford': 'STAN',
    'California': 'CAL', 'Cal': 'CAL', 'Golden Bears': 'CAL',
    'SMU': 'SMU', 'Mustangs': 'SMU',
    # Big Ten
    'Michigan': 'MICH', 'Wolverines': 'MICH',
    'Michigan State': 'MSU', 'Spartans': 'MSU',
    'Ohio State': 'OSU', 'Buckeyes': 'OSU',
    'Indiana': 'IND', 'Hoosiers': 'IND',
    'Purdue': 'PUR', 'Boilermakers': 'PUR',
    'Wisconsin': 'WIS', 'Badgers': 'WIS',
    'Iowa': 'IOWA',  'Hawkeyes': 'IOWA',
    'Illinois': 'ILL', 'Fighting Illini': 'ILL', 'Illini': 'ILL',
    'Minnesota': 'MINN', 'Golden Gophers': 'MINN', 'Gophers': 'MINN',
    'Penn State': 'PSU', 'Nittany Lions': 'PSU',
    'Maryland': 'UMD', 'Terrapins': 'UMD', 'Terps': 'UMD',
    'Nebraska': 'NEB', 'Cornhuskers': 'NEB', 'Huskers': 'NEB',
    'Northwestern': 'NW', 'Wildcats': 'NW',
    'Rutgers': 'RUT', 'Scarlet Knights': 'RUT',
    'Oregon': 'ORE', 'Ducks': 'ORE',
    'Washington': 'WASH', 'Huskies': 'WASH',
    'USC': 'USC', 'Trojans': 'USC',
    'UCLA': 'UCLA', 'Bruins': 'UCLA',
    # SEC
    'Kentucky': 'UK', 'UK': 'UK',
    'Kansas': 'KAN', 'Jayhawks': 'KAN',
    'KU': 'KU',
    'Tennessee': 'TENN', 'Volunteers': 'TENN', 'Vols': 'TENN',
    'Auburn': 'AUB',
    'Alabama': 'ALA', 'Crimson Tide': 'ALA',
    'Arkansas': 'ARK', 'Razorbacks': 'ARK',
    'Florida': 'FLA', 'Gators': 'FLA',
    'LSU': 'LSU',
    'Mississippi State': 'MSST', 'Bulldogs': 'MSST',
    'Ole Miss': 'MISS', 'Rebels': 'MISS',
    'Georgia': 'UGA',
    'South Carolina': 'SC', 'Gamecocks': 'SC',
    'Missouri': 'MIZ', 'Mizzou': 'MIZ',
    'Vanderbilt': 'VAN', 'Commodores': 'VAN',
    'Texas A&M': 'TAMU', 'Aggies': 'TAMU',
    'Texas': 'TEX', 'Longhorns': 'TEX',
    'Oklahoma': 'OU', 'Sooners': 'OU',
    # Big 12
    'Houston': 'HOU', 'Cougars': 'HOU',
    'Iowa State': 'ISU', 'Cyclones': 'ISU',
    'Baylor': 'BAY', 'Bears': 'BAY',
    'Kansas State': 'KSU',
    'TCU': 'TCU', 'Horned Frogs': 'TCU',
    'Texas Tech': 'TTU', 'Red Raiders': 'TTU',
    'West Virginia': 'WVU', 'Mountaineers': 'WVU',
    'Oklahoma State': 'OKST', 'Cowboys': 'OKST',
    'BYU': 'BYU', 'Cougars': 'BYU',
    'Cincinnati': 'CIN', 'Bearcats': 'CIN',
    'VCU': 'VCU', 'Saint Louis': 'SLU', "St. John's": 'SJU',
    'Northern Iowa': 'UNI', 'Santa Clara': 'SCU', 'California Baptist': 'CBU',
    'Tennessee State': 'TNST',
    'UCF': 'UCF', 'Knights': 'UCF',
    'Colorado': 'COL', 'Buffaloes': 'COL', 'Buffs': 'COL',
    'Arizona': 'ARIZ', 'Wildcats': 'ARIZ',
    'Arizona State': 'ASU', 'Sun Devils': 'ASU',
    'Utah': 'UTAH', 'Utes': 'UTAH',
    # Big East
    'UConn': 'CONN', 'Connecticut': 'CONN',
    'Villanova': 'NOVA',
    'Creighton': 'CREI',
    'Marquette': 'MARQ', 'Golden Eagles': 'MARQ',
    "St. John's": 'SJU', 'Red Storm': 'SJU',
    'Xavier': 'XAV', 'Musketeers': 'XAV',
    'Providence': 'PROV', 'Friars': 'PROV',
    'Seton Hall': 'SH', 'Pirates': 'SH',
    'Butler': 'BUT',
    'Georgetown': 'GTWN', 'Hoyas': 'GTWN',
    'DePaul': 'DEP', 'Blue Demons': 'DEP',
    # Other notable programs
    'Gonzaga': 'GONZ', 'Bulldogs': 'GONZ',
    'Memphis': 'MEM',
    'San Diego State': 'SDSU', 'Aztecs': 'SDSU',
    'Saint Marys': 'SMC', "Saint Mary's": 'SMC', 'Gaels': 'SMC',
    'Dayton': 'DAY', 'Flyers': 'DAY',
    'Nevada': 'NEV', 'Wolf Pack': 'NEV',
    'New Mexico': 'UNM', 'Lobos': 'UNM',
}


def _strip_ranking(name: str) -> str:
    """Strip leading ranking number from team name, e.g. '10 Michigan State' -> 'Michigan State'."""
    return re.sub(r'^\d+\s+', '', name.strip())


def get_team_code(team_name: str) -> Optional[str]:
    """Convert team name to Kalshi ticker code. Strips ranking numbers automatically."""
    team_name = _strip_ranking(team_name)
    if team_name in TEAM_CODES:
        return TEAM_CODES[team_name]
    for name, code in TEAM_CODES.items():
        if name.lower() == team_name.lower():
            return code
    # If it looks like it's already a code (all caps, <=4 chars), return it
    if team_name.isupper() and len(team_name) <= 4:
        return team_name
    return None


_CODE_DISPLAY_NAMES: Dict[str, str] = {
    'KU': 'Kansas',
    'UK': 'Kentucky',
}


def get_team_name(code: str) -> str:
    """Get display name from team code. Returns code if not found."""
    code_upper = code.upper()
    if code_upper in _CODE_DISPLAY_NAMES:
        return _CODE_DISPLAY_NAMES[code_upper]
    # Reverse lookup: find the first full team name for this code
    for name, c in TEAM_CODES.items():
        if c == code_upper and not name.isupper() and len(name) > 4:
            return name
    return code_upper


# Valid ticker codes (uppercase) used in Kalshi tickers
TICKER_TEAM_CODES = set(TEAM_CODES.values())


# =============================================================================
# TICKER PARSING
# =============================================================================

def extract_teams_from_ticker(ticker: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract teams from a Kalshi ticker like 'KXNCAABMENTION-26FEB13DUKEUNC'.

    Returns (away_code, home_code) or (None, None).
    """
    m = re.search(r'\d{2}[A-Z]{3}\d{2}([A-Z]+)$', ticker)
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


def parse_filename(filename: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Parse a transcript filename like '2026-02-13_duke-at-unc.txt'.
    Returns (date_str, away_team, home_team, year).
    """
    m = re.match(r'(\d{4})-(\d{1,2})-(\d{1,2})_([a-z0-9\-]+)-at-([a-z0-9\-]+)\.txt', filename, re.I)
    if m:
        date_str = f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"
        return date_str, m.group(4), m.group(5), m.group(1)
    return None, None, None, None


# =============================================================================
# TRANSCRIPTION VARIANT MAPPING
# =============================================================================

_AIRBALL_SEARCH = "Airball/Air ball/Air-ball"
_AIRBALLS_SEARCH = "Airballs/Air balls/Air-balls"
_AIRBALLED_SEARCH = "Airballed/Air balled/Air-balled"
_AIRBALL_COMBO_SEARCH = "Airball/Air ball/Air-ball/Airballs/Air balls/Air-balls/Airballed/Air balled/Air-balled"
_ALLEY_OOP_SEARCH = "Alley-oop/Alley oop/Alleyoop"
_PLAYOFF_SEARCH = "Playoff/Play off/Play-off"
_OVERTIME_SEARCH = "Overtime/Over time/Over-time"

NCAAB_TRANSCRIPTION_VARIANTS: Dict[str, str] = {
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
    "Airball / Airballed": _AIRBALL_COMBO_SEARCH,
    "Airball/Airballed": _AIRBALL_COMBO_SEARCH,
    "Airball / Airballs / Airballed": _AIRBALL_COMBO_SEARCH,
    "Airball/Airballs/Airballed": _AIRBALL_COMBO_SEARCH,
    "Alley-oop": _ALLEY_OOP_SEARCH,
    "Alley oop": _ALLEY_OOP_SEARCH,
    "Alleyoop": _ALLEY_OOP_SEARCH,
    "alley-oop": _ALLEY_OOP_SEARCH,
    "alley oop": _ALLEY_OOP_SEARCH,
    "alleyoop": _ALLEY_OOP_SEARCH,
    "Playoff": _PLAYOFF_SEARCH,
    "Play off": _PLAYOFF_SEARCH,
    "Play-off": _PLAYOFF_SEARCH,
    "playoff": _PLAYOFF_SEARCH,
    "Overtime": _OVERTIME_SEARCH,
    "Over time": _OVERTIME_SEARCH,
    "Over-time": _OVERTIME_SEARCH,
    "overtime": _OVERTIME_SEARCH,
}


def get_search_value_for_phrase(phrase: str) -> str:
    """Return search alternatives for a Kalshi phrase."""
    if not phrase:
        return phrase
    s = phrase.strip()
    if s in NCAAB_TRANSCRIPTION_VARIANTS:
        return NCAAB_TRANSCRIPTION_VARIANTS[s]
    lower = s.lower()
    for key, value in NCAAB_TRANSCRIPTION_VARIANTS.items():
        if key.lower() == lower:
            return value
    return phrase
