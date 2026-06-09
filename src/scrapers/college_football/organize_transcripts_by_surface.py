#!/usr/bin/env python3
"""
Organize college football transcripts by field surface (turf/grass).
"""

import os
import re
import shutil
from pathlib import Path
from typing import Optional, Tuple

# Mapping of college football teams to their stadium surface type
# Key format: lowercase team name or common variations
STADIUM_SURFACES = {
    # SEC Teams
    'alabama': 'grass',  # Bryant-Denny Stadium
    'georgia': 'grass',  # Sanford Stadium
    'texas': 'grass',  # DKR-Texas Memorial Stadium
    'tennessee': 'grass',  # Neyland Stadium (since 1994)
    'auburn': 'grass',  # Jordan-Hare Stadium
    'vanderbilt': 'grass',  # FirstBank Stadium
    'ole miss': 'grass',  # Vaught-Hemingway Stadium
    'mississippi': 'grass',  # Vaught-Hemingway Stadium
    'lsu': 'grass',  # Tiger Stadium
    'florida': 'grass',  # Ben Hill Griffin Stadium
    'kentucky': 'grass',  # Kroger Field
    'south carolina': 'grass',  # Williams-Brice Stadium
    'missouri': 'grass',  # Faurot Field
    'arkansas': 'grass',  # Donald W. Reynolds Razorback Stadium
    'mississippi state': 'grass',  # Davis Wade Stadium
    'texas a&m': 'grass',  # Kyle Field
    'a&m': 'grass',  # Kyle Field
    
    # Big 12 Teams
    'oklahoma': 'grass',  # Gaylord Family Oklahoma Memorial Stadium
    'oklahoma state': 'turf',  # Boone Pickens Stadium
    'kansas': 'grass',  # David Booth Kansas Memorial Stadium
    'kansas state': 'grass',  # Bill Snyder Family Football Stadium
    'iowa state': 'grass',  # Jack Trice Stadium
    'baylor': 'grass',  # McLane Stadium
    'tcu': 'grass',  # Amon G. Carter Stadium
    'west virginia': 'turf',  # Mountaineer Field
    'texas tech': 'grass',  # Jones AT&T Stadium
    'houston': 'grass',  # TDECU Stadium
    
    # Big Ten Teams
    'michigan': 'grass',  # Michigan Stadium
    'ohio state': 'grass',  # Ohio Stadium
    'penn state': 'grass',  # Beaver Stadium
    'michigan state': 'grass',  # Spartan Stadium
    'wisconsin': 'grass',  # Camp Randall Stadium
    'nebraska': 'turf',  # Memorial Stadium
    'iowa': 'grass',  # Kinnick Stadium
    'indiana': 'grass',  # Memorial Stadium
    'purdue': 'grass',  # Ross-Ade Stadium
    'illinois': 'grass',  # Memorial Stadium
    'northwestern': 'grass',  # Ryan Field
    'minnesota': 'turf',  # Huntington Bank Stadium
    'maryland': 'turf',  # SECU Stadium
    'rutgers': 'grass',  # SHI Stadium
    
    # ACC Teams
    'clemson': 'grass',  # Memorial Stadium
    'florida state': 'grass',  # Doak Campbell Stadium
    'miami': 'grass',  # Hard Rock Stadium
    'north carolina': 'grass',  # Kenan Stadium
    'north carolina state': 'turf',  # Carter-Finley Stadium
    'virginia tech': 'grass',  # Lane Stadium
    'louisville': 'grass',  # L&N Federal Credit Union Stadium
    'pittsburgh': 'grass',  # Acrisure Stadium
    'syracuse': 'turf',  # JMA Wireless Dome
    'boston college': 'grass',  # Alumni Stadium
    'virginia': 'grass',  # Scott Stadium
    'duke': 'grass',  # Wallace Wade Stadium
    'wake forest': 'grass',  # Allegacy Federal Credit Union Stadium
    'georgia tech': 'grass',  # Bobby Dodd Stadium
    'notre dame': 'turf',  # Notre Dame Stadium (switched 2014)
    
    # PAC-12 Teams (historical)
    'washington': 'turf',  # Husky Stadium
    'oregon': 'turf',  # Autzen Stadium
    'usc': 'grass',  # Los Angeles Memorial Coliseum
    'ucla': 'grass',  # Rose Bowl
    'stanford': 'grass',  # Stanford Stadium
    'california': 'grass',  # California Memorial Stadium
    'utah': 'turf',  # Rice-Eccles Stadium
    'colorado': 'grass',  # Folsom Field
    'arizona state': 'grass',  # Mountain America Stadium
    'arizona': 'grass',  # Arizona Stadium
    
    # Other notable teams
    'smu': 'grass',  # Gerald J. Ford Stadium
}

# Neutral site games that need special handling
NEUTRAL_SITES = {
    'championship': None,  # Will need to look up specific venue
    'bowl': None,  # Will need to look up specific bowl
    'sugar bowl': 'turf',  # Mercedes-Benz Superdome
    'rose bowl': 'grass',  # Rose Bowl
    'playoff': None,  # Will need to look up specific venue
}

def normalize_team_name(name: str) -> str:
    """Normalize team name for lookup."""
    name = name.lower()
    # Remove common prefixes
    name = re.sub(r'^#\d+\s+', '', name)  # Remove rankings like "#11"
    name = re.sub(r'\s+vols?$', '', name)  # Remove "Vols" suffix
    name = re.sub(r'\s+bulldogs?$', '', name)  # Remove "Bulldogs" suffix
    name = re.sub(r'\s+tigers?$', '', name)  # Remove "Tigers" suffix
    name = name.strip()
    return name

def extract_teams_from_filename(filename: str) -> Tuple[Optional[str], Optional[str]]:
    """Extract away and home team from filename."""
    # Common patterns:
    # "Team A vs Team B"
    # "Team A vs. Team B"
    # "Team A @ Team B"
    # "Team A at Team B"
    
    # Remove file extension
    name = os.path.splitext(filename)[0]
    
    # Handle special format: "date： (rank) Team A vs (rank) Team B Full Game： extra"
    # Try this pattern early before too much cleaning
    match = re.search(r'\((\d+)\)\s+(\w+(?:\s+\w+)?)\s+vs\.?\s+\((\d+)\)\s+(\w+(?:\s+\w+)?)(?:\s+Full|\s+Game|\s+|$)', name, re.IGNORECASE)
    if match:
        away = normalize_team_name(match.group(2))
        home_raw = match.group(4)
        # Remove "Full" or "Game" if captured
        home_raw = re.sub(r'\s+Full.*$', '', home_raw, flags=re.IGNORECASE)
        home_raw = re.sub(r'\s+Game.*$', '', home_raw, flags=re.IGNORECASE)
        home = normalize_team_name(home_raw)
        return away, home
    
    # Remove common prefixes like "Full replay", "Full Game", etc.
    name_clean = re.sub(r'^Full\s+replay\s+', '', name, flags=re.IGNORECASE)
    name_clean = re.sub(r'^Full\s+Game\s+', '', name_clean, flags=re.IGNORECASE)
    
    # For championship games with format "Year ... Championship Team A vs Team B"
    # Extract teams that come after "vs"
    name_lower = name_clean.lower()
    if 'championship' in name_lower or ('playoff' in name_lower and 'championship' not in name_lower):
        # Pattern: "... Championship Team A vs Team B ..."
        # Pattern: "... Playoff ... Team A vs Team B ..."
        match = re.search(r'(.+?)\s+vs\.?\s+(.+?)(?:\s+Full|\s+Game|\s+Replay|\s+HD|\s+\(|$)', name_clean, re.IGNORECASE)
        if match:
            away_raw = match.group(1)
            home_raw = match.group(2)
            # Remove championship/playoff descriptors from away team
            away_raw = re.sub(r'.*?(?:championship|playoff)\s+', '', away_raw, flags=re.IGNORECASE)
            # Extract team name - often has ranking like "#1 Michigan"
            away_match = re.search(r'#?\d+\s*(.+)', away_raw, re.IGNORECASE)
            if away_match:
                away = normalize_team_name(away_match.group(1))
            else:
                away = normalize_team_name(away_raw)
            home = normalize_team_name(home_raw)
            return away, home
    
    # Remove common suffixes that come after team names
    # Remove phrases like "Full Game", "HD", "NCAAF", dates, etc.
    name_clean = re.sub(r'\s+Full\s+Game[：:：].*$', '', name_clean, flags=re.IGNORECASE)  # Handle Chinese colon after "Full Game"
    name_clean = re.sub(r'\s+Full\s+Game.*$', '', name_clean, flags=re.IGNORECASE)
    name_clean = re.sub(r'\s+Replay.*$', '', name_clean, flags=re.IGNORECASE)
    name_clean = re.sub(r'\s+\(.*?\).*$', '', name_clean)  # Remove everything after first (
    name_clean = re.sub(r'\s+\|.*$', '', name_clean)  # Remove everything after |
    name_clean = re.sub(r'\s+[：:].*$', '', name_clean)  # Remove everything after colon (regular or Chinese)
    name_clean = re.sub(r'\s+HD.*$', '', name_clean, flags=re.IGNORECASE)
    name_clean = re.sub(r'\s+NCAAF.*$', '', name_clean, flags=re.IGNORECASE)
    name_clean = re.sub(r'\d{1,2}[⧸/]\d{1,2}[⧸/]\d{4}.*$', '', name_clean)  # Remove dates like "11⧸15⧸2025"
    name_clean = re.sub(r'\s+\(.*$', '', name_clean)  # Remove parenthetical notes like "(2024)"
    
    # Try "@" pattern first (for away/home games)
    match = re.search(r'(.+?)\s+@\s+(.+)', name_clean, re.IGNORECASE)
    if match:
        away_raw = match.group(1)
        home_raw = match.group(2)
        # Remove year or other suffixes from home team
        home_raw = re.sub(r'\s+\(\d{4}\).*$', '', home_raw)
        away = normalize_team_name(away_raw)
        home = normalize_team_name(home_raw)
        return away, home
    
    # Try "vs" pattern (most common)
    # First try to handle format like "date： (rank) Team A vs (rank) Team B Full Game： extra"
    match = re.search(r'\(\d+\)\s+(\w+(?:\s+\w+)?)\s+vs\.?\s+\(\d+\)\s+(\w+(?:\s+\w+)?)', name_clean, re.IGNORECASE)
    if match:
        away = normalize_team_name(match.group(1))
        home = normalize_team_name(match.group(2))
        return away, home
    
    match = re.search(r'(.+?)\s+vs\.?\s+(.+)', name_clean, re.IGNORECASE)
    
    if match:
        away_raw = match.group(1)
        home_raw = match.group(2)
        
        # Clean up away team - remove prefixes like "Game 13 - Big 12 Championship - #7"
        away_raw = re.sub(r'.*?-\s*#?\d+\s+', '', away_raw)  # Remove "Game X - ... #Y"
        away_raw = re.sub(r'^.*?championship\s*-\s*', '', away_raw, flags=re.IGNORECASE)
        # Remove date prefixes like "11⧸1⧸25： (9)" or "11/1/25： (9)"
        away_raw = re.sub(r'^\d+[⧸/]\d+[⧸/]\d+[：:]\s*', '', away_raw)
        away_raw = re.sub(r'^\(\d+\)\s+', '', away_raw)  # Remove leading (9) or similar
        # Remove parenthetical rankings like "(9)"
        away_raw = re.sub(r'^\(\d+\)\s*', '', away_raw)
        
        away = normalize_team_name(away_raw)
        
        # Clean up home team name - remove common descriptive phrases
        home_raw = re.sub(r'\s+Third\s+Saturday.*$', '', home_raw, flags=re.IGNORECASE)
        home_raw = re.sub(r'\s+Vols?.*$', '', home_raw, flags=re.IGNORECASE)
        home_raw = re.sub(r'\s+Football.*$', '', home_raw, flags=re.IGNORECASE)
        # Remove dates and other suffixes
        home_raw = re.sub(r'\s+DIEGO\s+PAVILLA.*$', '', home_raw, flags=re.IGNORECASE)
        home_raw = re.sub(r'\s+VS\s+ARCH\s+MANNING.*$', '', home_raw, flags=re.IGNORECASE)
        # Remove parenthetical rankings like "(20)"
        home_raw = re.sub(r'^\s*\(\d+\)\s*', '', home_raw)
        home_raw = re.sub(r'\s+\([^)]*\)', '', home_raw)  # Remove any parenthetical content
        home_raw = re.sub(r'\d{1,2}[⧸/]\d{1,2}[⧸/]\d{4}.*$', '', home_raw)
        home_raw = re.sub(r'\d{4}.*$', '', home_raw)  # Remove years at end
        
        home = normalize_team_name(home_raw)
        return away, home
    
    # Try "at" pattern
    match = re.search(r'(.+?)\s+at\s+(.+)', name_clean, re.IGNORECASE)
    if match:
        away_raw = match.group(1)
        home_raw = match.group(2)
        home_raw = re.sub(r'\s+\(\d{4}\).*$', '', home_raw)  # Remove (2024)
        away = normalize_team_name(away_raw)
        home = normalize_team_name(home_raw)
        return away, home
    
    return None, None

def get_surface_for_team(team: str) -> Optional[str]:
    """Get surface type for a team's home stadium."""
    team_lower = team.lower().strip()
    
    # Direct lookup
    if team_lower in STADIUM_SURFACES:
        return STADIUM_SURFACES[team_lower]
    
    # Try partial matches for common variations
    for key, surface in STADIUM_SURFACES.items():
        if key in team_lower or team_lower in key:
            return surface
    
    # Check for neutral site indicators
    for neutral, surface in NEUTRAL_SITES.items():
        if neutral in team_lower:
            return surface
    
    return None

def determine_surface(filename: str) -> Optional[str]:
    """Determine field surface from filename."""
    name_lower = filename.lower()
    
    # Check if it's a neutral site game first
    if 'national championship' in name_lower:
        # National Championship 2024 was in Houston (NRG Stadium) - turf
        if '2024' in name_lower or '2025' in name_lower:
            return 'turf'  # NRG Stadium in Houston
    elif 'big 12 championship' in name_lower:
        return 'turf'  # AT&T Stadium in Arlington
    elif 'sugar bowl' in name_lower:
        return 'turf'  # Mercedes-Benz Superdome
    elif 'rose bowl' in name_lower:
        return 'grass'  # Rose Bowl
    elif 'playoff' in name_lower and ('championship' in name_lower or 'national' in name_lower):
        # CFP National Championship
        if '2024' in name_lower or '2025' in name_lower:
            return 'turf'  # NRG Stadium in Houston for 2024
    
    # Extract teams for regular games
    away, home = extract_teams_from_filename(filename)
    
    if not home:
        return None
    
    # For regular games, the second team is typically the home team
    surface = get_surface_for_team(home)
    if surface:
        return surface
    
    # If home team lookup failed, try away team (sometimes format is reversed)
    surface = get_surface_for_team(away)
    return surface

def organize_transcripts(base_dir: str):
    """Organize transcripts into turf and grass subdirectories."""
    base_path = Path(base_dir)
    
    # Create subdirectories
    turf_dir = base_path / 'turf'
    grass_dir = base_path / 'grass'
    turf_dir.mkdir(exist_ok=True)
    grass_dir.mkdir(exist_ok=True)
    
    # Get all txt files
    txt_files = list(base_path.glob('*.txt'))
    
    organized = {'turf': [], 'grass': [], 'unknown': []}
    
    for file_path in txt_files:
        surface = determine_surface(file_path.name)
        
        if surface == 'turf':
            dest = turf_dir / file_path.name
            shutil.move(str(file_path), str(dest))
            organized['turf'].append(file_path.name)
        elif surface == 'grass':
            dest = grass_dir / file_path.name
            shutil.move(str(file_path), str(dest))
            organized['grass'].append(file_path.name)
        else:
            organized['unknown'].append(file_path.name)
            print(f"Warning: Could not determine surface for {file_path.name}")
    
    print(f"\nOrganized {len(organized['turf'])} files into turf/")
    print(f"Organized {len(organized['grass'])} files into grass/")
    if organized['unknown']:
        print(f"\nCould not determine surface for {len(organized['unknown'])} files:")
        for f in organized['unknown']:
            print(f"  - {f}")
    
    return organized

if __name__ == '__main__':
    transcript_dir = '/Users/jstenger/Documents/repos/kalshi-research/data/football/college/mcdonough-mcelroy/transcripts'
    organize_transcripts(transcript_dir)

