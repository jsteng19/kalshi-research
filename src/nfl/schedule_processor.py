#!/usr/bin/env python3
"""
NFL CSV Processor

Simple script to read NFL schedule CSVs and generate organized NFL+ URLs.
Supports multiple CSV formats and automatically detects the format.
"""

import csv
import re
import os
import argparse
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from pathlib import Path


class NFLCSVProcessor:
    """Processes NFL schedule CSVs and generates NFL+ URLs."""
    
    def __init__(self):
        self.team_mapping = {
            # Map various team name formats to URL-friendly names
            'arizona cardinals': 'cardinals', 'cardinals': 'cardinals', 'ari': 'cardinals',
            'atlanta falcons': 'falcons', 'falcons': 'falcons', 'atl': 'falcons',
            'baltimore ravens': 'ravens', 'ravens': 'ravens', 'bal': 'ravens',
            'buffalo bills': 'bills', 'bills': 'bills', 'buf': 'bills',
            'carolina panthers': 'panthers', 'panthers': 'panthers', 'car': 'panthers',
            'chicago bears': 'bears', 'bears': 'bears', 'chi': 'bears',
            'cincinnati bengals': 'bengals', 'bengals': 'bengals', 'cin': 'bengals',
            'cleveland browns': 'browns', 'browns': 'browns', 'cle': 'browns',
            'dallas cowboys': 'cowboys', 'cowboys': 'cowboys', 'dal': 'cowboys',
            'denver broncos': 'broncos', 'broncos': 'broncos', 'den': 'broncos',
            'detroit lions': 'lions', 'lions': 'lions', 'det': 'lions',
            'green bay packers': 'packers', 'packers': 'packers', 'gb': 'packers',
            'houston texans': 'texans', 'texans': 'texans', 'hou': 'texans',
            'indianapolis colts': 'colts', 'colts': 'colts', 'ind': 'colts',
            'jacksonville jaguars': 'jaguars', 'jaguars': 'jaguars', 'jax': 'jaguars',
            'kansas city chiefs': 'chiefs', 'chiefs': 'chiefs', 'kc': 'chiefs',
            'las vegas raiders': 'raiders', 'raiders': 'raiders', 'lv': 'raiders', 'lvr': 'raiders',
            'los angeles chargers': 'chargers', 'chargers': 'chargers', 'lac': 'chargers',
            'los angeles rams': 'rams', 'rams': 'rams', 'lar': 'rams',
            'miami dolphins': 'dolphins', 'dolphins': 'dolphins', 'mia': 'dolphins',
            'minnesota vikings': 'vikings', 'vikings': 'vikings', 'min': 'vikings',
            'new england patriots': 'patriots', 'patriots': 'patriots', 'ne': 'patriots',
            'new orleans saints': 'saints', 'saints': 'saints', 'no': 'saints',
            'new york giants': 'giants', 'giants': 'giants', 'nyg': 'giants',
            'new york jets': 'jets', 'jets': 'jets', 'nyj': 'jets',
            'philadelphia eagles': 'eagles', 'eagles': 'eagles', 'phi': 'eagles',
            'pittsburgh steelers': 'steelers', 'steelers': 'steelers', 'pit': 'steelers',
            'san francisco 49ers': '49ers', '49ers': '49ers', 'sf': '49ers',
            'seattle seahawks': 'seahawks', 'seahawks': 'seahawks', 'sea': 'seahawks',
            'tampa bay buccaneers': 'buccaneers', 'buccaneers': 'buccaneers', 'tb': 'buccaneers',
            'tennessee titans': 'titans', 'titans': 'titans', 'ten': 'titans',
            'washington commanders': 'commanders', 'commanders': 'commanders', 'was': 'commanders',
        }
    
    def normalize_team_name(self, team_name: str) -> str:
        """Convert any team name format to URL-friendly format."""
        clean_name = team_name.lower().strip()
        # Remove common prefixes/suffixes
        clean_name = re.sub(r'\s+(at|@|vs|v)\s+', ' ', clean_name)
        clean_name = clean_name.replace('.', '').replace(',', '')
        
        return self.team_mapping.get(clean_name, clean_name.replace(' ', '-'))
    
    def parse_date_string(self, date_str: str) -> str:
        """Parse various date formats to YYYY-MM-DD."""
        if not date_str or date_str.lower() in ['', 'tbd', 'n/a']:
            return "2025-01-01"  # Default fallback
        
        # Clean up the date string
        clean_date = date_str.strip().replace('.', '').replace(',', '')
        
        # Try various date formats
        formats = [
            "%Y-%m-%d",          # 2025-09-22
            "%m/%d/%Y",          # 09/22/2025
            "%m/%d/%y",          # 09/22/25
            "%b %d %Y",          # Sep 22 2025
            "%B %d %Y",          # September 22 2025
            "%d %b %Y",          # 22 Sep 2025
            "%d %B %Y",          # 22 September 2025
        ]
        
        # Fix common abbreviation issues
        clean_date = clean_date.replace('Sept ', 'Sep ')
        
        for fmt in formats:
            try:
                date_obj = datetime.strptime(clean_date, fmt)
                return date_obj.strftime("%Y-%m-%d")
            except ValueError:
                continue
        
        print(f"⚠️  Could not parse date: '{date_str}', using default")
        return "2025-01-01"
    
    def parse_matchup_string(self, matchup: str) -> Tuple[Optional[str], Optional[str]]:
        """Parse matchup string to get away and home teams."""
        if not matchup:
            return None, None
        
        # Clean up the matchup string
        clean = matchup.strip()
        
        # Remove location info in parentheses
        clean = re.sub(r'\s*\([^)]+\)', '', clean)
        
        # Split on various separators
        separators = [' at ', ' @ ', ' vs ', ' v ']
        for sep in separators:
            if sep in clean.lower():
                parts = clean.lower().split(sep.lower())
                if len(parts) == 2:
                    away = self.normalize_team_name(parts[0].strip())
                    home = self.normalize_team_name(parts[1].strip())
                    return away, home
        
        return None, None
    
    def determine_home_away_from_winner_loser(self, winner: str, loser: str, at_symbol: str = "") -> Tuple[str, str]:
        """
        Determine home vs away team from winner/loser and @ symbol.
        The @ symbol indicates the winner played AT the loser's home field.
        Returns (away_team, home_team)
        """
        winner_normalized = self.normalize_team_name(winner)
        loser_normalized = self.normalize_team_name(loser)
        
        if at_symbol.strip() == "@":
            # Winner played AT loser's home field
            # Winner is away, loser is home
            away_team = winner_normalized
            home_team = loser_normalized
        else:
            # Winner played at home
            # Winner is home, loser is away
            away_team = loser_normalized
            home_team = winner_normalized
        
        return away_team, home_team
    
    def extract_year_from_filename(self, csv_file: str) -> int:
        """Extract year from filename like '2024-nfl-schedule.csv'."""
        filename = Path(csv_file).name
        # Look for 4-digit year in filename
        year_match = re.search(r'(\d{4})', filename)
        if year_match:
            return int(year_match.group(1))
        # Default to current year if not found
        return datetime.now().year
    
    def detect_csv_format(self, csv_file: str) -> Dict[str, str]:
        """Detect the CSV format and return column mappings."""
        # For the specific 2024 NFL results format, we need to handle multiple unnamed columns
        # Let's read the raw header line first
        with open(csv_file, 'r', encoding='utf-8') as f:
            header_line = f.readline().strip()
        
        # Split by comma and detect the @ symbol column position
        raw_headers = header_line.split(',')
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            headers = [h.lower().strip() for h in reader.fieldnames or []]
            
            # Try to detect common column patterns
            mappings = {}
            
            # Date columns  
            date_candidates = ['date', 'game_date', 'gamedate', 'day', 'when']
            for col in headers:
                if any(candidate in col for candidate in date_candidates):
                    mappings['date'] = col
                    break
            
            # Matchup/teams columns
            matchup_candidates = ['matchup', 'game', 'teams', 'vs', 'opponent']
            for col in headers:
                if any(candidate in col for candidate in matchup_candidates) and col != mappings.get('date'):
                    mappings['matchup'] = col
                    break
            
            # Away team columns
            away_candidates = ['away', 'visitor', 'road', 'away_team']
            for col in headers:
                if any(candidate in col for candidate in away_candidates):
                    mappings['away'] = col
                    break
            
            # Home team columns
            home_candidates = ['home', 'home_team']
            for col in headers:
                if any(candidate in col for candidate in home_candidates):
                    mappings['home'] = col
                    break
            
            # Winner/Loser columns (for results format)
            # Need to check the original fieldnames, not lowercased headers
            original_fieldnames = reader.fieldnames or []
            for col in original_fieldnames:
                if col and ('winner' in col.lower() or 'Winner' in col):
                    mappings['winner'] = col
                    break
            
            for col in original_fieldnames:
                if col and ('loser' in col.lower() or 'Loser' in col):
                    mappings['loser'] = col
                    break
            
            # For the @ symbol, we need to track the column index
            # Find Winner/tie and Loser/tie positions in raw headers
            winner_idx = -1
            loser_idx = -1
            for i, header in enumerate(raw_headers):
                if 'winner/tie' in header.lower():
                    winner_idx = i
                elif 'loser/tie' in header.lower():
                    loser_idx = i
            
            # The @ symbol should be between winner and loser
            if winner_idx >= 0 and loser_idx >= 0 and loser_idx > winner_idx:
                at_symbol_idx = winner_idx + 1
                mappings['at_symbol_idx'] = at_symbol_idx
            
            # Week columns
            week_candidates = ['week', 'wk', 'week_num']
            for col in headers:
                if any(candidate in col for candidate in week_candidates):
                    mappings['week'] = col
                    break
            
            # Game type columns
            type_candidates = ['type', 'game_type', 'category', 'network']
            for col in headers:
                if any(candidate in col for candidate in type_candidates):
                    mappings['type'] = col
                    break
            
            return mappings
    
    def process_csv(self, csv_file: str, year: int = 2025) -> List[Dict]:
        """Process a CSV file and return game data."""
        print(f"📄 Processing CSV: {csv_file}")
        
        # Detect format
        mappings = self.detect_csv_format(csv_file)
        print(f"🔍 Detected columns: {mappings}")
        
        games = []
        
        # Also read raw lines to get @ symbol data
        with open(csv_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row_num, row in enumerate(reader, 1):
                try:
                    # Extract date - try 'Date' column first, then mapped date column
                    date_str = ""
                    if 'Date' in row:
                        date_str = row.get('Date', "")
                    elif 'date' in mappings:
                        date_str = row.get(mappings['date'], "")
                    
                    # Stop processing if we hit the playoffs section
                    if date_str and date_str.lower().strip() == 'playoffs':
                        print(f"📋 Reached playoffs section at row {row_num}, stopping regular season processing")
                        break
                    
                    game_date = self.parse_date_string(date_str)
                    
                    # Extract teams
                    away_team = None
                    home_team = None
                    
                    if 'matchup' in mappings:
                        # Single matchup column
                        matchup_str = row.get(mappings['matchup'], "")
                        away_team, home_team = self.parse_matchup_string(matchup_str)
                    elif 'away' in mappings and 'home' in mappings:
                        # Separate away/home columns
                        away_team = self.normalize_team_name(row.get(mappings['away'], ""))
                        home_team = self.normalize_team_name(row.get(mappings['home'], ""))
                    elif 'winner' in mappings and 'loser' in mappings:
                        # Winner/Loser format - need to determine home/away using raw CSV data
                        winner_name = row.get(mappings['winner'], "")
                        loser_name = row.get(mappings['loser'], "")
                        
                        # Get @ symbol from raw line if we have the index
                        at_symbol = ""
                        if 'at_symbol_idx' in mappings and row_num < len(lines):
                            raw_line = lines[row_num].strip()  # row_num is 1-based, lines[0] is header
                            raw_values = raw_line.split(',')
                            at_symbol_idx = mappings['at_symbol_idx']
                            if at_symbol_idx < len(raw_values):
                                at_symbol = raw_values[at_symbol_idx].strip()
                        
                        away_team, home_team = self.determine_home_away_from_winner_loser(
                            winner_name, loser_name, at_symbol
                        )
                    
                    if not away_team or not home_team:
                        print(f"⚠️  Row {row_num}: Could not parse teams from {row}")
                        continue
                    
                    # Extract week - try 'Week' column first, then mapped week column
                    week = 1
                    if 'Week' in row:
                        try:
                            week_str = row.get('Week', '1')
                            if week_str and week_str.strip():
                                week = int(week_str)
                        except (ValueError, TypeError):
                            week = 1
                    elif 'week' in mappings:
                        try:
                            week = int(row.get(mappings['week'], 1))
                        except (ValueError, TypeError):
                            week = 1
                    
                    # Generate URL
                    matchup_string = f"{away_team}-at-{home_team}"
                    nfl_url = f"https://www.nfl.com/plus/games/{matchup_string}-{year}-reg-{week}"
                    
                    # Determine game type
                    game_type = 'regular'
                    if 'type' in mappings:
                        type_str = row.get(mappings['type'], '').lower()
                        if 'thursday' in type_str or 'tnf' in type_str:
                            game_type = 'thursday_night'
                        elif 'monday' in type_str or 'mnf' in type_str:
                            game_type = 'monday_night'
                        elif 'sunday night' in type_str or 'snf' in type_str:
                            game_type = 'sunday_night'
                    
                    # Extract time for Sunday Night Football detection
                    game_time = row.get('Time', '') if 'Time' in row else ''
                    
                    game_info = {
                        'date': game_date,
                        'away_team': away_team,
                        'home_team': home_team,
                        'matchup_string': matchup_string,
                        'week': week,
                        'type': game_type,
                        'url': nfl_url,
                        'original_row': row_num,
                        'time': game_time
                    }
                    
                    games.append(game_info)
                    
                except Exception as e:
                    print(f"⚠️  Row {row_num}: Error processing - {e}")
                    continue
        
        print(f"✅ Processed {len(games)} games from {csv_file}")
        return games
    
    def organize_games(self, games: List[Dict]) -> Dict[str, List[Dict]]:
        """Organize games by day of the week."""
        organized = {
            'all': games,
            'thursday_night': [],
            'sunday_night': [],
            'monday_night': [],
        }
        
        # Group Sunday games by date to find the latest game each Sunday
        sunday_games_by_date = {}
        
        for game in games:
            try:
                # Parse the date to get day of week
                game_date = datetime.strptime(game['date'], '%Y-%m-%d')
                day_of_week = game_date.strftime('%A').lower()  # monday, tuesday, etc.
                
                if day_of_week == 'thursday':
                    organized['thursday_night'].append(game)
                elif day_of_week == 'sunday':
                    # Group Sunday games by date
                    date_str = game['date']
                    if date_str not in sunday_games_by_date:
                        sunday_games_by_date[date_str] = []
                    sunday_games_by_date[date_str].append(game)
                elif day_of_week == 'monday':
                    organized['monday_night'].append(game)
            except (ValueError, KeyError):
                # If we can't parse the date, skip categorization
                continue
        
        # For each Sunday, find the game with the latest timestamp and add to sunday_night
        for date_str, sunday_games in sunday_games_by_date.items():
            if sunday_games:
                # Find the game with the latest time
                latest_game = self.find_latest_game_by_time(sunday_games)
                organized['sunday_night'].append(latest_game)
        
        return organized
    
    def find_latest_game_by_time(self, games: List[Dict]) -> Dict:
        """Find the game with the latest time from a list of games."""
        if not games:
            return None
        
        def parse_time(time_str: str) -> int:
            """Convert time string like '8:20PM' to minutes since midnight for comparison."""
            if not time_str:
                return 0
            
            try:
                # Handle formats like '8:20PM', '1:00PM', etc.
                time_str = time_str.strip().upper()
                if 'PM' in time_str or 'AM' in time_str:
                    time_part = time_str.replace('PM', '').replace('AM', '').strip()
                    hour, minute = map(int, time_part.split(':'))
                    
                    # Convert to 24-hour format
                    if 'PM' in time_str and hour != 12:
                        hour += 12
                    elif 'AM' in time_str and hour == 12:
                        hour = 0
                    
                    return hour * 60 + minute
                else:
                    # If no AM/PM, assume it's already in 24-hour format
                    parts = time_str.split(':')
                    hour = int(parts[0])
                    minute = int(parts[1]) if len(parts) > 1 else 0
                    return hour * 60 + minute
            except (ValueError, IndexError):
                return 0
        
        # Find game with latest time
        latest_game = games[0]
        latest_time = parse_time(latest_game.get('time', ''))
        
        for game in games[1:]:
            game_time = parse_time(game.get('time', ''))
            if game_time > latest_time:
                latest_time = game_time
                latest_game = game
        
        return latest_game
    
    def write_output_files(self, organized_games: Dict[str, List[Dict]], output_dir: str, year: int):
        """Write organized CSV files with year-based naming."""
        os.makedirs(output_dir, exist_ok=True)
        
        # Mapping of game types to filename formats
        filename_mapping = {
            'all': f"{year}-all-games.csv",
            'thursday_night': f"{year}-thursday-night.csv",
            'sunday_night': f"{year}-sunday-night.csv", 
            'monday_night': f"{year}-monday-night.csv",
        }
        
        for game_type, game_list in organized_games.items():
            if not game_list:
                continue
                
            filename = os.path.join(output_dir, filename_mapping[game_type])
            
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['date', 'url'])  # Header
                
                for game in game_list:
                    writer.writerow([game['date'], game['url']])
            
            print(f"✅ Wrote {len(game_list)} games to {filename}")
    
    def print_summary(self, organized_games: Dict[str, List[Dict]]):
        """Print a summary of processed games."""
        print("\n" + "="*60)
        print("NFL CSV PROCESSING SUMMARY")
        print("="*60)
        
        for game_type, game_list in organized_games.items():
            if not game_list:
                continue
                
            print(f"\n{game_type.replace('_', ' ').title()}: {len(game_list)} games")
            
            if game_list:
                print("  Sample games:")
                for game in game_list[:3]:
                    print(f"    {game['date']}: {game['away_team']} at {game['home_team']}")
                if len(game_list) > 3:
                    print(f"    ... and {len(game_list) - 3} more")


def main():
    parser = argparse.ArgumentParser(description='Process NFL schedule CSV and generate NFL+ URLs')
    parser.add_argument('csv_file', help='Path to NFL schedule CSV file')
    parser.add_argument('--year', type=int, default=2025, help='Season year (default: 2025)')
    parser.add_argument('--output-dir', default='data-football', help='Output directory for URL files')
    parser.add_argument('--summary-only', action='store_true', help='Only print summary, don\'t write files')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.csv_file):
        print(f"❌ CSV file not found: {args.csv_file}")
        return
    
    # Process the CSV
    processor = NFLCSVProcessor()
    
    # Extract year from filename if not provided via command line
    if args.year == 2025:  # Default value, try to extract from filename
        extracted_year = processor.extract_year_from_filename(args.csv_file)
        year_to_use = extracted_year
    else:
        year_to_use = args.year
    
    games = processor.process_csv(args.csv_file, year_to_use)
    
    if not games:
        print("❌ No games found in CSV file")
        return
    
    # Organize games
    organized_games = processor.organize_games(games)
    
    # Print summary
    processor.print_summary(organized_games)
    
    # Write output files unless summary-only
    if not args.summary_only:
        processor.write_output_files(organized_games, args.output_dir, year_to_use)
        
        print(f"\n🎉 Complete! Use the generated CSV files with:")
        print(f"  # All games:")
        print(f"  python src/nfl/auto_nfl_downloader.py --file {args.output_dir}/{year_to_use}-all-games.csv")
        print(f"  # Monday Night Football only:")
        print(f"  python src/nfl/auto_nfl_downloader.py --file {args.output_dir}/{year_to_use}-monday-night.csv")
        print(f"  # Thursday Night Football only:")
        print(f"  python src/nfl/auto_nfl_downloader.py --file {args.output_dir}/{year_to_use}-thursday-night.csv")
        print(f"  # Sunday Night Football only:")
        print(f"  python src/nfl/auto_nfl_downloader.py --file {args.output_dir}/{year_to_use}-sunday-night.csv")


if __name__ == '__main__':
    main()
