#!/usr/bin/env python3
"""
Parse ICDB game list exports to CSV for the audio download pipeline.

The ICDB site (basketball.icdb.tv) exports game lists in this format:
    DD/MM/YYYY HH:MM
    Away Team @ Home Team
    Competition
    Channels
    Contributor

This script parses that format and outputs a CSV compatible with
game_audio_pipeline.py. Use --update <existing.csv> to merge in only
new games since the latest date in the existing CSV.
"""

import re
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple
import argparse


def parse_icdb_games(input_path: str, competition_filter: str = "NBA Regular Season") -> pd.DataFrame:
    """
    Parse ICDB game list text file.
    
    Args:
        input_path: Path to the text file
        competition_filter: Only include games matching this competition (None for all)
        
    Returns:
        DataFrame with columns: date, away_team, home_team, competition, channels
    """
    with open(input_path, 'r') as f:
        lines = [line.strip() for line in f.readlines()]
    
    games = []
    i = 0
    
    while i < len(lines):
        # Skip empty lines
        if not lines[i]:
            i += 1
            continue
        
        # Try to parse as date line (DD/MM/YYYY HH:MM)
        date_match = re.match(r'(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2})', lines[i])
        
        if date_match and i + 4 <= len(lines):
            date_str = date_match.group(1)
            time_str = date_match.group(2)
            
            # Parse the game entry
            match_line = lines[i + 1] if i + 1 < len(lines) else ''
            competition = lines[i + 2] if i + 2 < len(lines) else ''
            channels = lines[i + 3] if i + 3 < len(lines) else ''
            # contributor = lines[i + 4] if i + 4 < len(lines) else ''
            
            # Parse match (Away @ Home)
            if '@' in match_line:
                parts = match_line.split('@')
                away_team = parts[0].strip()
                home_team = parts[1].strip()
                
                # Convert date to YYYY-MM-DD format
                try:
                    dt = datetime.strptime(date_str, '%d/%m/%Y')
                    date_formatted = dt.strftime('%Y-%m-%d')
                except ValueError:
                    date_formatted = date_str
                
                games.append({
                    'date': date_formatted,
                    'away_team': away_team,
                    'home_team': home_team,
                    'competition': competition,
                    'channels': channels,
                })
            
            i += 5  # Move to next game entry
        else:
            i += 1
    
    df = pd.DataFrame(games)
    
    # Filter by competition if specified
    if competition_filter and not df.empty:
        df = df[df['competition'] == competition_filter].copy()
    
    # Sort by date descending (most recent first)
    if not df.empty:
        df = df.sort_values('date', ascending=False).reset_index(drop=True)
    
    return df


def get_latest_date_from_csv(csv_path: str) -> Optional[str]:
    """Return the latest game date (YYYY-MM-DD) in an existing CSV, or None."""
    path = Path(csv_path)
    if not path.exists():
        return None
    try:
        df = pd.read_csv(csv_path)
        if df.empty or 'date' not in df.columns:
            return None
        df['_parsed'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['_parsed'])
        if df.empty:
            return None
        return df['_parsed'].max().strftime('%Y-%m-%d')
    except Exception:
        return None


def add_pipeline_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add fields needed for the audio download pipeline.
    """
    if df.empty:
        return df
    
    df = df.copy()
    
    # Generate matchup string
    df['matchup'] = df['away_team'] + ' @ ' + df['home_team']
    
    # Add status columns for pipeline
    df['status'] = 'pending'
    df['audio_file'] = ''
    
    return df


def _merge_into_csv(
    parsed_df: pd.DataFrame,
    existing_path: Path,
    competition_filter: Optional[str],
) -> Tuple[pd.DataFrame, int]:
    """Merge parsed_df into existing CSV; return (combined_df, num_new_added)."""
    latest = get_latest_date_from_csv(str(existing_path))
    existing_df = pd.read_csv(existing_path)
    if latest and not parsed_df.empty:
        new_only = parsed_df[parsed_df['date'] > latest].copy()
        if new_only.empty:
            return existing_df, 0
        for col in ('matchup', 'status', 'audio_file'):
            if col not in existing_df.columns:
                if col == 'matchup' and 'away_team' in existing_df.columns and 'home_team' in existing_df.columns:
                    existing_df['matchup'] = existing_df['away_team'] + ' @ ' + existing_df['home_team']
                elif col == 'status':
                    existing_df['status'] = 'pending'
                elif col == 'audio_file':
                    existing_df['audio_file'] = ''
        new_only = add_pipeline_fields(new_only)
        combined = pd.concat([existing_df, new_only], ignore_index=True)
        combined = combined.drop_duplicates(subset=['date', 'away_team', 'home_team'], keep='first')
        combined = combined.sort_values('date', ascending=False).reset_index(drop=True)
        return combined, len(new_only)
    # No latest or parsed empty: replace or keep existing
    if parsed_df.empty:
        return existing_df, 0
    return add_pipeline_fields(parsed_df), len(parsed_df)


def _find_export_for_csv(csv_path: Path) -> Optional[Path]:
    """Find a text export for this CSV. Expects {stem}.txt or {stem}_export.txt where stem = csv stem minus _icdb."""
    stem = csv_path.stem
    if stem.endswith('_icdb'):
        stem = stem[:-5]  # ian_eagle_icdb -> ian_eagle
    d = csv_path.parent
    for name in (f"{stem}.txt", f"{stem}_export.txt"):
        p = d / name
        if p.exists():
            return p
    return None


def main():
    parser = argparse.ArgumentParser(
        description='Parse ICDB game list to CSV; optionally merge only new games into an existing CSV.',
    )
    parser.add_argument(
        'input',
        nargs='?',
        help='Input text file from ICDB (required unless --update-dir)',
    )
    parser.add_argument('-o', '--output', help='Output CSV file (default: input_games.csv, or --update path when using --update)')
    parser.add_argument(
        '--update',
        metavar='CSV',
        help='Merge only games newer than the latest in this existing CSV (e.g. data/nba/icdb/ian_eagle_icdb.csv)',
    )
    parser.add_argument(
        '--update-dir',
        metavar='DIR',
        help='Update all *_icdb.csv in DIR; for each CSV, use matching export {name}.txt or {name}_export.txt in same dir (e.g. data/nba/icdb)',
    )
    parser.add_argument('--competition', default='NBA Regular Season',
                        help='Filter by competition (default: NBA Regular Season)')
    parser.add_argument('--all', action='store_true', help='Include all competitions')

    args = parser.parse_args()
    competition_filter = None if args.all else args.competition

    # --- Update all CSVs in a directory (one line) ---
    if args.update_dir:
        dir_path = Path(args.update_dir)
        if not dir_path.is_dir():
            print(f"Error: not a directory: {dir_path}")
            return 1
        csv_files = sorted(dir_path.glob('*_icdb.csv'))
        if not csv_files:
            print(f"No *_icdb.csv files in {dir_path}")
            return 1
        total_new = 0
        for csv_path in csv_files:
            export_path = _find_export_for_csv(csv_path)
            if not export_path:
                stem = csv_path.stem[:-5] if csv_path.stem.endswith('_icdb') else csv_path.stem
                print(f"  {csv_path.name}: no export file (expect {stem}.txt or {stem}_export.txt)")
                continue
            try:
                df = parse_icdb_games(str(export_path), competition_filter)
                combined, n = _merge_into_csv(df, csv_path, competition_filter)
                combined.to_csv(csv_path, index=False)
                total_new += n
                print(f"  {csv_path.name}: +{n} new games (from {export_path.name})")
            except Exception as e:
                print(f"  {csv_path.name}: error - {e}")
        print(f"Total new games added: {total_new}")
        return 0

    # --- Single input / single --update ---
    if not args.input:
        print("Error: input file required (or use --update-dir)")
        return 1

    df = parse_icdb_games(args.input, competition_filter)
    print(f"Parsed {len(df)} games from {args.input}")

    if df.empty and not args.update:
        print("No games found!")
        return 1

    if args.update:
        existing_path = Path(args.update)
        if not existing_path.exists():
            print(f"Error: --update target does not exist: {existing_path}")
            return 1
        combined, _ = _merge_into_csv(df, existing_path, competition_filter)
        df = combined
        output_path = args.output or str(existing_path)
    else:
        df = add_pipeline_fields(df)
        output_path = args.output or str(Path(args.input).parent / f"{Path(args.input).stem}_games.csv")

    if df.empty:
        print("No games to write.")
        return 1

    df.to_csv(output_path, index=False)
    print(f"Saved to {output_path}")

    print(f"\n=== Summary ===")
    print(f"Total games: {len(df)}")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")

    print(f"\nFirst 10 games:")
    print(df[['date', 'away_team', 'home_team']].head(10).to_string(index=False))

    return 0


if __name__ == '__main__':
    exit(main())
