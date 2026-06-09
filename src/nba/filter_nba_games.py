#!/usr/bin/env python3
"""
Filter ICDB CSV files to only include NBA games.
Removes WNBA, NCAA, and other non-NBA competitions.
"""
import os
import pandas as pd
from pathlib import Path

def filter_nba_games(input_csv: str, output_csv: str = None) -> int:
    """
    Filter a CSV to only include NBA games.
    
    Args:
        input_csv: Path to input CSV file
        output_csv: Path to output CSV file (defaults to overwriting input)
    
    Returns:
        Number of games filtered out
    """
    if output_csv is None:
        output_csv = input_csv
    
    # Read CSV
    df = pd.read_csv(input_csv)
    original_count = len(df)
    
    # Filter to only NBA competitions
    # Include: NBA Regular Season, NBA Playoffs, NBA Cup, NBA Pre-Season, NBA Finals, etc.
    # Exclude: WNBA, NCAA, The Basketball Tournament, etc.
    df_filtered = df[df['competition'].str.startswith('NBA', na=False)]
    
    filtered_count = len(df_filtered)
    removed_count = original_count - filtered_count
    
    # Save filtered data
    df_filtered.to_csv(output_csv, index=False)
    
    return removed_count

def main():
    """Filter all ICDB CSV files in data/nba/icdb/"""
    icdb_dir = Path('data/nba/icdb')
    
    if not icdb_dir.exists():
        print(f"Error: {icdb_dir} directory not found")
        return
    
    csv_files = sorted(icdb_dir.glob('*.csv'))
    
    if not csv_files:
        print(f"No CSV files found in {icdb_dir}")
        return
    
    print(f"🔍 Filtering {len(csv_files)} CSV files to only include NBA games...\n")
    
    total_original = 0
    total_removed = 0
    
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            original_count = len(df)
            
            removed = filter_nba_games(str(csv_file))
            
            df_after = pd.read_csv(csv_file)
            filtered_count = len(df_after)
            
            total_original += original_count
            total_removed += removed
            
            if removed > 0:
                print(f"✅ {csv_file.name}")
                print(f"   Before: {original_count} games")
                print(f"   After:  {filtered_count} games")
                print(f"   Removed: {removed} non-NBA games\n")
            else:
                print(f"✅ {csv_file.name}: {original_count} games (already all NBA)")
        
        except Exception as e:
            print(f"❌ {csv_file.name}: Error - {e}\n")
    
    print("=" * 60)
    print(f"📊 Summary:")
    print(f"   Total files processed: {len(csv_files)}")
    print(f"   Total games before: {total_original}")
    print(f"   Total games after: {total_original - total_removed}")
    print(f"   Total removed: {total_removed}")
    print(f"   % NBA games: {100 * (total_original - total_removed) / total_original:.1f}%")

if __name__ == "__main__":
    main()
