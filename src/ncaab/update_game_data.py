#!/usr/bin/env python3
"""
Unified data updater for NCAAB game/announcer data.

Updates 506 Sports sources and saves to data/ncaab/game_announcers.csv.

Usage:
    python src/ncaab/update_game_data.py              # Update all
    python src/ncaab/update_game_data.py --506-only    # Only scrape 506
    python src/ncaab/update_game_data.py --merge-only  # Only merge
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

_project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.ncaab.parse_506 import (
    scrape_506_archive,
    scrape_506_live,
    games_to_dataframe,
    generate_kalshi_ticker,
    get_team_code,
)
from src.ncaab.ncaab_config import SERIES_TICKER

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR = _project_root / "data/ncaab"
GAME_ANNOUNCERS_CSV = DATA_DIR / "game_announcers.csv"
COMBINED_CSV = DATA_DIR / "combined_announcers.csv"


# ---------------------------------------------------------------------------
# 506 Sports Update
# ---------------------------------------------------------------------------

def update_506_data() -> pd.DataFrame:
    """
    Merge 506 archive + live data, save to game_announcers.csv.
    Live data takes priority for overlapping tickers.
    """
    archive_df = pd.DataFrame()
    try:
        archive_df = scrape_506_archive()
        print(f"  Archive: {len(archive_df)} games")
    except Exception as e:
        print(f"  Archive scrape failed: {e}")

    live_df = pd.DataFrame()
    try:
        live_df = scrape_506_live()
        print(f"  Live: {len(live_df)} games")
    except Exception as e:
        print(f"  Live scrape failed: {e}")

    if archive_df.empty and live_df.empty:
        print("  No 506 data found!")
        return pd.DataFrame()

    if live_df.empty:
        combined = archive_df
    elif archive_df.empty:
        combined = live_df
    else:
        live_tickers = set(live_df["ticker"])
        archive_deduped = archive_df[~archive_df["ticker"].isin(live_tickers)]
        combined = pd.concat([archive_deduped, live_df], ignore_index=True)
        combined = combined.sort_values("date").reset_index(drop=True)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_csv(GAME_ANNOUNCERS_CSV, index=False)
    print(f"  Saved {len(combined)} games to {GAME_ANNOUNCERS_CSV}")
    return combined


# ---------------------------------------------------------------------------
# Merge (currently just 506, but structured for future ICDB or other sources)
# ---------------------------------------------------------------------------

def merge_all_sources() -> pd.DataFrame:
    """
    Build combined_announcers.csv from available sources.
    Currently just 506 Sports data, but structured to add ICDB etc.
    """
    print("Merging sources...")

    output_cols = [
        "ticker", "date", "away_team", "home_team",
        "play_by_play", "color_commentator", "network",
    ]

    five06_df = pd.DataFrame()
    if GAME_ANNOUNCERS_CSV.exists():
        five06_df = pd.read_csv(GAME_ANNOUNCERS_CSV)
        print(f"  506 Sports: {len(five06_df)} rows")

    if five06_df.empty:
        print("  No data to merge")
        return pd.DataFrame(columns=output_cols)

    # Normalize columns
    for col in output_cols:
        if col not in five06_df.columns:
            five06_df[col] = ""

    combined = five06_df[output_cols].copy()
    combined = combined.dropna(subset=["ticker"])
    combined = combined[combined["ticker"].astype(str).str.strip() != ""]
    combined = combined.drop_duplicates(subset=["ticker"], keep="first")
    combined = combined.sort_values("date", ascending=False).reset_index(drop=True)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_csv(COMBINED_CSV, index=False)
    print(f"  Saved {len(combined)} games to {COMBINED_CSV}")
    return combined


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Update NCAAB game/announcer data")
    parser.add_argument("--506-only", action="store_true", dest="five06_only")
    parser.add_argument("--merge-only", action="store_true")
    args = parser.parse_args()

    os.chdir(_project_root)

    if args.five06_only:
        update_506_data()
    elif args.merge_only:
        merge_all_sources()
    else:
        update_506_data()
        merge_all_sources()

    print("\nDone.")


if __name__ == "__main__":
    main()
