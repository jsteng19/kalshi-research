#!/usr/bin/env python3
"""
Unified data updater for NBA game/announcer data.

Updates all game data sources and merges them into a single
combined_announcers.csv used by generate_report.py.

Usage:
    python src/nba/update_game_data.py              # Update all sources
    python src/nba/update_game_data.py --506-only    # Only scrape 506 Sports
    python src/nba/update_game_data.py --icdb-only   # Only update ICDB
    python src/nba/update_game_data.py --merge-only  # Only merge existing data
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

# Ensure src/nba and src/ are importable
_nba_dir = Path(__file__).resolve().parent
_src_dir = _nba_dir.parent
_project_root = _src_dir.parent
for p in (_nba_dir, _src_dir, _project_root):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from parse_506_archive import parse_game_line, parse_date_line, games_to_dataframe, get_team_code, TEAM_CODES

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR = _project_root / "data/nba"
GAME_ANNOUNCERS_CSV = DATA_DIR / "game_announcers.csv"
ICDB_DIR = DATA_DIR / "icdb"
COMBINED_CSV = ICDB_DIR / "combined_announcers.csv"


# ---------------------------------------------------------------------------
# 506 Sports — Archive
# ---------------------------------------------------------------------------

def scrape_506_archive() -> pd.DataFrame:
    """
    Scrape all 506 Sports week pages for the full current season via HTTP.

    Returns a DataFrame with columns matching game_announcers.csv.
    """
    from scrape_506_archive import scrape_506_nba_archive
    print("Scraping 506 Sports week pages (full season)...")
    df = scrape_506_nba_archive()
    print(f"  Found {len(df)} games from archive")
    return df


# ---------------------------------------------------------------------------
# 506 Sports — Live page (requests)
# ---------------------------------------------------------------------------

def scrape_506_live() -> pd.DataFrame:
    """
    Scrape the current-week 506sports.com/nba.php page using requests.

    Returns a DataFrame compatible with the archive format.
    """
    from scrape_506_archive import _get_session, _parse_week_page

    print("Scraping 506 Sports live page...")
    try:
        session = _get_session()
        resp = session.get("https://506sports.com/nba.php", timeout=20)
        resp.raise_for_status()
    except Exception as e:
        print(f"  Live scrape failed: {e}")
        return pd.DataFrame()

    games = _parse_week_page(resp.text, 2025)
    df = games_to_dataframe(games)
    print(f"  Found {len(df)} games from live page")
    return df


# ---------------------------------------------------------------------------
# 506 Combined Update
# ---------------------------------------------------------------------------

def update_506_data() -> pd.DataFrame:
    """
    Merge 506 archive + live data, save to game_announcers.csv.

    Live data takes priority for overlapping tickers.
    """
    archive_df = scrape_506_archive()
    live_df = scrape_506_live()

    if archive_df.empty and live_df.empty:
        print("  No 506 data found!")
        return pd.DataFrame()

    if live_df.empty:
        combined = archive_df
    elif archive_df.empty:
        combined = live_df
    else:
        # Live takes priority: drop archive rows where ticker matches live
        live_tickers = set(live_df["ticker"])
        archive_deduped = archive_df[~archive_df["ticker"].isin(live_tickers)]
        combined = pd.concat([archive_deduped, live_df], ignore_index=True)
        combined = combined.sort_values("date").reset_index(drop=True)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_csv(GAME_ANNOUNCERS_CSV, index=False)
    print(f"  Saved {len(combined)} games to {GAME_ANNOUNCERS_CSV}")
    return combined


# ---------------------------------------------------------------------------
# ICDB Update
# ---------------------------------------------------------------------------

def update_icdb_data() -> None:
    """
    Update each *_icdb.csv in data/nba/icdb/ with new games from ICDB.
    """
    from scrape_icdb import update_csv_from_scrape

    if not ICDB_DIR.is_dir():
        print(f"  ICDB directory not found: {ICDB_DIR}")
        return

    csv_files = sorted(ICDB_DIR.glob("*_icdb.csv"))
    if not csv_files:
        print("  No *_icdb.csv files found.")
        return

    print(f"Updating {len(csv_files)} ICDB files...")
    total_new = 0
    for csv_path in csv_files:
        print(f"  {csv_path.name}...", end=" ", flush=True)
        try:
            n = update_csv_from_scrape(str(csv_path))
            total_new += n
            print(f"+{n} new games")
        except Exception as e:
            print(f"error - {e}")

    print(f"  Total new ICDB games: {total_new}")


# ---------------------------------------------------------------------------
# Merge All Sources
# ---------------------------------------------------------------------------

def _generate_ticker(row: pd.Series) -> Optional[str]:
    """Generate a Kalshi ticker from a row with date + team info."""
    from parse_506_archive import generate_kalshi_ticker, get_team_code
    from datetime import datetime

    date = row.get("date")
    if pd.isna(date):
        return None

    # Parse date
    if isinstance(date, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(date.split(".")[0], fmt)
                break
            except ValueError:
                continue
        else:
            return None
    elif isinstance(date, (pd.Timestamp, datetime)):
        dt = date
    else:
        return None

    # Resolve team codes
    away = row.get("away_code") or row.get("away_team")
    home = row.get("home_code") or row.get("home_team")
    if not away or not home:
        return None

    away_code = away if len(str(away)) <= 3 and str(away).isupper() else get_team_code(str(away))
    home_code = home if len(str(home)) <= 3 and str(home).isupper() else get_team_code(str(home))

    if not away_code or not home_code:
        return None

    return generate_kalshi_ticker(dt, away_code, home_code)


def merge_all_sources() -> pd.DataFrame:
    """
    Combine 506 Sports data + ICDB per-announcer CSVs into
    combined_announcers.csv.

    ICDB has priority (more complete commentator details). 506 data fills in
    games where ICDB doesn't have coverage.
    """
    print("Merging all sources...")

    # ---- Load ICDB per-announcer CSVs ----
    icdb_rows = []
    if ICDB_DIR.is_dir():
        for csv_path in sorted(ICDB_DIR.glob("*_icdb.csv")):
            try:
                df = pd.read_csv(csv_path)
                if df.empty:
                    continue
                # Normalize column names
                renames = {
                    "main_commentator": "play_by_play",
                    "co_commentator": "color_commentator",
                    "channel": "network",
                }
                for old, new in renames.items():
                    if old in df.columns and new not in df.columns:
                        df = df.rename(columns={old: new})

                # Keep only NBA games
                if "competition" in df.columns:
                    nba_comps = {
                        "NBA Regular Season", "NBA Playoffs", "NBA Cup",
                        "NBA Pre-Season", "NBA Finals", "NBA All-Star Game",
                    }
                    df = df[df["competition"].isin(nba_comps)]

                icdb_rows.append(df)
            except Exception as e:
                print(f"  Warning: could not read {csv_path.name}: {e}")

    icdb_df = pd.concat(icdb_rows, ignore_index=True) if icdb_rows else pd.DataFrame()
    print(f"  ICDB: {len(icdb_df)} rows from {len(icdb_rows)} files")

    # ---- Load 506 data ----
    five06_df = pd.DataFrame()
    if GAME_ANNOUNCERS_CSV.exists():
        five06_df = pd.read_csv(GAME_ANNOUNCERS_CSV)
        print(f"  506 Sports: {len(five06_df)} rows")

    # ---- Generate tickers for ICDB rows that lack them ----
    if not icdb_df.empty:
        if "ticker" not in icdb_df.columns:
            icdb_df["ticker"] = None
        missing_mask = icdb_df["ticker"].isna() | (icdb_df["ticker"] == "")
        if missing_mask.any():
            icdb_df.loc[missing_mask, "ticker"] = icdb_df.loc[missing_mask].apply(
                _generate_ticker, axis=1
            )

    # ---- Build combined frame with consistent columns ----
    output_cols = [
        "ticker", "date", "away_team", "home_team",
        "play_by_play", "color_commentator", "network",
        "analysts", "presenters", "reporters", "match_url",
    ]

    def _normalize(df: pd.DataFrame) -> pd.DataFrame:
        """Ensure df has all output_cols, drop rows without ticker."""
        for col in output_cols:
            if col not in df.columns:
                df[col] = ""
        df = df[output_cols].copy()
        df = df.dropna(subset=["ticker"])
        df = df[df["ticker"].astype(str).str.strip() != ""]
        return df

    icdb_norm = _normalize(icdb_df) if not icdb_df.empty else pd.DataFrame(columns=output_cols)
    five06_norm = _normalize(five06_df) if not five06_df.empty else pd.DataFrame(columns=output_cols)

    # ---- Merge: ICDB priority, 506 fills gaps ----
    if icdb_norm.empty:
        combined = five06_norm
    elif five06_norm.empty:
        combined = icdb_norm
    else:
        # Remove 506 rows already covered by ICDB
        icdb_tickers = set(icdb_norm["ticker"])
        five06_new = five06_norm[~five06_norm["ticker"].isin(icdb_tickers)]
        combined = pd.concat([icdb_norm, five06_new], ignore_index=True)

    # Deduplicate by ticker (keep first = ICDB)
    combined = combined.drop_duplicates(subset=["ticker"], keep="first")
    combined = combined.sort_values("date", ascending=False).reset_index(drop=True)

    ICDB_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_csv(COMBINED_CSV, index=False)
    print(f"  Saved {len(combined)} games to {COMBINED_CSV}")
    return combined


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Update NBA game/announcer data from all sources.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--506-only", action="store_true", dest="five06_only",
                        help="Only scrape 506 Sports (archive + live)")
    parser.add_argument("--icdb-only", action="store_true",
                        help="Only update ICDB per-announcer CSVs")
    parser.add_argument("--merge-only", action="store_true",
                        help="Only merge existing data into combined CSV")
    args = parser.parse_args()

    if args.five06_only:
        update_506_data()
    elif args.icdb_only:
        update_icdb_data()
    elif args.merge_only:
        merge_all_sources()
    else:
        # Full update
        update_506_data()
        update_icdb_data()
        merge_all_sources()

    print("\nDone.")


if __name__ == "__main__":
    main()
