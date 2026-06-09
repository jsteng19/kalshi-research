#!/usr/bin/env python3
"""
Full NBA mentions pipeline: update data, find open events, match crews,
generate reports, and open in browser.

Usage:
    python src/nba/run_pipeline.py
    python src/nba/run_pipeline.py --skip-update        # skip data refresh
    python src/nba/run_pipeline.py --skip-update --skip-series  # use cached series results
"""

import argparse
import os
import subprocess
import sys
import webbrowser
from datetime import datetime
from pathlib import Path

# Ensure imports work
_project_root = Path(__file__).resolve().parent.parent.parent

# Load .env from project root so KALSHI_API_KEY_ID etc. are available
from dotenv import load_dotenv
load_dotenv(_project_root / ".env")
sys.path.insert(0, str(_project_root))
sys.path.insert(0, str(_project_root / "src"))
sys.path.insert(0, str(_project_root / "src" / "nba"))

from src.nba.kalshi_api import (
    get_open_event_tickers,
    get_event_markets,
    fetch_series_results,
    calculate_hit_rates,
    calculate_hit_rates_by_announcer,
)
from src.nba.generate_report import (
    load_announcer_data,
    lookup_crew_for_event,
    build_game_report,
    render_html_report,
)
from src.nba.nba_config import get_announcer_crew, find_crew_by_announcer_name

SERIES = "KXNBAMENTION"
COMBINED_CSV = "data/nba/icdb/combined_announcers.csv"
TRANSCRIPT_DIR = "data/nba/transcripts"


def step(msg: str) -> None:
    print(f"\n{'='*60}\n  {msg}\n{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="Full NBA mentions pipeline")
    parser.add_argument("--skip-update", action="store_true",
                        help="Skip data update (506 + ICDB + merge)")
    parser.add_argument("--skip-series", action="store_true",
                        help="Skip fetching series results (faster, no hit rates)")
    parser.add_argument("-o", "--output", default=None, help="Output HTML path")
    args = parser.parse_args()

    os.chdir(_project_root)

    # ------------------------------------------------------------------
    # 1. Update game data
    # ------------------------------------------------------------------
    if not args.skip_update:
        step("1/5  Updating game data (506 + ICDB + merge)")
        from src.nba.update_game_data import update_506_data, update_icdb_data, merge_all_sources
        try:
            update_506_data()
        except Exception as e:
            print(f"  506 update failed (non-fatal): {e}")
        try:
            update_icdb_data()
        except Exception as e:
            print(f"  ICDB update failed (non-fatal): {e}")
        try:
            merge_all_sources()
        except Exception as e:
            print(f"  Merge failed (non-fatal): {e}")
    else:
        step("1/5  Skipping data update (--skip-update)")

    # ------------------------------------------------------------------
    # 2. Find open events
    # ------------------------------------------------------------------
    step("2/5  Finding open events")
    event_tickers = get_open_event_tickers(SERIES)
    print(f"  Open events: {len(event_tickers)}")
    for t in event_tickers:
        print(f"    {t}")

    if not event_tickers:
        print("  No open events found. Exiting.")
        return

    # ------------------------------------------------------------------
    # 3. Match crews from combined_announcers.csv
    # ------------------------------------------------------------------
    step("3/5  Matching crews")
    announcers_df = load_announcer_data(COMBINED_CSV)

    matched = []
    unmatched = []
    for et in event_tickers:
        pbp, color, network, crew_key = lookup_crew_for_event(et, announcers_df)
        crew = get_announcer_crew(crew_key) if crew_key else None
        if crew:
            matched.append(et)
            print(f"  {et}  ->  {crew.name} / {color}  ({network})")
        else:
            unmatched.append(et)
            pbp_display = pbp or "unknown"
            print(f"  {et}  ->  NO MATCH (PBP: {pbp_display})")

    if not matched:
        print("\n  No events matched a known crew. Nothing to report.")
        return

    print(f"\n  Matched: {len(matched)}  |  Unmatched: {len(unmatched)}")

    # ------------------------------------------------------------------
    # 4. Fetch series results for hit rates
    # ------------------------------------------------------------------
    series_results = {}
    if not args.skip_series:
        step("4/5  Fetching series results (historical hit rates)")
        series_results = fetch_series_results(SERIES, delay=0.1, verbose=True)
    else:
        step("4/5  Skipping series results (--skip-series)")

    # ------------------------------------------------------------------
    # 5. Generate reports for matched games
    # ------------------------------------------------------------------
    step("5/5  Generating per-game reports")
    os.makedirs("reports", exist_ok=True)
    report_paths = []
    for et in matched:
        print(f"  Building report for {et}...")
        game = build_game_report(
            et, announcers_df, series_results,
            data_base_path=TRANSCRIPT_DIR,
        )
        crew_str = game.crew.name if game.crew else game.pbp_name or "unknown"
        print(f"    Crew: {crew_str} | Phrases: {len(game.phrases)}")

        html = render_html_report(game, series_results, announcers_df)
        output_path = args.output if (args.output and len(matched) == 1) else f"reports/{et}_report.html"
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)
        abs_path = os.path.abspath(output_path)
        report_paths.append(abs_path)
        print(f"    Saved: {output_path}")

    # Open all reports in browser
    print(f"\n  Opening {len(report_paths)} report(s) in browser...")
    for p in report_paths:
        webbrowser.open(f"file://{p}")
        print(f"    file://{p}")

    print("\nDone.")


if __name__ == "__main__":
    main()
