#!/usr/bin/env python3
"""
NCAAB Video Link Pipeline

Ranks NCAAB announcers by Kalshi game count, then searches YouTube for full
game videos for each of their games. Results are saved to
data/ncaab/video_links.csv with incremental update support.

Usage:
    # Top 10 announcers by Kalshi game count (default)
    python src/ncaab/run_video_pipeline.py

    # Top 20 announcers, scrape mode (no YouTube API quota)
    python src/ncaab/run_video_pipeline.py --top 20 --scrape

    # Specific announcers only
    python src/ncaab/run_video_pipeline.py --announcers "Dan Shulman,Ian Eagle"

    # Update-only: search only games not yet in the CSV
    python src/ncaab/run_video_pipeline.py --update

    # Also retry games that weren't found last time
    python src/ncaab/run_video_pipeline.py --update --retry

    # Dry run: show which games would be searched, don't search
    python src/ncaab/run_video_pipeline.py --dry-run

    # Skip Kalshi API (rank by total game count instead)
    python src/ncaab/run_video_pipeline.py --no-kalshi-rank
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

_project_root = Path(__file__).resolve().parent.parent.parent
load_dotenv(_project_root / ".env")
sys.path.insert(0, str(_project_root))

from src.ncaab.video_finder import (
    NCAABVideoFinder,
    NCAABGame,
    RESULTS_COLUMNS,
    load_existing_results,
    save_results,
    game_key,
    df_game_key,
    games_from_announcers_df,
)

SERIES = "KXNCAABMENTION"
ANNOUNCERS_CSV = _project_root / "data/ncaab" / "combined_announcers.csv"
GAME_CSV = _project_root / "data/ncaab" / "game_announcers.csv"
VIDEO_LINKS_CSV = _project_root / "data/ncaab" / "video_links.csv"


# ---------------------------------------------------------------------------
# Ranking helpers
# ---------------------------------------------------------------------------

def _load_announcers_df() -> pd.DataFrame:
    path = ANNOUNCERS_CSV if ANNOUNCERS_CSV.exists() else GAME_CSV
    if not path.exists():
        raise FileNotFoundError(
            f"No announcer CSV found at {ANNOUNCERS_CSV} or {GAME_CSV}.\n"
            "Run: python src/ncaab/update_game_data.py"
        )
    df = pd.read_csv(path, dtype=str).fillna('')
    print(f"  Loaded {len(df)} games from {path.relative_to(_project_root)}")
    return df


def rank_by_kalshi(announcers_df: pd.DataFrame, top_n: int) -> list[str]:
    """
    Fetch KXNCAABMENTION series from Kalshi, match to announcers_df,
    and return the top_n play-by-play announcers by matched game count.
    Falls back to total game count if Kalshi fetch fails.
    """
    try:
        from src.ncaab.kalshi_api import fetch_series_results
        print("  Fetching Kalshi series results...")
        series_results = fetch_series_results(SERIES, verbose=False)
        kalshi_tickers = set(series_results.keys())
        print(f"  Found {len(kalshi_tickers)} events in Kalshi series")

        # Match on exact ticker
        matched = announcers_df[announcers_df['ticker'].isin(kalshi_tickers)]
        if matched.empty:
            print("  Warning: no ticker matches — falling back to total game count")
            matched = announcers_df
    except Exception as e:
        print(f"  Kalshi fetch failed ({e}) — ranking by total game count")
        matched = announcers_df

    counts = matched['play_by_play'].value_counts()
    top = counts.head(top_n).index.tolist()
    print(f"\n  Top {top_n} announcers by Kalshi game count:")
    for i, name in enumerate(top, 1):
        kalshi_n = (matched['play_by_play'] == name).sum()
        total_n = (announcers_df['play_by_play'] == name).sum()
        print(f"    {i:2d}. {name:30s} {kalshi_n} Kalshi / {total_n} total games")
    return top


def rank_by_total(announcers_df: pd.DataFrame, top_n: int) -> list[str]:
    counts = announcers_df['play_by_play'].value_counts()
    top = counts.head(top_n).index.tolist()
    print(f"\n  Top {top_n} announcers by total game count:")
    for i, name in enumerate(top, 1):
        n = (announcers_df['play_by_play'] == name).sum()
        print(f"    {i:2d}. {name:30s} {n} games")
    return top


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------

def step(msg: str) -> None:
    print(f"\n{'='*62}\n  {msg}\n{'='*62}")


def revalidate_existing(
    existing: pd.DataFrame,
    finder: NCAABVideoFinder,
) -> pd.DataFrame:
    """
    Run validate_title against all existing 'found' rows.
    Rows that fail the new stricter rules are downgraded to 'not_found'
    so they will be re-searched on the next run.
    Returns an updated DataFrame.
    """
    if existing.empty:
        return existing

    df = existing.copy()
    found_mask = df['status'] == 'found'
    invalidated = 0

    for idx, row in df[found_mask].iterrows():
        game = NCAABGame(
            date=str(row.get('date', '')),
            away_team=str(row.get('away_team', '')),
            home_team=str(row.get('home_team', '')),
        )
        title = str(row.get('youtube_title', ''))
        try:
            dur_min = float(row.get('duration_min') or 0)
            dur_sec = int(dur_min * 60)
        except (ValueError, TypeError):
            dur_sec = 0

        channel = str(row.get('channel', '')).strip()
        reason = None
        if channel in finder.KNOWN_BAD_CHANNELS:
            ok, reason = False, f"known bad channel '{channel}'"
        else:
            ok, reason = finder.validate_title(title, game, duration_seconds=dur_sec)

        if not ok:
            df.at[idx, 'status'] = 'not_found'
            df.at[idx, 'youtube_url'] = ''
            df.at[idx, 'youtube_title'] = ''
            df.at[idx, 'duration_min'] = ''
            df.at[idx, 'channel'] = ''
            invalidated += 1
            print(f"  INVALIDATED {row.get('away_team')} at {row.get('home_team')} ({row.get('date')})")
            print(f"    title: {title[:70]}")
            print(f"    reason: {reason}")

    print(f"\n  Invalidated {invalidated} previously 'found' rows → re-queued for search")
    return df


def build_search_queue(
    games: list[NCAABGame],
    existing: pd.DataFrame,
    update_only: bool,
    retry_not_found: bool,
) -> list[NCAABGame]:
    """
    Determine which games still need a YouTube search.

    update_only=True  → only search games not yet in existing CSV
    retry_not_found   → also re-search games with status='not_found'
    """
    if existing.empty:
        return games

    existing_keys = set(existing.apply(df_game_key, axis=1))
    not_found_keys = set(
        existing[existing['status'] == 'not_found'].apply(df_game_key, axis=1)
    )
    found_keys = set(
        existing[existing['status'] == 'found'].apply(df_game_key, axis=1)
    )

    queue = []
    for g in games:
        k = game_key(g)
        if k in found_keys:
            continue  # already have a URL — always skip
        if update_only and k in existing_keys and not (retry_not_found and k in not_found_keys):
            continue  # in CSV but not retrying
        if not update_only or k not in existing_keys or (retry_not_found and k in not_found_keys):
            queue.append(g)

    return queue


def run_search(
    games_to_search: list[NCAABGame],
    existing: pd.DataFrame,
    finder: NCAABVideoFinder,
    max_results: int,
    dry_run: bool,
) -> pd.DataFrame:
    """
    Search YouTube for each game. Returns an updated results DataFrame.
    Existing rows for found games are preserved unchanged.
    """
    rows = {df_game_key(row): dict(row) for _, row in existing.iterrows()}

    total = len(games_to_search)
    found_count = 0

    for i, game in enumerate(games_to_search, 1):
        print(f"\n  [{i}/{total}] {game.display_name}")
        print(f"    PBP: {game.play_by_play} | Network: {game.network}")

        if dry_run:
            print(f"    (dry run — skipping search)")
            continue

        videos = finder.search_video(game, max_results=max_results)

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        key = game_key(game)

        if videos:
            best = videos[0]
            found_count += 1
            print(f"    ✓ {best.title[:70]}")
            print(f"      {best.duration_formatted} | {best.channel}")
            print(f"      {best.url}")
            rows[key] = {
                'ticker': game.ticker,
                'date': game.date,
                'away_team': game.away_team,
                'home_team': game.home_team,
                'network': game.network,
                'play_by_play': game.play_by_play,
                'color_commentator': game.color_commentator,
                'youtube_url': best.url,
                'youtube_title': best.title,
                'duration_min': round(best.duration_seconds / 60, 1),
                'channel': best.channel,
                'status': 'found',
                'searched_at': now,
            }
        else:
            print(f"    ✗ not found")
            rows[key] = {
                'ticker': game.ticker,
                'date': game.date,
                'away_team': game.away_team,
                'home_team': game.home_team,
                'network': game.network,
                'play_by_play': game.play_by_play,
                'color_commentator': game.color_commentator,
                'youtube_url': '',
                'youtube_title': '',
                'duration_min': '',
                'channel': '',
                'status': 'not_found',
                'searched_at': now,
            }

    print(f"\n  Searched {total} games | Found: {found_count} | Not found: {total - found_count}")

    result_df = pd.DataFrame(list(rows.values()))
    if not result_df.empty:
        result_df = result_df.sort_values(['play_by_play', 'date']).reset_index(drop=True)
    return result_df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build NCAAB video links for top announcers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        '--top', type=int, default=10,
        help='Number of top announcers to target (default: 10)',
    )
    parser.add_argument(
        '--announcers', default='',
        help='Comma-separated list of specific PBP announcer names to target',
    )
    parser.add_argument(
        '--no-kalshi-rank', action='store_true',
        help='Rank by total game count instead of Kalshi game count',
    )
    parser.add_argument(
        '--update', action='store_true',
        help='Update mode: only search games not already in video_links.csv',
    )
    parser.add_argument(
        '--retry', action='store_true',
        help='With --update: also retry games with status=not_found',
    )
    parser.add_argument(
        '--scrape', action='store_true',
        help='Use web scraping instead of YouTube API (avoids quota limits)',
    )
    parser.add_argument(
        '--max-results', type=int, default=5,
        help='Max candidate videos to examine per search query (default: 5)',
    )
    parser.add_argument(
        '--min-duration', type=int, default=50,
        help='Minimum video duration in minutes (default: 50)',
    )
    parser.add_argument(
        '--revalidate', action='store_true',
        help='Re-run validation on all existing found results; invalidated rows become not_found',
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Show what would be searched without actually searching',
    )
    parser.add_argument(
        '--output', default=str(VIDEO_LINKS_CSV),
        help=f'Output CSV path (default: {VIDEO_LINKS_CSV})',
    )
    args = parser.parse_args()

    os.chdir(_project_root)

    # ------------------------------------------------------------------
    # 1. Load announcer data
    # ------------------------------------------------------------------
    step("1/4  Loading announcer data")
    announcers_df = _load_announcers_df()

    # ------------------------------------------------------------------
    # 2. Select target announcers
    # ------------------------------------------------------------------
    step("2/4  Selecting target announcers")

    if args.announcers:
        target_announcers = [a.strip() for a in args.announcers.split(',') if a.strip()]
        print(f"  Using specified announcers ({len(target_announcers)}):")
        for a in target_announcers:
            n = (announcers_df['play_by_play'] == a).sum()
            print(f"    - {a}: {n} games")
    elif args.no_kalshi_rank:
        target_announcers = rank_by_total(announcers_df, args.top)
    else:
        target_announcers = rank_by_kalshi(announcers_df, args.top)

    if not target_announcers:
        print("  No announcers selected. Exiting.")
        return 1

    # ------------------------------------------------------------------
    # 3. Build game list and search queue
    # ------------------------------------------------------------------
    step("3/4  Building game list")

    all_games = games_from_announcers_df(announcers_df, target_announcers)
    existing = load_existing_results(args.output)

    # Re-validate existing found results with the current (stricter) rules
    if args.revalidate and not existing.empty:
        print("  Re-validating existing found results...")
        api_key = os.getenv('YOUTUBE_API_KEY')
        use_scrape = args.scrape or not api_key
        _finder = NCAABVideoFinder(
            api_key=api_key if not use_scrape else None,
            use_scraping=use_scrape,
        )
        existing = revalidate_existing(existing, _finder)
        if not args.dry_run:
            save_results(existing, args.output)
            print(f"  Saved revalidated CSV → {Path(args.output).relative_to(_project_root)}")

    print(f"  Total games for selected announcers: {len(all_games)}")
    print(f"  Existing results in CSV:             {len(existing)}")

    already_found = 0
    if not existing.empty:
        found_mask = existing['status'] == 'found'
        already_found = found_mask.sum()
        print(f"  Already found:                       {already_found}")

    queue = build_search_queue(
        all_games, existing,
        update_only=args.update,
        retry_not_found=args.retry,
    )

    print(f"  Games to search:                     {len(queue)}")

    if not queue:
        print("\n  Nothing to search. Use --retry to re-search not_found games.")
        if not existing.empty:
            save_results(existing, args.output)
        return 0

    # ------------------------------------------------------------------
    # 4. Search YouTube
    # ------------------------------------------------------------------
    step("4/4  Searching YouTube")

    api_key = os.getenv('YOUTUBE_API_KEY')
    use_scrape = args.scrape or not api_key

    if use_scrape and not args.scrape:
        print("  YOUTUBE_API_KEY not set — using scrape mode")

    finder = NCAABVideoFinder(
        api_key=api_key if not use_scrape else None,
        use_scraping=use_scrape,
    )
    finder.MIN_DURATION_SECONDS = args.min_duration * 60

    result_df = run_search(
        queue, existing, finder,
        max_results=args.max_results,
        dry_run=args.dry_run,
    )

    if not args.dry_run:
        save_results(result_df, args.output)
        print(f"\n  Saved {len(result_df)} rows → {Path(args.output).relative_to(_project_root)}")

        # Summary by announcer
        if not result_df.empty:
            print("\n  Results by announcer:")
            for pbp, grp in result_df.groupby('play_by_play'):
                n_found = (grp['status'] == 'found').sum()
                n_total = len(grp)
                print(f"    {pbp:30s} {n_found:3d}/{n_total} found")

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
