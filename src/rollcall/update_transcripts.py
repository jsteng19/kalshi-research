#!/usr/bin/env python3
"""
update_transcripts.py

Fetches new Trump transcripts from Factbase API, saves raw files, processes
them into speaker-filtered processed-transcripts, and does the same for Vance.

Usage:
    python src/update_transcripts.py
    python src/update_transcripts.py --since 2026-03-01
    python src/update_transcripts.py --lookback 14   # days back from latest file
"""

import argparse
import glob
import os
import re
import sys
from datetime import datetime, timedelta

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.rollcall.factbase_api import FactbaseAPI
from src.rollcall.process_transcripts import process_transcript


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def latest_file_date(search_dir: str) -> datetime | None:
    """Return the latest YYYY-MM-DD date found in filenames under search_dir."""
    today = datetime.now()
    latest = None
    for path in glob.glob(os.path.join(search_dir, "**", "*.txt"), recursive=True):
        m = re.match(r"^(\d{4}-\d{2}-\d{2})_", os.path.basename(path))
        if not m:
            continue
        try:
            d = datetime.strptime(m.group(1), "%Y-%m-%d")
        except ValueError:
            continue
        if d > today:
            continue
        if latest is None or d > latest:
            latest = d
    return latest


def cleanup(base_dir: str) -> None:
    """Remove empty .txt files and empty directories under base_dir."""
    removed = 0
    for txt in glob.glob(os.path.join(base_dir, "**", "*.txt"), recursive=True):
        if os.path.getsize(txt) == 0:
            os.remove(txt)
            removed += 1
    for root, dirs, _ in os.walk(base_dir, topdown=False):
        for d in dirs:
            dp = os.path.join(root, d)
            if os.path.isdir(dp) and not os.listdir(dp):
                os.rmdir(dp)
    if removed:
        print(f"  Removed {removed} empty file(s)")


# Category prefixes the old scraper embedded in filenames
_CATEGORY_PREFIXES = [
    "speech", "press_briefing", "press_conference", "press_gaggle",
    "remarks", "interview", "vlog", "prepared_remarks", "weekly_address",
    "donald_trump_vlog", "melania_trump_vlog",
]
# Suffix pattern: _-_february_24_2026  or  _-_februrary_23_2026  etc.
_DATE_SUFFIX_RE = re.compile(r"_-_[a-z]+_\d+_\d{4}$")


def _normalize_stem(stem: str) -> str:
    """Strip leading category prefix and trailing date suffix from a filename stem."""
    for prefix in _CATEGORY_PREFIXES:
        if stem.startswith(prefix + "_"):
            stem = stem[len(prefix) + 1:]
            break
    return _DATE_SUFFIX_RE.sub("", stem)


def dedup_dir(directory: str) -> int:
    """
    Remove duplicate files in a single directory.

    Two files are duplicates when they share the same date and their stems
    normalize to the same string (one is the 'clean' new-API name, the other
    has a category prefix and/or date suffix from the old scraper).

    Also deletes any '*_factbase_transcripts_roll_call.txt' garbage files.

    Keeps the clean (no prefix/suffix) version; if none exists, keeps the
    shortest filename.
    """
    from collections import defaultdict

    if not os.path.isdir(directory):
        return 0

    # Delete known-garbage files first
    removed = 0
    for fname in os.listdir(directory):
        if fname.endswith("_factbase_transcripts_roll_call.txt"):
            path = os.path.join(directory, fname)
            print(f"  Deleting garbage file: {fname}")
            os.remove(path)
            removed += 1

    # Group remaining files by (date, normalized_stem)
    groups: dict = defaultdict(list)
    for fname in os.listdir(directory):
        if not fname.endswith(".txt"):
            continue
        m = re.match(r"^(\d{4}-\d{2}-\d{2})_(.+)\.txt$", fname)
        if not m:
            continue
        date, stem = m.group(1), m.group(2)
        norm = _normalize_stem(stem)
        groups[(date, norm)].append(fname)

    for (date, norm), files in groups.items():
        if len(files) <= 1:
            continue
        # Prefer file whose stem is already the canonical form (no prefix/suffix)
        clean = [f for f in files if f == f"{date}_{norm}.txt"]
        decorated = [f for f in files if f not in clean]

        if clean:
            to_delete = decorated
            keeper = clean[0]
        else:
            files_sorted = sorted(files, key=len)
            keeper = files_sorted[0]
            to_delete = files_sorted[1:]

        for f in to_delete:
            print(f"  Removing duplicate: {f}  (keeping: {keeper})")
            os.remove(os.path.join(directory, f))
            removed += 1

    return removed


def dedup_tree(base_dir: str) -> int:
    """Run dedup_dir on every subdirectory under base_dir."""
    total = 0
    for root, dirs, _ in os.walk(base_dir):
        total += dedup_dir(root)
    if total:
        print(f"  Deduped {total} file(s) under {os.path.basename(base_dir)}/")
    return total


# ---------------------------------------------------------------------------
# Trump raw transcripts
# ---------------------------------------------------------------------------

def fetch_trump(data_dir: str, since: datetime, max_workers: int = 12) -> int:
    api = FactbaseAPI(person="trump", max_workers=max_workers)
    print(f"\n[Trump] Fetching transcripts since {since.strftime('%Y-%m-%d')} ...")
    transcripts = api.get_transcripts_since(since)
    print(f"[Trump] {len(transcripts)} transcripts found")
    if not transcripts:
        return 0
    processed = api.process_transcripts(transcripts, data_dir=data_dir, skip_existing=True)
    cleanup(os.path.join(data_dir, "raw-transcripts"))
    return len(processed)


# ---------------------------------------------------------------------------
# Trump processed transcripts
# ---------------------------------------------------------------------------

def process_trump(data_dir: str, since: datetime) -> int:
    raw_dir = os.path.join(data_dir, "raw-transcripts")
    out_dir = os.path.join(data_dir, "processed-transcripts")
    trump_patterns = [r"Donald\s+Trump\s*:\s*"]
    briefing_patterns = [r"Karoline\s+Leavitt\s*:\s*"]

    count = 0
    for root, _, files in os.walk(raw_dir):
        rel = os.path.relpath(root, raw_dir)
        for fname in sorted(files):
            if not fname.endswith(".txt"):
                continue
            m = re.match(r"^(\d{4}-\d{2}-\d{2})_", fname)
            if m:
                try:
                    if datetime.strptime(m.group(1), "%Y-%m-%d") < since:
                        continue
                except ValueError:
                    pass
            out_path = os.path.join(out_dir, rel, fname)
            if os.path.exists(out_path):
                continue
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            category = rel.lower()
            patterns = briefing_patterns if "press briefing" in category else trump_patterns
            try:
                process_transcript(os.path.join(root, fname), out_path, patterns)
                count += 1
            except Exception as e:
                print(f"  Error processing {fname}: {e}")
    cleanup(out_dir)
    return count


# ---------------------------------------------------------------------------
# Vance processed transcripts (extracted from Trump raw)
# ---------------------------------------------------------------------------

def process_vance(trump_raw_dir: str, vance_out_dir: str, since: datetime) -> int:
    vance_patterns = [r"J\.?D\.?\s+Vance\s*:\s*", r"JD\s+Vance\s*:\s*"]

    # Build set of existing output filenames (basename only, to handle manual moves)
    existing = set()
    for root, _, files in os.walk(vance_out_dir):
        for f in files:
            if f.endswith(".txt"):
                existing.add(f)

    count = 0
    for root, _, files in os.walk(trump_raw_dir):
        rel = os.path.relpath(root, trump_raw_dir)
        for fname in sorted(files):
            if not fname.endswith(".txt"):
                continue
            m = re.match(r"^(\d{4}-\d{2}-\d{2})_", fname)
            if m:
                try:
                    if datetime.strptime(m.group(1), "%Y-%m-%d") < since:
                        continue
                except ValueError:
                    pass
            if fname in existing:
                continue
            out_path = os.path.join(vance_out_dir, rel, fname)
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            try:
                process_transcript(os.path.join(root, fname), out_path, vance_patterns)
                count += 1
            except Exception as e:
                print(f"  Error processing Vance/{fname}: {e}")
    cleanup(vance_out_dir)
    return count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Update Factbase transcripts")
    parser.add_argument("--since", help="Fetch since date (YYYY-MM-DD). Overrides --lookback.")
    parser.add_argument("--lookback", type=int, default=7,
                        help="Days before latest existing transcript to start fetching (default: 7)")
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--dedup-only", action="store_true",
                        help="Only deduplicate existing files, skip fetching")
    args = parser.parse_args()

    data_dir = os.path.join(PROJECT_ROOT, "data")
    raw_dir = os.path.join(data_dir, "raw-transcripts")
    processed_dir = os.path.join(data_dir, "processed-transcripts")
    vance_dir = os.path.join(PROJECT_ROOT, "data/vance", "processed-transcripts")

    if args.dedup_only:
        print("Deduplicating raw-transcripts ...")
        dedup_tree(raw_dir)
        print("Deduplicating processed-transcripts ...")
        dedup_tree(processed_dir)
        print("Deduplicating data/vance/processed-transcripts ...")
        dedup_tree(vance_dir)
        print("\nDone.")
        return

    # Determine since_date
    if args.since:
        since = datetime.strptime(args.since, "%Y-%m-%d")
        print(f"Using provided since date: {since.strftime('%Y-%m-%d')}")
    else:
        latest = latest_file_date(raw_dir)
        if latest:
            since = latest - timedelta(days=args.lookback)
            print(f"Latest existing transcript: {latest.strftime('%Y-%m-%d')}")
        else:
            since = datetime.now() - timedelta(days=30)
            print("No existing transcripts found, defaulting to 30-day lookback")
        print(f"Fetching since: {since.strftime('%Y-%m-%d')}")

    # Step 1: fetch raw
    n_raw = fetch_trump(data_dir, since, max_workers=args.workers)
    print(f"[Trump] Saved {n_raw} new raw transcript(s)")

    # Step 2: dedup raw
    print("[Trump] Deduplicating raw-transcripts ...")
    dedup_tree(raw_dir)

    # Step 3: process Trump
    print(f"\n[Trump] Processing raw → processed-transcripts ...")
    n_processed = process_trump(data_dir, since)
    print(f"[Trump] Processed {n_processed} transcript(s)")

    # Step 4: dedup processed
    print("[Trump] Deduplicating processed-transcripts ...")
    dedup_tree(processed_dir)

    # Step 5: process Vance
    vance_since = latest_file_date(vance_dir)
    vance_since = (vance_since - timedelta(days=7)) if vance_since else since
    print(f"\n[Vance] Extracting from Trump raw since {vance_since.strftime('%Y-%m-%d')} ...")
    n_vance = process_vance(raw_dir, vance_dir, vance_since)
    print(f"[Vance] Processed {n_vance} transcript(s)")

    # Step 6: dedup vance
    print("[Vance] Deduplicating processed-transcripts ...")
    dedup_tree(vance_dir)

    print("\nDone.")


if __name__ == "__main__":
    main()
