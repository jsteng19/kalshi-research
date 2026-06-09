#!/usr/bin/env python3
"""
Extract play-by-play and color commentary from diarized transcripts.

Diarized files use labels like:
  Speaker Play by play announcer: ...
  Speaker Color Commentary announcer: ...
  Speaker All other: ...
  Speaker Play by play announcer - 1: ...
  Speaker Color Commentary announcer - 2: ...

This script walks data/nba/transcripts (all subfolders), finds diarized or
transcript .txt files with these labels, and writes:
  <crew>/play_by_play/<basename>.txt   — play-by-play only
  <crew>/color_commentary/<basename>.txt — color commentary only

Usage:
  python -m src.nba.extract_pbp_color_transcripts
  python -m src.nba.extract_pbp_color_transcripts --base data/nba/transcripts
"""

import argparse
import os
import re
import sys
from pathlib import Path

# Project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# Speaker label patterns: match "Speaker <role>:" or "Speaker <role> - N:"
PBP_PATTERN = re.compile(
    r"^Speaker\s+Play\s+by\s+play\s+announcer(?:\s*-\s*\d+)?\s*:\s*",
    re.IGNORECASE,
)
COLOR_PATTERN = re.compile(
    r"^Speaker\s+Color\s+Commentary\s+announcer(?:\s*-\s*\d+)?\s*:\s*",
    re.IGNORECASE,
)


def parse_diarized_by_role(content: str) -> tuple[str, str]:
    """
    Parse diarized transcript text and return (play_by_play_text, color_commentary_text).
    Each is a single string with that role's segments joined by spaces.
    Handles "Speaker Play by play announcer: ...", "Speaker ... - 1: ...", etc.
    """
    pbp_parts = []
    color_parts = []
    # Split on newline followed by "Speaker " so each segment is "Role: text"
    segments = re.split(r"\n\s*Speaker\s+", content, flags=re.IGNORECASE)
    for seg in segments:
        seg = seg.strip()
        if not seg:
            continue
        # First segment may start with "Speaker " if there was no leading newline
        if seg.lower().startswith("speaker "):
            seg = seg[8:].strip()
        if not seg:
            continue
        # Segment is "Play by play announcer: text" or "Play by play announcer - 1: text" (no "Speaker " prefix after split)
        if re.match(r"Play\s+by\s+play\s+announcer(?:\s*-\s*\d+)?\s*:", seg, re.IGNORECASE):
            text = re.sub(
                r"^Play\s+by\s+play\s+announcer(?:\s*-\s*\d+)?\s*:\s*",
                "",
                seg,
                count=1,
                flags=re.IGNORECASE,
            )
            text = " ".join(text.split())
            if text:
                pbp_parts.append(text)
        elif re.match(r"Color\s+Commentary\s+announcer(?:\s*-\s*\d+)?\s*:", seg, re.IGNORECASE):
            text = re.sub(
                r"^Color\s+Commentary\s+announcer(?:\s*-\s*\d+)?\s*:\s*",
                "",
                seg,
                count=1,
                flags=re.IGNORECASE,
            )
            text = " ".join(text.split())
            if text:
                color_parts.append(text)
    pbp_text = " ".join(pbp_parts) if pbp_parts else ""
    color_text = " ".join(color_parts) if color_parts else ""
    return pbp_text, color_text


def has_role_labels(content: str) -> bool:
    """Return True if content contains Play by play or Color Commentary speaker labels."""
    return bool(
        re.search(r"Speaker\s+Play\s+by\s+play\s+announcer", content, re.IGNORECASE)
        or re.search(r"Speaker\s+Color\s+Commentary\s+announcer", content, re.IGNORECASE)
    )


def extract_from_file(input_path: str) -> tuple[str, str] | None:
    """
    Read a diarized transcript file and return (pbp_text, color_text).
    Returns None if file doesn't contain role labels.
    """
    with open(input_path, "r", encoding="utf-8") as f:
        content = f.read()
    if not has_role_labels(content):
        return None
    return parse_diarized_by_role(content)


def process_base_dir(base_path: str, dry_run: bool = False) -> dict:
    """
    Walk base_path (e.g. data/nba/transcripts). For each first-level subfolder (crew)
    that contains diarized/ or transcripts/ with .txt files, extract PBP and color
    and write to <crew>/play_by_play/ and <crew>/color_commentary/.
    Prefers diarized over transcripts when both have the same filename.

    Returns dict with counts: processed, skipped_no_labels, written_pbp, written_color.
    """
    base = Path(base_path)
    if not base.is_dir():
        return {"error": f"Not a directory: {base_path}", "processed": 0}

    stats = {"processed": 0, "skipped_no_labels": 0, "written_pbp": 0, "written_color": 0}

    # First-level subdirs are crew folders (e.g. michael-grady, ian-eagle)
    for crew_dir in sorted(base.iterdir()):
        if not crew_dir.is_dir():
            continue
        pbp_dir = crew_dir / "play_by_play"
        color_dir = crew_dir / "color_commentary"
        if not dry_run:
            pbp_dir.mkdir(parents=True, exist_ok=True)
            color_dir.mkdir(parents=True, exist_ok=True)

        # Collect (stem, path) from diarized first, then transcripts; prefer diarized
        seen_stems = set()
        files_to_process = []
        for subname in ("diarized", "transcripts"):
            src_dir = crew_dir / subname
            if not src_dir.is_dir():
                continue
            for txt in sorted(src_dir.glob("*.txt")):
                if txt.stem not in seen_stems:
                    seen_stems.add(txt.stem)
                    files_to_process.append(txt)

        for txt in files_to_process:
            result = extract_from_file(str(txt))
            if result is None:
                stats["skipped_no_labels"] += 1
                continue
            pbp_text, color_text = result
            stats["processed"] += 1
            stem = txt.stem
            if not dry_run:
                if pbp_text:
                    (pbp_dir / f"{stem}.txt").write_text(pbp_text, encoding="utf-8")
                    stats["written_pbp"] += 1
                if color_text:
                    (color_dir / f"{stem}.txt").write_text(color_text, encoding="utf-8")
                    stats["written_color"] += 1
            else:
                if pbp_text:
                    stats["written_pbp"] += 1
                if color_text:
                    stats["written_color"] += 1

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Extract play-by-play and color commentary from diarized NBA transcripts."
    )
    parser.add_argument(
        "--base",
        default="data/nba/transcripts",
        help="Base directory (e.g. data/nba/transcripts) containing crew subfolders",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report what would be written",
    )
    args = parser.parse_args()
    base_path = args.base
    if not os.path.isabs(base_path):
        base_path = str(PROJECT_ROOT / base_path)
    print(f"Base path: {base_path}")
    if args.dry_run:
        print("Dry run — no files will be written.")
    stats = process_base_dir(base_path, dry_run=args.dry_run)
    if "error" in stats:
        print(stats["error"])
        sys.exit(1)
    print(f"Processed: {stats['processed']}")
    print(f"Skipped (no role labels): {stats['skipped_no_labels']}")
    print(f"Written play_by_play: {stats['written_pbp']}")
    print(f"Written color_commentary: {stats['written_color']}")


if __name__ == "__main__":
    main()
