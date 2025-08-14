import argparse
import re
import subprocess
from datetime import datetime
from typing import Iterable, List, Optional, Tuple

import pandas as pd
from dateutil import parser as dateparser

try:
    # Python 3.9+
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


HEADER_NAME_LINE = "Donald Trump"
HANDLE = "@realDonaldTrump"

# Matches lines like:
# @realDonaldTrump •  Truth Social • August 7, 2025 @ 9:27 PM ET
# @realDonaldTrump • Deleted •  Truth Social • August 8, 2025 @ 10:33 AM ET
HEADER_HANDLE_PATTERN = re.compile(
    r"^@realDonaldTrump\s*•\s*(?:Deleted\s*•\s*)?Truth Social\s*•\s*(?P<datetime>.+?)\s*ET\s*$"
)


def run_textutil_to_text(rtf_path: str) -> List[str]:
    """Convert the RTF to plaintext using macOS textutil and return lines.

    Raises CalledProcessError if textutil fails.
    """
    process = subprocess.run(
        ["textutil", "-convert", "txt", "-stdout", rtf_path],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    text = process.stdout
    # Normalize line endings and strip trailing spaces to simplify parsing
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    return normalized.split("\n")


def iter_blocks(lines: Iterable[str]) -> Iterable[List[str]]:
    """Yield blocks of lines starting with a name header line until next header or EOF.

    A block must start with a line exactly equal to HEADER_NAME_LINE.
    """
    buffer: List[str] = []
    in_block = False
    for line in lines:
        if line.strip() == HEADER_NAME_LINE:
            # Yield previous block if any
            if in_block and buffer:
                yield buffer
            buffer = [line]
            in_block = True
        else:
            if in_block:
                buffer.append(line)
            # else ignore leading lines until first header
    if in_block and buffer:
        yield buffer


def parse_header_datetime(block: List[str]) -> Optional[datetime]:
    """Extract ET datetime from the header lines within a block.

    Returns a timezone-aware datetime in America/New_York if ZoneInfo available,
    otherwise returns naive datetime parsed by dateutil (best effort).
    """
    # Expect second non-empty line to be the handle + platform + datetime
    handle_line: Optional[str] = None
    for line in block[1:5]:  # Look in first few lines after name for safety
        if line.strip():
            handle_line = line.strip()
            break
    if not handle_line:
        return None

    m = HEADER_HANDLE_PATTERN.match(handle_line)
    if not m:
        return None

    dt_part = m.group("datetime")  # Example: "August 7, 2025 @ 9:27 PM"
    # Remove the literal '@' that sometimes exists in the middle
    dt_part_clean = dt_part.replace("@", "").strip()

    try:
        dt = dateparser.parse(dt_part_clean, fuzzy=True)
    except Exception:
        return None

    # Attach America/New_York timezone if available
    if ZoneInfo is not None:
        try:
            eastern = ZoneInfo("America/New_York")
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=eastern)
            else:
                dt = dt.astimezone(eastern)
        except Exception:
            # Fallback: leave as parsed
            pass
    return dt


def extract_content_after_marker(block: List[str]) -> str:
    """Extract content lines following 'View on Truth Social' or 'View Image' marker.

    Joins soft-wrapped lines into paragraphs separated by blank lines. Returns
    a normalized string with internal consecutive whitespace collapsed for cleanliness.
    """
    # Find the index of the marker line
    marker_idx: Optional[int] = None
    for idx, line in enumerate(block):
        if line.strip() in {"View on Truth Social", "View Image"}:
            marker_idx = idx
            break
    if marker_idx is None:
        return ""

    # Lines after marker until the next header start belong to content.
    content_lines = block[marker_idx + 1 :]

    # Build paragraphs by treating contiguous non-empty lines as a paragraph
    paragraphs: List[str] = []
    current: List[str] = []
    for raw_line in content_lines:
        line = raw_line.rstrip()
        if line.strip() == HEADER_NAME_LINE:
            # Safety: encountered start of next block due to missing blank
            break
        if line.strip() == "":
            if current:
                paragraphs.append(" ".join(s.strip() for s in current if s.strip()))
                current = []
            # else multiple blank lines collapse
        else:
            current.append(line)
    if current:
        paragraphs.append(" ".join(s.strip() for s in current if s.strip()))

    # Join paragraphs with double newlines; then strip
    content = "\n\n".join(p for p in paragraphs if p)
    return content.strip()


def parse_rtf_posts(rtf_path: str) -> pd.DataFrame:
    """Parse the RTF file into a DataFrame with columns: post_date, status_text.

    Filters out entries with empty status_text.
    post_date is returned as naive string YYYY-MM-DD HH:MM:SS in ET to match CSV.
    """
    lines = run_textutil_to_text(rtf_path)
    records: List[Tuple[str, str]] = []

    for block in iter_blocks(lines):
        dt = parse_header_datetime(block)
        if dt is None:
            continue
        status_text = extract_content_after_marker(block)
        if not status_text:
            # Skip image-only or empty posts
            continue
        # Make datetime naive in ET for consistency with existing CSV
        if dt.tzinfo is not None:
            try:
                eastern = ZoneInfo("America/New_York") if ZoneInfo else None
                if eastern is not None:
                    dt = dt.astimezone(eastern)
            except Exception:
                pass
            dt = dt.replace(tzinfo=None)
        post_date_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        records.append((post_date_str, status_text))

    df = pd.DataFrame(records, columns=["post_date", "status_text"])
    return df


def load_existing_csv(csv_path: str) -> pd.DataFrame:
    """Load the existing CSV and return just Trump's posts with date + text."""
    df = pd.read_csv(csv_path, dtype=str)
    # Keep only Trump's own account posts
    mask = (df.get("account_handle").fillna("") == "realDonaldTrump") | (
        df.get("account_name").fillna("") == "Donald J. Trump"
    )
    df = df.loc[mask, ["post_date", "status_text"]].copy()
    # Normalize whitespace in status_text
    df["status_text"] = df["status_text"].fillna("").apply(lambda s: re.sub(r"\s+", " ", s).strip())
    # Drop empties as user wants text content
    df = df[df["status_text"] != ""]
    # Ensure post_date format is consistent
    def normalize_date(s: str) -> str:
        s = (s or "").strip()
        if not s:
            return s
        try:
            dt = dateparser.parse(s, fuzzy=True)
            # Treat as ET naive to align with RTF parsing
            if dt.tzinfo is not None:
                dt = dt.astimezone(ZoneInfo("America/New_York")).replace(tzinfo=None) if ZoneInfo else dt
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return s

    df["post_date"] = df["post_date"].astype(str).apply(normalize_date)
    return df


def merge_datasets(csv_df: pd.DataFrame, rtf_df: pd.DataFrame) -> pd.DataFrame:
    """Merge and drop duplicates by (post_date, status_text)."""
    combined = pd.concat([csv_df, rtf_df], ignore_index=True)
    combined.drop_duplicates(subset=["post_date", "status_text"], inplace=True)
    combined.sort_values(by=["post_date"], inplace=True)
    combined.reset_index(drop=True, inplace=True)
    return combined


def main():
    parser = argparse.ArgumentParser(description="Parse Trump Truth Social RTF exports and merge with CSV.")
    parser.add_argument(
        "--rtf",
        default="/Users/jstenger/Documents/repos/kalshi-research/data/truth-social/TXT.rtf",
        help="Absolute path to the RTF export file.",
    )
    parser.add_argument(
        "--csv",
        default="/Users/jstenger/Documents/repos/kalshi-research/data/truth-social/trump_truths_dataset.csv",
        help="Absolute path to the existing CSV dataset.",
    )
    parser.add_argument(
        "--out",
        default="/Users/jstenger/Documents/repos/kalshi-research/data/truth-social/trump_truths_full.csv",
        help="Absolute path for the merged CSV output.",
    )
    args = parser.parse_args()

    rtf_df = parse_rtf_posts(args.rtf)
    csv_df = load_existing_csv(args.csv)
    merged = merge_datasets(csv_df, rtf_df)
    merged.to_csv(args.out, index=False)
    print(f"Wrote merged dataset with {len(merged)} rows to {args.out}")


if __name__ == "__main__":
    main() 