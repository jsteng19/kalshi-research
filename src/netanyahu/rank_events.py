#!/usr/bin/env python3
"""
Rank and filter gov.il PMO news/events for UNGA speech relevance.

Inputs: CSV with columns [title, url, date]
Outputs: two CSVs (included/excluded) annotated with score, reasons, and tags.

Usage example:
  python -m src.netanyahu.rank_events \
    --input "/Users/jstenger/Documents/repos/kalshi-research/data-netanyahu/news_urls copy.csv" \
    --output-included "/Users/jstenger/Documents/repos/kalshi-research/data-netanyahu/unga_subset_included.csv" \
    --output-excluded "/Users/jstenger/Documents/repos/kalshi-research/data-netanyahu/unga_subset_excluded.csv" \
    --focus-date 2025-09-25 --since 2025-08-01 --until 2025-09-25 \
    --min-score 12 --window-days 60
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

import pandas as pd

try:
    # Prefer importing the helper to back-fill dates from URLs if possible
    from src.netanyahu.extract_govil_news import extract_date_from_url as extract_date_from_url_helper
except Exception:  # pragma: no cover - optional import fallback
    def extract_date_from_url_helper(url: str) -> str:
        return ""


# ------------------------ Keyword Definitions ------------------------

EVENT_TYPE_PATTERNS: Dict[str, List[str]] = {
    "speech": [r"\bspeech(es)?\b", r"address(es)?", r"\bremarks?\b"],
    "press": [r"press (conference|briefing)"],
    "interview": [r"\binterview(s)?\b"],
    "meeting": [r"\bmeeting(s)?\b", r"\bmeets? with\b", r"\bconcluded an extended meeting\b"],
    "statement": [r"^statement by pm netanyahu", r"\bstatement(s)?\b", r"^statements by pm netanyahu"],
    "government_meeting": [r"start of the government meeting"],
    "visit": [r"\bvisit(s|ed)?\b", r"at the scene", r"at the air force command center"],
    "ceremony": [r"ceremony", r"dedication", r"cornerstone", r"museum", r"toast", r"greeting(s)?"],
}

FOREIGN_ENGAGEMENT_PATTERNS: List[str] = [
    r"\bUS\b", r"United States", r"United Nations", r"\bUN\b", r"UNGA", r"General Assembly", r"New York",
    r"Washington", r"White House", r"Secretary of State", r"Ambassador", r"President ", r"Prime Minister ", r"Joe Biden", r"Donald Trump",
    r"German|French|British|UK|Indian|Ecuador|Cypriot|Greek|Italian|Jordanian|Egyptian|Emirati|Canadian",
]

TOPIC_PATTERNS: List[str] = [
    r"Hamas", r"Hezbollah", r"Iran|Iranian", r"Houthi|Houthis|Yemen", r"Gaza", r"Lebanon",
    r"hostage(s)?", r"terror(ist|ism)?", r"ICJ|ICC", r"UNRWA", r"Red Sea", r"Saudi Arabia", r"Israel", r"Palestine",
]

EXCLUSION_PATTERNS: List[str] = [
    r"greeting", r"toast", r"dedication", r"cornerstone", r"museum", r"opens? .* school year",
    r"congratulat(e|ions)", r"\battend(ed|s)?\b", r"annual ceremony", r"\bprize\b",
]

NETANYAHU_MENTION_PATTERNS: List[str] = [
    r"\bPM Netanyahu\b", r"\bPrime Minister Benjamin Netanyahu\b", r"\bPrime Minister Netanyahu\b",
    r"Netanyahu's", r"Benjamin Netanyahu",
]

GENERIC_PMO_PATTERNS: List[str] = [
    r"Prime Minister's Office", r"PM's Office", r"National Security Council",
]


@dataclass
class ScoredEvent:
    title: str
    url: str
    date: pd.Timestamp | None
    score: int
    event_type: str
    tags: List[str]
    reasons: List[str]


def compile_patterns(patterns: Iterable[str]) -> List[re.Pattern]:
    return [re.compile(p, re.IGNORECASE) for p in patterns]


COMPILED_EVENT_TYPES: Dict[str, List[re.Pattern]] = {
    k: compile_patterns(v) for k, v in EVENT_TYPE_PATTERNS.items()
}
COMPILED_FOREIGN = compile_patterns(FOREIGN_ENGAGEMENT_PATTERNS)
COMPILED_TOPICS = compile_patterns(TOPIC_PATTERNS)
COMPILED_EXCLUDE = compile_patterns(EXCLUSION_PATTERNS)
COMPILED_NETANYAHU = compile_patterns(NETANYAHU_MENTION_PATTERNS)
COMPILED_GENERIC_PMO = compile_patterns(GENERIC_PMO_PATTERNS)


def detect_event_type(title: str) -> Tuple[str, List[str]]:
    matched_types: List[str] = []
    for event_type, patterns in COMPILED_EVENT_TYPES.items():
        if any(p.search(title) for p in patterns):
            matched_types.append(event_type)
    # Prioritize more contentful types
    priority = ["speech", "press", "interview", "statement", "meeting", "government_meeting", "visit", "ceremony"]
    for t in priority:
        if t in matched_types:
            return t, matched_types
    return "other", matched_types


def any_match(patterns: List[re.Pattern], text: str) -> bool:
    return any(p.search(text) for p in patterns)


def compute_score(title: str, date: pd.Timestamp | None, focus_date: pd.Timestamp | None, window_days: int) -> Tuple[int, str, List[str]]:
    score = 0
    reasons: List[str] = []
    tags: List[str] = []

    # Require Netanyahu mention - if not present, return very low score
    if not any_match(COMPILED_NETANYAHU, title):
        return -100, "no_netanyahu", ["no Netanyahu mention"]

    # Event type weights
    event_type, matched_types = detect_event_type(title)
    if event_type == "speech":
        score += 9; reasons.append("speech/address/remarks"); tags.append("speech")
    elif event_type == "press":
        score += 8; reasons.append("press event"); tags.append("press")
    elif event_type == "interview":
        score += 8; reasons.append("interview"); tags.append("interview")
    elif event_type == "statement":
        score += 6; reasons.append("PM statement"); tags.append("statement")
    elif event_type == "meeting":
        score += 6; reasons.append("meeting"); tags.append("meeting")
    elif event_type == "government_meeting":
        score += 4; reasons.append("gov meeting remarks"); tags.append("gov_meeting")
    elif event_type == "visit":
        score += 3; reasons.append("site visit"); tags.append("visit")
    elif event_type == "ceremony":
        score -= 10; reasons.append("ceremony/ceremonial"); tags.append("ceremony")

    # Netanyahu explicitly mentioned (already checked above, but add bonus)
    score += 12; reasons.append("Netanyahu explicitly in title"); tags.append("netanyahu")
    
    # Generic PMO/NSC without Netanyahu lowers relevance (this won't trigger since we require Netanyahu)
    if any_match(COMPILED_GENERIC_PMO, title) and not any_match(COMPILED_NETANYAHU, title):
        score -= 100; reasons.append("generic PMO/NSC")

    # Foreign engagement signals
    if any_match(COMPILED_FOREIGN, title):
        score += 10; reasons.append("foreign/UN signal"); tags.append("foreign")

    # Topic signals
    if any_match(COMPILED_TOPICS, title):
        score += 4; reasons.append("core topic (Iran/Hamas/etc.)"); tags.append("topic")

    # Exclusions
    if any_match(COMPILED_EXCLUDE, title):
        score -= 10; reasons.append("ceremonial/greeting/minor")

    # # Recency / proximity to UNGA focus date
    # if date is not None and focus_date is not None and pd.notna(date) and pd.notna(focus_date):
    #     delta_days = abs((focus_date - date).days)
    #     if delta_days <= 7:
    #         score += 6; reasons.append("within 7d of UNGA"); tags.append("time")
    #     elif delta_days <= 14:
    #         score += 5; reasons.append("within 14d of UNGA"); tags.append("time")
    #     elif delta_days <= 30:
    #         score += 3; reasons.append("within 30d of UNGA"); tags.append("time")
    #     elif delta_days <= window_days:
    #         score += 2; reasons.append(f"within {window_days}d of UNGA"); tags.append("time")

    return score, event_type, reasons


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Score and filter PM Netanyahu events for UNGA relevance")
    parser.add_argument("--input", required=True, help="Path to input CSV with columns [title,url,date]")
    parser.add_argument("--output-included", required=True, help="Output CSV for included rows with scores")
    parser.add_argument("--output-excluded", required=True, help="Output CSV for excluded rows with scores")
    parser.add_argument("--focus-date", default="2025-09-25", help="UNGA speech date (YYYY-MM-DD)")
    parser.add_argument("--since", default="2000-01-01", help="Only consider rows on/after this date (YYYY-MM-DD)")
    parser.add_argument("--until", default="2025-09-30", help="Only consider rows on/before this date (YYYY-MM-DD)")
    parser.add_argument("--min-score", type=int, default=12, help="Minimum score to include")
    parser.add_argument("--window-days", type=int, default=60, help="Days around focus date for time bonus")
    return parser.parse_args()


def ensure_date(series: pd.Series, url_series: pd.Series) -> pd.Series:
    # Try parse existing dates
    parsed = pd.to_datetime(series, errors="coerce")
    # Fill missing by extracting from URL
    needs = parsed.isna()
    if needs.any():
        filled = [extract_date_from_url_helper(u) if n else None for u, n in zip(url_series, needs)]
        parsed = parsed.fillna(pd.to_datetime(pd.Series(filled), errors="coerce"))
    return parsed


def main() -> None:
    args = parse_args()
    df = pd.read_csv(args.input)
    for col in ("title", "url"):
        if col not in df.columns:
            raise SystemExit(f"Missing required column: {col}")
    if "date" not in df.columns:
        df["date"] = ""

    df["date_parsed"] = ensure_date(df["date"], df["url"])  # may remain NaT

    # Bound to date window if provided
    since = pd.to_datetime(args.since, errors="coerce")
    until = pd.to_datetime(args.until, errors="coerce")
    if pd.notna(since):
        df = df[(df["date_parsed"].isna()) | (df["date_parsed"] >= since)]
    if pd.notna(until):
        df = df[(df["date_parsed"].isna()) | (df["date_parsed"] <= until)]

    focus_date = pd.to_datetime(args.focus_date, errors="coerce")

    # Score
    scores: List[int] = []
    types: List[str] = []
    reasons_col: List[str] = []
    for title, d in zip(df["title"].astype(str), df["date_parsed"]):
        s, t, rs = compute_score(title, d, focus_date, args.window_days)
        scores.append(s)
        types.append(t)
        reasons_col.append("; ".join(rs))

    df["score"] = scores
    df["event_type"] = types
    df["reasons"] = reasons_col

    # Inclusion: must involve Netanyahu explicitly and pass score threshold
    involves_netanyahu = df["title"].astype(str).str.contains(
        r"PM Netanyahu|Prime Minister Benjamin Netanyahu|Prime Minister Netanyahu|Netanyahu's|Benjamin Netanyahu",
        case=False,
        regex=True,
        na=False,
    )

    included = df[involves_netanyahu & (df["score"] >= args.min_score)].copy()
    excluded = df[~(involves_netanyahu & (df["score"] >= args.min_score))].copy()

    # Sort by score desc, then date desc
    included = included.sort_values(by=["score", "date_parsed"], ascending=[False, False])
    excluded = excluded.sort_values(by=["date_parsed"], ascending=[False])

    # Output
    included_out_cols = ["title", "url", "date", "date_parsed", "event_type", "score", "reasons"]
    excluded_out_cols = ["title", "url", "date", "date_parsed", "event_type", "score", "reasons"]
    included.to_csv(args.output_included, index=False, columns=included_out_cols)
    excluded.to_csv(args.output_excluded, index=False, columns=excluded_out_cols)

    print(f"Included: {len(included)} rows => {args.output_included}")
    print(f"Excluded: {len(excluded)} rows => {args.output_excluded}")


if __name__ == "__main__":
    main()
