#!/usr/bin/env python3
"""
NCAAB Phrase Analysis Report Generator

Generates interactive HTML reports with:
- Auto-detected crew from combined_announcers.csv
- Live Kalshi prices and historical hit rates
- Multi-column frequency table (processed, diarized, play-by-play, color)
- Announcer comparison, phrase trends, and recent contexts
- FP/FN analysis (ready for transcript data)

Usage:
    python src/ncaab/generate_report.py KXNCAABMENTION-26FEB13DUKEUNC
    python src/ncaab/generate_report.py --all-open
"""

import argparse
import base64
import io
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

_project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_project_root))
sys.path.insert(0, str(_project_root / "src"))

from src.models.phrase_analysis import (
    process_directory, find_phrase_context, count_phrases, compile_patterns,
    get_date_from_filename, read_transcript,
)
from src.utils.regex_pattern_generator import generate_regex_patterns
from src.ncaab.ncaab_config import (
    ANNOUNCER_CREWS, AnnouncerCrew, SERIES_TICKER,
    get_announcer_crew, get_all_announcer_names, get_team_name,
    extract_teams_from_ticker, get_search_value_for_phrase, get_team_code,
    find_crew_by_announcer_name,
)
from src.ncaab.validate_transcripts import DEFAULT_MIN_FULL_GAME_WORDS
from src.ncaab.kalshi_api import (
    get_open_event_tickers, get_event_markets, get_event_vwaps,
    get_market_phrases, fetch_series_results, fetch_historical_vwaps,
    calculate_hit_rates, calculate_hit_rates_by_announcer, MarketInfo,
)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class GameReport:
    """All data needed to render one game's report."""
    event_ticker: str
    away_code: str
    home_code: str
    away_name: str
    home_name: str
    crew_key: Optional[str]
    crew: Optional[AnnouncerCrew]
    pbp_name: str
    color_name: str
    network: str
    market_infos: List[MarketInfo]
    phrases: List[str]
    overall_rates: pd.DataFrame
    announcer_rates: pd.DataFrame
    away_team_rates: pd.DataFrame
    home_team_rates: pd.DataFrame
    search_patterns: Dict[str, str]
    df_transcripts: pd.DataFrame
    df_raw_transcripts: pd.DataFrame
    df_diarized: pd.DataFrame
    df_play_by_play: pd.DataFrame
    df_color_commentary: pd.DataFrame
    df_all_transcripts: pd.DataFrame
    df_color_all: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_color_crew: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_march_madness_cleaned: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_march_madness_raw: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_validation_games: pd.DataFrame = field(default_factory=pd.DataFrame)
    validation_mode: str = ""
    include_probable: bool = False
    min_full_game_words: int = DEFAULT_MIN_FULL_GAME_WORDS
    df_away_team: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_home_team: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_away_team_crew: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_home_team_crew: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_away_team_validated: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_home_team_validated: pd.DataFrame = field(default_factory=pd.DataFrame)
    team_history_counts: Dict[str, int] = field(default_factory=dict)
    hist_crew_vwaps: Dict[str, float] = field(default_factory=dict)
    hist_crew_vwap_n: Dict[str, int] = field(default_factory=dict)
    announcer_matched_tickers: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Announcer data helpers
# ---------------------------------------------------------------------------

def load_announcer_data(path: str = "data/ncaab/game_announcers.csv") -> pd.DataFrame:
    if not os.path.exists(path):
        print(f"  Warning: {path} not found")
        return pd.DataFrame()
    df = pd.read_csv(path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def _ticker_date_variants(event_ticker: str) -> List[str]:
    """Generate +/- 1 day ticker variants for date-boundary mismatches."""
    from src.nba.kalshi_api import _extract_date_from_ticker

    base_date = _extract_date_from_ticker(event_ticker)
    if base_date is None:
        return []

    m = re.match(r"(KXNCAABMENTION)-\d{2}[A-Z]{3}\d{2}(.+)", event_ticker)
    if not m:
        return []
    prefix, teams = m.groups()

    variants = []
    for delta in (-1, 1):
        d = base_date + timedelta(days=delta)
        date_str = d.strftime("%y%b%d").upper()
        variants.append(f"{prefix}-{date_str}{teams}")
    return variants


def lookup_crew_for_event(
    event_ticker: str,
    announcers_df: pd.DataFrame,
) -> Tuple[str, str, str, Optional[str]]:
    """Look up announcer info for an event. Returns (pbp, color, network, crew_key)."""
    pbp, color, network, crew_key = "", "", "", None
    if announcers_df.empty:
        return pbp, color, network, crew_key

    candidates = [event_ticker] + _ticker_date_variants(event_ticker)
    match = pd.DataFrame()
    for candidate in candidates:
        match = announcers_df[announcers_df["ticker"] == candidate]
        if not match.empty:
            break

    if match.empty:
        return pbp, color, network, crew_key
    row = match.iloc[0]
    pbp = str(row.get("play_by_play", "") or "")
    color = str(row.get("color_commentator", "") or "")
    network = str(row.get("network", "") or "")
    crew_key = find_crew_by_announcer_name(pbp)
    return pbp, color, network, crew_key


# ---------------------------------------------------------------------------
# Transcript helpers
# ---------------------------------------------------------------------------

_MONTH_NAMES = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
                'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']


def transcript_filename_to_ticker(filename: str) -> Optional[str]:
    """Convert a transcript filename to a Kalshi ticker."""
    m = re.match(r'(\d{4})-(\d{2})-(\d{2})_(.+)-vs-(.+)\.txt', filename, re.I)
    if not m:
        return None
    year, month, day, away_raw, home_raw = m.groups()
    from src.ncaab.ncaab_config import get_team_code
    away = get_team_code(away_raw)
    home = get_team_code(home_raw)
    if not away or not home:
        return None
    return f"{SERIES_TICKER}-{year[2:]}{_MONTH_NAMES[int(month)-1]}{day}{away}{home}"


def clean_matchup_team_label(label: str) -> str:
    """Strip rankings and broadcast prefixes from a team label."""
    text = str(label or "").strip()
    text = re.sub(r"^(Champ\.|Final|Semifinal|Semi)\s*:\s*", "", text)
    text = re.sub(r"^[A-Za-z0-9 .()/&'-]+:\s*", "", text)
    text = re.sub(r"^\([^)]+\)\s*", "", text)
    text = re.sub(r"^[A-Z]{0,3}\d+\s+", "", text)
    text = re.sub(r"^\d+\s+", "", text)
    return text.strip()


def normalize_text(text: str) -> str:
    """Normalize a team or transcript string for loose matching."""
    text = str(text or "").lower()
    text = text.replace("&", " and ")
    text = text.replace("st.", "saint")
    text = text.replace("st ", "saint ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def match_transcripts_to_tickers(df: pd.DataFrame, target_tickers: set[str]) -> pd.DataFrame:
    """Filter transcript rows to games matching a set of Kalshi tickers."""
    if df.empty or not target_tickers:
        return pd.DataFrame()
    matched = []
    for idx, row in df.iterrows():
        fn = str(row.get("file", "") or "")
        ticker = transcript_filename_to_ticker(fn) if fn else None
        if ticker and ticker in target_tickers:
            matched.append(idx)
    return df.loc[matched].copy() if matched else pd.DataFrame()


def _tickers_for_team(series_results: Dict[str, Dict[str, Optional[str]]], team_code: str) -> set[str]:
    """Return resolved series tickers where team_code appears as either away or home team."""
    tickers: set[str] = set()
    target = (team_code or "").upper()
    for ticker in series_results.keys():
        away, home = extract_teams_from_ticker(str(ticker))
        if (away or "").upper() == target or (home or "").upper() == target:
            tickers.add(str(ticker))
    return tickers


def match_transcripts_to_team_code(
    df: pd.DataFrame,
    series_results: Dict[str, Dict[str, Optional[str]]],
    team_code: str,
    exclude_tickers: Optional[set[str]] = None,
) -> pd.DataFrame:
    """Filter transcript rows to games whose ticker history includes the given team code."""
    if df.empty or not team_code:
        return pd.DataFrame()
    tickers = _tickers_for_team(series_results, team_code.upper())
    if exclude_tickers:
        tickers = {t for t in tickers if t not in exclude_tickers}
    matched = match_transcripts_to_tickers(df, tickers)
    if not matched.empty:
        return matched
    team_name = get_team_name(team_code)
    return match_transcripts_to_team_name(df, team_name)


def _normalize_color_value(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def _color_commentator_match(series: pd.Series, color_commentator: str) -> pd.Series:
    target = _normalize_color_value(color_commentator)
    if not target:
        return pd.Series([False] * len(series), index=series.index)
    return series.fillna("").map(
        lambda raw: target in [_normalize_color_value(part) for part in str(raw).split(",") if part.strip()]
    )


def _load_subdir(base: str, subdir: str, search_patterns: Dict[str, str]) -> pd.DataFrame:
    path = os.path.join(base, subdir)
    if os.path.exists(path) and any(f.endswith(".txt") for f in os.listdir(path)):
        return process_directory(path, search_patterns, verbose=False)
    return pd.DataFrame()


def _resolve_crew_base(data_base_path: str, crew: AnnouncerCrew) -> str:
    """
    Resolve a data path that may be:
    - the global root, e.g. data/ncaab/transcripts
    - a specific crew folder, e.g. data/ncaab/transcripts/jason-benetti
    - a direct transcripts subset folder, e.g. .../validated-report/transcripts
    """
    base = Path(data_base_path)
    # Global root like data/ncaab/transcripts — navigate into crew folder
    if base.name == "transcripts" and (base / crew.folder).exists():
        return str(base / crew.folder)
    if (base / "transcripts").exists():
        return str(base)
    candidate = base / crew.folder
    if (candidate / "transcripts").exists():
        return str(candidate)
    return str(candidate)


def load_all_crew_transcripts(
    crew: AnnouncerCrew,
    search_patterns: Dict[str, str],
    data_base_path: str = "data/ncaab/transcripts",
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    base = _resolve_crew_base(data_base_path, crew)
    return (
        _load_subdir(base, "transcripts", search_patterns),
        _load_subdir(base, "diarized", search_patterns),
        _load_subdir(base, "play_by_play", search_patterns),
        _load_subdir(base, "color_commentary", search_patterns),
    )


def load_all_crews_combined(
    search_patterns: Dict[str, str],
    data_base_path: str = "data/ncaab/transcripts",
) -> pd.DataFrame:
    base = Path(data_base_path)
    if base.name == "transcripts" and base.exists():
        df = process_directory(str(base), search_patterns, verbose=False)
        return df if not df.empty else pd.DataFrame()
    if (base / "transcripts").exists():
        df = process_directory(str(base / "transcripts"), search_patterns, verbose=False)
        return df if not df.empty else pd.DataFrame()

    all_dfs = []
    for crew in ANNOUNCER_CREWS.values():
        path = os.path.join(data_base_path, crew.folder, "transcripts")
        if os.path.exists(path):
            df = process_directory(path, search_patterns, verbose=False)
            if not df.empty:
                all_dfs.append(df)
    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()


def load_all_validated_combined(
    search_patterns: Dict[str, str],
    data_base_path: str = "data/ncaab/transcripts",
) -> pd.DataFrame:
    """Load all crew validated-cleaned corpora from subset directories."""
    root = Path(data_base_path)
    if root.name == "transcripts" and root.exists():
        root = root
    all_dfs: List[pd.DataFrame] = []
    for subset_dir in root.glob("*/subsets/confirmed-only/cleaned/transcripts"):
        if subset_dir.exists():
            df = process_directory(str(subset_dir), search_patterns, verbose=False)
            if not df.empty:
                all_dfs.append(df)
    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()


def process_file_rows(rows_df: pd.DataFrame, search_patterns: Dict[str, str], path_col: str) -> pd.DataFrame:
    if rows_df.empty:
        return pd.DataFrame()
    compiled = compile_patterns(search_patterns)
    out = []
    for _, row in rows_df.iterrows():
        path_str = str(row.get(path_col, "")).strip()
        if not path_str:
            continue
        path = Path(path_str)
        if not path.exists():
            continue
        text = read_transcript(str(path))
        counts = count_phrases(text, search_patterns, compiled)
        out.append(
            {
                "date": pd.to_datetime(str(row.get("game_date", "")), errors="coerce"),
                "file": path.name,
                "category": path.parent.name,
                "text_length": len(text.split()),
                "text": text,
                **counts,
            }
        )
    return pd.DataFrame(out)


def is_march_madness_game(row: pd.Series) -> bool:
    round_name = str(row.get("round_name", "")).lower()
    keywords = [
        "final four", "sweet 16", "elite 8", "elite eight", "first four",
        "first round", "second round", "national championship",
        "round of 64", "round of 32", "ncaa tournament",
    ]
    return any(keyword in round_name for keyword in keywords)


def transcript_filename_to_teams(filename: str) -> tuple[Optional[str], Optional[str]]:
    """Parse transcript filenames of the form YYYY-MM-DD_away-vs-home.txt."""
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})_(.+)-vs-(.+)\.txt", filename, re.I)
    if not m:
        return None, None
    away_raw, home_raw = m.group(4), m.group(5)
    return clean_matchup_team_label(away_raw), clean_matchup_team_label(home_raw)


def transcript_filename_to_team_codes(filename: str) -> tuple[Optional[str], Optional[str]]:
    """Parse transcript filenames into canonical team codes when possible."""
    away, home = transcript_filename_to_teams(filename)
    away_code = get_team_code(away) if away else None
    home_code = get_team_code(home) if home else None
    return away_code, home_code


def _team_norm(team: str) -> str:
    return normalize_text(clean_matchup_team_label(team))


def match_transcripts_to_team_name(df: pd.DataFrame, team_name: str) -> pd.DataFrame:
    """Filter transcript DataFrame to rows whose filenames mention the team name."""
    if df.empty or not team_name:
        return pd.DataFrame()
    target_code = get_team_code(team_name)
    target_norm = _team_norm(team_name)
    matched = []
    for idx, row in df.iterrows():
        fn = str(row.get("file", "") or "")
        if not fn:
            continue
        away_code, home_code = transcript_filename_to_team_codes(fn)
        if away_code and home_code and target_code:
            if away_code == target_code or home_code == target_code:
                matched.append(idx)
                continue
        away, home = transcript_filename_to_teams(fn)
        if away and home and (_team_norm(away) == target_norm or _team_norm(home) == target_norm):
            matched.append(idx)
    return df.loc[matched].copy() if matched else pd.DataFrame()


def load_validated_transcript_sets(
    validation_csv: str,
    pbp_name: str,
    search_patterns: Dict[str, str],
    include_probable: bool,
    min_full_game_words: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = pd.read_csv(validation_csv, dtype=str).fillna("")
    df = df[df["play_by_play"].str.lower() == pbp_name.lower()].copy()
    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    for col in ["raw_word_count", "cleaned_word_count"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["used_word_count"] = df["cleaned_word_count"].where(df["cleaned_path"] != "", df["raw_word_count"])
    allowed = {"confirmed_valid"}
    if include_probable:
        allowed.add("probable_valid")
    df = df[df["final_validation_status"].isin(allowed)].copy()
    df = df[df["used_word_count"] >= min_full_game_words].copy()
    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df["is_march_madness"] = df.apply(is_march_madness_game, axis=1)
    df_cleaned = process_file_rows(df[df["cleaned_path"] != ""], search_patterns, "cleaned_path")
    df_raw = process_file_rows(df, search_patterns, "transcript_path")
    mm_rows = df[df["is_march_madness"]].copy()
    df_mm_cleaned = process_file_rows(mm_rows[mm_rows["cleaned_path"] != ""], search_patterns, "cleaned_path")
    df_mm_raw = process_file_rows(mm_rows, search_patterns, "transcript_path")
    return df, df_cleaned, df_raw, df_mm_cleaned, df_mm_raw


# ---------------------------------------------------------------------------
# Build report data
# ---------------------------------------------------------------------------

def build_game_report(
    event_ticker: str,
    announcers_df: pd.DataFrame,
    series_results: Dict[str, Dict[str, Optional[str]]],
    crew_override: Optional[str] = None,
    data_base_path: str = "data/ncaab/transcripts",
    validation_csv: Optional[str] = None,
    include_probable: bool = False,
    min_full_game_words: int = DEFAULT_MIN_FULL_GAME_WORDS,
    color_commentator_override: Optional[str] = None,
) -> GameReport:
    """Assemble all data for a single game report."""
    away_code, home_code = extract_teams_from_ticker(event_ticker)
    away_code = (away_code or "").upper()
    home_code = (home_code or "").upper()
    away_name = get_team_name(away_code) if away_code else "Away"
    home_name = get_team_name(home_code) if home_code else "Home"
    away_label = away_code or away_name
    home_label = home_code or home_name

    pbp, color, network, crew_key = lookup_crew_for_event(event_ticker, announcers_df)
    if color_commentator_override:
        color = color_commentator_override
    if crew_override:
        crew_key = crew_override
    crew = get_announcer_crew(crew_key) if crew_key else None
    if crew_override and crew is None:
        # Allow reports to be generated from validated corpora even when the
        # event-specific announcer lookup is missing or the crew is not in the
        # live announcer table.
        display_name = crew_override.replace("-", " ").strip().title()
        crew = AnnouncerCrew(
            name=display_name,
            folder=crew_override,
            play_by_play=[display_name],
            color=[],
        )
        crew_key = crew_override
    if crew_override and not pbp and crew:
        pbp = crew.name

    try:
        market_infos = get_event_markets(event_ticker)
    except Exception as e:
        print(f"  Warning: could not fetch markets for {event_ticker}: {e}")
        market_infos = []
    try:
        vwaps = get_event_vwaps([m.ticker for m in market_infos], event_ticker=event_ticker)
        for mi in market_infos:
            mi.vwap = vwaps.get(mi.ticker)
    except Exception as e:
        print(f"  Warning: VWAP fetch failed: {e}")
    phrases = [m.phrase for m in market_infos]

    announcer_names = get_all_announcer_names(crew) if crew else []
    phrase_to_search = {p: get_search_value_for_phrase(p) for p in phrases}
    phrase_to_search.update({a: a for a in announcer_names})
    search_patterns = generate_regex_patterns(phrase_to_search)

    overall_rates = calculate_hit_rates(series_results, phrases)
    announcer_rates = pd.DataFrame()
    announcer_matched_tickers: List[str] = []
    if pbp:
        announcer_rates = calculate_hit_rates_by_announcer(
            series_results, announcers_df, pbp, phrases
        )
        # Collect the matched tickers for display
        if not announcers_df.empty:
            announcer_matched_tickers = sorted(
                announcers_df.loc[
                    announcers_df["play_by_play"].str.lower() == pbp.lower(), "ticker"
                ].dropna().tolist()
            )

    df_trans = df_raw_trans = df_diar = df_pbp = df_color = pd.DataFrame()
    df_color_all = df_color_crew = pd.DataFrame()
    df_all_validated = pd.DataFrame()
    df_away_team_crew = df_home_team_crew = pd.DataFrame()
    df_away_team_validated = df_home_team_validated = pd.DataFrame()
    df_mm_cleaned = df_mm_raw = df_validation_games = pd.DataFrame()
    validation_mode = ""
    if crew:
        if validation_csv:
            df_validation_games, df_trans, df_raw_trans, df_mm_cleaned, df_mm_raw = load_validated_transcript_sets(
                validation_csv,
                pbp or crew.name,
                search_patterns,
                include_probable,
                min_full_game_words,
            )
            validation_mode = "confirmed + probable" if include_probable else "confirmed only"
        else:
            df_trans, df_diar, df_pbp, df_color = load_all_crew_transcripts(
                crew, search_patterns, data_base_path
            )
        counts = [
            f"transcripts={len(df_trans)}", f"diarized={len(df_diar)}",
            f"play_by_play={len(df_pbp)}", f"color_commentary={len(df_color)}",
        ]
        print(f"    Loaded: {', '.join(counts)}")

    df_all = load_all_crews_combined(search_patterns, data_base_path)
    if crew:
        df_all_validated = load_all_validated_combined(search_patterns, data_base_path)

    away_team_rates = pd.DataFrame()
    home_team_rates = pd.DataFrame()
    team_history_counts: Dict[str, int] = {}
    if away_code and home_code and phrases:
        away_series = {
            ticker: results
            for ticker, results in series_results.items()
            if ticker != event_ticker and away_code in {(extract_teams_from_ticker(ticker)[0] or "").upper(), (extract_teams_from_ticker(ticker)[1] or "").upper()}
        }
        home_series = {
            ticker: results
            for ticker, results in series_results.items()
            if ticker != event_ticker and home_code in {(extract_teams_from_ticker(ticker)[0] or "").upper(), (extract_teams_from_ticker(ticker)[1] or "").upper()}
        }
        away_team_rates = calculate_hit_rates(away_series, phrases) if away_series else pd.DataFrame()
        home_team_rates = calculate_hit_rates(home_series, phrases) if home_series else pd.DataFrame()
        team_history_counts[away_code] = len(away_series)
        team_history_counts[home_code] = len(home_series)

    if color and not announcers_df.empty:
        color_mask = _color_commentator_match(announcers_df.get("color_commentator", pd.Series(dtype=str)), color)
        color_games = announcers_df[color_mask].copy()
        color_tickers = set(color_games["ticker"].dropna().astype(str))
        if not df_all.empty:
            df_color_all = match_transcripts_to_tickers(df_all, color_tickers)

        if pbp:
            pbp_mask = announcers_df["play_by_play"].fillna("").str.lower() == pbp.lower()
            exact_games = announcers_df[pbp_mask & color_mask].copy()
            exact_tickers = set(exact_games["ticker"].dropna().astype(str))
            if not df_validation_games.empty:
                exact_rows = df_validation_games[
                    _color_commentator_match(df_validation_games.get("analysts", pd.Series(dtype=str)), color)
                ].copy()
                df_color_crew = process_file_rows(exact_rows[exact_rows["cleaned_path"] != ""], search_patterns, "cleaned_path")
            else:
                source_df = df_trans if not df_trans.empty else df_raw_trans
                df_color_crew = match_transcripts_to_tickers(source_df, exact_tickers)
        print(f"    Color filter: {color} all={len(df_color_all)} exact={len(df_color_crew)}")

    df_away_team = df_home_team = pd.DataFrame()
    if not df_all.empty and away_name and home_name:
        df_away_team = match_transcripts_to_team_name(df_all, away_name)
        df_home_team = match_transcripts_to_team_name(df_all, home_name)
        print(f"    Team filter: {away_label}={len(df_away_team)}, {home_label}={len(df_home_team)}")

    if not df_trans.empty and away_name and home_name:
        df_away_team_crew = match_transcripts_to_team_name(df_trans, away_name)
        df_home_team_crew = match_transcripts_to_team_name(df_trans, home_name)

    if not df_all_validated.empty and away_name and home_name:
        df_away_team_validated = match_transcripts_to_team_name(df_all_validated, away_name)
        df_home_team_validated = match_transcripts_to_team_name(df_all_validated, home_name)

    # Historical VWAP
    hist_crew_vwaps: Dict[str, float] = {}
    hist_crew_vwap_n: Dict[str, int] = {}
    if pbp and not announcers_df.empty and phrases:
        ann_tickers = list(
            announcers_df.loc[
                announcers_df["play_by_play"].str.lower() == pbp.lower(), "ticker"
            ].dropna()
        )
        ann_tickers = [t for t in ann_tickers if t != event_ticker]
        if ann_tickers:
            print(f"    Fetching historical VWAP for {pbp} ({len(ann_tickers)} events)...")
            hist_data = fetch_historical_vwaps(ann_tickers, verbose=True)
            for phrase in phrases:
                vals = [ev[phrase] for ev in hist_data.values() if phrase in ev]
                if vals:
                    hist_crew_vwaps[phrase] = float(np.mean(vals))
                    hist_crew_vwap_n[phrase] = len(vals)

    return GameReport(
        event_ticker=event_ticker,
        away_code=away_code, home_code=home_code,
        away_name=away_name, home_name=home_name,
        crew_key=crew_key, crew=crew,
        pbp_name=pbp, color_name=color, network=network,
        market_infos=market_infos, phrases=phrases,
        overall_rates=overall_rates, announcer_rates=announcer_rates,
        away_team_rates=away_team_rates, home_team_rates=home_team_rates,
        search_patterns=search_patterns,
        df_transcripts=df_trans, df_diarized=df_diar,
        df_raw_transcripts=df_raw_trans,
        df_play_by_play=df_pbp, df_color_commentary=df_color,
        df_all_transcripts=df_all,
        df_color_all=df_color_all,
        df_color_crew=df_color_crew,
        df_march_madness_cleaned=df_mm_cleaned,
        df_march_madness_raw=df_mm_raw,
        df_validation_games=df_validation_games,
        validation_mode=validation_mode,
        include_probable=include_probable,
        min_full_game_words=min_full_game_words,
        df_away_team=df_away_team,
        df_home_team=df_home_team,
        df_away_team_crew=df_away_team_crew,
        df_home_team_crew=df_home_team_crew,
        df_away_team_validated=df_away_team_validated,
        df_home_team_validated=df_home_team_validated,
        team_history_counts=team_history_counts,
        hist_crew_vwaps=hist_crew_vwaps,
        hist_crew_vwap_n=hist_crew_vwap_n,
        announcer_matched_tickers=announcer_matched_tickers,
    )


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

_CSS = """
:root {
    --primary: #003087; --secondary: #c41230; --accent: #ffc72c;
    --success: #38a169; --warning: #d69e2e; --danger: #e53e3e;
    --bg: #f7fafc; --card-bg: #ffffff; --text: #2d3748;
    --text-muted: #718096; --border: #e2e8f0;
}
* { box-sizing: border-box; }
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: var(--bg); color: var(--text); line-height: 1.5;
    margin: 0; padding: 12px;
}
.container { max-width: 98vw; margin: 0 auto; }
h1 { color: var(--primary); border-bottom: 3px solid var(--accent); padding-bottom: 8px; font-size: 1.4em; }
h2 { color: var(--secondary); margin-top: 20px; font-size: 1.1em; }
h3 { color: var(--primary); font-size: 1em; }
.card {
    background: var(--card-bg); border-radius: 6px; padding: 12px;
    margin: 10px 0; box-shadow: 0 1px 3px rgba(0,0,0,0.1);
}
.header-grid {
    display: grid; grid-template-columns: 1fr auto; gap: 12px; align-items: start;
}
.header-info { font-size: 0.9em; }
.header-info p { margin: 2px 0; }
.stats-grid {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(110px, 1fr));
    gap: 8px; margin: 12px 0;
}
.stat-box {
    background: linear-gradient(135deg, var(--primary), var(--secondary));
    color: white; padding: 10px; border-radius: 6px; text-align: center;
}
.stat-box .value { font-size: 1.5em; font-weight: bold; }
.stat-box .label { opacity: 0.9; font-size: 0.8em; }
details {
    background: var(--card-bg); border: 1px solid var(--border);
    border-radius: 6px; margin: 8px 0;
}
summary {
    padding: 10px 14px; cursor: pointer; font-weight: 600;
    background: #f8f9fa; border-radius: 6px 6px 0 0; list-style: none;
    font-size: 0.95em;
}
summary::-webkit-details-marker { display: none; }
summary::before { content: '\\25B6  '; color: var(--accent); }
details[open] summary::before { content: '\\25BC  '; }
details[open] summary { border-bottom: 1px solid var(--border); }
.details-content { padding: 12px; }
table { width: 100%; border-collapse: collapse; margin: 8px 0; font-size: 0.82em; }
th, td { padding: 5px 8px; text-align: left; border-bottom: 1px solid var(--border); }
th { background: var(--primary); color: white; position: sticky; top: 0; white-space: nowrap; font-size: 0.9em; }
td { white-space: nowrap; }
tr:hover { background: #f7fafc; }
.context-box {
    background: #f8f9fa; border-left: 3px solid var(--accent);
    padding: 8px 12px; margin: 6px 0; font-size: 0.85em;
}
.context-box .source { color: var(--text-muted); font-size: 0.8em; margin-bottom: 4px; }
.context-highlight { background: #fff3cd; padding: 1px 3px; border-radius: 2px; font-weight: 600; }
.chart-img { max-width: 100%; height: auto; border-radius: 6px; }
.generated-time { color: var(--text-muted); font-size: 0.8em; text-align: right; margin-top: 20px; }
.delta-positive { color: var(--success); font-weight: bold; }
.delta-negative { color: var(--danger); font-weight: bold; }
.delta-neutral { color: var(--text-muted); }
.phrase-contexts { columns: 2; column-gap: 16px; }
.phrase-contexts .context-box { break-inside: avoid; }
@media (max-width: 1200px) { .phrase-contexts { columns: 1; } }
@media (max-width: 768px) { .stats-grid { grid-template-columns: 1fr 1fr; } }
"""


def _rate_cell(rate) -> str:
    if rate is None or (isinstance(rate, float) and np.isnan(rate)):
        return "-"
    return f"{rate:.0%}"


def _delta_cell(delta) -> str:
    if delta is None or (isinstance(delta, float) and np.isnan(delta)):
        return '<span class="delta-neutral">-</span>'
    sign = "+" if delta >= 0 else ""
    cls = "delta-positive" if delta > 0.05 else ("delta-negative" if delta < -0.05 else "delta-neutral")
    return f'<span class="{cls}">{sign}{delta:.0%}</span>'


def _freq_cell(df: pd.DataFrame, phrase: str) -> str:
    if df.empty or phrase not in df.columns:
        return "-"
    hits = int((df[phrase] > 0).sum())
    total = len(df)
    pct = hits / total * 100 if total > 0 else 0
    return f"{pct:.0f}% ({hits}/{total})"


def _kalshi_rate_cell(rates_df: pd.DataFrame, phrase: str) -> str:
    if rates_df.empty:
        return "-"
    row = rates_df.loc[rates_df["Phrase"] == phrase]
    if row.empty:
        return "-"
    r = row.iloc[0]
    rate = r["Hit Rate"]
    if rate is None or (isinstance(rate, float) and np.isnan(rate)):
        return "-"
    return f"{rate:.0%} ({int(r['Hits'])}/{int(r['Total'])})"


def _rates_identical(overall: pd.DataFrame, announcer: pd.DataFrame, phrases: List[str]) -> bool:
    """Check if overall and announcer-specific rates are identical for all phrases."""
    if overall.empty and announcer.empty:
        return True
    if overall.empty or announcer.empty:
        return False
    for p in phrases:
        ov = overall.loc[overall["Phrase"] == p]
        an = announcer.loc[announcer["Phrase"] == p]
        if ov.empty != an.empty:
            return False
        if not ov.empty and not an.empty:
            if int(ov.iloc[0]["Hits"]) != int(an.iloc[0]["Hits"]) or int(ov.iloc[0]["Total"]) != int(an.iloc[0]["Total"]):
                return False
    return True


def _dfs_identical(a: pd.DataFrame, b: pd.DataFrame, phrases: List[str]) -> bool:
    """Check if two transcript DataFrames produce the same freq values for all phrases."""
    if a.empty and b.empty:
        return True
    if a.empty or b.empty or len(a) != len(b):
        return False
    for p in phrases:
        if p not in a.columns or p not in b.columns:
            if (p not in a.columns) != (p not in b.columns):
                return False
            continue
        if int((a[p] > 0).sum()) != int((b[p] > 0).sum()):
            return False
    return True


def _dedup_sources(
    sources: List[Tuple[str, pd.DataFrame]],
    phrases: List[str],
) -> List[Tuple[str, pd.DataFrame]]:
    """Remove sources whose freq data is identical to an earlier source."""
    result: List[Tuple[str, pd.DataFrame]] = []
    for name, df in sources:
        is_dup = False
        for existing_name, existing_df in result:
            if _dfs_identical(df, existing_df, phrases):
                is_dup = True
                break
        if not is_dup:
            result.append((name, df))
    return result


def _render_combined_table(game: GameReport) -> str:
    if not game.market_infos:
        return '<p style="color:var(--text-muted);">No market data available.</p>'

    crew_name = game.crew.name if game.crew else game.pbp_name or "Crew"

    rate_sources: List[Tuple[str, pd.DataFrame]] = []
    if not game.away_team_rates.empty:
        rate_sources.append((f"Kalshi {game.away_code} ({game.team_history_counts.get(game.away_code, len(game.away_team_rates))})", game.away_team_rates))
    if not game.home_team_rates.empty:
        rate_sources.append((f"Kalshi {game.home_code} ({game.team_history_counts.get(game.home_code, len(game.home_team_rates))})", game.home_team_rates))

    sources: List[Tuple[str, pd.DataFrame]] = []
    if not game.df_transcripts.empty:
        sources.append((f"Validated Cleaned ({len(game.df_transcripts)})", game.df_transcripts))
    if not game.df_raw_transcripts.empty:
        sources.append((f"Validated Raw ({len(game.df_raw_transcripts)})", game.df_raw_transcripts))
    if not game.df_color_crew.empty:
        label = game.color_name or "Color"
        sources.append((f"PBP + {label} ({len(game.df_color_crew)})", game.df_color_crew))
    if not game.df_color_all.empty:
        label = game.color_name or "Color"
        sources.append((f"All + {label} ({len(game.df_color_all)})", game.df_color_all))
    if not game.df_march_madness_cleaned.empty:
        sources.append((f"MM Cleaned ({len(game.df_march_madness_cleaned)})", game.df_march_madness_cleaned))
    if not game.df_march_madness_raw.empty:
        sources.append((f"MM Raw ({len(game.df_march_madness_raw)})", game.df_march_madness_raw))
    if not game.df_away_team.empty:
        sources.append((f"All Crews {game.away_code} ({len(game.df_away_team)})", game.df_away_team))
    if not game.df_home_team.empty:
        sources.append((f"All Crews {game.home_code} ({len(game.df_home_team)})", game.df_home_team))
    if not game.df_away_team_crew.empty:
        sources.append((f"Crew {game.away_code} ({len(game.df_away_team_crew)})", game.df_away_team_crew))
    if not game.df_home_team_crew.empty:
        sources.append((f"Crew {game.home_code} ({len(game.df_home_team_crew)})", game.df_home_team_crew))
    if not game.df_away_team_validated.empty:
        sources.append((f"All Validated {game.away_code} ({len(game.df_away_team_validated)})", game.df_away_team_validated))
    if not game.df_home_team_validated.empty:
        sources.append((f"All Validated {game.home_code} ({len(game.df_home_team_validated)})", game.df_home_team_validated))
    if not game.df_diarized.empty:
        sources.append((f"Raw ({len(game.df_diarized)})", game.df_diarized))
    if not game.df_play_by_play.empty:
        sources.append((f"PBP ({len(game.df_play_by_play)})", game.df_play_by_play))
    if not game.df_color_commentary.empty:
        sources.append((f"Color ({len(game.df_color_commentary)})", game.df_color_commentary))

    # Deduplicate sources with identical frequency data
    sources = _dedup_sources(sources, game.phrases)

    overall_lu: Dict[str, float] = {}
    if not game.overall_rates.empty:
        for _, r in game.overall_rates.iterrows():
            overall_lu[r["Phrase"]] = r["Hit Rate"]

    has_hist = bool(game.hist_crew_vwaps)
    # Skip the overall column if it's identical to the announcer-specific column
    show_both_rates = not _rates_identical(game.overall_rates, game.announcer_rates, game.phrases)

    headers = ["Phrase"]
    if show_both_rates:
        headers.append("Kalshi Rate (All)")
    headers.append(f"Kalshi Rate ({crew_name})" if show_both_rates else "Kalshi Rate")
    headers.append("VWAP")
    if has_hist:
        headers.append(f"Avg VWAP ({crew_name})")
    headers += ["Bid/Ask", "Last", "Vol", "Delta"]
    headers.extend(name for name, _ in rate_sources)
    headers.extend(name for name, _ in sources)

    html = '<div style="overflow-x:auto;"><table>\n<tr>'
    html += "".join(f"<th>{h}</th>" for h in headers)
    html += "</tr>\n"

    for mi in game.market_infos:
        p = mi.phrase
        display = get_search_value_for_phrase(p)

        ovr_str = _kalshi_rate_cell(game.overall_rates, p)
        ann_str = _kalshi_rate_cell(game.announcer_rates, p)
        vwap_str = f"{mi.vwap:.0f}c" if mi.vwap is not None else "-"

        bid, ask = mi.yes_bid, mi.yes_ask
        if bid is not None and ask is not None:
            mid = (bid + ask) / 2 / 100.0
            price_str = f"{bid}c/{ask}c"
        else:
            mid = None
            price_str = "-"
        last_str = f"{mi.last_price}c" if mi.last_price is not None else "-"
        vol_str = f"{mi.volume:,}" if mi.volume is not None else "-"

        ovr_rate = overall_lu.get(p)
        delta = (ovr_rate - mid) if (ovr_rate is not None and mid is not None and not np.isnan(ovr_rate)) else None

        hist_cell = ""
        if has_hist:
            hv = game.hist_crew_vwaps.get(p)
            hn = game.hist_crew_vwap_n.get(p, 0)
            hist_cell = f"<td>{hv:.0f}c (n={hn})</td>" if hv is not None else "<td>-</td>"

        rate_cells = "".join(f"<td>{_kalshi_rate_cell(rates_df, p)}</td>" for _, rates_df in rate_sources)
        freq_cells = "".join(f"<td>{_freq_cell(df, p)}</td>" for _, df in sources)

        # Use the best available rate for display when rates are identical
        rate_str = ann_str if not game.announcer_rates.empty else ovr_str
        ovr_cell = f"<td>{ovr_str}</td>" if show_both_rates else ""
        ann_cell = f"<td>{rate_str}</td>" if not show_both_rates else f"<td>{ann_str}</td>"

        html += (
            f"<tr><td>{display}</td>"
            f"{ovr_cell}{ann_cell}"
            f"<td>{vwap_str}</td>{hist_cell}<td>{price_str}</td><td>{last_str}</td>"
            f"<td>{vol_str}</td><td>{_delta_cell(delta)}</td>"
            f"{rate_cells}{freq_cells}</tr>\n"
        )

    html += "</table></div>"
    return html


def _render_validation_summary(game: GameReport) -> str:
    if game.df_validation_games.empty:
        return '<p style="color:var(--text-muted);">No validation metadata supplied for this report.</p>'

    df = game.df_validation_games.copy()
    for col in ["raw_word_count", "cleaned_word_count", "used_word_count"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    mode = game.validation_mode or "directory-based"
    mm_count = int(df["is_march_madness"].sum()) if "is_march_madness" in df.columns else 0
    avg_raw = df["raw_word_count"].mean()
    med_raw = df["raw_word_count"].median()
    avg_used = df["used_word_count"].mean()
    med_used = df["used_word_count"].median()
    confirmed_count = int((df["final_validation_status"] == "confirmed_valid").sum())
    probable_count = int((df["final_validation_status"] == "probable_valid").sum())
    avg_pbp_mentions = pd.to_numeric(
        df.get("cleaned_pbp_first_name_mentions", pd.Series([0] * len(df))),
        errors="coerce",
    ).fillna(0).mean()
    history_str = "n/a"
    if game.team_history_counts:
        history_str = ", ".join(f"{team}={count}" for team, count in game.team_history_counts.items())

    return f"""
<p><strong>Validation mode:</strong> {mode}</p>
<p><strong>Included games:</strong> {len(df)} | <strong>March Madness games:</strong> {mm_count}</p>
<p><strong>Status mix:</strong> confirmed_valid {confirmed_count} | probable_valid {probable_count}</p>
<p><strong>Word counts:</strong> raw avg {avg_raw:.0f}, raw median {med_raw:.0f}, used avg {avg_used:.0f}, used median {med_used:.0f}</p>
<p><strong>Name evidence:</strong> cleaned PBP first-name mentions average {avg_pbp_mentions:.1f} per included game.</p>
<p><strong>Kalshi history events:</strong> {history_str}</p>
<p><strong>Criteria:</strong> include only transcripts that pass team/date matching, announcer-intro mismatch checks, and the minimum full-game floor of {game.min_full_game_words:,} words. By default only <code>confirmed_valid</code> games are included; <code>--include-probable</code> also includes structurally valid games lacking strong announcer-name confirmation.</p>
"""


def _render_included_games_table(game: GameReport) -> str:
    if game.df_validation_games.empty:
        return '<p style="color:var(--text-muted);">No validated game list available.</p>'
    df = game.df_validation_games.copy()
    cols = [
        "game_date", "season", "round_name", "matchup", "is_march_madness",
        "final_validation_status",
        "pbp_first_name_mentions", "cleaned_pbp_first_name_mentions",
        "analyst_first_name_mentions", "cleaned_analyst_first_name_mentions",
        "raw_word_count", "cleaned_word_count", "used_word_count",
        "transcript_path", "cleaned_path",
    ]
    display = df[cols].copy()
    for col in [
        "pbp_first_name_mentions", "cleaned_pbp_first_name_mentions",
        "analyst_first_name_mentions", "cleaned_analyst_first_name_mentions",
        "raw_word_count", "cleaned_word_count", "used_word_count",
    ]:
        display[col] = pd.to_numeric(display[col], errors="coerce").fillna(0).astype(int)
    for col in ["transcript_path", "cleaned_path"]:
        display[col] = display[col].map(lambda p: Path(str(p)).name if str(p).strip() else "")
    display.rename(
        columns={
            "game_date": "Date",
            "season": "Season",
            "round_name": "Round",
            "matchup": "Matchup",
            "is_march_madness": "March Madness",
            "final_validation_status": "Status",
            "pbp_first_name_mentions": "Raw PBP Mentions",
            "cleaned_pbp_first_name_mentions": "Cleaned PBP Mentions",
            "analyst_first_name_mentions": "Raw Analyst Mentions",
            "cleaned_analyst_first_name_mentions": "Cleaned Analyst Mentions",
            "raw_word_count": "Raw Words",
            "cleaned_word_count": "Cleaned Words",
            "used_word_count": "Used Words",
            "transcript_path": "Raw File",
            "cleaned_path": "Cleaned File",
        },
        inplace=True,
    )
    html = '<div style="overflow-x:auto;"><table><tr>'
    html += "".join(f"<th>{col}</th>" for col in display.columns)
    html += "</tr>\n"
    for _, row in display.sort_values("Date", ascending=False).iterrows():
        html += "<tr>"
        for col in display.columns:
            html += f"<td>{row[col]}</td>"
        html += "</tr>\n"
    html += "</table></div>"
    return html


def _calculate_fp_fn(
    df: pd.DataFrame,
    series_results: Dict[str, Dict[str, Optional[str]]],
    phrases: List[str],
) -> Dict[str, Dict[str, int]]:
    out: Dict[str, Dict[str, int]] = {}
    for phrase in phrases:
        tp = fp = fn = tn = 0
        if phrase not in df.columns:
            out[phrase] = {"tp": 0, "fp": 0, "fn": 0, "tn": 0, "matched": 0}
            continue
        for _, row in df.iterrows():
            fn_raw = row.get("file", "")
            ticker = transcript_filename_to_ticker(Path(str(fn_raw)).name if fn_raw else "")
            if not ticker or ticker not in series_results:
                continue
            res = series_results[ticker].get(phrase)
            if res is None:
                continue
            in_trans = row[phrase] > 0
            kalshi_yes = str(res).strip().lower() == "yes"
            if in_trans and kalshi_yes:
                tp += 1
            elif in_trans and not kalshi_yes:
                fp += 1
            elif not in_trans and kalshi_yes:
                fn += 1
            else:
                tn += 1
        out[phrase] = {"tp": tp, "fp": fp, "fn": fn, "tn": tn, "matched": tp + fp + fn + tn}
    return out


def _fp_fn_stats_identical(crew_stats: Dict, all_stats: Dict, phrases: List[str]) -> bool:
    """Check if crew-specific and all-crew FP/FN stats are identical."""
    if not crew_stats or not all_stats:
        return False
    for p in phrases:
        if crew_stats.get(p) != all_stats.get(p):
            return False
    return True


def _render_fp_fn_table(game: GameReport, series_results: Dict) -> str:
    crew_name = game.crew.name if game.crew else game.pbp_name or "Crew"
    crew_df = game.df_transcripts if not game.df_transcripts.empty else game.df_diarized
    crew_stats = _calculate_fp_fn(crew_df, series_results, game.phrases) if not crew_df.empty else {}
    all_stats = _calculate_fp_fn(game.df_all_transcripts, series_results, game.phrases) if not game.df_all_transcripts.empty else {}

    if not crew_stats and not all_stats:
        return '<p style="color:var(--text-muted);">No matched games for FP/FN analysis (add transcripts to data/ncaab/transcripts/).</p>'

    # Skip "All Crews" column if identical to crew-specific
    show_both = not _fp_fn_stats_identical(crew_stats, all_stats, game.phrases)
    stats_groups: List[Tuple[str, Dict]] = []
    if crew_stats:
        stats_groups.append((crew_name, crew_stats))
    if all_stats and (show_both or not crew_stats):
        label = "All Crews" if show_both else crew_name
        stats_groups.append((label, all_stats))

    html = '<div style="overflow-x:auto;"><table>\n'
    html += "<tr><th rowspan='2'>Phrase</th>"
    for label, _ in stats_groups:
        html += f"<th colspan='4'>{label}</th>"
    html += "</tr>\n<tr>"
    for _ in stats_groups:
        html += "<th>Games</th><th>FP</th><th>FN</th><th>Acc.</th>"
    html += "</tr>\n"

    for phrase in game.phrases:
        display = get_search_value_for_phrase(phrase)
        html += f"<tr><td>{display}</td>"
        for _, stats in stats_groups:
            s = stats.get(phrase, {"tp": 0, "fp": 0, "fn": 0, "tn": 0, "matched": 0})
            m = s["matched"]
            if m == 0:
                html += "<td>0</td><td>-</td><td>-</td><td>-</td>"
                continue
            fp_d = s["fp"] + s["tp"]
            fn_d = s["fn"] + s["tn"]
            fp_str = f"{s['fp']}/{fp_d} ({s['fp']/fp_d*100:.0f}%)" if fp_d else "-"
            fn_str = f"{s['fn']}/{fn_d} ({s['fn']/fn_d*100:.0f}%)" if fn_d else "-"
            acc = (s["tp"] + s["tn"]) / m * 100
            html += f"<td>{m}</td><td>{fp_str}</td><td>{fn_str}</td><td>{acc:.0f}%</td>"
        html += "</tr>\n"

    html += "</table></div>"
    return html


def _gather_recent_contexts(
    game: GameReport,
    max_per_phrase: int = 6,
    context_window: int = 150,
    data_base_path: str = "data/ncaab/transcripts",
) -> Dict[str, List[Tuple[str, str]]]:
    """Gather recent transcript contexts for each phrase.

    Returns {phrase: [(source_label, highlighted_context), ...]}.
    """
    import html as html_mod

    results: Dict[str, List[Tuple[str, str]]] = {}
    if not game.crew and not game.df_all_transcripts.empty:
        dirs_to_scan = []
        for crew in ANNOUNCER_CREWS.values():
            p = os.path.join(data_base_path, crew.folder, "transcripts")
            if os.path.isdir(p):
                dirs_to_scan.append((crew.name, p))
    elif game.crew:
        base = os.path.join(data_base_path, game.crew.folder)
        dirs_to_scan = []
        for subdir, label in [("transcripts", "Processed"), ("diarized", "Raw"),
                               ("play_by_play", "PBP"), ("color_commentary", "Color")]:
            p = os.path.join(base, subdir)
            if os.path.isdir(p):
                dirs_to_scan.append((label, p))
    else:
        return results

    for phrase in game.phrases:
        search_val = get_search_value_for_phrase(phrase)
        pattern = game.search_patterns.get(phrase) or game.search_patterns.get(search_val)
        if not pattern:
            continue
        try:
            compiled = re.compile(pattern, re.IGNORECASE)
        except re.error:
            continue

        phrase_contexts: List[Tuple[str, str]] = []
        for label, dir_path in dirs_to_scan:
            if len(phrase_contexts) >= max_per_phrase:
                break
            try:
                files = sorted(
                    [f for f in os.listdir(dir_path) if f.endswith(".txt")],
                    reverse=True,
                )
            except OSError:
                continue
            for fn in files:
                if len(phrase_contexts) >= max_per_phrase:
                    break
                fpath = os.path.join(dir_path, fn)
                try:
                    with open(fpath, "r", encoding="utf-8", errors="replace") as fh:
                        text = fh.read()
                except OSError:
                    continue
                contexts = find_phrase_context(text, pattern, window=context_window)
                for ctx in contexts[:2]:
                    safe = html_mod.escape(ctx)
                    highlighted = compiled.sub(
                        lambda m: f'<span class="context-highlight">{m.group()}</span>',
                        safe,
                    )
                    source = f"{label} / {fn}"
                    phrase_contexts.append((source, highlighted))
                    if len(phrase_contexts) >= max_per_phrase:
                        break

        if phrase_contexts:
            results[phrase] = phrase_contexts
    return results


def _render_recent_contexts(contexts: Dict[str, List[Tuple[str, str]]], game: GameReport) -> str:
    if not contexts:
        return '<p style="color:var(--text-muted);">No transcript contexts available.</p>'

    html = ""
    for phrase in game.phrases:
        if phrase not in contexts:
            continue
        display = get_search_value_for_phrase(phrase)
        items = contexts[phrase]
        html += f'<details style="margin:6px 0;"><summary>{display} ({len(items)} matches)</summary>'
        html += '<div class="details-content phrase-contexts">'
        for source, ctx in items:
            html += f'<div class="context-box"><div class="source">{source}</div>{ctx}</div>'
        html += "</div></details>\n"
    return html


def _render_transcript_summary(game: GameReport, series_results: Dict) -> str:
    """Render a Transcript & Data Summary section with word counts, name stats, and matched tickers."""
    n = lambda df: len(df) if not df.empty else 0
    html = ""

    # --- Data source overview ---
    mode = game.validation_mode or "directory-based (no validation CSV)"
    html += f'<p><strong>Loading mode:</strong> {mode}</p>\n'
    html += f'<p><strong>Min full-game words:</strong> {game.min_full_game_words:,}</p>\n'

    # --- Transcript counts ---
    sources = [
        ("Crew transcripts (cleaned)", game.df_transcripts),
        ("Crew transcripts (raw)", game.df_raw_transcripts),
        ("Diarized", game.df_diarized),
        ("Play-by-play", game.df_play_by_play),
        ("Color commentary", game.df_color_commentary),
        ("All crews combined", game.df_all_transcripts),
        ("March Madness (cleaned)", game.df_march_madness_cleaned),
        ("March Madness (raw)", game.df_march_madness_raw),
    ]
    html += '<table><tr><th>Source</th><th>Games</th><th>Avg Words</th><th>Med Words</th><th>Min Words</th><th>Max Words</th></tr>\n'
    for label, df in sources:
        count = n(df)
        if count == 0:
            continue
        wc = df["text_length"] if "text_length" in df.columns else pd.Series(dtype=float)
        avg_w = f"{wc.mean():.0f}" if not wc.empty else "—"
        med_w = f"{wc.median():.0f}" if not wc.empty else "—"
        min_w = f"{wc.min():.0f}" if not wc.empty else "—"
        max_w = f"{wc.max():.0f}" if not wc.empty else "—"
        html += f'<tr><td>{label}</td><td>{count}</td><td>{avg_w}</td><td>{med_w}</td><td>{min_w}</td><td>{max_w}</td></tr>\n'
    html += '</table>\n'

    # --- Announcer name appearance stats ---
    if game.crew:
        announcer_names = get_all_announcer_names(game.crew)
        name_cols = [name for name in announcer_names if not game.df_transcripts.empty and name in game.df_transcripts.columns]
        if name_cols:
            df = game.df_transcripts
            html += '<h4 style="margin:12px 0 6px;">Announcer Name Appearances (crew transcripts)</h4>\n'
            html += '<table><tr><th>Name</th><th>Games w/ ≥1</th><th>Total Mentions</th><th>Avg/Game</th><th>Max</th></tr>\n'
            for name in name_cols:
                total = int(df[name].sum())
                games_with = int((df[name] > 0).sum())
                avg = f"{df[name].mean():.1f}"
                mx = int(df[name].max())
                html += f'<tr><td>{name}</td><td>{games_with}/{len(df)}</td><td>{total}</td><td>{avg}</td><td>{mx}</td></tr>\n'
            html += '</table>\n'
        # Also check all-crews combined
        if not game.df_all_transcripts.empty:
            all_name_cols = [name for name in announcer_names if name in game.df_all_transcripts.columns]
            if all_name_cols:
                df_all = game.df_all_transcripts
                html += '<h4 style="margin:12px 0 6px;">Announcer Name Appearances (all crews combined)</h4>\n'
                html += '<table><tr><th>Name</th><th>Games w/ ≥1</th><th>Total Mentions</th><th>Avg/Game</th><th>Max</th></tr>\n'
                for name in all_name_cols:
                    total = int(df_all[name].sum())
                    games_with = int((df_all[name] > 0).sum())
                    avg = f"{df_all[name].mean():.1f}"
                    mx = int(df_all[name].max())
                    html += f'<tr><td>{name}</td><td>{games_with}/{len(df_all)}</td><td>{total}</td><td>{avg}</td><td>{mx}</td></tr>\n'
                html += '</table>\n'

    # --- Kalshi history ---
    if game.team_history_counts:
        html += '<h4 style="margin:12px 0 6px;">Kalshi Team History</h4>\n'
        html += '<p>' + ' &nbsp;|&nbsp; '.join(
            f'<strong>{team}:</strong> {count} resolved events'
            for team, count in game.team_history_counts.items()
        ) + '</p>\n'

    # --- Matched announcer tickers ---
    tickers = game.announcer_matched_tickers
    resolved_tickers = [t for t in tickers if t in series_results]
    unresolved_tickers = [t for t in tickers if t not in series_results]
    html += f'<h4 style="margin:12px 0 6px;">Announcer Matched Kalshi Events ({len(resolved_tickers)} resolved, {len(unresolved_tickers)} unresolved, {len(tickers)} total)</h4>\n'
    if tickers:
        html += '<div style="font-family:monospace;font-size:0.82em;line-height:1.6;">\n'
        for t in tickers:
            away, home = extract_teams_from_ticker(t)
            away_name = get_team_name(away) if away else "?"
            home_name = get_team_name(home) if home else "?"
            resolved = t in series_results
            status = "✅" if resolved else "⏳"
            # Show results if resolved
            result_info = ""
            if resolved and series_results[t]:
                results = series_results[t]
                hits = [p for p, r in results.items() if r and r.lower() == "yes"]
                if hits:
                    result_info = f' — hits: {", ".join(hits)}'
                else:
                    result_info = " — no hits"
            is_current = " ⬅️ <strong>current</strong>" if t == game.event_ticker else ""
            html += f'{status} {t} ({away_name} @ {home_name}){result_info}{is_current}<br>\n'
        html += '</div>\n'
    else:
        html += '<p style="color:var(--text-muted);">No announcer events found in combined_announcers.csv</p>\n'

    return html


def render_html_report(game: GameReport, series_results: Dict, announcers_df: pd.DataFrame) -> str:
    crew_display = game.crew.name if game.crew else (game.pbp_name or "Unknown")
    n = lambda df: len(df) if not df.empty else 0

    # Gather recent contexts
    contexts = _gather_recent_contexts(game)

    # Only show stat boxes that have data
    stat_boxes = [
        (len(game.phrases), "Phrases"),
    ]
    for val, label in [
        (n(game.df_transcripts), "Validated Cleaned"),
        (n(game.df_raw_transcripts), "Validated Raw"),
        (n(game.df_march_madness_cleaned), "MM Cleaned"),
        (n(game.df_diarized), "Diarized"),
        (n(game.df_all_transcripts), "All Crews"),
    ]:
        if val > 0:
            stat_boxes.append((val, label))
    stat_boxes.append((len(series_results), "Kalshi Events"))

    stat_html = "\n".join(
        f'    <div class="stat-box"><div class="value">{v}</div><div class="label">{l}</div></div>'
        for v, l in stat_boxes
    )

    parts = [f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{game.event_ticker} | NCAAB Report</title>
<style>{_CSS}</style>
</head><body><div class="container">
<h1>{game.away_name} @ {game.home_name}</h1>
<div class="card">
  <div class="header-grid">
    <div class="header-info">
      <p><strong>Event:</strong> {game.event_ticker} &nbsp; <strong>Crew:</strong> {crew_display}{(' / ' + game.color_name) if game.color_name else ''} &nbsp; <strong>Network:</strong> {game.network or 'N/A'}</p>
    </div>
    <div style="color:var(--text-muted);font-size:0.8em;">{datetime.now().strftime('%Y-%m-%d %H:%M')}</div>
  </div>
</div>

<div class="stats-grid">
{stat_html}
</div>

<details open><summary>Prices, Rates &amp; Frequency</summary>
<div class="details-content">{_render_combined_table(game)}</div>
</details>

<details><summary>Recent Contexts</summary>
<div class="details-content">{_render_recent_contexts(contexts, game)}</div>
</details>

<details><summary>Transcript vs. Kalshi (FP / FN)</summary>
<div class="details-content">{_render_fp_fn_table(game, series_results)}</div>
</details>

<details><summary>Transcript &amp; Data Summary</summary>
<div class="details-content">{_render_transcript_summary(game, series_results)}</div>
</details>
"""]

    if not game.df_validation_games.empty:
        parts.append(f"""
<details><summary>Validation Summary</summary>
<div class="details-content">{_render_validation_summary(game)}</div>
</details>

<details><summary>Included Games</summary>
<div class="details-content">{_render_included_games_table(game)}</div>
</details>
""")

    parts.append("""
<div class="generated-time">Report complete.</div>
</div></body></html>""")

    return "".join(parts)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate NCAAB phrase analysis reports")
    parser.add_argument("event_ticker", nargs="?")
    parser.add_argument("--all-open", action="store_true")
    parser.add_argument("--crew", "-c", default=None)
    parser.add_argument("-o", "--output", default=None)
    parser.add_argument("--data-dir", default="data/ncaab/transcripts")
    parser.add_argument("--validation-csv", default=None)
    parser.add_argument("--include-probable", action="store_true")
    parser.add_argument("--series", default=SERIES_TICKER)
    parser.add_argument("--min-full-game-words", type=int, default=DEFAULT_MIN_FULL_GAME_WORDS)
    parser.add_argument("--color-commentator", default=None)
    args = parser.parse_args()

    os.chdir(_project_root)

    event_tickers = []
    if args.all_open:
        print("Fetching open events...")
        event_tickers = get_open_event_tickers(args.series)
        print(f"  Found {len(event_tickers)} open events")
    elif args.event_ticker:
        event_tickers = [args.event_ticker]
    else:
        print("Error: provide an event ticker or use --all-open")
        sys.exit(1)

    print("Loading announcer data...")
    announcers_df = load_announcer_data()
    print("Fetching series results...")
    series_results = fetch_series_results(args.series, delay=0.1, verbose=True)

    os.makedirs("reports", exist_ok=True)
    for et in event_tickers:
        print(f"\nBuilding report for {et}...")
        game = build_game_report(et, announcers_df, series_results,
                                 crew_override=args.crew, data_base_path=args.data_dir,
                                 validation_csv=args.validation_csv,
                                 include_probable=args.include_probable,
                                 min_full_game_words=args.min_full_game_words,
                                 color_commentator_override=args.color_commentator)
        html = render_html_report(game, series_results, announcers_df)
        out = args.output if (args.output and len(event_tickers) == 1) else f"reports/{et}_report.html"
        with open(out, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"  Saved: {out}")


if __name__ == "__main__":
    main()
