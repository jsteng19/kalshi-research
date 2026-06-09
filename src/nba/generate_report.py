#!/usr/bin/env python3
"""
NBA Phrase Analysis Report Generator (v2)

Generates interactive HTML reports with:
- Auto-detected crew from combined_announcers.csv
- Live Kalshi prices and historical hit rates
- Discrepancy table (base rate vs current price)
- Multi-column frequency table (processed, diarized, play-by-play,
  color commentary, color-commentator-filtered, exact crew match)
- Announcer comparison, phrase trends, and recent contexts

Usage:
    python src/nba/generate_report.py KXNBAMENTION-26FEB09MILORL
    python src/nba/generate_report.py --all-open
    python src/nba/generate_report.py --all-open --crew mike-breen
"""

import argparse
import base64
import io
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root / "src" / "nba"))

from src.models.phrase_analysis import (
    process_directory,
    find_phrase_context,
)
from src.utils.regex_pattern_generator import generate_regex_patterns
from src.nba.nba_config import (
    ANNOUNCER_CREWS,
    AnnouncerCrew,
    get_announcer_crew,
    get_all_announcer_names,
    get_team_name,
    extract_teams_from_ticker,
    get_search_value_for_phrase,
    find_crew_by_announcer_name,
)
from src.nba.kalshi_api import (
    get_open_event_tickers,
    get_event_markets,
    get_event_vwaps,
    get_market_phrases,
    fetch_series_results,
    fetch_historical_vwaps,
    calculate_hit_rates,
    calculate_hit_rates_by_announcer,
    MarketInfo,
)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class GameReport:
    """All data needed to render one game's report section."""
    event_ticker: str
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
    search_patterns: Dict[str, str]
    # Transcript DataFrames (one per source)
    df_transcripts: pd.DataFrame       # crew/<folder>/transcripts
    df_diarized: pd.DataFrame           # crew/<folder>/diarized
    df_play_by_play: pd.DataFrame       # crew/<folder>/play_by_play
    df_color_commentary: pd.DataFrame   # crew/<folder>/color_commentary
    # Combined "All" across all crews
    df_all_transcripts: pd.DataFrame
    # ICDB-filtered
    df_color_all: pd.DataFrame          # all PBP + this color commentator
    df_color_crew: pd.DataFrame         # exact PBP+color match
    # Team-filtered
    df_away_team: pd.DataFrame = field(default_factory=pd.DataFrame)   # all games involving away team
    df_home_team: pd.DataFrame = field(default_factory=pd.DataFrame)   # all games involving home team
    df_home_team_home: pd.DataFrame = field(default_factory=pd.DataFrame)  # home team at home only
    # Historical VWAP
    hist_crew_vwaps: Dict[str, float] = field(default_factory=dict)   # phrase -> avg vwap (cents)
    hist_crew_vwap_n: Dict[str, int] = field(default_factory=dict)    # phrase -> num events with data
    # Additional summary data
    away_code: str = ""
    home_code: str = ""
    team_history_counts: Dict[str, int] = field(default_factory=dict)
    announcer_matched_tickers: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Announcer data helpers
# ---------------------------------------------------------------------------

def load_announcer_data(
    path: str = "data/nba/icdb/combined_announcers.csv",
) -> pd.DataFrame:
    """Load the combined announcers CSV."""
    if not os.path.exists(path):
        print(f"  Warning: {path} not found")
        return pd.DataFrame()
    df = pd.read_csv(path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def _ticker_date_variants(event_ticker: str) -> List[str]:
    """Generate +/- 1 day ticker variants for late-game date-boundary mismatches.

    Kalshi sometimes dates late-night games to the next calendar day while
    506 Sports uses the scheduled game date.
    """
    from src.nba.kalshi_api import _extract_date_from_ticker
    from datetime import timedelta

    base_date = _extract_date_from_ticker(event_ticker)
    if base_date is None:
        return []

    # Extract team codes from the end of the ticker (last 6 chars = AWYHOM)
    m = re.match(r"(KXNBAMENTION)-\d{2}[A-Z]{3}\d{2}(.+)", event_ticker)
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
    """
    Look up announcer info for an event from the combined CSV.
    Returns (pbp_name, color_name, network, crew_key).

    Handles Kalshi/506 date-boundary mismatches by trying +/- 1 day variants.
    """
    pbp, color, network, crew_key = "", "", "", None
    if announcers_df.empty:
        return pbp, color, network, crew_key

    # Try exact ticker first, then +/- 1 day
    candidates = [event_ticker] + _ticker_date_variants(event_ticker)
    match = pd.DataFrame()
    for candidate in candidates:
        match = announcers_df[announcers_df["ticker"] == candidate]
        if not match.empty:
            break

    if match.empty:
        return pbp, color, network, crew_key
    row = match.iloc[0]

    def _clean_text(value: Any) -> str:
        if pd.isna(value):
            return ""
        text = str(value).strip()
        return "" if text.lower() == "nan" else text

    pbp = _clean_text(row.get("play_by_play", ""))
    color = _clean_text(row.get("color_commentator", ""))
    network = _clean_text(row.get("network", ""))
    crew_key = find_crew_by_announcer_name(pbp)
    return pbp, color, network, crew_key


# ---------------------------------------------------------------------------
# Transcript filename <-> ticker mapping (ported from old report)
# ---------------------------------------------------------------------------

_TEAM_NAME_TO_CODE = {
    'atlanta-hawks': 'ATL', 'hawks': 'ATL',
    'boston-celtics': 'BOS', 'celtics': 'BOS',
    'brooklyn-nets': 'BKN', 'nets': 'BKN',
    'charlotte-hornets': 'CHA', 'hornets': 'CHA',
    'chicago-bulls': 'CHI', 'bulls': 'CHI',
    'cleveland-cavaliers': 'CLE', 'cavaliers': 'CLE',
    'dallas-mavericks': 'DAL', 'mavericks': 'DAL',
    'denver-nuggets': 'DEN', 'nuggets': 'DEN',
    'detroit-pistons': 'DET', 'pistons': 'DET',
    'golden-state-warriors': 'GSW', 'warriors': 'GSW',
    'houston-rockets': 'HOU', 'rockets': 'HOU',
    'indiana-pacers': 'IND', 'pacers': 'IND',
    'los-angeles-clippers': 'LAC', 'clippers': 'LAC', 'la-clippers': 'LAC',
    'los-angeles-lakers': 'LAL', 'lakers': 'LAL', 'la-lakers': 'LAL',
    'memphis-grizzlies': 'MEM', 'grizzlies': 'MEM',
    'miami-heat': 'MIA', 'heat': 'MIA',
    'milwaukee-bucks': 'MIL', 'bucks': 'MIL',
    'minnesota-timberwolves': 'MIN', 'timberwolves': 'MIN',
    'new-orleans-pelicans': 'NOP', 'pelicans': 'NOP',
    'new-york-knicks': 'NYK', 'knicks': 'NYK',
    'oklahoma-city-thunder': 'OKC', 'thunder': 'OKC',
    'orlando-magic': 'ORL', 'magic': 'ORL',
    'philadelphia-76ers': 'PHI', '76ers': 'PHI', 'sixers': 'PHI',
    'phoenix-suns': 'PHX', 'suns': 'PHX',
    'portland-trail-blazers': 'POR', 'trail-blazers': 'POR', 'blazers': 'POR',
    'sacramento-kings': 'SAC', 'kings': 'SAC',
    'san-antonio-spurs': 'SAS', 'spurs': 'SAS',
    'toronto-raptors': 'TOR', 'raptors': 'TOR',
    'utah-jazz': 'UTA', 'jazz': 'UTA',
    'washington-wizards': 'WAS', 'wizards': 'WAS',
    'atl': 'ATL', 'bos': 'BOS', 'bkn': 'BKN', 'cha': 'CHA', 'chi': 'CHI',
    'cle': 'CLE', 'dal': 'DAL', 'den': 'DEN', 'det': 'DET', 'gsw': 'GSW',
    'hou': 'HOU', 'ind': 'IND', 'lac': 'LAC', 'lal': 'LAL', 'mem': 'MEM',
    'mia': 'MIA', 'mil': 'MIL', 'min': 'MIN', 'nop': 'NOP', 'nyk': 'NYK',
    'okc': 'OKC', 'orl': 'ORL', 'phi': 'PHI', 'phx': 'PHX', 'por': 'POR',
    'sac': 'SAC', 'sas': 'SAS', 'tor': 'TOR', 'uta': 'UTA', 'was': 'WAS',
}

_MONTH_NAMES = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
                'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']


def transcript_filename_to_ticker(filename: str) -> Optional[str]:
    """Convert a transcript filename to a Kalshi ticker."""
    m = re.match(r'(\d{4})-(\d{2})-(\d{2})_(.+)-at-(.+)\.txt', filename, re.I)
    if not m:
        return None
    year, month, day, away_raw, home_raw = m.groups()
    away = _TEAM_NAME_TO_CODE.get(away_raw.lower().strip())
    home = _TEAM_NAME_TO_CODE.get(home_raw.lower().strip())
    if not away:
        away = away_raw.upper() if len(away_raw) == 3 else None
    if not home:
        home = home_raw.upper() if len(home_raw) == 3 else None
    if not away or not home:
        return None
    return f"KXNBAMENTION-{year[2:]}{_MONTH_NAMES[int(month)-1]}{day}{away}{home}"


def match_transcripts_to_icdb(df: pd.DataFrame, icdb_tickers: set) -> pd.DataFrame:
    """Filter transcript DataFrame to games matching ICDB tickers."""
    if df.empty or not icdb_tickers:
        return pd.DataFrame()
    matched = []
    for idx, row in df.iterrows():
        fn = row.get("file", "")
        if fn:
            ticker = transcript_filename_to_ticker(fn)
            if ticker and ticker in icdb_tickers:
                matched.append(idx)
    return df.loc[matched].copy() if matched else pd.DataFrame()


def _tickers_for_team(announcers_df: pd.DataFrame, team_code: str, home_only: bool = False) -> set:
    """Return set of tickers where *team_code* appears (away or home, or home-only)."""
    tickers = set()
    for ticker in announcers_df["ticker"].dropna():
        away, home = extract_teams_from_ticker(ticker)
        if home_only:
            if home == team_code:
                tickers.add(ticker)
        else:
            if away == team_code or home == team_code:
                tickers.add(ticker)
    return tickers


# ---------------------------------------------------------------------------
# Transcript loading
# ---------------------------------------------------------------------------

def _load_subdir(base: str, subdir: str, search_patterns: Dict[str, str]) -> pd.DataFrame:
    """Load a transcript subdirectory if it exists and has files."""
    path = os.path.join(base, subdir)
    if os.path.exists(path) and any(f.endswith(".txt") for f in os.listdir(path)):
        return process_directory(path, search_patterns, verbose=False)
    return pd.DataFrame()


def load_all_crew_transcripts(
    crew: AnnouncerCrew,
    search_patterns: Dict[str, str],
    data_base_path: str = "data/nba/transcripts",
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Load transcript data for a crew from all subdirectories.
    Returns (transcripts, diarized, play_by_play, color_commentary).
    """
    base = os.path.join(data_base_path, crew.folder)
    return (
        _load_subdir(base, "transcripts", search_patterns),
        _load_subdir(base, "diarized", search_patterns),
        _load_subdir(base, "play_by_play", search_patterns),
        _load_subdir(base, "color_commentary", search_patterns),
    )


def load_all_crews_combined(
    search_patterns: Dict[str, str],
    data_base_path: str = "data/nba/transcripts",
) -> pd.DataFrame:
    """Load processed transcripts across all crews, combined."""
    all_dfs = []
    for crew in ANNOUNCER_CREWS.values():
        path = os.path.join(data_base_path, crew.folder, "transcripts")
        if os.path.exists(path):
            df = process_directory(path, search_patterns, verbose=False)
            if not df.empty:
                all_dfs.append(df)
    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()


# ---------------------------------------------------------------------------
# Chart helper
# ---------------------------------------------------------------------------

def create_phrase_chart(df: pd.DataFrame, phrase: str, display_label: Optional[str] = None) -> str:
    """Create bar chart for a phrase over games, return as base64 PNG."""
    if phrase not in df.columns or df.empty:
        return ""
    label = display_label or phrase
    df_sorted = df.sort_values("date").reset_index(drop=True)
    counts = df_sorted[phrase].values
    avg = float(counts.mean()) if len(counts) > 0 else 0.0
    appearances_with = int((counts > 0).sum())
    total = len(counts)
    pct = (appearances_with / total * 100) if total > 0 else 0

    fig, ax1 = plt.subplots(figsize=(14, 6))
    xvals = np.arange(len(df_sorted))
    ax1.bar(xvals, counts, alpha=0.7, color="#1d428a", width=0.8)
    ax1.axhline(y=avg, color="#c9082a", linestyle="--", alpha=0.7, label=f"Average ({avg:.1f})")
    ax1.set_ylabel("Count", color="#1d428a")
    ax1.set_title(f'"{label}" - {pct:.0f}% of games ({appearances_with}/{total})')
    ax1.set_xlabel("Game (chronological)")
    ax1.grid(True, alpha=0.3)
    if "text_length" in df_sorted.columns:
        ax2 = ax1.twinx()
        ax2.plot(xvals, df_sorted["text_length"].values, "o-", color="#95a5a6", alpha=0.4, markersize=3)
        ax2.set_ylabel("Word Count", color="#95a5a6")
    step = max(1, len(xvals) // 15)
    ax1.set_xticks(xvals[::step])
    dates = df_sorted["date"].values
    filenames = df_sorted["file"].values
    labels_list = []
    for i in range(0, len(dates), step):
        try:
            d = pd.Timestamp(dates[i]).strftime("%Y-%m-%d")
        except Exception:
            d = "N/A"
        short = Path(str(filenames[i])).stem[:20]
        labels_list.append(f"{d}\n{short}")
    ax1.set_xticklabels(labels_list, rotation=45, ha="right", fontsize=7)
    ax1.legend(loc="upper left")
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=100, bbox_inches="tight")
    buf.seek(0)
    img_data = base64.b64encode(buf.read()).decode()
    plt.close()
    return img_data


# ---------------------------------------------------------------------------
# Build report data
# ---------------------------------------------------------------------------

def build_game_report(
    event_ticker: str,
    announcers_df: pd.DataFrame,
    series_results: Dict[str, Dict[str, Optional[str]]],
    crew_override: Optional[str] = None,
    data_base_path: str = "data/nba/transcripts",
    fetch_live_vwap: bool = False,
    fetch_historical_vwap: bool = False,
) -> GameReport:
    """Assemble all data for a single game report."""
    # Teams
    away_code, home_code = extract_teams_from_ticker(event_ticker)
    away_name = get_team_name(away_code) if away_code else "Away"
    home_name = get_team_name(home_code) if home_code else "Home"

    # Crew lookup
    pbp, color, network, crew_key = lookup_crew_for_event(event_ticker, announcers_df)
    if crew_override:
        crew_key = crew_override
    crew = get_announcer_crew(crew_key) if crew_key else None
    if crew and not pbp:
        pbp = crew.name

    # Live markets + VWAP (candlestick API)
    try:
        market_infos = get_event_markets(event_ticker)
    except Exception as e:
        print(f"  Warning: could not fetch markets for {event_ticker}: {e}")
        market_infos = []
    if fetch_live_vwap:
        try:
            vwaps = get_event_vwaps([m.ticker for m in market_infos], event_ticker=event_ticker)
            for mi in market_infos:
                mi.vwap = vwaps.get(mi.ticker)
        except Exception as e:
            print(f"  Warning: VWAP fetch failed for {event_ticker}: {e}")
    phrases = [m.phrase for m in market_infos]

    # Search patterns
    announcer_names = get_all_announcer_names(crew) if crew else []
    phrase_to_search = {p: get_search_value_for_phrase(p) for p in phrases}
    phrase_to_search.update({a: a for a in announcer_names})
    search_patterns = generate_regex_patterns(phrase_to_search)

    # Hit rates
    overall_rates = calculate_hit_rates(series_results, phrases)
    announcer_rates = pd.DataFrame()
    announcer_matched_tickers: List[str] = []
    if pbp:
        announcer_rates = calculate_hit_rates_by_announcer(
            series_results, announcers_df, pbp, phrases, series_ticker="KXNBAMENTION"
        )
        if not announcers_df.empty:
            pbp_series = announcers_df["play_by_play"].fillna("").str.lower()
            announcer_matched_tickers = sorted(
                announcers_df.loc[
                    pbp_series == pbp.lower(), "ticker"
                ].dropna().astype(str).tolist()
            )
            announcer_matched_tickers = [
                t for t in announcer_matched_tickers
                if t.startswith("KXNBAMENTION-")
            ]

    # Load all transcript subdirs for this crew
    df_trans = df_diar = df_pbp = df_color = pd.DataFrame()
    if crew:
        df_trans, df_diar, df_pbp, df_color = load_all_crew_transcripts(
            crew, search_patterns, data_base_path
        )
        counts = [
            f"transcripts={len(df_trans)}", f"diarized={len(df_diar)}",
            f"play_by_play={len(df_pbp)}", f"color_commentary={len(df_color)}",
        ]
        print(f"    Loaded: {', '.join(counts)}")

    # All crews combined
    df_all = load_all_crews_combined(search_patterns, data_base_path)

    # ICDB color commentator filtering
    df_color_all = pd.DataFrame()   # all PBP with this color commentator
    df_color_crew = pd.DataFrame()  # exact PBP+color match
    if color and not announcers_df.empty and not df_all.empty:
        color_series = announcers_df["color_commentator"].fillna("").str.lower()
        pbp_series = announcers_df["play_by_play"].fillna("").str.lower()
        # All games with this color commentator (any PBP)
        color_games = announcers_df[
            color_series == color.lower()
        ]
        color_tickers = set(color_games["ticker"].dropna())
        df_color_all = match_transcripts_to_icdb(df_all, color_tickers)

        # Exact PBP + color match
        if pbp:
            exact_games = announcers_df[
                (pbp_series == pbp.lower()) &
                (color_series == color.lower())
            ]
            exact_tickers = set(exact_games["ticker"].dropna())
            # Match against this crew's transcripts specifically
            source = df_trans if not df_trans.empty else df_diar
            df_color_crew = match_transcripts_to_icdb(source, exact_tickers)

        print(f"    ICDB filter: All+{color}={len(df_color_all)}, "
              f"{pbp}+{color}={len(df_color_crew)}")

    # Team-based filtering
    df_away_team = df_home_team = df_home_team_home = pd.DataFrame()
    team_history_counts: Dict[str, int] = {}
    if not announcers_df.empty and not df_all.empty and away_code and home_code:
        away_tickers = _tickers_for_team(announcers_df, away_code)
        home_tickers = _tickers_for_team(announcers_df, home_code)
        home_home_tickers = _tickers_for_team(announcers_df, home_code, home_only=True)
        df_away_team = match_transcripts_to_icdb(df_all, away_tickers)
        df_home_team = match_transcripts_to_icdb(df_all, home_tickers)
        df_home_team_home = match_transcripts_to_icdb(df_all, home_home_tickers)
        team_history_counts[away_code] = len([t for t in away_tickers if t in series_results])
        team_history_counts[home_code] = len([t for t in home_tickers if t in series_results])
        print(f"    Team filter: {away_name}={len(df_away_team)}, "
              f"{home_name}={len(df_home_team)}, "
              f"{home_name} Home={len(df_home_team_home)}")

    # Historical VWAP for announcer's past games
    hist_crew_vwaps: Dict[str, float] = {}
    hist_crew_vwap_n: Dict[str, int] = {}
    if pbp and not announcers_df.empty and phrases:
        pbp_series = announcers_df["play_by_play"].fillna("").str.lower()
        current_date, _ = _event_date_sort_key(event_ticker)
        ann_tickers = list(
            announcers_df.loc[
                pbp_series == pbp.lower(), "ticker"
            ].dropna().astype(str)
        )
        # Exclude the current event from historical data
        ann_tickers = [
            t for t in ann_tickers
            if t.startswith("KXNBAMENTION-")
            and t != event_ticker
            and _event_date_sort_key(t)[0] < current_date
        ]
        if ann_tickers:
            print(f"    Fetching historical VWAP for {pbp} ({len(ann_tickers)} events)...")
            hist_data = fetch_historical_vwaps(
                ann_tickers,
                verbose=True,
                fetch_missing=fetch_historical_vwap,
            )
            for phrase in phrases:
                vals = [ev[phrase] for ev in hist_data.values() if phrase in ev]
                if vals:
                    hist_crew_vwaps[phrase] = float(np.mean(vals))
                    hist_crew_vwap_n[phrase] = len(vals)

    return GameReport(
        event_ticker=event_ticker,
        away_name=away_name,
        home_name=home_name,
        crew_key=crew_key,
        crew=crew,
        pbp_name=pbp,
        color_name=color,
        network=network,
        market_infos=market_infos,
        phrases=phrases,
        overall_rates=overall_rates,
        announcer_rates=announcer_rates,
        search_patterns=search_patterns,
        df_transcripts=df_trans,
        df_diarized=df_diar,
        df_play_by_play=df_pbp,
        df_color_commentary=df_color,
        df_all_transcripts=df_all,
        df_color_all=df_color_all,
        df_color_crew=df_color_crew,
        df_away_team=df_away_team,
        df_home_team=df_home_team,
        df_home_team_home=df_home_team_home,
        hist_crew_vwaps=hist_crew_vwaps,
        hist_crew_vwap_n=hist_crew_vwap_n,
        away_code=away_code or "",
        home_code=home_code or "",
        team_history_counts=team_history_counts,
        announcer_matched_tickers=announcer_matched_tickers,
    )


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

_CSS = """
:root {
    --primary: #1d428a; --secondary: #c9082a; --accent: #fdb927;
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
.matrix-wrap { overflow-x: auto; max-height: 70vh; border: 1px solid var(--border); }
.matrix-table { font-size: 0.78em; margin: 0; }
.matrix-table th { top: 0; z-index: 2; }
.matrix-table td, .matrix-table th { text-align: center; min-width: 54px; }
.matrix-table .event-cell { text-align: left; min-width: 210px; position: sticky; left: 0; background: white; z-index: 1; }
.matrix-table th.event-cell { background: var(--primary); z-index: 3; }
.matrix-yes { background: #c6f6d5; color: #22543d; font-weight: 700; }
.matrix-no { background: #fed7d7; color: #742a2a; font-weight: 700; }
.matrix-missing { background: #edf2f7; color: #a0aec0; }
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
    """Format a frequency cell as 'X% (n/N)'."""
    if df.empty or phrase not in df.columns:
        return "-"
    hits = int((df[phrase] > 0).sum())
    total = len(df)
    pct = hits / total * 100 if total > 0 else 0
    return f"{pct:.0f}% ({hits}/{total})"


def _kalshi_rate_cell(rates_df: pd.DataFrame, phrase: str) -> str:
    """Format Kalshi rate as 'X% (hits/total)'."""
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
        for _, existing_df in result:
            if _dfs_identical(df, existing_df, phrases):
                is_dup = True
                break
        if not is_dup:
            result.append((name, df))
    return result


def _render_combined_table(game: GameReport) -> str:
    """Single table with Kalshi rates, VWAP, prices, and transcript frequencies."""
    if not game.market_infos:
        return '<p style="color:var(--text-muted);">No market data available.</p>'

    crew_name = game.crew.name if game.crew else game.pbp_name or "Crew"

    # Build list of (header_name, dataframe) for transcript sources
    sources: List[Tuple[str, pd.DataFrame]] = []
    if not game.df_transcripts.empty:
        sources.append((f"Processed ({len(game.df_transcripts)})", game.df_transcripts))
    if not game.df_diarized.empty:
        sources.append((f"Raw ({len(game.df_diarized)})", game.df_diarized))
    if not game.df_play_by_play.empty:
        sources.append((f"PBP ({len(game.df_play_by_play)})", game.df_play_by_play))
    if not game.df_color_commentary.empty:
        sources.append((f"Color ({len(game.df_color_commentary)})", game.df_color_commentary))
    if not game.df_color_crew.empty:
        sources.append((f"{crew_name}+{game.color_name} ({len(game.df_color_crew)})", game.df_color_crew))
    if not game.df_color_all.empty:
        sources.append((f"All+{game.color_name} ({len(game.df_color_all)})", game.df_color_all))
    if not game.df_away_team.empty:
        away_label = game.away_code or game.away_name
        sources.append((f"{away_label} ({len(game.df_away_team)})", game.df_away_team))
    if not game.df_home_team.empty:
        home_label = game.home_code or game.home_name
        sources.append((f"{home_label} ({len(game.df_home_team)})", game.df_home_team))
    if not game.df_home_team_home.empty:
        home_label = game.home_code or game.home_name
        sources.append((f"{home_label} Home ({len(game.df_home_team_home)})", game.df_home_team_home))

    # Deduplicate sources with identical frequency data
    sources = _dedup_sources(sources, game.phrases)

    # Build lookup for overall rates (need rate value for delta calc)
    overall_lu: Dict[str, float] = {}
    if not game.overall_rates.empty:
        for _, r in game.overall_rates.iterrows():
            overall_lu[r["Phrase"]] = r["Hit Rate"]

    # Header
    has_hist = bool(game.hist_crew_vwaps)
    show_both_rates = not _rates_identical(game.overall_rates, game.announcer_rates, game.phrases)

    headers = ["Phrase"]
    if show_both_rates:
        headers.append("Kalshi Rate (All)")
    headers.append(f"Kalshi Rate ({crew_name})" if show_both_rates else "Kalshi Rate")
    headers.append("VWAP")
    if has_hist:
        headers.append(f"Avg VWAP ({crew_name})")
    headers += ["Bid/Ask", "Last", "Vol", "Delta"]
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

        # Historical avg VWAP for crew
        hist_cell = ""
        if has_hist:
            hv = game.hist_crew_vwaps.get(p)
            hn = game.hist_crew_vwap_n.get(p, 0)
            hist_cell = f"<td>{hv:.0f}c (n={hn})</td>" if hv is not None else "<td>-</td>"

        freq_cells = "".join(f"<td>{_freq_cell(df, p)}</td>" for _, df in sources)

        # Use best available rate when rates are identical
        rate_str = ann_str if not game.announcer_rates.empty else ovr_str
        ovr_cell = f"<td>{ovr_str}</td>" if show_both_rates else ""
        ann_cell = f"<td>{ann_str}</td>" if show_both_rates else f"<td>{rate_str}</td>"

        html += (
            f"<tr><td>{display}</td>"
            f"{ovr_cell}{ann_cell}"
            f"<td>{vwap_str}</td>{hist_cell}<td>{price_str}</td><td>{last_str}</td>"
            f"<td>{vol_str}</td><td>{_delta_cell(delta)}</td>"
            f"{freq_cells}</tr>\n"
        )

    html += "</table></div>"
    return html


# ---------------------------------------------------------------------------
# False positive / false negative analysis
# ---------------------------------------------------------------------------

def _calculate_fp_fn(
    df: pd.DataFrame,
    series_results: Dict[str, Dict[str, Optional[str]]],
    phrases: List[str],
) -> Dict[str, Dict[str, int]]:
    """
    For each phrase, compare transcript presence to Kalshi result.

    Only considers *matched* games — those with both transcript data and a
    resolved Kalshi result for the phrase.

    Returns {phrase: {tp, fp, fn, tn, matched}}.
    """
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


def _fp_fn_cell(count: int, denom: int) -> str:
    if denom == 0:
        return "-"
    pct = count / denom * 100
    return f"{count}/{denom} ({pct:.0f}%)"


def _fp_fn_stats_identical(crew_stats: Dict, all_stats: Dict, phrases: List[str]) -> bool:
    """Check if crew-specific and all-crew FP/FN stats are identical."""
    if not crew_stats or not all_stats:
        return False
    for p in phrases:
        if crew_stats.get(p) != all_stats.get(p):
            return False
    return True


def _render_fp_fn_table(
    game: GameReport,
    series_results: Dict[str, Dict[str, Optional[str]]],
) -> str:
    """Render false-positive / false-negative table for crew and all crews."""
    crew_name = game.crew.name if game.crew else game.pbp_name or "Crew"

    crew_df = game.df_transcripts if not game.df_transcripts.empty else game.df_diarized
    crew_stats = _calculate_fp_fn(crew_df, series_results, game.phrases) if not crew_df.empty else {}
    all_stats = _calculate_fp_fn(game.df_all_transcripts, series_results, game.phrases) if not game.df_all_transcripts.empty else {}

    if not crew_stats and not all_stats:
        return '<p style="color:var(--text-muted);">No matched games for FP/FN analysis.</p>'

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
            fp_str = _fp_fn_cell(s["fp"], s["fp"] + s["tp"])
            fn_str = _fp_fn_cell(s["fn"], s["fn"] + s["tn"])
            acc = (s["tp"] + s["tn"]) / m * 100
            html += f"<td>{m}</td><td>{fp_str}</td><td>{fn_str}</td><td>{acc:.0f}%</td>"
        html += "</tr>\n"

    html += "</table></div>"
    return html


def _render_announcer_comparison(
    game: GameReport,
    series_results: Dict[str, Dict[str, Optional[str]]],
    announcers_df: pd.DataFrame,
) -> str:
    """Announcer comparison table."""
    if announcers_df.empty or not game.phrases:
        return ""
    pbp_names = announcers_df["play_by_play"].dropna().unique()
    if len(pbp_names) == 0:
        return ""

    top_phrases = game.phrases[:5]
    rows_html = []
    for name in sorted(pbp_names):
        ann_tickers = set(
            announcers_df.loc[
                announcers_df["play_by_play"].str.lower() == name.lower(), "ticker"
            ]
        )
        n_games = sum(1 for t in ann_tickers if t in series_results)
        if n_games == 0:
            continue
        filtered = {k: v for k, v in series_results.items() if k in ann_tickers}
        rates = calculate_hit_rates(filtered, top_phrases)
        if rates.empty:
            continue
        is_current = name.lower() == game.pbp_name.lower()
        hl = ' style="background:#e8f4fd;"' if is_current else ""
        mk = " *" if is_current else ""
        cells = ""
        for ph in top_phrases:
            pr = rates.loc[rates["Phrase"] == ph, "Hit Rate"]
            cells += f"<td>{_rate_cell(pr.iloc[0] if not pr.empty else None)}</td>"
        rows_html.append(f'<tr{hl}><td><strong>{name}{mk}</strong></td><td>{n_games}</td>{cells}</tr>')

    if not rows_html:
        return ""
    ph_headers = "".join(f"<th>{get_search_value_for_phrase(p)[:20]}</th>" for p in top_phrases)
    return f"""
<h3>Announcer Comparison (Kalshi hit rates)</h3>
<div style="overflow-x:auto;"><table>
<tr><th>Announcer</th><th>Games</th>{ph_headers}</tr>
{''.join(rows_html)}
</table></div>
<p style="color:var(--text-muted);font-size:0.85em;">* = this game's announcer. Top 5 phrases shown.</p>
"""


def _event_date_sort_key(ticker: str) -> Tuple[pd.Timestamp, str]:
    from src.nba.kalshi_api import _extract_date_from_ticker

    dt = _extract_date_from_ticker(ticker)
    if dt is None:
        return pd.Timestamp.min, ticker
    return pd.Timestamp(dt), ticker


def _resolved_kx_history_for_announcer(
    game: GameReport,
    series_results: Dict[str, Dict[str, Optional[str]]],
) -> List[str]:
    """Past resolved KXNBAMENTION events called by this game's PBP announcer."""
    if not game.pbp_name:
        return []
    current_date, _ = _event_date_sort_key(game.event_ticker)
    tickers = []
    for ticker in game.announcer_matched_tickers:
        if not ticker.startswith("KXNBAMENTION-") or ticker == game.event_ticker:
            continue
        event_date, _ = _event_date_sort_key(ticker)
        if event_date >= current_date:
            continue
        results = series_results.get(ticker)
        if not results:
            continue
        if not any(str(v).lower() in ("yes", "no") for v in results.values() if v is not None):
            continue
        tickers.append(ticker)
    return sorted(set(tickers), key=_event_date_sort_key, reverse=True)


def _render_phrase_occurrence_matrix(
    game: GameReport,
    series_results: Dict[str, Dict[str, Optional[str]]],
) -> str:
    """Render phrase-by-event Kalshi resolution history for this announcer."""
    history_tickers = _resolved_kx_history_for_announcer(game, series_results)
    if not history_tickers:
        return '<p style="color:var(--text-muted);">No past resolved KXNBAMENTION events found for this announcer.</p>'

    phrase_headers = "".join(
        f"<th title=\"{get_search_value_for_phrase(p)}\">{get_search_value_for_phrase(p)[:18]}</th>"
        for p in game.phrases
    )
    html = '<div class="matrix-wrap"><table class="matrix-table">\n'
    html += f'<tr><th class="event-cell">Past KXNBAMENTION Event</th>{phrase_headers}</tr>\n'

    for ticker in history_tickers:
        away, home = extract_teams_from_ticker(ticker)
        away_name = get_team_name(away) if away else "?"
        home_name = get_team_name(home) if home else "?"
        event_results = series_results.get(ticker, {})
        label = f"{ticker}<br><span style=\"color:var(--text-muted);font-weight:400;\">{away_name} @ {home_name}</span>"
        html += f'<tr><td class="event-cell">{label}</td>'
        for phrase in game.phrases:
            result = event_results.get(phrase)
            result_norm = str(result).strip().lower() if result is not None else ""
            if result_norm == "yes":
                html += '<td class="matrix-yes" title="Resolved YES">Y</td>'
            elif result_norm == "no":
                html += '<td class="matrix-no" title="Resolved NO">N</td>'
            else:
                html += '<td class="matrix-missing" title="Phrase was not present as a market on this event">&nbsp;</td>'
        html += '</tr>\n'

    html += '</table></div>'
    html += '<p style="color:var(--text-muted);font-size:0.85em;">Grey cells mean the phrase was not present as a Kalshi market for that past KXNBAMENTION event.</p>'
    return html


def _render_phrase_trends(game: GameReport) -> str:
    """Collapsible phrase trend charts from processed transcripts."""
    df = game.df_transcripts if not game.df_transcripts.empty else game.df_diarized
    if df.empty:
        return '<p style="color:var(--text-muted);">No transcript data for charts.</p>'
    parts = []
    for phrase in game.phrases:
        if phrase not in df.columns:
            continue
        display = get_search_value_for_phrase(phrase)
        try:
            chart = create_phrase_chart(df, phrase, display)
        except Exception:
            chart = ""
        count = int((df[phrase] > 0).sum())
        total = len(df)
        pct = count / total * 100 if total > 0 else 0
        img = f'<img src="data:image/png;base64,{chart}" class="chart-img">' if chart else "<p>No chart</p>"
        parts.append(f"""
<details>
    <summary>{display} - {pct:.0f}% ({count}/{total} games)</summary>
    <div class="details-content">{img}</div>
</details>""")
    return "\n".join(parts) if parts else '<p style="color:var(--text-muted);">No phrase data.</p>'


def _gather_recent_contexts(
    game: GameReport,
    max_per_phrase: int = 6,
    context_window: int = 150,
    data_base_path: str = "data/nba/transcripts",
) -> Dict[str, List[Tuple[str, str]]]:
    """Gather recent transcript contexts for each phrase with highlighted matches."""
    import html as html_mod

    results: Dict[str, List[Tuple[str, str]]] = {}
    dirs_to_scan: List[Tuple[str, str]] = []
    if game.crew:
        base = os.path.join(data_base_path, game.crew.folder)
        for subdir, label in [("transcripts", "Processed"), ("diarized", "Raw"),
                              ("play_by_play", "PBP"), ("color_commentary", "Color")]:
            p = os.path.join(base, subdir)
            if os.path.isdir(p):
                dirs_to_scan.append((label, p))
    else:
        for crew in ANNOUNCER_CREWS.values():
            p = os.path.join(data_base_path, crew.folder, "transcripts")
            if os.path.isdir(p):
                dirs_to_scan.append((crew.name, p))

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

    # --- Transcript counts ---
    sources = [
        ("Crew transcripts (processed)", game.df_transcripts),
        ("Crew diarized (raw)", game.df_diarized),
        ("Play-by-play", game.df_play_by_play),
        ("Color commentary", game.df_color_commentary),
        ("All crews combined", game.df_all_transcripts),
        ("Color filter (crew+color)", game.df_color_crew),
        ("Color filter (all+color)", game.df_color_all),
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
            html += '<table><tr><th>Name</th><th>Games w/ &ge;1</th><th>Total Mentions</th><th>Avg/Game</th><th>Max</th></tr>\n'
            for name in name_cols:
                total = int(df[name].sum())
                games_with = int((df[name] > 0).sum())
                avg = f"{df[name].mean():.1f}"
                mx = int(df[name].max())
                html += f'<tr><td>{name}</td><td>{games_with}/{len(df)}</td><td>{total}</td><td>{avg}</td><td>{mx}</td></tr>\n'
            html += '</table>\n'
        if not game.df_all_transcripts.empty:
            all_name_cols = [name for name in announcer_names if name in game.df_all_transcripts.columns]
            if all_name_cols:
                df_all = game.df_all_transcripts
                html += '<h4 style="margin:12px 0 6px;">Announcer Name Appearances (all crews combined)</h4>\n'
                html += '<table><tr><th>Name</th><th>Games w/ &ge;1</th><th>Total Mentions</th><th>Avg/Game</th><th>Max</th></tr>\n'
                for name in all_name_cols:
                    total = int(df_all[name].sum())
                    games_with = int((df_all[name] > 0).sum())
                    avg = f"{df_all[name].mean():.1f}"
                    mx = int(df_all[name].max())
                    html += f'<tr><td>{name}</td><td>{games_with}/{len(df_all)}</td><td>{total}</td><td>{avg}</td><td>{mx}</td></tr>\n'
                html += '</table>\n'

    # --- Kalshi team history ---
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
            status = "&#9989;" if resolved else "&#8987;"
            result_info = ""
            if resolved and series_results[t]:
                results = series_results[t]
                hits = [p for p, r in results.items() if r and r.lower() == "yes"]
                if hits:
                    result_info = f' &mdash; hits: {", ".join(hits)}'
                else:
                    result_info = " &mdash; no hits"
            is_current = " &larr; <strong>current</strong>" if t == game.event_ticker else ""
            html += f'{status} {t} ({away_name} @ {home_name}){result_info}{is_current}<br>\n'
        html += '</div>\n'
    else:
        html += '<p style="color:var(--text-muted);">No announcer events found in combined_announcers.csv</p>\n'

    return html


def render_html_report(game: GameReport, series_results: Dict, announcers_df: pd.DataFrame) -> str:
    """Render a complete single-game HTML report."""
    crew_display = game.crew.name if game.crew else (game.pbp_name or "Unknown")
    n = lambda df: len(df) if not df.empty else 0

    # Gather recent contexts with highlighting
    contexts = _gather_recent_contexts(game)

    # Only show stat boxes that have data
    stat_boxes = [(len(game.phrases), "Phrases")]
    for val, label in [
        (n(game.df_transcripts), "Processed"),
        (n(game.df_diarized), "Diarized"),
        (n(game.df_play_by_play), "Play-by-Play"),
        (n(game.df_color_commentary), "Color Commentary"),
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
<title>{game.event_ticker} | NBA Report</title>
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

<details open><summary>Announcer Past KXNBAMENTION Phrase Matrix</summary>
<div class="details-content">{_render_phrase_occurrence_matrix(game, series_results)}</div>
</details>

<details><summary>Transcript &amp; Data Summary</summary>
<div class="details-content">{_render_transcript_summary(game, series_results)}</div>
</details>

<details><summary>Phrase Trends</summary>
<div class="details-content">{_render_phrase_trends(game)}</div>
</details>
"""]

    ann_html = _render_announcer_comparison(game, series_results, announcers_df)
    if ann_html:
        parts.append(f"""
<details><summary>Announcer Comparison</summary>
<div class="details-content">{ann_html}</div>
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
    parser = argparse.ArgumentParser(
        description="Generate NBA phrase analysis reports with live prices and hit rates.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("event_ticker", nargs="?", help="Kalshi event ticker")
    parser.add_argument("--all-open", action="store_true", help="Generate for all open events")
    parser.add_argument("--crew", "-c", default=None, help="Override crew detection")
    parser.add_argument("-o", "--output", default=None, help="Output HTML path")
    parser.add_argument("--data-dir", default="data/nba/transcripts", help="Transcript data dir")
    parser.add_argument("--icdb-path", default="data/nba/icdb/combined_announcers.csv")
    parser.add_argument("--series", default="KXNBAMENTION", help="Series ticker")
    parser.add_argument("--fetch-live-vwap", action="store_true", help="Fetch current-event VWAP candles")
    parser.add_argument("--fetch-historical-vwap", action="store_true", help="Backfill missing historical VWAP candles")
    parser.add_argument("--list-crews", action="store_true")
    args = parser.parse_args()

    if args.list_crews:
        for key, crew in ANNOUNCER_CREWS.items():
            print(f"  {key:<20} {crew.name}")
        return

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
    announcers_df = load_announcer_data(args.icdb_path)
    print("Fetching series results...")
    series_results = fetch_series_results(args.series, delay=0.1, verbose=True)

    os.makedirs("reports", exist_ok=True)
    paths = []
    for et in event_tickers:
        print(f"\nBuilding report for {et}...")
        game = build_game_report(et, announcers_df, series_results,
                                 crew_override=args.crew, data_base_path=args.data_dir,
                                 fetch_live_vwap=args.fetch_live_vwap,
                                 fetch_historical_vwap=args.fetch_historical_vwap)
        html = render_html_report(game, series_results, announcers_df)
        if args.output and len(event_tickers) == 1:
            out = args.output
        else:
            out = f"reports/{et}_report.html"
        with open(out, "w", encoding="utf-8") as f:
            f.write(html)
        paths.append(os.path.abspath(out))
        print(f"  Saved: {out}")

    for p in paths:
        print(f"  file://{p}")


if __name__ == "__main__":
    main()
