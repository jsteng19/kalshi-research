#!/usr/bin/env python3
"""
Kalshi API wrapper for NCAAB Mentions markets.

Thin wrapper around the NBA kalshi_api module, defaulting to KXNCAABMENTION
series and data/ncaab cache paths.
"""

import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

_project_root = Path(__file__).resolve().parent.parent.parent

# Reuse the NBA kalshi_api module (all the heavy lifting is there)
sys.path.insert(0, str(_project_root / "src"))
from nba.kalshi_api import (
    MarketInfo,
    _get_md,
    _get_all_events,
    _extract_phrase,
    _attr,
    _extract_date_from_ticker,
    _get_vwaps_batch,
    _vwap_from_candles,
    _to_numeric,
    get_event_markets,
    get_event_vwaps,
    results_to_dataframe,
)

SERIES = "KXNCAABMENTION"
_DEFAULT_VWAP_CACHE = str(_project_root / "data/ncaab" / "cache" / "vwap_cache.json")
_DEFAULT_SERIES_CACHE = str(_project_root / "data/ncaab" / "cache" / "series_results_cache.json")


def get_open_event_tickers(series_ticker: str = SERIES) -> List[str]:
    """Return tickers for all currently-open NCAAB mention events."""
    from nba.kalshi_api import get_open_event_tickers as _get_open
    return _get_open(series_ticker)


def get_market_phrases(event_ticker: str) -> List[str]:
    """Return just the phrase list for an event."""
    return [m.phrase for m in get_event_markets(event_ticker)]


def fetch_series_results(
    series_ticker: str = SERIES,
    cache_path: str = _DEFAULT_SERIES_CACHE,
    delay: float = 0.1,
    verbose: bool = True,
    refresh_cache: bool = False,
) -> Dict[str, Dict[str, Optional[str]]]:
    """Fetch results for every event in the NCAAB series."""
    from nba.kalshi_api import fetch_series_results as _fetch
    return _fetch(series_ticker, cache_path=cache_path, delay=delay, verbose=verbose, refresh_cache=refresh_cache)


def fetch_historical_vwaps(
    event_tickers: List[str],
    cache_path: str = _DEFAULT_VWAP_CACHE,
    delay: float = 0.12,
    verbose: bool = True,
) -> Dict[str, Dict[str, float]]:
    """Fetch VWAP for multiple events with disk caching."""
    from nba.kalshi_api import fetch_historical_vwaps as _fetch
    return _fetch(event_tickers, cache_path=cache_path, delay=delay, verbose=verbose)


def calculate_hit_rates(
    results: Dict[str, Dict[str, Optional[str]]],
    phrases: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Calculate hit rates from series results."""
    from nba.kalshi_api import calculate_hit_rates as _calc
    return _calc(results, phrases)


def calculate_hit_rates_by_announcer(
    results: Dict[str, Dict[str, Optional[str]]],
    announcers_df: pd.DataFrame,
    pbp_name: str,
    phrases: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Calculate hit rates filtered to events called by a specific announcer."""
    from nba.kalshi_api import calculate_hit_rates_by_announcer as _calc
    return _calc(results, announcers_df, pbp_name, phrases)
