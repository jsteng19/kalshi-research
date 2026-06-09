#!/usr/bin/env python3
"""
Kalshi API wrapper for NBA Mentions markets.

Uses the official kalshi_python_sync SDK via kalshi_tools instead of the
deprecated ``kalshi`` package.

Functions:
    get_open_event_tickers  - list currently active KXNBAMENTION events
    get_event_markets       - market-level detail (phrase, result, prices)
    get_market_phrases      - just the phrase list for an event
    fetch_series_results    - resolved results for all events in a series
    calculate_hit_rates     - hit/miss rates from series results
"""

import json
import os
import re
import sys
import time
import importlib.util
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Load .env from project root so KALSHI_API_KEY_ID etc. are available
# ---------------------------------------------------------------------------
_project_root = Path(__file__).resolve().parent.parent.parent
_dotenv_path = _project_root / ".env"
if _dotenv_path.exists():
    from dotenv import load_dotenv
    load_dotenv(_dotenv_path)

# Map legacy env var names to what kalshi_tools expects
if not os.environ.get("KALSHI_API_KEY_ID") and os.environ.get("KALSHI_READ_KEYID"):
    os.environ["KALSHI_API_KEY_ID"] = os.environ["KALSHI_READ_KEYID"]
if not os.environ.get("KALSHI_PRIVATE_KEY_PATH") and os.environ.get("KALSHI_READ_KEYFILE"):
    os.environ["KALSHI_PRIVATE_KEY_PATH"] = os.environ["KALSHI_READ_KEYFILE"]

# ---------------------------------------------------------------------------
# Add kalshi_tools to sys.path (lives beside this repo)
# ---------------------------------------------------------------------------
_kalshi_tools_src = _project_root.parent / "kalshi_tools" / "src"
if str(_kalshi_tools_src) not in sys.path:
    sys.path.insert(0, str(_kalshi_tools_src))


def _get_md():
    """Lazily create a MarketData instance."""
    # Avoid importing kalshi_tools package top-level, which may include
    # optional exports that are not present in all local checkouts.
    core_dir = _project_root.parent / "kalshi_tools" / "src" / "kalshi_tools" / "core"
    client_path = core_dir / "client.py"
    market_data_path = core_dir / "market_data.py"

    client_spec = importlib.util.spec_from_file_location("kalshi_tools_core_client", client_path)
    market_data_spec = importlib.util.spec_from_file_location("kalshi_tools_core_market_data", market_data_path)

    if client_spec is None or client_spec.loader is None:
        raise ImportError(f"Unable to load kalshi_tools client module from {client_path}")
    if market_data_spec is None or market_data_spec.loader is None:
        raise ImportError(f"Unable to load kalshi_tools market_data module from {market_data_path}")

    client_mod = importlib.util.module_from_spec(client_spec)
    market_data_mod = importlib.util.module_from_spec(market_data_spec)
    client_spec.loader.exec_module(client_mod)
    market_data_spec.loader.exec_module(market_data_mod)

    MarketData = market_data_mod.MarketData
    client = client_mod.KalshiClient(
        api_key_id=client_mod._resolve_api_key_id(),
        private_key_path=client_mod._resolve_private_key_path(),
        api_base="https://external-api.kalshi.com/trade-api/v2",
    )
    return MarketData(client)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class MarketInfo:
    """Per-market snapshot returned by get_event_markets."""
    ticker: str
    phrase: str
    result: Optional[str]       # 'yes' / 'no' / None (unresolved)
    yes_bid: Optional[int]      # cents
    yes_ask: Optional[int]      # cents
    last_price: Optional[int]   # cents
    volume: Optional[int]
    vwap: Optional[float] = None  # cents (volume-weighted average price)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _get_all_events(md, series_ticker: str, status: Optional[str] = None) -> list:
    """Fetch all events in a series using pykalshi's fetch_all."""
    from pykalshi import MarketStatus
    kwargs = dict(series_ticker=series_ticker, limit=100, fetch_all=True)
    if status:
        try:
            kwargs["status"] = MarketStatus(status.lower())
        except (ValueError, KeyError):
            pass
    return list(md.client.get_events(**kwargs))


def get_open_event_tickers(series_ticker: str = "KXNBAMENTION") -> List[str]:
    """
    Return tickers for all currently-open events in the series.

    An event is open when its status is 'open'.
    """
    md = _get_md()
    events = _get_all_events(md, series_ticker, status="open")
    tickers = []
    for evt in events:
        if hasattr(evt, "event_ticker"):
            tickers.append(evt.event_ticker)
        elif isinstance(evt, dict):
            tickers.append(evt["event_ticker"])
    return tickers


def _extract_phrase(market_obj) -> Optional[str]:
    """Extract the phrase string from a market object/dict."""
    if hasattr(market_obj, "custom_strike"):
        cs = market_obj.custom_strike
    elif isinstance(market_obj, dict):
        cs = market_obj.get("custom_strike")
    else:
        cs = None

    if cs and isinstance(cs, dict):
        word = cs.get("Word")
        if word:
            return word

    # Fallback: yes_sub_title
    if hasattr(market_obj, "yes_sub_title"):
        return market_obj.yes_sub_title
    if isinstance(market_obj, dict):
        return market_obj.get("yes_sub_title")
    return None


def _attr(obj, name, default=None):
    """Get attribute from a Pydantic model or dict."""
    if hasattr(obj, name):
        return getattr(obj, name)
    if isinstance(obj, dict):
        return obj.get(name, default)
    return default


def _cents_from_dollars(value) -> Optional[int]:
    """Convert Kalshi fixed-point dollar strings like '0.5600' to cents."""
    if value in (None, ""):
        return None
    try:
        return int(round(float(value) * 100))
    except (TypeError, ValueError):
        return None


def _volume_from_fp(value) -> Optional[int]:
    """Convert fixed-point contract count strings to whole-contract display volume."""
    if value in (None, ""):
        return None
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return None


def _get_event_markets_public(event_ticker: str) -> List[MarketInfo]:
    """Fetch event markets from Kalshi's unauthenticated event endpoint."""
    cache_path = _DEFAULT_EVENT_MARKETS_CACHE_DIR / f"{event_ticker}.json"
    base = "https://external-api.kalshi.com/trade-api/v2/events/"
    url = base + urllib.parse.quote(event_ticker, safe="")
    req = urllib.request.Request(url, headers={"User-Agent": "kalshi-research/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        os.makedirs(_DEFAULT_EVENT_MARKETS_CACHE_DIR, exist_ok=True)
        with open(cache_path, "w") as f:
            json.dump(payload, f)
    except Exception:
        if not cache_path.exists():
            raise
        with open(cache_path) as f:
            payload = json.load(f)

    infos: List[MarketInfo] = []
    for mkt in payload.get("markets", []):
        phrase = _extract_phrase(mkt)
        if phrase is None:
            continue
        infos.append(MarketInfo(
            ticker=_attr(mkt, "ticker", ""),
            phrase=phrase,
            result=_attr(mkt, "result") or None,
            yes_bid=_attr(mkt, "yes_bid") or _cents_from_dollars(_attr(mkt, "yes_bid_dollars")),
            yes_ask=_attr(mkt, "yes_ask") or _cents_from_dollars(_attr(mkt, "yes_ask_dollars")),
            last_price=_attr(mkt, "last_price") or _cents_from_dollars(_attr(mkt, "last_price_dollars")),
            volume=_attr(mkt, "volume") or _volume_from_fp(_attr(mkt, "volume_fp")),
        ))
    return infos


def get_event_markets(event_ticker: str) -> List[MarketInfo]:
    """
    Fetch all markets for an event with phrase, result, and prices.
    """
    md = _get_md()
    try:
        markets = md.get_markets(event_ticker=event_ticker, limit=200)
    except Exception:
        return _get_event_markets_public(event_ticker)
    infos = []
    for mkt in markets:
        phrase = _extract_phrase(mkt)
        if phrase is None:
            continue
        infos.append(MarketInfo(
            ticker=_attr(mkt, "ticker", ""),
            phrase=phrase,
            result=_attr(mkt, "result"),
            yes_bid=_attr(mkt, "yes_bid"),
            yes_ask=_attr(mkt, "yes_ask"),
            last_price=_attr(mkt, "last_price"),
            volume=_attr(mkt, "volume"),
        ))
    return infos


# ---------------------------------------------------------------------------
# VWAP via candlestick API
# ---------------------------------------------------------------------------

_MONTH_MAP = {m: i + 1 for i, m in enumerate(
    ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
     "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
)}


def _extract_date_from_ticker(event_ticker: str) -> Optional[datetime]:
    """Extract game date from a ticker like KXNBAMENTION-26FEB09MILORL."""
    m = re.match(r"\w+-(\d{2})([A-Z]{3})(\d{2})", event_ticker)
    if not m:
        return None
    yy, mon, dd = m.groups()
    month_num = _MONTH_MAP.get(mon)
    if not month_num:
        return None
    return datetime(2000 + int(yy), month_num, int(dd))


def _vwap_from_candles(candles) -> Optional[float]:
    """Calculate VWAP (cents) from a list of candlestick objects."""
    total_value = 0.0
    total_volume = 0
    for c in candles:
        price_obj = _attr(c, "price")
        if price_obj is None:
            continue
        mean_price = _attr(price_obj, "mean")
        volume = _attr(c, "volume")
        if mean_price is not None and volume:
            total_value += mean_price * volume
            total_volume += volume
    return total_value / total_volume if total_volume > 0 else None


def _get_vwaps_batch(
    md,
    market_tickers: List[str],
    game_date: Optional[datetime] = None,
) -> Dict[str, float]:
    """Batch-fetch daily candles and compute VWAP per market ticker."""
    if not market_tickers:
        return {}
    if game_date is None:
        game_date = datetime.now()
    start_ts = int((game_date - timedelta(days=7)).timestamp())
    end_ts = int((game_date + timedelta(days=2)).timestamp())

    vwaps: Dict[str, float] = {}

    # batch endpoint accepts up to 100 tickers
    from pykalshi import CandlestickPeriod
    for i in range(0, len(market_tickers), 100):
        chunk = market_tickers[i : i + 100]
        try:
            resp = md.client.get_candlesticks_batch(
                tickers=chunk,
                start_ts=start_ts,
                end_ts=end_ts,
                period=CandlestickPeriod.ONE_DAY,
            )
            for ticker, candle_resp in resp.items():
                candles = getattr(candle_resp, "candlesticks", None) or []
                if candles:
                    v = _vwap_from_candles(candles)
                    if v is not None:
                        vwaps[ticker] = v
        except Exception:
            # Fallback: per-market fetch via kalshi_tools wrapper
            for ticker in chunk:
                try:
                    candles = md.get_candlesticks(
                        ticker, period_interval=1440,
                        start_ts=start_ts, end_ts=end_ts,
                    )
                    v = _vwap_from_candles(candles)
                    if v is not None:
                        vwaps[ticker] = v
                except Exception:
                    pass
                time.sleep(0.05)
    return vwaps


def get_event_vwaps(
    market_tickers: List[str],
    event_ticker: Optional[str] = None,
) -> Dict[str, float]:
    """
    Fetch VWAP (cents) for market tickers using batch candlestick API.

    Returns {market_ticker: vwap_cents}.
    """
    game_date = _extract_date_from_ticker(event_ticker) if event_ticker else None
    md = _get_md()
    return _get_vwaps_batch(md, market_tickers, game_date)


# ---------------------------------------------------------------------------
# VWAP cache (incremental, persists between runs)
# ---------------------------------------------------------------------------

_DEFAULT_VWAP_CACHE = str(_project_root / "data/nba" / "cache" / "vwap_cache.json")
_DEFAULT_SERIES_CACHE = str(_project_root / "data/nba" / "cache" / "series_results_cache.json")
_DEFAULT_EVENT_MARKETS_CACHE_DIR = _project_root / "data/nba" / "cache" / "event_markets"


def _load_vwap_cache(path: str) -> dict:
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {"version": 1, "events": {}}


def _save_vwap_cache(cache: dict, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(cache, f, indent=2)


def _load_series_cache(path: str) -> dict:
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {"version": 1, "series": {}}


def _save_series_cache(cache: dict, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(cache, f, indent=2)


def fetch_historical_vwaps(
    event_tickers: List[str],
    cache_path: str = _DEFAULT_VWAP_CACHE,
    delay: float = 0.12,
    verbose: bool = True,
    fetch_missing: bool = True,
) -> Dict[str, Dict[str, float]]:
    """
    Fetch VWAP for multiple events, using a disk cache so only new events
    are actually fetched.

    Returns ``{event_ticker: {phrase: vwap_cents}}``.
    """
    cache = _load_vwap_cache(cache_path)
    needed = [et for et in event_tickers if et not in cache["events"]]

    if verbose:
        cached_n = len(event_tickers) - len(needed)
        print(f"  VWAP: {cached_n} cached, {len(needed)} to fetch")

    if not needed or not fetch_missing:
        return {
            et: cache["events"][et]["vwaps"]
            for et in event_tickers
            if et in cache["events"]
        }

    md = _get_md()

    for idx, et in enumerate(needed):
        game_date = _extract_date_from_ticker(et)
        if not game_date:
            continue

        # Get markets for this event (need tickers + phrases)
        try:
            markets = md.get_markets(event_ticker=et, limit=200)
        except Exception:
            time.sleep(delay * 5)
            try:
                markets = md.get_markets(event_ticker=et, limit=200)
            except Exception:
                continue

        ticker_phrase: List[Tuple[str, str]] = []
        for mkt in markets:
            phrase = _extract_phrase(mkt)
            ticker = _attr(mkt, "ticker", "")
            if phrase and ticker:
                ticker_phrase.append((ticker, phrase))

        if not ticker_phrase:
            cache["events"][et] = {
                "timestamp": datetime.now().isoformat(),
                "vwaps": {},
            }
            continue

        # Batch candle fetch
        mkt_tickers = [t for t, _ in ticker_phrase]
        vwaps = _get_vwaps_batch(md, mkt_tickers, game_date)

        mkt_to_phrase = {t: p for t, p in ticker_phrase}
        phrase_vwaps = {}
        for mt, v in vwaps.items():
            p = mkt_to_phrase.get(mt)
            if p is not None:
                phrase_vwaps[p] = round(v, 2)

        cache["events"][et] = {
            "timestamp": datetime.now().isoformat(),
            "vwaps": phrase_vwaps,
        }

        if verbose and (idx + 1) % 10 == 0:
            print(f"    {idx + 1}/{len(needed)} events processed")

        if idx < len(needed) - 1:
            time.sleep(delay)

    # Persist
    if needed:
        _save_vwap_cache(cache, cache_path)
        if verbose:
            print(f"  VWAP cache saved: {len(cache['events'])} events total")

    return {
        et: cache["events"][et]["vwaps"]
        for et in event_tickers
        if et in cache["events"]
    }


def get_market_phrases(event_ticker: str) -> List[str]:
    """
    Return just the phrase list for an event.

    Drop-in replacement for the old ``nba_config.get_market_phrases()``.
    """
    return [m.phrase for m in get_event_markets(event_ticker)]


def fetch_series_results(
    series_ticker: str = "KXNBAMENTION",
    cache_path: str = _DEFAULT_SERIES_CACHE,
    delay: float = 0.1,
    verbose: bool = True,
    refresh_cache: bool = False,
) -> Dict[str, Dict[str, Optional[str]]]:
    """
    Fetch results for every event in a series.

    Returns:
        {event_ticker: {phrase: 'yes'/'no'/None, ...}, ...}
    """
    def _event_is_resolved(event_results: Dict[str, Optional[str]]) -> bool:
        return bool(event_results) and any(v in ("yes", "no") for v in event_results.values() if v is not None)

    cache = _load_series_cache(cache_path)
    series_cache = cache.setdefault("series", {}).setdefault(series_ticker, {})
    cached_events = series_cache.get("events", {})
    cached_events = {
        et: er for et, er in cached_events.items()
        if _event_is_resolved(er)
    }
    series_cache["events"] = cached_events

    md = _get_md()
    try:
        events = _get_all_events(md, series_ticker)
    except Exception as e:
        if cached_events and not refresh_cache:
            if verbose:
                print(
                    f"  Warning: could not refresh {series_ticker} events ({e}); "
                    f"using {len(cached_events)} cached resolved events"
                )
            return cached_events
        raise
    def _status_is_open(status) -> bool:
        if status is None:
            return False
        return "open" in str(status).lower()

    today = datetime.now().date()
    event_tickers = []
    event_statuses: Dict[str, object] = {}
    for evt in events:
        t = _attr(evt, "event_ticker")
        if t:
            event_tickers.append(t)
            event_statuses[t] = _attr(evt, "status")

    if verbose:
        print(f"Found {len(event_tickers)} events in {series_ticker}")

    needed = []
    for et in event_tickers:
        if not refresh_cache and et in cached_events:
            continue
        event_date = _extract_date_from_ticker(et)
        if _status_is_open(event_statuses.get(et)):
            continue
        if event_date is not None and event_date.date() >= today:
            continue
        needed.append(et)

    if verbose:
        print(f"  Series cache: {len(cached_events)} cached resolved, {len(needed)} to fetch")

    results: Dict[str, Dict[str, Optional[str]]] = {}
    for idx, et in enumerate(event_tickers):
        cached = cached_events.get(et)
        if et not in needed and cached is not None:
            results[et] = cached
            continue

        event_results: Dict[str, Optional[str]] = {}
        try:
            markets = md.get_markets(event_ticker=et, limit=200)
        except Exception as e:
            if verbose:
                print(f"  Error on {et}: {e}")
            time.sleep(delay * 10)
            try:
                markets = md.get_markets(event_ticker=et, limit=200)
            except Exception:
                continue

        for mkt in markets:
            phrase = _extract_phrase(mkt)
            if phrase is not None:
                event_results[phrase] = _attr(mkt, "result")
        results[et] = event_results
        if _event_is_resolved(event_results):
            cached_events[et] = event_results

        if idx < len(event_tickers) - 1:
            time.sleep(delay)

    if verbose:
        resolved = sum(
            1 for er in results.values()
            if any(v is not None for v in er.values())
        )
        print(f"  {resolved} events with resolved markets")

    series_cache["events"] = cached_events
    series_cache["timestamp"] = datetime.now().isoformat()
    cache["series"][series_ticker] = series_cache
    if event_tickers and (needed or refresh_cache):
        _save_series_cache(cache, cache_path)
        if verbose:
            print(f"  Series cache saved: {len(cached_events)} events total")

    return results


def results_to_dataframe(
    results: Dict[str, Dict[str, Optional[str]]],
) -> pd.DataFrame:
    """Convert results dict to a DataFrame (event_ticker index, phrase columns)."""
    df = pd.DataFrame(results).T
    df = df.fillna("")
    df.index.name = "event_ticker"
    return df


def _to_numeric(val):
    """yes -> 1, no -> 0, else NA."""
    if pd.isna(val):
        return pd.NA
    if isinstance(val, str):
        v = val.strip().lower()
        if v == "yes":
            return 1
        if v == "no":
            return 0
        if v == "":
            return pd.NA
    return pd.NA


def calculate_hit_rates(
    results: Dict[str, Dict[str, Optional[str]]],
    phrases: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Calculate hit rates from series results.

    Args:
        results: output of fetch_series_results()
        phrases: if given, restrict to these phrases

    Returns:
        DataFrame with columns: Phrase, Hit Rate, Hits, Misses, Total
    """
    df = results_to_dataframe(results)
    cols = [p for p in (phrases or df.columns) if p in df.columns]

    stats = []
    for phrase in cols:
        numeric = df[phrase].apply(_to_numeric)
        hits = int((numeric == 1).sum())
        misses = int((numeric == 0).sum())
        total = hits + misses
        stats.append({
            "Phrase": phrase,
            "Hit Rate": hits / total if total > 0 else np.nan,
            "Hits": hits,
            "Misses": misses,
            "Total": total,
        })

    if not stats:
        return pd.DataFrame(columns=["Phrase", "Hit Rate", "Hits", "Misses", "Total"])
    return pd.DataFrame(stats).sort_values("Total", ascending=False)


def calculate_hit_rates_by_announcer(
    results: Dict[str, Dict[str, Optional[str]]],
    announcers_df: pd.DataFrame,
    pbp_name: str,
    phrases: Optional[List[str]] = None,
    series_ticker: str = "KXNBAMENTION",
) -> pd.DataFrame:
    """
    Calculate hit rates filtered to events called by a specific play-by-play
    announcer (matched via combined_announcers.csv ticker column).
    """
    if announcers_df.empty:
        return pd.DataFrame()

    pbp_series = announcers_df["play_by_play"].fillna("").str.lower()
    matching_tickers = set(
        announcers_df.loc[pbp_series == pbp_name.lower(), "ticker"]
        .dropna()
        .astype(str)
    )
    matching_tickers = {
        t for t in matching_tickers
        if t.startswith(f"{series_ticker}-")
    }
    filtered = {k: v for k, v in results.items() if k in matching_tickers}
    if not filtered:
        return pd.DataFrame()
    return calculate_hit_rates(filtered, phrases)
