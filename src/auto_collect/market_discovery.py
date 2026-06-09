#!/usr/bin/env python3
"""
Kalshi Mention Market Discovery Module

Discovers and classifies "mention markets" - markets where resolution depends on 
whether a speaker mentions specific words/phrases during an event.

Usage:
    from src.auto_collect.market_discovery import MentionMarketDiscovery
    
    discovery = MentionMarketDiscovery(min_transcripts=10)
    mention_events = discovery.discover_open_mention_events()
    
    # Get events needing more transcript data
    events_needing_data = discovery.get_events_needing_data()
    
    # Or use individual functions
    from src.auto_collect.market_discovery import get_mention_series, get_open_mention_events
    series = get_mention_series()
    events = get_open_mention_events(series)
    
    # Check existing data with minimum threshold
    from src.auto_collect.market_discovery import check_existing_data, get_events_needing_data
    status = check_existing_data(event, min_transcripts=10)
    if status['needs_more']:
        print(f"Need {status['min_transcripts'] - status['transcript_count']} more files")

CLI Usage:
    # Discover events needing more data (default min: 5 transcripts)
    python -m src.auto_collect.market_discovery --needs-data
    
    # Specify custom minimum transcript count
    python -m src.auto_collect.market_discovery --needs-data --min-transcripts 10
"""

import re
import time
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field, asdict
import json


# Keywords that indicate a "mention" style market
MENTION_KEYWORDS = [
    'mention',
    'say',
    'said', 
    'speak',
    'spoken',
    'word',
    'phrase',
    'utter',
    'state',
    'announce',
    'declare',
    'reference',
]

# Known mention series tickers (seed list - discovery will find more)
KNOWN_MENTION_SERIES = [
    'KXNFLMENTION',      # NFL broadcast mentions
    'KXNBAMENTION',      # NBA broadcast mentions  
    'KXMAMDANIMENTION',  # Mamdani mentions
    'KXCFPMENTION',      # College Football Playoff mentions
    'KXTRUMPMENTION',    # Trump mentions
    'KXTRUMPSAY',        # Trump say markets
    'KXFEDMENTION',      # Fed press conference mentions
    # Discovery will find additional series automatically
]

# Patterns that indicate a mention-style market (in ticker or title)
MENTION_TICKER_PATTERNS = [
    r'MENTION',          # Most common pattern
    r'SAY$',             # Ends with SAY
    r'SAYNICKNAME',      # Nickname variants
    r'SAYMONTH',         # Monthly say markets
]

MENTION_TITLE_PATTERNS = [
    r'will\s+\w+\s+say\b',           # "will X say"
    r'what\s+will\s+\w+\s+(say|mention)',  # "what will X say/mention"
    r'announcers\s+say',              # broadcast mentions
    r'will\s+\w+\s+mention',          # "will X mention"
]

# Data directory for storing transcript data
DEFAULT_DATA_DIR = 'data/mentions'

# Default minimum number of transcript files to consider an event "complete"
DEFAULT_MIN_TRANSCRIPTS = 5


@dataclass
class MarketInfo:
    """Information about a single market within an event."""
    ticker: str
    title: str
    yes_sub_title: str  # The phrase being tracked
    status: str
    result: Optional[str] = None
    yes_bid: Optional[float] = None
    yes_ask: Optional[float] = None
    volume: Optional[int] = None
    
    @property
    def phrase(self) -> str:
        """Alias for yes_sub_title - the phrase being tracked."""
        return self.yes_sub_title


@dataclass  
class MentionEvent:
    """Information about a mention market event."""
    event_ticker: str
    series_ticker: str
    title: str
    category: Optional[str] = None
    sub_title: Optional[str] = None
    status: str = 'unknown'
    
    # Timing
    open_time: Optional[datetime] = None
    close_time: Optional[datetime] = None
    expected_expiration: Optional[datetime] = None
    
    # Extracted metadata
    speaker: Optional[str] = None
    event_description: Optional[str] = None
    source_info: Optional[str] = None
    
    # Markets (phrases) within this event
    markets: List[MarketInfo] = field(default_factory=list)
    
    # Classification
    is_mention_market: bool = True
    classification_confidence: float = 1.0
    classification_method: str = 'keyword'
    
    @property
    def phrases(self) -> List[str]:
        """Get all phrases being tracked in this event."""
        return [m.phrase for m in self.markets if m.phrase]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        d = asdict(self)
        # Convert datetime objects to ISO strings
        for key in ['open_time', 'close_time', 'expected_expiration']:
            if d[key] is not None:
                d[key] = d[key].isoformat()
        return d


def _get_kalshi_client():
    """Get Kalshi API client with production settings."""
    from kalshi import constants
    constants.use_prod()
    from kalshi.rest import market
    return market


def get_all_series(
    delay_seconds: float = 0.5,
    verbose: bool = True
) -> List[Dict[str, Any]]:
    """
    Fetch all available series from Kalshi.
    
    Returns:
        List of series dictionaries with metadata
    """
    market = _get_kalshi_client()
    
    try:
        # The API may have pagination - handle it
        all_series = []
        cursor = None
        
        while True:
            params = {'limit': 100}
            if cursor:
                params['cursor'] = cursor
                
            response = market.GetSeries(**params) if hasattr(market, 'GetSeries') else {}
            
            series_list = response.get('series', [])
            all_series.extend(series_list)
            
            cursor = response.get('cursor')
            if not cursor or not series_list:
                break
                
            time.sleep(delay_seconds)
        
        if verbose:
            print(f"Found {len(all_series)} total series")
        return all_series
        
    except Exception as e:
        if verbose:
            print(f"Error fetching series: {e}")
            print("Falling back to known mention series...")
        return []


def get_all_open_events(
    delay_seconds: float = 0.3,
    max_batches: int = 30,
    verbose: bool = True
) -> List[Dict[str, Any]]:
    """
    Fetch all open events from Kalshi using pagination.
    
    Args:
        delay_seconds: Delay between API calls
        max_batches: Maximum number of pagination batches (safety limit)
        verbose: Print progress
        
    Returns:
        List of all open event dictionaries
    """
    market = _get_kalshi_client()
    
    all_events = []
    cursor = None
    
    for batch_num in range(max_batches):
        params = {'limit': 200, 'status': 'open', 'with_nested_markets': False}
        if cursor:
            params['cursor'] = cursor
        
        try:
            result = market.GetEvents(**params)
            events = result.get('events', [])
            all_events.extend(events)
            
            if verbose and batch_num == 0:
                print(f"Fetching open events...")
            
            cursor = result.get('cursor')
            if not cursor or not events:
                break
                
            time.sleep(delay_seconds)
            
        except Exception as e:
            if verbose:
                print(f"Error fetching events: {e}")
            break
    
    if verbose:
        print(f"Found {len(all_events)} total open events")
    
    return all_events


def is_mention_event(event: Dict[str, Any]) -> Tuple[bool, float, str]:
    """
    Determine if an event is a mention-style market.
    
    Args:
        event: Event dictionary from API
        
    Returns:
        Tuple of (is_mention, confidence, classification_reason)
    """
    title = event.get('title', '')
    ticker = event.get('event_ticker', '').upper()
    series = (event.get('series_ticker', '') or '').upper()
    
    # Check ticker patterns (highest confidence)
    for pattern in MENTION_TICKER_PATTERNS:
        if re.search(pattern, ticker) or re.search(pattern, series):
            return True, 0.95, f'ticker_pattern:{pattern}'
    
    # Check known series
    for known in KNOWN_MENTION_SERIES:
        if ticker.startswith(known) or series.startswith(known):
            return True, 0.95, 'known_series'
    
    # Check title patterns
    title_lower = title.lower()
    for pattern in MENTION_TITLE_PATTERNS:
        if re.search(pattern, title_lower):
            return True, 0.85, f'title_pattern:{pattern}'
    
    return False, 0.0, 'no_match'


def discover_all_mention_events(
    delay_seconds: float = 0.3,
    min_confidence: float = 0.5,
    verbose: bool = True
) -> Tuple[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
    """
    Discover all mention-style events by scanning all open events.
    
    This is more comprehensive than checking known series, as it finds
    new mention series automatically.
    
    Args:
        delay_seconds: Delay between API calls
        min_confidence: Minimum confidence for classification
        verbose: Print progress
        
    Returns:
        Tuple of (mention_events, events_by_series)
    """
    # Get all open events
    all_events = get_all_open_events(delay_seconds=delay_seconds, verbose=verbose)
    
    # Filter for mention events
    mention_events = []
    events_by_series = {}
    
    for event in all_events:
        is_mention, confidence, reason = is_mention_event(event)
        
        if is_mention and confidence >= min_confidence:
            event['_is_mention'] = True
            event['_confidence'] = confidence
            event['_classification_reason'] = reason
            mention_events.append(event)
            
            # Group by series
            series = event.get('series_ticker', 'UNKNOWN') or 'UNKNOWN'
            if series not in events_by_series:
                events_by_series[series] = []
            events_by_series[series].append(event)
    
    if verbose:
        print(f"Found {len(mention_events)} mention events across {len(events_by_series)} series")
    
    return mention_events, events_by_series


def is_mention_series(series: Dict[str, Any]) -> Tuple[bool, float, str]:
    """
    Determine if a series is a mention-type market.
    
    Args:
        series: Series metadata dictionary
        
    Returns:
        Tuple of (is_mention, confidence, method)
    """
    ticker = series.get('ticker', '').upper()
    title = series.get('title', '').lower()
    category = series.get('category', '').lower()
    
    # Check known tickers first
    if ticker in KNOWN_MENTION_SERIES or any(ticker.startswith(k) for k in KNOWN_MENTION_SERIES):
        return True, 1.0, 'known_ticker'
    
    # Check for "mention" in ticker
    if 'mention' in ticker.lower():
        return True, 0.95, 'ticker_keyword'
    
    # Check category
    if category in ['mentions', 'mention', 'speech', 'broadcast']:
        return True, 0.9, 'category'
    
    # Check title for keywords
    for keyword in MENTION_KEYWORDS:
        if keyword in title:
            return True, 0.8, f'title_keyword:{keyword}'
    
    # Check for pattern: "Will X say Y" or "What will X mention"
    mention_patterns = [
        r'will\s+\w+\s+(?:say|mention|speak|utter)',
        r'what\s+will\s+\w+\s+(?:say|mention)',
        r'(?:say|mention|speak)\s+during',
        r'word.*(?:said|spoken|mentioned)',
    ]
    
    for pattern in mention_patterns:
        if re.search(pattern, title):
            return True, 0.85, f'title_pattern:{pattern}'
    
    return False, 0.0, 'no_match'


def get_mention_series(
    include_unknown: bool = False,
    min_confidence: float = 0.7,
    delay_seconds: float = 0.5,
    verbose: bool = True
) -> List[Dict[str, Any]]:
    """
    Get all series that appear to be mention markets.
    
    Args:
        include_unknown: If True, also return series that might be mentions (lower confidence)
        min_confidence: Minimum confidence threshold for classification
        delay_seconds: Delay between API calls
        verbose: Print progress information
        
    Returns:
        List of series dictionaries with classification metadata added
    """
    # Start with known series
    mention_series = []
    
    # Try to fetch all series and filter
    all_series = get_all_series(delay_seconds=delay_seconds, verbose=verbose)
    
    if all_series:
        for series in all_series:
            is_mention, confidence, method = is_mention_series(series)
            
            if is_mention and confidence >= min_confidence:
                series['_is_mention'] = True
                series['_confidence'] = confidence
                series['_classification_method'] = method
                mention_series.append(series)
            elif include_unknown and confidence > 0:
                series['_is_mention'] = False
                series['_confidence'] = confidence
                series['_classification_method'] = method
                mention_series.append(series)
    
    # Ensure known series are included even if API call failed
    known_tickers = {s.get('ticker') for s in mention_series}
    for ticker in KNOWN_MENTION_SERIES:
        if ticker not in known_tickers:
            mention_series.append({
                'ticker': ticker,
                '_is_mention': True,
                '_confidence': 1.0,
                '_classification_method': 'known_ticker',
            })
    
    if verbose:
        print(f"Found {len(mention_series)} mention series")
        
    return mention_series


def get_events_for_series(
    series_ticker: str,
    status_filter: Optional[str] = None,
    delay_seconds: float = 0.5,
    verbose: bool = True
) -> List[Dict[str, Any]]:
    """
    Get all events for a specific series.
    
    Args:
        series_ticker: The series ticker to fetch events for
        status_filter: Filter by status ('open', 'closed', 'settled', etc.)
        delay_seconds: Delay between API calls
        verbose: Print progress
        
    Returns:
        List of event dictionaries
    """
    market = _get_kalshi_client()
    
    try:
        response = market.GetEvents(series_ticker=series_ticker)
        events = response.get('events', [])
        
        if status_filter:
            # Note: status might be on the event or need to check markets
            events = [e for e in events if e.get('status', '').lower() == status_filter.lower()]
        
        if verbose:
            print(f"Found {len(events)} events for series {series_ticker}")
            
        return events
        
    except Exception as e:
        if verbose:
            print(f"Error fetching events for {series_ticker}: {e}")
        return []


def get_markets_for_event(
    event_ticker: str,
    delay_seconds: float = 0.5,
    verbose: bool = False
) -> List[Dict[str, Any]]:
    """
    Get all markets (phrases) for a specific event.
    
    Args:
        event_ticker: The event ticker
        delay_seconds: Delay for rate limiting
        verbose: Print progress
        
    Returns:
        List of market dictionaries
    """
    market_api = _get_kalshi_client()
    
    try:
        response = market_api.GetMarkets(event_ticker=event_ticker)
        markets = response.get('markets', [])
        
        if verbose:
            print(f"Found {len(markets)} markets for event {event_ticker}")
            
        return markets
        
    except Exception as e:
        if verbose:
            print(f"Error fetching markets for {event_ticker}: {e}")
        return []


def extract_speaker_from_title(title: str) -> Optional[str]:
    """
    Try to extract the speaker name from an event title.
    
    Examples:
        "What will Trump say during..." -> "Trump"
        "Will Biden mention..." -> "Biden"
        "NFL: Eagles vs Bills - Mentions" -> None (no single speaker)
    """
    # Common patterns
    patterns = [
        r'[Ww]hat\s+will\s+(\w+(?:\s+\w+)?)\s+(?:say|mention|speak)',
        r'[Ww]ill\s+(\w+(?:\s+\w+)?)\s+(?:say|mention|speak|utter)',
        r'(\w+(?:\s+\w+)?)\s+(?:speech|remarks|address|briefing)',
        r'(?:speech|remarks|address)\s+by\s+(\w+(?:\s+\w+)?)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, title)
        if match:
            speaker = match.group(1).strip()
            # Filter out common false positives
            if speaker.lower() not in ['the', 'a', 'an', 'what', 'will', 'be']:
                return speaker
    
    return None


def parse_event_to_mention_event(
    event: Dict[str, Any],
    series_ticker: str,
    markets: List[Dict[str, Any]] = None
) -> MentionEvent:
    """
    Parse raw event data into a MentionEvent object.
    
    Args:
        event: Raw event dictionary from API
        series_ticker: The series this event belongs to
        markets: Optional pre-fetched markets for this event
        
    Returns:
        MentionEvent object with parsed data
    """
    event_ticker = event.get('event_ticker', '')
    title = event.get('title', '')
    
    # Parse timestamps
    def parse_ts(ts_str):
        if not ts_str:
            return None
        try:
            return datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
        except:
            return None
    
    # Parse markets
    market_infos = []
    if markets:
        for m in markets:
            market_infos.append(MarketInfo(
                ticker=m.get('ticker', ''),
                title=m.get('title', ''),
                yes_sub_title=m.get('yes_sub_title', ''),
                status=m.get('status', 'unknown'),
                result=m.get('result'),
                yes_bid=m.get('yes_bid'),
                yes_ask=m.get('yes_ask'),
                volume=m.get('volume'),
            ))
    
    # Extract speaker from title
    speaker = extract_speaker_from_title(title)
    
    return MentionEvent(
        event_ticker=event_ticker,
        series_ticker=series_ticker,
        title=title,
        category=event.get('category'),
        sub_title=event.get('sub_title'),
        status=event.get('status', 'unknown'),
        open_time=parse_ts(event.get('open_time')),
        close_time=parse_ts(event.get('close_time')),
        expected_expiration=parse_ts(event.get('expected_expiration_time')),
        speaker=speaker,
        event_description=event.get('description'),
        markets=market_infos,
    )


def get_open_mention_events(
    series_tickers: List[str] = None,
    include_markets: bool = True,
    delay_seconds: float = 1.0,
    scan_all_events: bool = True,
    verbose: bool = True
) -> List[MentionEvent]:
    """
    Get all currently open mention market events.
    
    Args:
        series_tickers: Specific series to check (if None and scan_all_events=True, discovers all)
        include_markets: Whether to fetch individual market details (phrases)
        delay_seconds: Delay between API calls
        scan_all_events: If True, scan ALL open events to find mention markets (recommended)
        verbose: Print progress
        
    Returns:
        List of MentionEvent objects for open events
    """
    market_api = _get_kalshi_client()
    open_events = []
    
    if scan_all_events and series_tickers is None:
        # Use comprehensive discovery - scans all open events
        mention_events_raw, events_by_series = discover_all_mention_events(
            delay_seconds=delay_seconds * 0.3,  # Faster for initial scan
            verbose=verbose
        )
        
        if verbose:
            print(f"\nFetching market details for {len(mention_events_raw)} events...")
        
        for i, event in enumerate(mention_events_raw):
            event_ticker = event.get('event_ticker')
            series_ticker = event.get('series_ticker', '')
            
            # Fetch market details if requested
            markets = []
            if include_markets:
                time.sleep(delay_seconds)
                markets = get_markets_for_event(event_ticker, verbose=False)
                
                if verbose and (i + 1) % 10 == 0:
                    print(f"  Processed {i + 1}/{len(mention_events_raw)} events...")
            
            mention_event = parse_event_to_mention_event(event, series_ticker, markets)
            mention_event.classification_confidence = event.get('_confidence', 0.9)
            mention_event.classification_method = event.get('_classification_reason', 'scan')
            open_events.append(mention_event)
    
    else:
        # Legacy method: check specific series
        if series_tickers is None:
            mention_series = get_mention_series(verbose=verbose, delay_seconds=delay_seconds)
            series_tickers = [s.get('ticker') for s in mention_series if s.get('ticker')]
        
        if verbose:
            print(f"Checking {len(series_tickers)} series for open events...")
        
        for series_ticker in series_tickers:
            if not series_ticker:
                continue
                
            events = get_events_for_series(
                series_ticker, 
                delay_seconds=delay_seconds,
                verbose=False
            )
            
            for event in events:
                event_ticker = event.get('event_ticker')
                
                # Check if event has open markets
                markets = []
                if include_markets:
                    time.sleep(delay_seconds)
                    markets = get_markets_for_event(event_ticker, verbose=False)
                
                # Check status - event is "open" if any market is active
                has_active = any(m.get('status', '').lower() == 'active' for m in markets) if markets else False
                event_status = event.get('status', '').lower()
                
                is_open = has_active or event_status in ['open', 'active']
                
                if is_open:
                    mention_event = parse_event_to_mention_event(event, series_ticker, markets)
                    open_events.append(mention_event)
                    
                    if verbose:
                        print(f"  Found open event: {event_ticker} - {mention_event.title[:50]}...")
    
    if verbose:
        print(f"\nTotal open mention events: {len(open_events)}")
        
    return open_events


def get_event_details(
    event_ticker: str,
    delay_seconds: float = 0.5,
    verbose: bool = True
) -> Optional[MentionEvent]:
    """
    Get detailed information about a specific event.
    
    Args:
        event_ticker: The event ticker to fetch
        delay_seconds: Delay for rate limiting
        verbose: Print progress
        
    Returns:
        MentionEvent object or None if not found
    """
    market_api = _get_kalshi_client()
    
    try:
        # Get event info
        response = market_api.GetEvent(event_ticker=event_ticker)
        event = response.get('event', {})
        
        if not event:
            if verbose:
                print(f"Event not found: {event_ticker}")
            return None
        
        # Get markets
        time.sleep(delay_seconds)
        markets = get_markets_for_event(event_ticker, verbose=False)
        
        # Parse series ticker from event ticker (usually prefix before date)
        series_ticker = event.get('series_ticker', '')
        if not series_ticker:
            # Try to extract from event ticker
            match = re.match(r'^([A-Z]+)', event_ticker)
            if match:
                series_ticker = match.group(1)
        
        mention_event = parse_event_to_mention_event(event, series_ticker, markets)
        
        if verbose:
            print(f"Event: {mention_event.title}")
            print(f"  Status: {mention_event.status}")
            print(f"  Phrases: {mention_event.phrases[:5]}..." if len(mention_event.phrases) > 5 else f"  Phrases: {mention_event.phrases}")
            if mention_event.speaker:
                print(f"  Speaker: {mention_event.speaker}")
        
        return mention_event
        
    except Exception as e:
        if verbose:
            print(f"Error fetching event {event_ticker}: {e}")
        return None


class MentionMarketDiscovery:
    """
    Main class for discovering and managing mention markets.
    
    Usage:
        discovery = MentionMarketDiscovery()
        
        # Discover all open mention events
        events = discovery.discover_open_mention_events()
        
        # Get details for specific event
        event = discovery.get_event("KXNFLMENTION-25DEC28PHIBUF")
        
        # Save state for incremental updates
        discovery.save_state("mention_markets_state.json")
    """
    
    def __init__(
        self,
        known_series: List[str] = None,
        delay_seconds: float = 1.0,
        verbose: bool = True,
        min_transcripts: int = DEFAULT_MIN_TRANSCRIPTS,
        data_dir: str = DEFAULT_DATA_DIR
    ):
        """
        Initialize the discovery module.
        
        Args:
            known_series: Additional series tickers to always check
            delay_seconds: Default delay between API calls
            verbose: Default verbosity setting
            min_transcripts: Minimum number of transcript files to consider an event "complete"
            data_dir: Base directory for transcript data
        """
        self.known_series = list(KNOWN_MENTION_SERIES)
        if known_series:
            self.known_series.extend(known_series)
        
        self.delay_seconds = delay_seconds
        self.verbose = verbose
        self.min_transcripts = min_transcripts
        self.data_dir = data_dir
        
        # State tracking
        self._discovered_events: Dict[str, MentionEvent] = {}
        self._last_discovery_time: Optional[datetime] = None
    
    def discover_open_mention_events(
        self,
        force_refresh: bool = False,
        scan_all: bool = True,
        include_markets: bool = True
    ) -> List[MentionEvent]:
        """
        Discover all currently open mention market events.
        
        Args:
            force_refresh: If True, re-fetch even if recently discovered
            scan_all: If True (default), scan ALL open events to find mention markets.
                     If False, only check known series tickers.
            include_markets: If True (default), fetch individual market/phrase details.
            
        Returns:
            List of open MentionEvent objects
        """
        events = get_open_mention_events(
            series_tickers=None if scan_all else self.known_series,
            include_markets=include_markets,
            delay_seconds=self.delay_seconds,
            scan_all_events=scan_all,
            verbose=self.verbose
        )
        
        # Update state
        for event in events:
            self._discovered_events[event.event_ticker] = event
        self._last_discovery_time = datetime.now()
        
        return events
    
    def get_event(self, event_ticker: str, use_cache: bool = True) -> Optional[MentionEvent]:
        """
        Get details for a specific event.
        
        Args:
            event_ticker: The event ticker
            use_cache: If True, return cached version if available
            
        Returns:
            MentionEvent object or None
        """
        if use_cache and event_ticker in self._discovered_events:
            return self._discovered_events[event_ticker]
        
        event = get_event_details(
            event_ticker,
            delay_seconds=self.delay_seconds,
            verbose=self.verbose
        )
        
        if event:
            self._discovered_events[event_ticker] = event
            
        return event
    
    def add_series(self, series_ticker: str):
        """Add a series ticker to monitor."""
        if series_ticker not in self.known_series:
            self.known_series.append(series_ticker)
    
    @property
    def discovered_events(self) -> Dict[str, MentionEvent]:
        """Get all discovered events."""
        return self._discovered_events
    
    @property
    def open_events(self) -> List[MentionEvent]:
        """Get events that are currently open."""
        return [e for e in self._discovered_events.values() 
                if e.status.lower() in ['open', 'active']]
    
    def get_events_needing_data(
        self,
        events: List[MentionEvent] = None,
        min_transcripts: int = None
    ) -> List[Tuple[MentionEvent, Dict[str, Any]]]:
        """
        Get events that need more transcript data.
        
        Args:
            events: Events to check (defaults to discovered events)
            min_transcripts: Override the instance's min_transcripts setting
            
        Returns:
            List of (event, data_status) tuples for events needing more data
        """
        if events is None:
            events = list(self._discovered_events.values())
        
        if min_transcripts is None:
            min_transcripts = self.min_transcripts
        
        return get_events_needing_data(
            events,
            min_transcripts=min_transcripts,
            base_dir=self.data_dir,
            verbose=self.verbose
        )
    
    def check_event_data(self, event: MentionEvent, min_transcripts: int = None) -> Dict[str, Any]:
        """
        Check existing data for a specific event.
        
        Args:
            event: The event to check
            min_transcripts: Override the instance's min_transcripts setting
            
        Returns:
            Data status dictionary
        """
        if min_transcripts is None:
            min_transcripts = self.min_transcripts
        
        return check_existing_data(
            event,
            base_dir=self.data_dir,
            min_transcripts=min_transcripts
        )
    
    def save_state(self, path: str):
        """
        Save current state to JSON file for incremental updates.
        
        Args:
            path: Path to save state file
        """
        state = {
            'known_series': self.known_series,
            'last_discovery_time': self._last_discovery_time.isoformat() if self._last_discovery_time else None,
            'events': {k: v.to_dict() for k, v in self._discovered_events.items()},
        }
        
        with open(path, 'w') as f:
            json.dump(state, f, indent=2)
        
        if self.verbose:
            print(f"Saved state to {path}")
    
    def load_state(self, path: str) -> bool:
        """
        Load state from JSON file.
        
        Args:
            path: Path to state file
            
        Returns:
            True if loaded successfully
        """
        try:
            with open(path, 'r') as f:
                state = json.load(f)
            
            # Restore known series
            if 'known_series' in state:
                for s in state['known_series']:
                    if s not in self.known_series:
                        self.known_series.append(s)
            
            # Restore last discovery time
            if state.get('last_discovery_time'):
                self._last_discovery_time = datetime.fromisoformat(state['last_discovery_time'])
            
            # Note: We don't restore events from state since they may be stale
            # The state is mainly for tracking what was previously discovered
            
            if self.verbose:
                print(f"Loaded state from {path}")
                print(f"  Last discovery: {self._last_discovery_time}")
                print(f"  Known series: {len(self.known_series)}")
            
            return True
            
        except FileNotFoundError:
            if self.verbose:
                print(f"No state file found at {path}")
            return False
        except Exception as e:
            if self.verbose:
                print(f"Error loading state: {e}")
            return False


def get_data_path_for_event(event: MentionEvent, base_dir: str = DEFAULT_DATA_DIR) -> str:
    """
    Get the data directory path for storing transcripts for an event.
    
    Args:
        event: MentionEvent object
        base_dir: Base data directory
        
    Returns:
        Path string like 'data/mentions/nfl/KXNFLMENTION-NFLNFCCHAMP'
    """
    import os
    
    # Determine category from series
    series_lower = event.series_ticker.lower()
    if 'nfl' in series_lower:
        category = 'nfl'
    elif 'nba' in series_lower:
        category = 'nba'
    elif 'cfp' in series_lower:
        category = 'cfb'
    else:
        # Use first part of series as category
        category = series_lower.replace('kx', '').replace('mention', '') or 'other'
    
    return os.path.join(base_dir, category, event.event_ticker)


def check_existing_data(
    event: MentionEvent, 
    base_dir: str = DEFAULT_DATA_DIR,
    min_transcripts: int = DEFAULT_MIN_TRANSCRIPTS
) -> Dict[str, Any]:
    """
    Check what data already exists for an event.
    
    Args:
        event: MentionEvent object
        base_dir: Base data directory
        min_transcripts: Minimum number of transcript files to consider "sufficient"
        
    Returns:
        Dictionary with:
        - exists: bool
        - path: str
        - transcript_count: int
        - last_updated: datetime or None
        - needs_more: bool (True if transcript_count < min_transcripts)
        - min_transcripts: int (the threshold used)
    """
    import os
    from pathlib import Path
    
    data_path = get_data_path_for_event(event, base_dir)
    path = Path(data_path)
    
    result = {
        'exists': path.exists(),
        'path': data_path,
        'transcript_count': 0,
        'last_updated': None,
        'files': [],
        'min_transcripts': min_transcripts,
        'needs_more': True,
    }
    
    if path.exists():
        # Count transcript files
        transcript_files = list(path.glob('*.txt')) + list(path.glob('*.json'))
        result['transcript_count'] = len(transcript_files)
        result['files'] = [f.name for f in transcript_files]
        
        # Get most recent modification time
        if transcript_files:
            latest = max(f.stat().st_mtime for f in transcript_files)
            result['last_updated'] = datetime.fromtimestamp(latest)
    
    # Check if we have enough transcripts
    result['needs_more'] = result['transcript_count'] < min_transcripts
    
    return result


def get_events_needing_data(
    events: List[MentionEvent],
    min_transcripts: int = DEFAULT_MIN_TRANSCRIPTS,
    base_dir: str = DEFAULT_DATA_DIR,
    verbose: bool = True
) -> List[Tuple[MentionEvent, Dict[str, Any]]]:
    """
    Filter events to only those that need more transcript data.
    
    Args:
        events: List of MentionEvent objects to check
        min_transcripts: Minimum number of transcript files required
        base_dir: Base data directory
        verbose: Print progress
        
    Returns:
        List of (event, data_status) tuples for events needing more data
    """
    needs_data = []
    
    for event in events:
        status = check_existing_data(event, base_dir=base_dir, min_transcripts=min_transcripts)
        if status['needs_more']:
            needs_data.append((event, status))
    
    if verbose:
        print(f"Found {len(needs_data)}/{len(events)} events needing more data (min_transcripts={min_transcripts})")
    
    return needs_data


# CLI interface
if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Discover Kalshi mention markets')
    parser.add_argument('--series', '-s', help='Specific series ticker to check')
    parser.add_argument('--event', '-e', help='Specific event ticker to fetch details')
    parser.add_argument('--list-series', action='store_true', help='List all known mention series')
    parser.add_argument('--discover', action='store_true', help='Discover all open mention events')
    parser.add_argument('--needs-data', action='store_true', help='Show only events needing more transcript data')
    parser.add_argument('--min-transcripts', '-m', type=int, default=DEFAULT_MIN_TRANSCRIPTS,
                        help=f'Minimum transcript files to consider complete (default: {DEFAULT_MIN_TRANSCRIPTS})')
    parser.add_argument('--delay', type=float, default=1.0, help='Delay between API calls')
    parser.add_argument('--output', '-o', help='Output file for results (JSON)')
    
    args = parser.parse_args()
    
    if args.list_series:
        print("Known mention series:")
        for s in KNOWN_MENTION_SERIES:
            print(f"  {s}")
        
        print("\nDiscovering additional series...")
        series = get_mention_series(delay_seconds=args.delay)
        for s in series:
            ticker = s.get('ticker', 'unknown')
            conf = s.get('_confidence', 0)
            method = s.get('_classification_method', 'unknown')
            print(f"  {ticker} (confidence: {conf:.2f}, method: {method})")
    
    elif args.event:
        event = get_event_details(args.event, delay_seconds=args.delay)
        if event:
            print(json.dumps(event.to_dict(), indent=2, default=str))
    
    elif args.discover or args.series or args.needs_data:
        series_to_check = [args.series] if args.series else None
        events = get_open_mention_events(
            series_tickers=series_to_check,
            delay_seconds=args.delay
        )
        
        # Filter to events needing more data if requested
        if args.needs_data:
            events_with_status = get_events_needing_data(
                events,
                min_transcripts=args.min_transcripts,
                verbose=True
            )
            
            print(f"\nEvents needing more data (< {args.min_transcripts} transcripts):")
            for event, status in events_with_status:
                print(f"\n{event.event_ticker}")
                print(f"  Title: {event.title}")
                print(f"  Current transcripts: {status['transcript_count']}")
                print(f"  Phrases: {len(event.phrases)}")
                if event.speaker:
                    print(f"  Speaker: {event.speaker}")
            
            if args.output:
                output_data = [
                    {**e.to_dict(), '_data_status': s} 
                    for e, s in events_with_status
                ]
                with open(args.output, 'w') as f:
                    json.dump(output_data, f, indent=2, default=str)
                print(f"\nSaved to {args.output}")
        else:
            print(f"\nFound {len(events)} open mention events:")
            for event in events:
                print(f"\n{event.event_ticker}")
                print(f"  Title: {event.title}")
                print(f"  Status: {event.status}")
                print(f"  Phrases: {len(event.phrases)}")
                if event.speaker:
                    print(f"  Speaker: {event.speaker}")
            
            if args.output:
                output_data = [e.to_dict() for e in events]
                with open(args.output, 'w') as f:
                    json.dump(output_data, f, indent=2, default=str)
                print(f"\nSaved to {args.output}")
    
    else:
        parser.print_help()
