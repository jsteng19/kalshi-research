"""
Kalshi Mention Markets Automation Pipeline

Modules for discovering, collecting, validating, and reporting on mention markets.

Usage:
    from src.auto_collect import collect_for_event, collect_for_market
    
    # Collect historical transcripts for an event
    result = collect_for_event("What will Will Smith say during The Tonight Show?")
    print(f"Collected {len(result['transcripts'])} transcripts")
    
    # Or by market ticker
    result = collect_for_market("KXCOLBERTMENTION-26JAN21")
"""

from .market_discovery import (
    MentionMarketDiscovery,
    MentionEvent,
    MarketInfo,
    get_all_series,
    get_all_open_events,
    get_mention_series,
    get_open_mention_events,
    get_event_details,
    get_data_path_for_event,
    check_existing_data,
    discover_all_mention_events,
    is_mention_event,
    KNOWN_MENTION_SERIES,
    MENTION_TICKER_PATTERNS,
    MENTION_TITLE_PATTERNS,
    DEFAULT_DATA_DIR,
)

from .transcript_collector import (
    TranscriptCollector,
    SpeakerMatch,
    collect_for_ticker,
)

from .collector import (
    collect_for_event,
    collect_for_market,
    preview_videos,
    parse_event_context,
    EventContext,
    VideoPreview,
)

from .transcript_processor import (
    process_transcript,
    process_file as process_transcript_file,
    process_batch as process_transcripts_batch,
)

from .transcript_api import (
    YouTubeTranscriptAPI,
    get_transcripts_batch,
)

from .strategies.base import (
    CollectionStrategy,
    CollectionResult,
    get_strategy,
)

__all__ = [
    # Discovery
    'MentionMarketDiscovery',
    'MentionEvent',
    'MarketInfo',
    'get_all_series',
    'get_all_open_events',
    'get_mention_series', 
    'get_open_mention_events',
    'get_event_details',
    'get_data_path_for_event',
    'check_existing_data',
    'discover_all_mention_events',
    'is_mention_event',
    'KNOWN_MENTION_SERIES',
    'MENTION_TICKER_PATTERNS',
    'MENTION_TITLE_PATTERNS',
    'DEFAULT_DATA_DIR',
    
    # Main Collection Pipeline (recommended!)
    'collect_for_event',
    'collect_for_market',
    'preview_videos',
    'parse_event_context',
    'EventContext',
    'VideoPreview',
    
    # Transcript Processing (LLM)
    'process_transcript',
    'process_transcript_file',
    'process_transcripts_batch',
    
    # Transcript API
    'YouTubeTranscriptAPI',
    'get_transcripts_batch',
    
    # Legacy/Advanced Collection
    'TranscriptCollector',
    'SpeakerMatch',
    'collect_for_ticker',
    'CollectionStrategy',
    'CollectionResult',
    'get_strategy',
]
