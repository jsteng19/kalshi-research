"""
Collection strategies for mention market transcripts.

Recommended approach:
1. Use youtube_scraper for video discovery (no API needed, no costs)
2. Use YouTubeTranscriptDownloader for transcription (existing tool)
3. Fall back to Firecrawl for web-based transcripts (government sites, etc.)
"""

from .base import CollectionStrategy, CollectionResult
from .youtube import YouTubeChannelStrategy, YouTubeSearchStrategy
from .youtube_scraper import (
    YouTubeSearchScraper,
    VideoResult,
    search_youtube_videos,
    discover_and_transcribe,
)
from .firecrawl import (
    FirecrawlStrategy, 
    search_discover_videos,
    build_transcript_prompt,
    TRANSCRIPT_SCHEMA,
    VIDEO_DISCOVERY_SCHEMA,
)

__all__ = [
    # Base
    'CollectionStrategy',
    'CollectionResult',
    
    # YouTube (API-based)
    'YouTubeChannelStrategy',
    'YouTubeSearchStrategy',
    
    # YouTube Scraper (no API needed - recommended!)
    'YouTubeSearchScraper',
    'VideoResult',
    'search_youtube_videos',
    'discover_and_transcribe',
    
    # Firecrawl (for web transcripts)
    'FirecrawlStrategy',
    'search_discover_videos',
    'build_transcript_prompt',
    'TRANSCRIPT_SCHEMA',
    'VIDEO_DISCOVERY_SCHEMA',
]
