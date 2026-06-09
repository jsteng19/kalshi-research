"""
YouTube-based transcript collection strategies.

Uses the existing YouTube transcript downloader tools in src/youtube/
"""

import os
import re
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from .base import CollectionStrategy, CollectionResult


class YouTubeChannelStrategy(CollectionStrategy):
    """
    Collect transcripts from a specific YouTube channel.
    
    Best for: Late night shows, official government channels, creator channels
    """
    
    name = "youtube_channel"
    
    def __init__(self):
        self._downloader = None
    
    @property
    def downloader(self):
        """Lazy load the YouTube transcript downloader."""
        if self._downloader is None:
            from src.scrapers.youtube.youtube_transcript_downloader import YouTubeTranscriptDownloader
            self._downloader = YouTubeTranscriptDownloader(use_proxy=True)
        return self._downloader
    
    def collect(
        self,
        speaker_config: Dict[str, Any],
        target_date: Optional[datetime] = None,
        max_videos: int = 20,
        days_back: int = 30,
        **kwargs
    ) -> CollectionResult:
        """
        Collect transcripts from a YouTube channel.
        
        Args:
            speaker_config: Configuration from speakers.yaml
            target_date: Target date to search around
            max_videos: Maximum number of videos to fetch
            days_back: How many days back to search
            
        Returns:
            CollectionResult with transcripts
        """
        result = CollectionResult(success=False, source_type=self.name)
        
        youtube_config = speaker_config.get('youtube_config', {})
        channel_id = youtube_config.get('channel_id')
        
        if not channel_id:
            result.add_error("No channel_id specified in youtube_config")
            return result
        
        try:
            # Get video list from channel
            # This is a simplified version - full implementation would use YouTube Data API
            videos = self._get_channel_videos(
                channel_id,
                max_results=max_videos,
                target_date=target_date,
                days_back=days_back
            )
            
            result.metadata['videos_found'] = len(videos)
            
            # Download transcripts for each video
            for video in videos:
                try:
                    transcript = self._download_transcript(video['video_id'])
                    if transcript:
                        result.add_transcript(
                            text=transcript['text'],
                            date=video.get('date'),
                            title=video.get('title', ''),
                            source_url=f"https://youtube.com/watch?v={video['video_id']}",
                            speaker=speaker_config.get('name', ''),
                            video_id=video['video_id'],
                            is_generated=transcript.get('is_generated', False)
                        )
                except Exception as e:
                    result.add_error(f"Failed to get transcript for {video['video_id']}: {str(e)}")
            
            result.success = result.transcript_count > 0
            
        except Exception as e:
            result.add_error(f"Channel collection failed: {str(e)}")
        
        return result
    
    def _get_channel_videos(
        self,
        channel_id: str,
        max_results: int = 20,
        target_date: Optional[datetime] = None,
        days_back: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Get recent videos from a YouTube channel.
        
        Note: This requires YouTube Data API for full functionality.
        For now, returns empty list - use agent mode for discovery.
        """
        # TODO: Implement YouTube Data API integration
        # For now, this strategy works best with pre-discovered video URLs
        return []
    
    def _download_transcript(self, video_id: str) -> Optional[Dict[str, Any]]:
        """Download transcript for a specific video."""
        try:
            transcript = self.downloader.download_transcript(video_id)
            if transcript:
                # Combine all snippets into full text
                full_text = " ".join([s.text for s in transcript])
                return {
                    'text': full_text,
                    'language': transcript.language,
                    'is_generated': transcript.is_generated,
                }
        except Exception as e:
            raise Exception(f"Transcript download failed: {str(e)}")
        
        return None
    
    def collect_from_urls(
        self,
        video_urls: List[str],
        speaker_config: Dict[str, Any]
    ) -> CollectionResult:
        """
        Collect transcripts from a list of video URLs.
        
        This is the most reliable method - provide discovered URLs directly.
        
        Args:
            video_urls: List of YouTube video URLs
            speaker_config: Configuration from speakers.yaml
            
        Returns:
            CollectionResult with transcripts
        """
        result = CollectionResult(success=False, source_type=self.name)
        
        for url in video_urls:
            video_id = self.downloader.extract_video_id(url)
            if not video_id:
                result.add_error(f"Could not extract video ID from: {url}")
                continue
            
            try:
                # Get metadata
                title, upload_date = self.downloader.get_video_metadata(video_id)
                
                # Get transcript
                transcript = self._download_transcript(video_id)
                
                if transcript:
                    date = None
                    if upload_date:
                        try:
                            date = datetime.strptime(upload_date, '%Y-%m-%d')
                        except:
                            pass
                    
                    result.add_transcript(
                        text=transcript['text'],
                        date=date,
                        title=title or '',
                        source_url=url,
                        speaker=speaker_config.get('name', ''),
                        video_id=video_id,
                        is_generated=transcript.get('is_generated', False)
                    )
                else:
                    result.add_error(f"No transcript available for: {url}")
                    
            except Exception as e:
                result.add_error(f"Failed to process {url}: {str(e)}")
        
        result.success = result.transcript_count > 0
        return result


class YouTubeSearchStrategy(CollectionStrategy):
    """
    Search YouTube for videos and collect transcripts.
    
    Best for: Press briefings, interviews, speeches (when channel isn't known)
    """
    
    name = "youtube_search"
    
    def __init__(self):
        self._downloader = None
    
    @property
    def downloader(self):
        """Lazy load the YouTube transcript downloader."""
        if self._downloader is None:
            from src.scrapers.youtube.youtube_transcript_downloader import YouTubeTranscriptDownloader
            self._downloader = YouTubeTranscriptDownloader(use_proxy=True)
        return self._downloader
    
    def collect(
        self,
        speaker_config: Dict[str, Any],
        target_date: Optional[datetime] = None,
        event_title: Optional[str] = None,
        max_results: int = 10,
        **kwargs
    ) -> CollectionResult:
        """
        Search YouTube and collect transcripts.
        
        Args:
            speaker_config: Configuration from speakers.yaml
            target_date: Target date to search for
            event_title: Specific event title to search for
            max_results: Maximum number of results to process
            
        Returns:
            CollectionResult with transcripts
        """
        result = CollectionResult(success=False, source_type=self.name)
        
        youtube_config = speaker_config.get('youtube_config', {})
        search_queries = youtube_config.get('search_queries', [])
        
        if not search_queries:
            # Build default search query from speaker name
            name = speaker_config.get('name', '')
            search_queries = [f"{name} interview", f"{name} speech"]
        
        # Add event title to search if provided
        if event_title:
            search_queries = [f"{event_title}"] + search_queries
        
        # Add date to queries if provided
        if target_date:
            date_str = target_date.strftime('%B %Y')
            search_queries = [f"{q} {date_str}" for q in search_queries]
        
        result.metadata['search_queries'] = search_queries
        
        # Note: Full YouTube search requires Data API
        # For now, this builds the search queries for agent mode
        result.metadata['manual_search_urls'] = [
            f"https://www.youtube.com/results?search_query={q.replace(' ', '+')}"
            for q in search_queries
        ]
        
        result.add_error(
            "YouTube search requires manual discovery or YouTube Data API. "
            "Use the search URLs in metadata or agent mode."
        )
        
        return result
    
    def collect_from_search_results(
        self,
        video_urls: List[str],
        speaker_config: Dict[str, Any]
    ) -> CollectionResult:
        """
        Collect transcripts from manually discovered search results.
        
        Args:
            video_urls: List of YouTube video URLs from search
            speaker_config: Configuration from speakers.yaml
            
        Returns:
            CollectionResult with transcripts
        """
        # Delegate to channel strategy's URL-based collection
        channel_strategy = YouTubeChannelStrategy()
        return channel_strategy.collect_from_urls(video_urls, speaker_config)


def search_youtube_manual(query: str) -> str:
    """
    Generate a YouTube search URL for manual searching.
    
    This is used in agent mode - the agent opens the URL, finds relevant videos,
    and feeds them back to collect_from_urls.
    """
    encoded_query = query.replace(' ', '+')
    return f"https://www.youtube.com/results?search_query={encoded_query}"


def build_search_queries(
    speaker_name: str,
    event_type: str,
    event_title: Optional[str] = None,
    target_date: Optional[datetime] = None
) -> List[str]:
    """
    Build search queries for finding videos.
    
    Args:
        speaker_name: Name of the speaker
        event_type: Type of event (interview, speech, press_briefing, etc.)
        event_title: Specific event title
        target_date: Target date
        
    Returns:
        List of search queries to try
    """
    queries = []
    
    # Build base queries
    if event_title:
        queries.append(event_title)
        queries.append(f"{speaker_name} {event_title}")
    
    event_suffixes = {
        'press_briefing': ['press briefing full', 'press conference full'],
        'interview': ['interview full', 'full interview'],
        'speech': ['speech full', 'full speech', 'remarks'],
        'late_night': ['full interview', 'extended interview'],
        'podcast': ['full episode', 'podcast episode'],
    }
    
    suffixes = event_suffixes.get(event_type, [''])
    
    for suffix in suffixes:
        queries.append(f"{speaker_name} {suffix}".strip())
    
    # Add date if provided
    if target_date:
        date_str = target_date.strftime('%B %d %Y')
        queries = [f"{q} {date_str}" for q in queries] + queries
    
    return queries
