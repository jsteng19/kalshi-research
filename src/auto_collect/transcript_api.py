"""
YouTube Transcript API Client

Uses youtube-transcript.io API for reliable transcript fetching.
Much more reliable than scraping, handles rate limits gracefully.
"""

import os
import re
import time
from typing import Dict, List, Optional, Any
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()


class YouTubeTranscriptAPI:
    """
    Client for youtube-transcript.io API.
    
    Usage:
        api = YouTubeTranscriptAPI()
        transcript = api.get_transcript("dQw4w9WgXcQ")
        print(transcript['text'])
    """
    
    BASE_URL = "https://www.youtube-transcript.io/api/transcripts"
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv('YOUTUBE_TRANSCRIPT_API_KEY')
        if not self.api_key:
            raise ValueError("YOUTUBE_TRANSCRIPT_API_KEY not set")
        
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Basic {self.api_key}",
            "Content-Type": "application/json",
        })
    
    def extract_video_id(self, url: str) -> Optional[str]:
        """Extract video ID from various YouTube URL formats."""
        patterns = [
            r'(?:v=|/v/|youtu\.be/)([a-zA-Z0-9_-]{11})',
            r'(?:embed/)([a-zA-Z0-9_-]{11})',
            r'^([a-zA-Z0-9_-]{11})$',
        ]
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        return None
    
    def get_transcript(self, video_id: str) -> Optional[Dict[str, Any]]:
        """
        Get transcript for a single video.
        
        Args:
            video_id: YouTube video ID
            
        Returns:
            Dict with 'text', 'segments', and metadata, or None if not available
        """
        result = self.get_transcripts([video_id])
        if result and video_id in result:
            return result[video_id]
        return None
    
    def get_transcripts(self, video_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Get transcripts for multiple videos in one request.
        
        Args:
            video_ids: List of YouTube video IDs
            
        Returns:
            Dict mapping video_id -> transcript data
        """
        if not video_ids:
            return {}
        
        try:
            response = self.session.post(
                self.BASE_URL,
                json={"ids": video_ids},
                timeout=60,
            )
            
            if response.status_code == 200:
                data = response.json()
                return self._parse_response(data, video_ids)
            elif response.status_code == 429:
                print(f"Rate limited. Waiting...")
                time.sleep(10)
                return self.get_transcripts(video_ids)
            else:
                print(f"API error: {response.status_code} - {response.text[:200]}")
                return {}
                
        except Exception as e:
            print(f"Request error: {e}")
            return {}
    
    def _parse_response(self, data: Any, video_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """Parse API response into structured format."""
        results = {}

        # Handle different response formats
        if isinstance(data, list):
            for item in data:
                # Extract video_id from the item itself to avoid order mismatch
                video_id = None
                if isinstance(item, dict):
                    video_id = item.get('videoId') or item.get('video_id') or item.get('id')
                if video_id and video_id in set(video_ids):
                    results[video_id] = self._parse_transcript_item(item, video_id)
                else:
                    # Log unmatched items for debugging
                    print(f"Warning: could not match transcript response item to a requested video_id "
                          f"(keys: {list(item.keys()) if isinstance(item, dict) else type(item).__name__})")
        elif isinstance(data, dict):
            for video_id in video_ids:
                if video_id in data:
                    results[video_id] = self._parse_transcript_item(data[video_id], video_id)

        return results
    
    def _parse_transcript_item(self, item: Any, video_id: str) -> Dict[str, Any]:
        """Parse a single transcript item."""
        if isinstance(item, str):
            return {
                'video_id': video_id,
                'text': item,
                'segments': [],
                'url': f"https://www.youtube.com/watch?v={video_id}",
            }
        elif isinstance(item, dict):
            # Extract full text from segments if available
            segments = item.get('segments', item.get('transcript', []))
            if segments and isinstance(segments, list):
                full_text = ' '.join(
                    s.get('text', s) if isinstance(s, dict) else str(s) 
                    for s in segments
                )
            else:
                full_text = item.get('text', '')
            
            return {
                'video_id': video_id,
                'text': full_text,
                'segments': segments,
                'title': item.get('title', ''),
                'url': f"https://www.youtube.com/watch?v={video_id}",
            }
        else:
            return {
                'video_id': video_id,
                'text': str(item) if item else '',
                'segments': [],
                'url': f"https://www.youtube.com/watch?v={video_id}",
            }
    
    def get_transcript_from_url(self, url: str) -> Optional[Dict[str, Any]]:
        """Get transcript from a YouTube URL."""
        video_id = self.extract_video_id(url)
        if not video_id:
            print(f"Could not extract video ID from: {url}")
            return None
        return self.get_transcript(video_id)


def get_transcripts_batch(
    urls: List[str],
    batch_size: int = 10,
    delay: float = 1.0,
) -> Dict[str, Dict[str, Any]]:
    """
    Get transcripts for a list of URLs in batches.
    
    Args:
        urls: List of YouTube URLs
        batch_size: Number of videos per API call
        delay: Delay between batches
        
    Returns:
        Dict mapping video_id -> transcript data
    """
    api = YouTubeTranscriptAPI()
    all_results = {}
    
    # Extract video IDs
    video_ids = []
    url_map = {}
    for url in urls:
        vid = api.extract_video_id(url)
        if vid:
            video_ids.append(vid)
            url_map[vid] = url
    
    # Process in batches
    for i in range(0, len(video_ids), batch_size):
        batch = video_ids[i:i + batch_size]
        print(f"Processing batch {i//batch_size + 1}/{(len(video_ids) + batch_size - 1)//batch_size}...")
        
        results = api.get_transcripts(batch)
        all_results.update(results)
        
        if i + batch_size < len(video_ids):
            time.sleep(delay)
    
    return all_results


if __name__ == '__main__':
    # Test
    api = YouTubeTranscriptAPI()
    
    # Test with a known video
    test_url = "https://www.youtube.com/watch?v=rL73erU76M0"  # Will Smith memoir interview
    video_id = api.extract_video_id(test_url)
    
    print(f"Testing with video: {video_id}")
    result = api.get_transcript(video_id)
    
    if result:
        print(f"✅ Got transcript ({len(result['text'])} chars)")
        print(f"Preview: {result['text'][:500]}...")
    else:
        print("❌ No transcript")
