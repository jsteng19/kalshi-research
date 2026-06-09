"""
Firecrawl-based web scraping strategy for transcript collection.

Firecrawl is an AI-powered web scraping service that can extract
structured data from web pages, handle JavaScript rendering, and
even use LLMs to intelligently extract content.

Supports three modes:
1. /scrape - Direct URL scraping with markdown/HTML extraction
2. /extract - LLM-powered structured data extraction from URLs
3. /agent - Autonomous web agent that searches, navigates, and extracts
"""

import os
import time
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable
from dotenv import load_dotenv

from .base import CollectionStrategy, CollectionResult

# Load environment variables
load_dotenv()


# Schema for video URL discovery (lightweight - agent finds URLs, we transcribe later)
VIDEO_DISCOVERY_SCHEMA = {
    "type": "object",
    "properties": {
        "videos": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "title": {"type": "string", "description": "Video title"},
                    "url": {"type": "string", "description": "Full YouTube URL"},
                    "date": {"type": "string", "description": "Upload date (YYYY-MM-DD)"},
                    "channel": {"type": "string", "description": "Channel name"},
                    "duration": {"type": "string", "description": "Video duration"},
                },
                "required": ["url", "title"]
            }
        }
    }
}

# Default schema for transcript extraction (heavier - agent extracts full text)
TRANSCRIPT_SCHEMA = {
    "type": "object",
    "properties": {
        "transcripts": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "date": {"type": "string", "description": "Date of event (YYYY-MM-DD)"},
                    "title": {"type": "string", "description": "Title of the event"},
                    "full_text": {"type": "string", "description": "Complete transcript text"},
                    "speaker": {"type": "string", "description": "Primary speaker name"},
                    "event_type": {"type": "string", "description": "Type: speech, interview, press briefing, etc."},
                    "source_url": {"type": "string", "description": "URL where transcript was found"},
                },
                "required": ["title", "full_text"]
            }
        }
    }
}


class FirecrawlStrategy(CollectionStrategy):
    """
    Collect transcripts using Firecrawl web scraping.
    
    Best for:
    - Government transcript pages
    - News article transcripts
    - Official press release pages
    - Any page that needs JavaScript rendering
    """
    
    name = "firecrawl"
    
    def __init__(self):
        self._client = None
        self.api_key = os.getenv('FIRECRAWL_API_KEY')
    
    @property
    def client(self):
        """Lazy load the Firecrawl client."""
        if self._client is None:
            if not self.api_key:
                raise ValueError("FIRECRAWL_API_KEY not found in environment variables")
            
            try:
                from firecrawl import FirecrawlApp
                self._client = FirecrawlApp(api_key=self.api_key)
            except ImportError:
                raise ImportError("firecrawl-py not installed. Run: pip install firecrawl-py")
        
        return self._client
    
    def collect(
        self,
        speaker_config: Dict[str, Any],
        target_date: Optional[datetime] = None,
        urls: Optional[List[str]] = None,
        **kwargs
    ) -> CollectionResult:
        """
        Collect transcripts by scraping web pages.
        
        Args:
            speaker_config: Configuration from speakers.yaml
            target_date: Target date to search for
            urls: Specific URLs to scrape (if not provided, uses transcript_sources from config)
            
        Returns:
            CollectionResult with transcripts
        """
        result = CollectionResult(success=False, source_type=self.name)
        
        # Get URLs to scrape
        if urls is None:
            urls = speaker_config.get('transcript_sources', [])
        
        if not urls:
            result.add_error("No URLs provided and no transcript_sources in config")
            return result
        
        for url in urls:
            try:
                transcript_data = self.scrape_transcript(url, speaker_config)
                if transcript_data:
                    result.add_transcript(**transcript_data)
            except Exception as e:
                result.add_error(f"Failed to scrape {url}: {str(e)}")
        
        result.success = result.transcript_count > 0
        return result
    
    def scrape_transcript(
        self,
        url: str,
        speaker_config: Dict[str, Any],
        extract_schema: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Scrape a single URL for transcript content.
        
        Args:
            url: URL to scrape
            speaker_config: Speaker configuration
            extract_schema: Optional schema for structured extraction
            
        Returns:
            Dictionary with transcript data or None
        """
        try:
            # Use Firecrawl's scrape endpoint
            scrape_result = self.client.scrape_url(
                url,
                params={
                    'formats': ['markdown', 'html'],
                    'onlyMainContent': True,
                }
            )
            
            if not scrape_result:
                return None
            
            # Extract content
            content = scrape_result.get('markdown') or scrape_result.get('content', '')
            
            if not content:
                return None
            
            # Extract metadata
            metadata = scrape_result.get('metadata', {})
            title = metadata.get('title', '')
            
            # Try to extract date from metadata or content
            date = None
            date_str = metadata.get('publishedTime') or metadata.get('modifiedTime')
            if date_str:
                try:
                    date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                except:
                    pass
            
            return {
                'text': content,
                'date': date,
                'title': title,
                'source_url': url,
                'speaker': speaker_config.get('name', ''),
            }
            
        except Exception as e:
            raise Exception(f"Firecrawl scrape failed: {str(e)}")
    
    def extract_structured(
        self,
        url: str,
        prompt: str,
        schema: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Use Firecrawl's LLM extraction to get structured data.
        
        Args:
            url: URL to extract from
            prompt: Natural language prompt describing what to extract
            schema: Optional JSON schema for the output
            
        Returns:
            Extracted data dictionary
        """
        try:
            # Use Firecrawl's extract endpoint
            extract_params = {
                'prompt': prompt,
            }
            
            if schema:
                extract_params['schema'] = schema
            
            result = self.client.scrape_url(
                url,
                params={
                    'formats': ['extract'],
                    'extract': extract_params,
                }
            )
            
            return result.get('extract')
            
        except Exception as e:
            raise Exception(f"Firecrawl extraction failed: {str(e)}")
    
    def search_and_extract(
        self,
        query: str,
        speaker_config: Dict[str, Any],
        max_results: int = 5
    ) -> CollectionResult:
        """
        Search the web and extract transcripts from results.
        
        Uses Firecrawl's search capabilities to find relevant pages,
        then extracts transcript content.
        
        Args:
            query: Search query
            speaker_config: Speaker configuration
            max_results: Maximum number of results to process
            
        Returns:
            CollectionResult with transcripts
        """
        result = CollectionResult(success=False, source_type=self.name)
        
        try:
            # Use Firecrawl's search (if available)
            search_result = self.client.search(
                query,
                params={
                    'limit': max_results,
                }
            )
            
            if not search_result or 'data' not in search_result:
                result.add_error(f"No search results for: {query}")
                return result
            
            # Process each result
            for item in search_result.get('data', []):
                url = item.get('url')
                if url:
                    try:
                        transcript_data = self.scrape_transcript(url, speaker_config)
                        if transcript_data:
                            result.add_transcript(**transcript_data)
                    except Exception as e:
                        result.add_error(f"Failed to process {url}: {str(e)}")
            
            result.success = result.transcript_count > 0
            
        except Exception as e:
            result.add_error(f"Search failed: {str(e)}")
        
        return result

    def agent_collect(
        self,
        prompt: str,
        speaker_config: Dict[str, Any],
        urls: Optional[List[str]] = None,
        schema: Optional[Dict[str, Any]] = None,
        max_credits: int = 100,
        model: str = "spark-1-mini",
        strict_url_constraint: bool = False,
        poll_interval: int = 5,
        timeout: int = 300,
        on_status: Optional[Callable[[str, Dict], None]] = None,
    ) -> CollectionResult:
        """
        Use Firecrawl's /agent endpoint for autonomous transcript collection.
        
        The agent autonomously searches, navigates, and extracts data based on
        a natural language prompt. Perfect for finding transcripts when you
        don't have exact URLs.
        
        Args:
            prompt: Natural language description of what to find/extract
                   e.g., "Find the transcript of Bernie Sanders' interview on
                   The Late Show with Stephen Colbert from January 2026"
            speaker_config: Configuration from speakers.yaml
            urls: Optional list of URLs to constrain search (domains or pages)
            schema: Optional JSON schema for structured output (defaults to TRANSCRIPT_SCHEMA)
            max_credits: Maximum credits to spend on this request (cost control)
            model: "spark-1-mini" (faster/cheaper) or "spark-1-pro" (more accurate)
            strict_url_constraint: If True, only search within provided URLs
            poll_interval: Seconds between status polls
            timeout: Maximum seconds to wait for completion
            on_status: Optional callback for status updates fn(status, data)
            
        Returns:
            CollectionResult with transcripts
        """
        result = CollectionResult(success=False, source_type=f"{self.name}_agent")
        
        # Use transcript schema if not provided
        if schema is None:
            schema = TRANSCRIPT_SCHEMA
        
        try:
            # Start the agent job
            agent_params = {
                'prompt': prompt,
                'schema': schema,
                'maxCredits': max_credits,
                'model': model,
            }
            
            if urls:
                agent_params['urls'] = urls
                agent_params['strictConstrainToURLs'] = strict_url_constraint
            
            # Call the agent endpoint - prompt must be keyword arg
            job = self.client.agent(
                urls=urls,
                prompt=prompt,
                schema=schema,
                max_credits=max_credits,
                model=model,
                strict_constrain_to_urls=strict_url_constraint if urls else None,
            )
            
            if not job or 'id' not in job:
                result.add_error("Failed to start agent job")
                return result
            
            job_id = job['id']
            result.metadata['job_id'] = job_id
            
            if on_status:
                on_status('started', {'job_id': job_id})
            
            # Poll for completion
            start_time = time.time()
            while True:
                elapsed = time.time() - start_time
                if elapsed > timeout:
                    result.add_error(f"Agent job timed out after {timeout}s")
                    return result
                
                # Check status
                status = self.client.get_agent_status(job_id)
                state = status.get('status', 'unknown')
                
                if on_status:
                    on_status(state, status)
                
                if state == 'completed':
                    # Extract results
                    data = status.get('data', {})
                    transcripts = data.get('transcripts', [])
                    
                    for t in transcripts:
                        result.add_transcript(
                            text=t.get('full_text', ''),
                            date=self._parse_date(t.get('date')),
                            title=t.get('title', ''),
                            source_url=t.get('source_url', ''),
                            speaker=t.get('speaker') or speaker_config.get('name', ''),
                            event_type=t.get('event_type', ''),
                        )
                    
                    result.success = result.transcript_count > 0
                    result.metadata['credits_used'] = status.get('creditsUsed', 0)
                    return result
                
                elif state == 'failed':
                    result.add_error(f"Agent job failed: {status.get('error', 'Unknown error')}")
                    return result
                
                elif state in ('pending', 'running', 'processing'):
                    time.sleep(poll_interval)
                
                else:
                    result.add_error(f"Unknown agent state: {state}")
                    return result
            
        except AttributeError:
            # client.agent() may not exist in older SDK versions
            result.add_error(
                "Firecrawl agent endpoint not available. "
                "Update firecrawl-py to latest version: pip install -U firecrawl-py"
            )
        except Exception as e:
            result.add_error(f"Agent collection failed: {str(e)}")
        
        return result
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse a date string to datetime."""
        if not date_str:
            return None
        try:
            # Try ISO format first
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except:
            pass
        try:
            # Try YYYY-MM-DD
            return datetime.strptime(date_str, '%Y-%m-%d')
        except:
            pass
        return None


def scrape_with_firecrawl(
    url: str,
    api_key: Optional[str] = None
) -> Optional[str]:
    """
    Simple function to scrape a URL with Firecrawl.
    
    Args:
        url: URL to scrape
        api_key: Optional API key (defaults to env var)
        
    Returns:
        Markdown content or None
    """
    key = api_key or os.getenv('FIRECRAWL_API_KEY')
    if not key:
        raise ValueError("No API key provided")
    
    try:
        from firecrawl import FirecrawlApp
        app = FirecrawlApp(api_key=key)
        result = app.scrape_url(url, params={'formats': ['markdown']})
        return result.get('markdown')
    except Exception as e:
        print(f"Scrape failed: {e}")
        return None


def extract_transcript_from_page(
    url: str,
    api_key: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Extract transcript content from a page using Firecrawl's LLM extraction.
    
    Args:
        url: URL containing a transcript
        api_key: Optional API key
        
    Returns:
        Dictionary with transcript data
    """
    key = api_key or os.getenv('FIRECRAWL_API_KEY')
    if not key:
        raise ValueError("No API key provided")
    
    try:
        from firecrawl import FirecrawlApp
        app = FirecrawlApp(api_key=key)
        
        result = app.scrape_url(
            url,
            params={
                'formats': ['extract'],
                'extract': {
                    'prompt': """Extract the transcript from this page. Return:
                    - full_transcript: The complete transcript text
                    - speaker: The main speaker's name
                    - date: The date of the speech/interview (YYYY-MM-DD format)
                    - title: The title of the event
                    - event_type: Type of event (speech, interview, press briefing, etc.)
                    """
                }
            }
        )
        
        return result.get('extract')
        
    except Exception as e:
        print(f"Extraction failed: {e}")
        return None


def agent_find_transcript(
    speaker: str,
    event_description: str,
    target_date: Optional[str] = None,
    hint_urls: Optional[List[str]] = None,
    max_credits: int = 50,
    api_key: Optional[str] = None,
    verbose: bool = True,
) -> Optional[Dict[str, Any]]:
    """
    Use Firecrawl's /agent to autonomously find and extract a transcript.
    
    This is the simplest way to find a transcript when you don't have the exact URL.
    The agent will search the web, navigate to relevant pages, and extract the content.
    
    Args:
        speaker: Name of the speaker (e.g., "Bernie Sanders")
        event_description: Description of the event (e.g., "Late Show with Stephen Colbert")
        target_date: Optional date string (e.g., "January 21, 2026")
        hint_urls: Optional URLs to prioritize searching (e.g., YouTube, C-SPAN)
        max_credits: Maximum credits to spend (cost control)
        api_key: Optional API key (defaults to env var)
        verbose: Print status updates
        
    Returns:
        Dictionary with transcript data or None
        
    Example:
        >>> result = agent_find_transcript(
        ...     speaker="Bernie Sanders",
        ...     event_description="The Late Show with Stephen Colbert interview",
        ...     target_date="January 21, 2026",
        ...     hint_urls=["youtube.com", "cbs.com"],
        ... )
        >>> if result:
        ...     print(result['transcripts'][0]['full_text'][:500])
    """
    key = api_key or os.getenv('FIRECRAWL_API_KEY')
    if not key:
        raise ValueError("No API key provided")
    
    # Build the prompt
    date_str = f" on {target_date}" if target_date else ""
    prompt = f"""Find the complete transcript of {speaker}'s appearance on {event_description}{date_str}.

Search for:
1. Official video uploads (YouTube, network websites)
2. Transcript repositories 
3. News articles with full quotes

Extract the full transcript text, including all dialogue and any notable moments.
Include the date, title, speaker name, and source URL for each transcript found."""

    if verbose:
        print(f"🔍 Agent searching for: {speaker} - {event_description}")
    
    try:
        from firecrawl import FirecrawlApp
        app = FirecrawlApp(api_key=key)
        
        agent_params = {
            'prompt': prompt,
            'schema': TRANSCRIPT_SCHEMA,
            'maxCredits': max_credits,
            'model': 'spark-1-mini',
        }
        
        if hint_urls:
            agent_params['urls'] = hint_urls
        
        # Start agent job - prompt must be keyword arg
        job = app.agent(
            urls=hint_urls,
            prompt=prompt,
            schema=TRANSCRIPT_SCHEMA,
            max_credits=max_credits,
            model='spark-1-mini',
        )
        job_id = job.get('id')
        
        if not job_id:
            print("❌ Failed to start agent job")
            return None
        
        if verbose:
            print(f"  Job started: {job_id}")
        
        # Poll for completion
        import time
        start = time.time()
        timeout = 300  # 5 minutes
        
        while time.time() - start < timeout:
            status = app.get_agent_status(job_id)
            state = status.get('status', 'unknown')
            
            if verbose and state in ('running', 'processing'):
                print(f"  Status: {state}...")
            
            if state == 'completed':
                data = status.get('data', {})
                transcripts = data.get('transcripts', [])
                
                if verbose:
                    credits = status.get('creditsUsed', 0)
                    print(f"✅ Found {len(transcripts)} transcript(s) (used {credits} credits)")
                
                return data
            
            elif state == 'failed':
                error = status.get('error', 'Unknown error')
                if verbose:
                    print(f"❌ Agent failed: {error}")
                return None
            
            time.sleep(5)
        
        if verbose:
            print("⏱️ Agent timed out")
        return None
        
    except AttributeError:
        print("❌ Firecrawl agent endpoint not available. Update: pip install -U firecrawl-py")
        return None
    except Exception as e:
        print(f"❌ Agent error: {e}")
        return None


def search_discover_videos(
    speaker: str,
    event_description: str,
    target_date: Optional[str] = None,
    limit: int = 10,
    api_key: Optional[str] = None,
    verbose: bool = True,
) -> List[Dict[str, Any]]:
    """
    Use Firecrawl search to find YouTube videos (simpler/cheaper than agent).
    
    This uses Firecrawl's search endpoint to find videos, which is faster
    and more cost-effective than the agent endpoint.
    
    Args:
        speaker: Speaker name
        event_description: Event/show name
        target_date: Target date string
        limit: Max results to return
        api_key: Optional API key
        verbose: Print progress
        
    Returns:
        List of video dictionaries with 'url', 'title' keys
    """
    key = api_key or os.getenv('FIRECRAWL_API_KEY')
    if not key:
        raise ValueError("No API key provided")
    
    # Build search query
    date_str = target_date or ""
    query = f"{speaker} {event_description} {date_str} full interview site:youtube.com"
    
    if verbose:
        print(f"🔍 Searching: {query}")
    
    try:
        from firecrawl import FirecrawlApp
        app = FirecrawlApp(api_key=key)
        
        results = app.search(query=query, limit=limit)
        
        videos = []
        if results and results.web:
            for item in results.web:
                if 'youtube.com/watch' in item.url:
                    videos.append({
                        'url': item.url,
                        'title': item.title,
                        'description': item.description,
                    })
        
        if verbose:
            print(f"✅ Found {len(videos)} YouTube video(s)")
        
        return videos
        
    except Exception as e:
        if verbose:
            print(f"❌ Search error: {e}")
        return []


def agent_discover_videos(
    speaker: str,
    event_description: str,
    target_date: Optional[str] = None,
    hint_urls: Optional[List[str]] = None,
    max_credits: int = 100,
    api_key: Optional[str] = None,
    verbose: bool = True,
) -> List[Dict[str, Any]]:
    """
    Use Firecrawl agent to discover video URLs (lightweight).
    
    This is the recommended approach: let the agent find videos,
    then use our YouTube transcript tools to get the actual transcripts.
    
    Args:
        speaker: Speaker name
        event_description: Event/show name
        target_date: Target date string
        hint_urls: URLs to prioritize (e.g., specific YouTube channels)
        max_credits: Credit limit
        api_key: Optional API key
        verbose: Print progress
        
    Returns:
        List of video dictionaries with 'url', 'title', 'date' keys
        
    Example:
        >>> videos = agent_discover_videos(
        ...     speaker="Will Smith",
        ...     event_description="The Tonight Show",
        ...     target_date="January 2026",
        ... )
        >>> for v in videos:
        ...     print(v['url'])  # Feed these to YouTube transcript downloader
    """
    key = api_key or os.getenv('FIRECRAWL_API_KEY')
    if not key:
        raise ValueError("No API key provided")
    
    date_str = f" from {target_date}" if target_date else ""
    prompt = f"""Find YouTube videos of {speaker} appearing on {event_description}{date_str}.

Return video URLs with titles and dates. Prioritize:
1. Full interviews over short clips
2. Official channel uploads
3. Recent uploads matching the target date"""

    if verbose:
        print(f"🔍 Agent searching for {speaker} videos...")
    
    try:
        from firecrawl import FirecrawlApp
        app = FirecrawlApp(api_key=key)
        
        result = app.agent(
            urls=hint_urls,
            prompt=prompt,
            schema=VIDEO_DISCOVERY_SCHEMA,
            max_credits=max_credits,
            model='spark-1-mini',
        )
        
        if verbose:
            print(f"  Status: {result.status}, Credits: {result.credits_used}")
        
        if result.status == 'completed' and result.data:
            videos = result.data.get('videos', [])
            if verbose:
                print(f"✅ Found {len(videos)} video(s)")
            return videos
        elif result.error:
            if verbose:
                print(f"❌ Agent error: {result.error}")
            return []
        else:
            return []
            
    except Exception as e:
        if verbose:
            print(f"❌ Error: {e}")
        return []


def discover_and_transcribe(
    speaker: str,
    event_description: str,
    target_date: Optional[str] = None,
    output_dir: str = "data/mentions/transcripts",
    max_credits: int = 100,
    verbose: bool = True,
) -> List[str]:
    """
    Two-phase collection: agent discovers videos, then we transcribe them.
    
    This is the most cost-effective approach:
    1. Firecrawl agent finds relevant YouTube URLs (cheap)
    2. YouTube transcript downloader gets transcripts (free)
    
    Args:
        speaker: Speaker name
        event_description: Event/show description
        target_date: Target date
        output_dir: Where to save transcripts
        max_credits: Max Firecrawl credits for discovery
        verbose: Print progress
        
    Returns:
        List of saved transcript file paths
    """
    import sys
    from pathlib import Path
    
    # Phase 1: Discover videos
    if verbose:
        print("=" * 50)
        print("Phase 1: Video Discovery (Firecrawl Agent)")
        print("=" * 50)
    
    videos = agent_discover_videos(
        speaker=speaker,
        event_description=event_description,
        target_date=target_date,
        max_credits=max_credits,
        verbose=verbose,
    )
    
    if not videos:
        if verbose:
            print("No videos found. Try manual search or different keywords.")
        return []
    
    # Phase 2: Transcribe videos
    if verbose:
        print()
        print("=" * 50)
        print("Phase 2: Transcription (YouTube API)")
        print("=" * 50)
    
    saved_files = []
    
    try:
        # Add project root to path
        project_root = Path(__file__).parent.parent.parent.parent
        sys.path.insert(0, str(project_root))
        
        from src.scrapers.youtube.youtube_transcript_downloader import YouTubeTranscriptDownloader
        
        downloader = YouTubeTranscriptDownloader(use_proxy=True)
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        for video in videos:
            url = video.get('url')
            if not url:
                continue
            
            video_id = downloader.extract_video_id(url)
            if not video_id:
                if verbose:
                    print(f"  ⚠ Could not extract ID from: {url}")
                continue
            
            if verbose:
                print(f"  📝 Transcribing: {video.get('title', video_id)[:50]}...")
            
            try:
                transcript = downloader.download_transcript(video_id)
                if transcript:
                    # Save transcript
                    title = video.get('title', video_id)
                    safe_title = "".join(c if c.isalnum() or c in ' -_' else '' for c in title)[:50]
                    date = video.get('date', 'unknown')
                    filename = f"{date}_{safe_title}.txt"
                    filepath = output_path / filename
                    
                    full_text = " ".join([s.text for s in transcript])
                    
                    with open(filepath, 'w') as f:
                        f.write(f"Source: {url}\n")
                        f.write(f"Title: {title}\n")
                        f.write(f"Date: {date}\n")
                        f.write(f"Speaker: {speaker}\n")
                        f.write("=" * 80 + "\n\n")
                        f.write(full_text)
                    
                    saved_files.append(str(filepath))
                    if verbose:
                        print(f"    ✅ Saved: {filename}")
                else:
                    if verbose:
                        print(f"    ❌ No transcript available")
                        
            except Exception as e:
                if verbose:
                    print(f"    ❌ Error: {e}")
    
    except ImportError:
        if verbose:
            print("YouTube transcript downloader not available")
            print("Video URLs found:")
            for v in videos:
                print(f"  • {v.get('url')}")
    
    if verbose:
        print()
        print(f"✅ Saved {len(saved_files)} transcript(s) to {output_dir}")
    
    return saved_files


def build_transcript_prompt(
    speaker: str,
    event_title: str,
    event_type: str = "interview",
    target_date: Optional[str] = None,
    phrases_to_track: Optional[List[str]] = None,
) -> str:
    """
    Build an optimized prompt for the Firecrawl agent to find transcripts.
    
    Args:
        speaker: Speaker name
        event_title: Full event title
        event_type: Type of event (interview, speech, press briefing, etc.)
        target_date: Target date string
        phrases_to_track: Optional list of phrases we're looking for (for context)
        
    Returns:
        Optimized prompt string
    """
    date_str = f" on or around {target_date}" if target_date else ""
    
    # Add event-type specific search guidance
    source_hints = {
        'late_night': "Check official show YouTube channels (CBS, NBC, ABC) for full interviews.",
        'press_briefing': "Check C-SPAN, official government websites, and news network coverage.",
        'speech': "Check C-SPAN, official organization websites, and news coverage.",
        'interview': "Check news network YouTube channels, podcasts, and news articles.",
        'podcast': "Check podcast platforms (YouTube, Spotify) for full episodes.",
    }
    
    hint = source_hints.get(event_type, "Check YouTube, news sites, and official sources.")
    
    prompt = f"""Find the complete transcript of "{event_title}"{date_str}.

Speaker: {speaker}
Event type: {event_type}

{hint}

Requirements:
1. Find the FULL transcript, not just clips or highlights
2. Include timestamps if available
3. Capture all dialogue from {speaker}
4. Note the source URL for verification
"""
    
    if phrases_to_track:
        phrases_str = ", ".join(phrases_to_track[:5])
        prompt += f"\nContext: We're tracking whether these phrases are mentioned: {phrases_str}"
    
    return prompt
