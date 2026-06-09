"""
Mention Market Transcript Collector

End-to-end pipeline for collecting relevant transcripts for a mention market event.

The Pipeline:
1. Parse the market event to extract speaker, event type, target date
2. Search YouTube for relevant historical content (past interviews, speeches)
3. PREVIEW: Show found videos, allow manual selection
4. Download transcripts via youtube-transcript.io API
5. PROCESS: Extract just the speaker's dialogue using LLM
6. Save and organize for analysis

Usage:
    from src.auto_collect.collector import collect_for_event, preview_videos
    
    # Interactive mode - preview before downloading
    preview = preview_videos("What will Will Smith say during The Tonight Show?")
    preview.show()  # Display found videos
    preview.select([0, 2, 5])  # Select specific videos
    result = preview.collect()  # Download selected only
    
    # Or automatic mode
    result = collect_for_event("What will Will Smith say...")
"""

import os
import re
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field

from .transcript_api import YouTubeTranscriptAPI, get_transcripts_batch
from .strategies.youtube_scraper import search_youtube_videos, VideoResult


@dataclass
class VideoPreview:
    """Interactive preview of found videos for manual selection."""
    
    context: 'EventContext'
    videos: List[VideoResult]
    selected_indices: List[int] = field(default_factory=list)
    
    def __post_init__(self):
        # Select all by default
        self.selected_indices = list(range(len(self.videos)))
    
    def show(self, show_all: bool = False) -> str:
        """Display videos with selection status. Returns formatted string."""
        lines = []
        lines.append(f"\n{'='*70}")
        lines.append(f"📺 Found {len(self.videos)} videos for: {self.context.speaker}")
        lines.append(f"{'='*70}")
        
        for i, v in enumerate(self.videos):
            selected = "✅" if i in self.selected_indices else "❌"
            score = getattr(v, '_relevance_score', 0)
            lines.append(f"")
            lines.append(f"[{i}] {selected} {v.title[:60]}...")
            lines.append(f"    Duration: {v.duration_formatted} | Channel: {v.channel[:30]}")
            lines.append(f"    URL: {v.url}")
            if show_all:
                lines.append(f"    Published: {v.published_text} | Score: {score}")
        
        lines.append(f"\n{'='*70}")
        lines.append(f"Selected: {len(self.selected_indices)}/{len(self.videos)} videos")
        lines.append(f"{'='*70}")
        
        output = "\n".join(lines)
        print(output)
        return output
    
    def select(self, indices: List[int]):
        """Select specific videos by index."""
        self.selected_indices = [i for i in indices if 0 <= i < len(self.videos)]
        print(f"✅ Selected {len(self.selected_indices)} videos")
    
    def select_all(self):
        """Select all videos."""
        self.selected_indices = list(range(len(self.videos)))
        print(f"✅ Selected all {len(self.videos)} videos")
    
    def deselect_all(self):
        """Deselect all videos."""
        self.selected_indices = []
        print("❌ Deselected all videos")
    
    def toggle(self, index: int):
        """Toggle selection of a single video."""
        if index in self.selected_indices:
            self.selected_indices.remove(index)
            print(f"❌ Deselected [{index}]")
        else:
            self.selected_indices.append(index)
            self.selected_indices.sort()
            print(f"✅ Selected [{index}]")
    
    def add(self, indices: List[int]):
        """Add videos to selection."""
        for i in indices:
            if 0 <= i < len(self.videos) and i not in self.selected_indices:
                self.selected_indices.append(i)
        self.selected_indices.sort()
        print(f"✅ Now selected: {len(self.selected_indices)} videos")
    
    def remove(self, indices: List[int]):
        """Remove videos from selection."""
        for i in indices:
            if i in self.selected_indices:
                self.selected_indices.remove(i)
        print(f"✅ Now selected: {len(self.selected_indices)} videos")
    
    def get_selected_videos(self) -> List[VideoResult]:
        """Get list of selected videos."""
        return [self.videos[i] for i in self.selected_indices]
    
    def collect(
        self,
        output_dir: Optional[str] = None,
        process_with_llm: bool = False,
        model: str = "gemini-2.0-flash",
    ) -> Dict[str, Any]:
        """
        Collect transcripts for selected videos only.
        
        Args:
            output_dir: Where to save transcripts
            process_with_llm: If True, extract just the speaker's dialogue
            model: LLM model for processing
        """
        selected = self.get_selected_videos()
        
        if not selected:
            print("❌ No videos selected")
            return {'transcripts': [], 'saved_files': []}
        
        print(f"\n📥 Collecting {len(selected)} transcripts...")
        
        # Collect transcripts
        transcripts = collect_transcripts(selected, verbose=True)
        
        # Determine output directory
        if output_dir is None:
            if self.context.participants:
                safe_name = '_'.join([re.sub(r'[^\w\s-]', '', p).replace(' ', '_').lower() for p in self.context.participants[:2]])
                output_dir = f"data/mentions/{self.context.event_type}/{safe_name}"
            else:
                safe_speaker = re.sub(r'[^\w\s-]', '', self.context.speaker).replace(' ', '_').lower()
                output_dir = f"data/mentions/{self.context.event_type}/{safe_speaker}"
        
        # Save
        saved_files = save_transcripts(transcripts, self.context, output_dir)
        
        print(f"✅ Saved {len(saved_files)} transcripts to {output_dir}")
        
        # Optional LLM processing
        if process_with_llm and saved_files:
            print(f"\n🤖 Processing with LLM to extract {self.context.speaker}'s dialogue...")
            from .transcript_processor import process_batch
            process_batch(output_dir, self.context.speaker, model=model)
        
        return {
            'context': self.context.__dict__,
            'transcripts': transcripts,
            'saved_files': saved_files,
        }
    
    def to_json(self) -> str:
        """Export preview to JSON for later use."""
        return json.dumps({
            'context': {
                'event_ticker': self.context.event_ticker,
                'title': self.context.title,
                'speaker': self.context.speaker,
                'event_type': self.context.event_type,
                'show_name': self.context.show_name,
            },
            'videos': [
                {
                    'index': i,
                    'video_id': v.video_id,
                    'title': v.title,
                    'url': v.url,
                    'duration': v.duration_formatted,
                    'channel': v.channel,
                    'selected': i in self.selected_indices,
                }
                for i, v in enumerate(self.videos)
            ],
            'selected_indices': self.selected_indices,
        }, indent=2)
    
    def save(self, path: str):
        """Save preview to file."""
        with open(path, 'w') as f:
            f.write(self.to_json())
        print(f"💾 Saved preview to {path}")


@dataclass
class EventContext:
    """Parsed context from a mention market event."""
    event_ticker: str
    title: str
    speaker: str
    event_type: str  # late_night, press_briefing, speech, interview, fight, mma, etc.
    show_name: Optional[str] = None
    venue: Optional[str] = None
    target_date: Optional[datetime] = None
    phrases: List[str] = field(default_factory=list)
    # For fight events, store both fighters
    participants: List[str] = field(default_factory=list)  # e.g., ["Gaethje", "Pimblett"]
    
    def get_search_queries(self, extended: bool = False) -> List[str]:
        """
        Generate search queries for finding relevant content.
        
        Args:
            extended: If True, generate many more query variations for broader coverage
        """
        queries = []
        
        # Fight/MMA events - search for full fights
        if self.event_type in ['fight', 'mma', 'boxing', 'ufc']:
            if len(self.participants) >= 2:
                fighter1, fighter2 = self.participants[0], self.participants[1]
                # Full fight queries
                queries.append(f"{fighter1} vs {fighter2} full fight")
                queries.append(f"{fighter1} {fighter2} full fight")
                queries.append(f"{fighter1} vs {fighter2} fight")
                queries.append(f"{fighter1} {fighter2} fight")
                # Also search for each fighter individually (in case of highlights or separate videos)
                queries.append(f"{fighter1} vs {fighter2}")
            elif self.speaker:
                # Fallback if only one participant extracted
                queries.append(f"{self.speaker} full fight")
                queries.append(f"{self.speaker} fight")
        
        if self.event_type == 'late_night' and self.show_name:
            # Late night shows - search for past appearances on the same show
            queries.append(f"{self.speaker} {self.show_name} full interview")
            queries.append(f"{self.speaker} {self.show_name}")
        
        if self.event_type == 'press_briefing':
            queries.append(f"{self.speaker} press briefing full")
            queries.append(f"{self.speaker} press conference")
            if extended:
                queries.append(f"{self.speaker} briefing")
                queries.append(f"{self.speaker} presser")
                queries.append(f"{self.speaker} press")
        
        if self.event_type == 'speech':
            queries.append(f"{self.speaker} speech full")
            queries.append(f"{self.speaker} remarks")
            if extended:
                queries.append(f"{self.speaker} address")
                queries.append(f"{self.speaker} keynote")
        
        # General queries (skip for fight events as they have specific queries above)
        if self.event_type not in ['fight', 'mma', 'boxing', 'ufc']:
            queries.append(f"{self.speaker} interview full")
            queries.append(f"{self.speaker} interview")
            
            if extended:
                # Extended queries for broader coverage
                # Format variations
                queries.append(f"{self.speaker} full interview")
                queries.append(f"{self.speaker} complete interview")
                queries.append(f"{self.speaker} exclusive interview")
                queries.append(f"{self.speaker} one on one")
                queries.append(f"{self.speaker} 1 on 1")
                
                # Q&A / Press formats
                queries.append(f"{self.speaker} press conference")
                queries.append(f"{self.speaker} press conference full")
                queries.append(f"{self.speaker} presser")
                queries.append(f"{self.speaker} Q&A")
                queries.append(f"{self.speaker} questions")
                queries.append(f"{self.speaker} answers questions")
                queries.append(f"{self.speaker} town hall")
                queries.append(f"{self.speaker} town hall full")
                
                # News networks
                queries.append(f"{self.speaker} CNN interview")
                queries.append(f"{self.speaker} Fox News interview")
                queries.append(f"{self.speaker} MSNBC interview")
                queries.append(f"{self.speaker} NBC interview")
                queries.append(f"{self.speaker} ABC interview")
                queries.append(f"{self.speaker} CBS interview")
                queries.append(f"{self.speaker} PBS interview")
                queries.append(f"{self.speaker} NPR interview")
                queries.append(f"{self.speaker} BBC interview")
                
                # Podcast/long-form
                queries.append(f"{self.speaker} podcast")
                queries.append(f"{self.speaker} podcast full")
                queries.append(f"{self.speaker} long form")
                queries.append(f"{self.speaker} conversation")
                queries.append(f"{self.speaker} sit down")
                queries.append(f"{self.speaker} speaks")
                queries.append(f"{self.speaker} talks")
                
                # Recent/specific time
                queries.append(f"{self.speaker} interview 2025")
                queries.append(f"{self.speaker} interview 2024")
                queries.append(f"{self.speaker} interview 2023")
                queries.append(f"{self.speaker} latest interview")
                queries.append(f"{self.speaker} recent interview")
        
        if self.venue and self.event_type not in ['fight', 'mma', 'boxing', 'ufc']:
            queries.insert(0, f"{self.speaker} {self.venue} interview full")
            queries.insert(1, f"{self.speaker} {self.venue} interview")
            queries.append(f"{self.speaker} on {self.venue}")
            if extended:
                queries.append(f"{self.speaker} {self.venue} full segment")
                queries.append(f"{self.speaker} {self.venue} full show")

        # De-duplicate while preserving order
        deduped = []
        seen = set()
        for q in queries:
            q_clean = q.strip()
            if not q_clean:
                continue
            key = q_clean.lower()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(q_clean)

        # Limit based on extended mode
        max_queries = 50 if extended else 5
        return deduped[:max_queries]
    
    @property
    def search_queries(self) -> List[str]:
        """Generate search queries for finding relevant content (basic mode)."""
        return self.get_search_queries(extended=False)


def parse_event_context(
    event_title: str, 
    event_ticker: str = "",
    event_type: Optional[str] = None,
    venue: Optional[str] = None,
) -> EventContext:
    """
    Parse market event title to extract speaker, event type, etc.
    
    Args:
        event_title: The event title to parse
        event_ticker: Optional Kalshi event ticker
        event_type: Optional manual override for event type (e.g., 'fight', 'mma', 'interview')
    
    Examples:
        "What will Bernie Sanders say during The Late Show with Stephen Colbert?"
        "What will Scott Bessent say during his Press Briefing?"
        "What will Will Smith say during The Tonight Show Starring Jimmy Fallon?"
        "What will the announcers say during the Gaethje vs Pimblett fight?"
    """
    title = event_title
    title_lower = title.lower()
    
    # If event_type is manually provided, use it
    if event_type:
        detected_type = event_type.lower()
    else:
        detected_type = None
    
    # Extract speaker name (for non-fight events)
    speaker = ""
    participants = []  # For fight events
    
    # Check for fight/MMA events first (before speaker extraction)
    fight_keywords = ['fight', 'vs', 'versus', 'ufc', 'mma', 'boxing', 'match']
    is_fight_event = detected_type in ['fight', 'mma', 'boxing', 'ufc'] or any(kw in title_lower for kw in fight_keywords)
    
    if is_fight_event and not detected_type:
        detected_type = 'fight'  # Default to 'fight' if not specified
        # Try to detect more specific types
        if 'ufc' in title_lower or 'mma' in title_lower:
            detected_type = 'mma'
        elif 'boxing' in title_lower:
            detected_type = 'boxing'
    
    # Extract fighter names from "X vs Y" or "X versus Y" patterns
    if is_fight_event:
        # Pattern: "Gaethje vs Pimblett" - match capitalized names around vs/versus
        # Use case-sensitive match for proper names
        vs_patterns = [
            # Match "FirstName LastName vs FirstName LastName" (e.g., "Justin Gaethje vs Dan Hooker")
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(?:vs\.?|versus)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)',
            # Simpler: just the last names
            r'([A-Z][a-z]+)\s+(?:vs\.?|versus)\s+([A-Z][a-z]+)',
        ]
        for pattern in vs_patterns:
            # Case-sensitive to get proper names only
            match = re.search(pattern, title)
            if match:
                p1, p2 = match.group(1).strip(), match.group(2).strip()
                # Filter out common words that might get matched
                common_words = {'the', 'a', 'an', 'vs', 'versus', 'fight', 'during', 'what', 'will', 'say'}
                if p1.lower() not in common_words and p2.lower() not in common_words:
                    participants = [p1, p2]
                    # Use first fighter as "speaker" for compatibility
                    speaker = participants[0]
                    break
        
        # If no case-sensitive match, try case-insensitive as fallback
        if not participants:
            match = re.search(r'(\w+)\s+(?:vs\.?|versus)\s+(\w+)', title, re.IGNORECASE)
            if match:
                p1, p2 = match.group(1).strip(), match.group(2).strip()
                common_words = {'the', 'a', 'an', 'vs', 'versus', 'fight', 'during', 'what', 'will', 'say'}
                if p1.lower() not in common_words and p2.lower() not in common_words:
                    participants = [p1, p2]
                    speaker = participants[0]
    
    # Extract speaker name for non-fight events
    if not speaker and not is_fight_event:
        speaker_patterns = [
            r'[Ww]hat will ([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?(?:[A-Z][a-z]+)?(?:\s+[A-Z][a-z]+)?)\s+say',
            r'[Ww]ill ([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?(?:[A-Z][a-z]+)?(?:\s+[A-Z][a-z]+)?)\s+(?:say|mention)',
        ]
        for pattern in speaker_patterns:
            match = re.search(pattern, title)
            if match:
                speaker = match.group(1).strip()
                break
    
    # Detect event type and show name (if not manually set or fight event)
    if not detected_type:
        event_type = "interview"  # default
        show_name = None
        
        # Late night shows
        late_night_shows = {
            'late show': ('late_night', 'The Late Show with Stephen Colbert'),
            'colbert': ('late_night', 'The Late Show with Stephen Colbert'),
            'tonight show': ('late_night', 'The Tonight Show Starring Jimmy Fallon'),
            'jimmy fallon': ('late_night', 'The Tonight Show Starring Jimmy Fallon'),
            'kimmel': ('late_night', 'Jimmy Kimmel Live'),
            'jimmy kimmel': ('late_night', 'Jimmy Kimmel Live'),
            'late night with seth': ('late_night', 'Late Night with Seth Meyers'),
            'seth meyers': ('late_night', 'Late Night with Seth Meyers'),
        }
        
        for keyword, (etype, sname) in late_night_shows.items():
            if keyword in title_lower:
                event_type = etype
                show_name = sname
                break
        
        # Press briefings
        if 'press briefing' in title_lower or 'press conference' in title_lower:
            event_type = 'press_briefing'

        # Explicit interviews
        if 'interview' in title_lower:
            event_type = 'interview'
        
        # Speeches
        if any(x in title_lower for x in ['speech', 'address', 'remarks at', 'state of the union']):
            event_type = 'speech'
        
        # WEF/Davos
        if 'wef' in title_lower or 'davos' in title_lower:
            event_type = 'wef_panel'
        
        # Earnings calls
        if 'earnings' in title_lower:
            event_type = 'earnings_call'
    else:
        event_type = detected_type
        show_name = None

    def _clean_venue(value: str) -> str:
        cleaned = re.sub(r'\s+', ' ', value).strip()
        cleaned = cleaned.strip(' "\'.,:;!?()[]')
        cleaned = re.sub(r'^(?:the\s+)', '', cleaned, flags=re.IGNORECASE)
        return cleaned

    def _extract_venue_from_title(full_title: str) -> Optional[str]:
        patterns = [
            r'during\s+(?:his|her|their|the)\s+(.+?)\s+interview',
            r'during\s+(.+?)\s+interview',
            r'on\s+msnbc(?:\'s)?\s+["“]?([^"”?\n]+?)["”]?(?:\?|$)',
            r'on\s+["“]?([^"”?\n]+?)["”]?(?:\s+interview|\?|$)',
        ]
        for pattern in patterns:
            match = re.search(pattern, full_title, re.IGNORECASE)
            if not match:
                continue
            candidate = _clean_venue(match.group(1))
            if candidate:
                return candidate
        return None

    parsed_venue = _clean_venue(venue) if venue else _extract_venue_from_title(title)
    
    return EventContext(
        event_ticker=event_ticker,
        title=title,
        speaker=speaker,
        event_type=event_type,
        show_name=show_name,
        venue=parsed_venue,
        participants=participants,
    )


def search_relevant_videos(
    context: EventContext,
    max_results_per_query: int = 10,
    verbose: bool = True,
    min_duration_minutes: int = 0,
    extended_search: bool = False,
    min_videos_target: int = 0,
) -> List[VideoResult]:
    """
    Search YouTube for videos relevant to the event context.
    
    Args:
        context: Event context with speaker/participants info
        max_results_per_query: Max results per search query
        verbose: Print progress
        min_duration_minutes: Minimum video duration in minutes
        extended_search: If True, use many more query variations
        min_videos_target: If set and > 0, automatically enable extended search
                          if initial results are below this target
    
    Returns deduplicated list of videos sorted by relevance.
    """
    all_videos = []
    seen_ids = set()

    # Determine if we should use extended search
    use_extended = extended_search or (min_videos_target > 20)

    # In extended mode, bump per-query results to capture more unique videos
    if use_extended:
        max_results_per_query = max(max_results_per_query, 20)

    # Get queries based on mode
    queries = context.get_search_queries(extended=use_extended)
    
    if verbose:
        mode = "extended" if use_extended else "standard"
        print(f"  Search mode: {mode} ({len(queries)} queries)")
    
    for i, query in enumerate(queries):
        if verbose:
            print(f"  [{i+1}/{len(queries)}] Searching: {query}")
        
        # For fight events, the query already contains both fighters
        # For other events, build query from speaker + event description
        if context.event_type in ['fight', 'mma', 'boxing', 'ufc']:
            # Query is already complete (e.g., "Gaethje vs Pimblett full fight")
            # Pass empty speaker to avoid duplication
            event_description = query
            speaker = ""
        else:
            # Traditional format: speaker + event description
            event_description = query.replace(context.speaker, '').strip()
            speaker = context.speaker
        
        videos = search_youtube_videos(
            speaker=speaker,
            event_description=event_description,
            max_results=max_results_per_query,
            min_duration_minutes=min_duration_minutes,
            verbose=False,
        )
        
        new_count = 0
        for v in videos:
            if v.video_id not in seen_ids:
                seen_ids.add(v.video_id)
                all_videos.append(v)
                new_count += 1
        
        if verbose and new_count > 0:
            print(f"      Found {new_count} new videos (total: {len(all_videos)})")
        
        # Early exit if we have enough videos and not in extended mode
        if not use_extended and len(all_videos) >= min_videos_target * 2:
            break
    
    if verbose:
        print(f"  Found {len(all_videos)} unique videos total")
    
    # If we didn't find enough and haven't tried extended yet, try it
    if min_videos_target > 0 and len(all_videos) < min_videos_target and not use_extended:
        if verbose:
            print(f"  Only found {len(all_videos)}, trying extended search...")
        return search_relevant_videos(
            context=context,
            max_results_per_query=max_results_per_query,
            verbose=verbose,
            min_duration_minutes=min_duration_minutes,
            extended_search=True,
            min_videos_target=min_videos_target,
        )
    
    return all_videos


def parse_relative_date(published_text: str) -> Optional[datetime]:
    """
    Parse relative date text like '2 years ago' to approximate datetime.
    
    Args:
        published_text: Text like "2 years ago", "3 months ago", "5 days ago"
        
    Returns:
        Approximate datetime or None if unparseable
    """
    from datetime import timedelta
    
    if not published_text:
        return None
    
    text = published_text.lower().strip()
    now = datetime.now()
    
    patterns = [
        (r'(\d+)\s*year', 365),
        (r'(\d+)\s*month', 30),
        (r'(\d+)\s*week', 7),
        (r'(\d+)\s*day', 1),
        (r'(\d+)\s*hour', 0),
        (r'(\d+)\s*minute', 0),
    ]
    
    for pattern, days_per_unit in patterns:
        match = re.search(pattern, text)
        if match:
            num = int(match.group(1))
            return now - timedelta(days=num * days_per_unit)
    
    # Check for "Streamed X ago" format
    if 'streamed' in text:
        for pattern, days_per_unit in patterns:
            match = re.search(pattern, text)
            if match:
                num = int(match.group(1))
                return now - timedelta(days=num * days_per_unit)
    
    return None


def filter_relevant_videos(
    videos: List[VideoResult],
    context: EventContext,
    min_duration_seconds: int = 60,
    max_videos: int = 20,
    strict_speaker_match: bool = False,
    strict_venue_match: bool = False,
    earliest_date: Optional[str] = None,
) -> List[VideoResult]:
    """
    Filter videos to keep only the most relevant ones.
    
    Args:
        videos: List of video results to filter
        context: Event context with speaker info
        min_duration_seconds: Minimum video duration in seconds
        max_videos: Maximum number of videos to return
        strict_speaker_match: If True, require full name match (e.g., "Ben Shapiro" not just "Shapiro")
        earliest_date: Earliest publish date to include (e.g., "2023-01-01" or "2023")
    
    Filters:
    - Must mention speaker's FULL name in title (not just first name)
    - For fight events, must mention at least one fighter
    - Minimum duration
    - Filter by earliest date if specified
    - Prefer interviews/full content over clips
    """
    # Parse earliest_date if provided
    earliest_datetime = None
    if earliest_date:
        try:
            # Try various formats
            for fmt in ['%Y-%m-%d', '%Y-%m', '%Y']:
                try:
                    earliest_datetime = datetime.strptime(earliest_date, fmt)
                    break
                except ValueError:
                    continue
        except Exception:
            pass
    filtered = []
    speaker_lower = context.speaker.lower() if context.speaker else ""
    speaker_parts = speaker_lower.split() if speaker_lower else []
    
    # For fight events, check for participant names
    is_fight_event = context.event_type in ['fight', 'mma', 'boxing', 'ufc']
    participant_lowers = [p.lower() for p in context.participants] if context.participants else []

    venue_phrase = (context.venue or "").lower().strip()
    venue_tokens = []
    if venue_phrase:
        ignore = {'the', 'with', 'show', 'interview', 'news', 'during', 'live', 'on', 'at'}
        venue_tokens = [
            token for token in re.findall(r'[a-z0-9]+', venue_phrase)
            if token not in ignore
        ]
    
    # For matching, require either full name OR last name (more specific)
    # This prevents "Will Ferrell" matching "Will Smith"
    full_name_match = speaker_lower
    last_name_match = speaker_parts[-1] if len(speaker_parts) > 1 else None
    first_name_match = speaker_parts[0] if speaker_parts else None
    
    # Whole-word skip patterns prevent false positives (e.g., "Newsom" vs "news")
    skip_keyword_patterns = [
        r'\breaction\b',
        r'\bexplained\b',
        r'\banalysis\b',
        r'\btrailer\b',
        r'\bparody\b',
    ]

    for v in videos:
        title_lower = v.title.lower()
        channel_lower = v.channel.lower() if v.channel else ""
        description_lower = v.description.lower() if getattr(v, 'description', None) else ""
        haystack = f"{title_lower} {channel_lower} {description_lower}"
        has_full_name = False
        has_last_name = False
        has_first_name = False
        
        # For fight events, check if title mentions at least one fighter
        if is_fight_event and participant_lowers:
            has_fighter = any(
                fighter.lower() in title_lower 
                for fighter in context.participants
            )
            if not has_fighter:
                continue
        elif not is_fight_event and speaker_lower:
            # Must mention speaker - require FULL name or at least last name
            has_full_name = full_name_match in title_lower
            has_last_name = last_name_match and last_name_match in title_lower
            has_first_name = first_name_match and first_name_match in title_lower
            
            # Strict mode: require full name match
            if strict_speaker_match:
                if not has_full_name:
                    continue
            else:
                # Check for potential false positives (different first name with same last name)
                # e.g., "Ben Shapiro" vs "Josh Shapiro" or "Gov. Shapiro"
                if has_last_name and not has_full_name and last_name_match:
                    # Check if there's a different first name or title before the last name
                    # Build patterns to check for "X Lastname" where X != first_name
                    # Pattern 1: Direct name like "Josh Shapiro"
                    pattern1 = rf'\b(\w+)\s+{re.escape(last_name_match)}\b'
                    # Pattern 2: Title prefix like "Gov. Shapiro", "Sen. Shapiro"
                    pattern2 = rf'\b(gov|sen|rep|dr|mr|mrs|ms|president|senator|governor|judge|mayor)\.?\s+{re.escape(last_name_match)}\b'
                    
                    # Check for title prefix first - indicates a different person with title
                    title_match = re.search(pattern2, title_lower)
                    if title_match:
                        # Has a title prefix but not the full name - likely different person
                        continue
                    
                    # Check for different first name
                    match = re.search(pattern1, title_lower)
                    if match:
                        found_first_name = match.group(1)
                        # If we found a different first name, it's a different person
                        if found_first_name != first_name_match and found_first_name not in ['the', 'a', 'and', 'vs', 'with', 'from', 'by', 'to', 'on', 'in']:
                            continue  # Skip - different person (e.g., "Josh Shapiro" != "Ben Shapiro")
                
                # For common first names, require the last name too
                common_first_names = ['will', 'john', 'mike', 'david', 'james', 'chris', 'tom', 'joe', 'ben', 'josh', 'matt', 'mark', 'dan', 'steve', 'jim', 'bob', 'bill', 'paul', 'scott', 'adam', 'alex', 'ryan', 'kevin', 'brian', 'jason', 'justin', 'eric', 'andrew', 'tim', 'jeff', 'greg', 'tony', 'anthony', 'michael', 'robert', 'richard', 'charles', 'joseph', 'thomas', 'christopher', 'daniel', 'matthew', 'donald', 'steven', 'kenneth', 'george', 'edward', 'brian', 'ronald', 'timothy', 'jose', 'larry', 'jeffrey', 'frank', 'scott', 'raymond', 'dennis']
                first_name = speaker_parts[0] if speaker_parts else ''
                
                if first_name in common_first_names:
                    # Must have last name for common first names
                    if not has_full_name and not has_last_name:
                        continue
                else:
                    # For unique first names, full name OR any part works
                    if not has_full_name and not any(part in title_lower for part in speaker_parts):
                        continue

        if venue_phrase:
            has_full_venue = venue_phrase in haystack
            matched_tokens = sum(1 for token in venue_tokens if token in haystack)
            min_token_hits = min(2, len(venue_tokens)) if venue_tokens else 0
            has_venue = has_full_venue or (min_token_hits > 0 and matched_tokens >= min_token_hits)
            if strict_venue_match and not has_venue:
                continue
        else:
            has_venue = False
        
        # Skip if too short
        if v.duration_seconds < min_duration_seconds:
            continue
        
        # Skip if published before earliest date
        if earliest_datetime:
            video_date = parse_relative_date(v.published_text)
            if video_date and video_date < earliest_datetime:
                continue
        
        # Skip obvious non-relevant content
        if any(re.search(pattern, title_lower) for pattern in skip_keyword_patterns):
            continue
        
        # Boost score for certain keywords
        score = 0
        if 'full' in title_lower:
            score += 3
        if 'interview' in title_lower:
            score += 2
        if 'podcast' in title_lower:
            score += 2
        if context.show_name and context.show_name.lower() in title_lower:
            score += 5
        if venue_phrase and has_venue:
            score += 4
        
        # For fight events, boost for fight-related keywords
        if is_fight_event:
            if 'full fight' in title_lower or 'full match' in title_lower:
                score += 5
            if 'fight' in title_lower or 'match' in title_lower:
                score += 3
            # Boost if both fighters mentioned
            if len(participant_lowers) >= 2:
                if all(fighter.lower() in title_lower for fighter in context.participants[:2]):
                    score += 5
        else:
            # Bonus for having full speaker name
            if speaker_lower and full_name_match in title_lower:
                score += 5  # Higher bonus for exact full name match
            elif has_last_name and has_first_name:
                score += 3  # Both parts present but not adjacent
        
        # Store score for sorting
        v._relevance_score = score
        filtered.append(v)
    
    # Sort by relevance score (descending)
    filtered.sort(key=lambda x: getattr(x, '_relevance_score', 0), reverse=True)
    
    return filtered[:max_videos]


def deduplicate_videos(
    videos: List[VideoResult],
    speaker: str = "",
    verbose: bool = True,
) -> List[VideoResult]:
    """
    Remove content-level duplicates (same event uploaded by different channels).

    Tries LLM-based grouping first (Gemini Flash), falls back to rule-based
    title/duration heuristics. For each duplicate group, keeps the video with
    the longest duration (most complete version).

    Args:
        videos: List of videos to deduplicate
        speaker: Speaker name (excluded from rule-based keyword matching)
        verbose: Print progress

    Returns:
        Deduplicated list of videos
    """
    if len(videos) <= 1:
        return videos

    # Try LLM dedup first, fall back to rule-based
    groups = _llm_dedup_groups(videos, verbose=verbose)
    if groups is None:
        groups = _rule_based_dedup_groups(videos, speaker=speaker, verbose=verbose)

    if not groups:
        return videos

    # Build set of indices to remove (keep longest in each group)
    remove_indices: set = set()
    for group in groups:
        if len(group) < 2:
            continue
        # Find the video with the longest duration in the group
        best_idx = max(group, key=lambda i: videos[i].duration_seconds)
        for idx in group:
            if idx != best_idx:
                remove_indices.add(idx)

    if remove_indices and verbose:
        print(f"   Dedup: removed {len(remove_indices)} content duplicates")

    return [v for i, v in enumerate(videos) if i not in remove_indices]


def _llm_dedup_groups(
    videos: List[VideoResult],
    verbose: bool = True,
) -> Optional[List[List[int]]]:
    """
    Use Gemini Flash to identify groups of videos covering the same event.

    Returns list of groups (each group is a list of indices), or None if
    LLM is unavailable.
    """
    try:
        from google import genai
        from google.genai import types
    except ImportError:
        if verbose:
            print("   Dedup: google-genai not installed, using rule-based fallback")
        return None

    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        if verbose:
            print("   Dedup: GOOGLE_API_KEY not set, using rule-based fallback")
        return None

    # Build compact metadata for the LLM
    video_lines = []
    for i, v in enumerate(videos):
        mins = v.duration_seconds // 60
        video_lines.append(f"{i}|{v.title}|{v.channel}|{mins}m|{v.published_text}")

    metadata_block = "\n".join(video_lines)

    prompt = f"""You are a deduplication assistant. Below is a list of YouTube videos (index|title|channel|duration|published).

Many different channels upload the SAME event (e.g., the same press briefing, interview, or speech). Group videos that cover the SAME event together.

Only group videos that are clearly the same event — not just the same speaker on different occasions.

Return ONLY a JSON array of arrays, where each inner array contains the indices of videos covering the same event. Only include groups with 2+ videos. If no duplicates exist, return [].

Example output: [[0,3,7],[1,4]]

Videos:
{metadata_block}

JSON:"""

    try:
        client = genai.Client(api_key=api_key)
        config = types.GenerateContentConfig(
            temperature=0.0,
            max_output_tokens=1024,
        )
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt,
            config=config,
        )
        text = response.text.strip()

        # Extract JSON from response (handle markdown code blocks)
        if "```" in text:
            match = re.search(r'```(?:json)?\s*(\[.*?\])\s*```', text, re.DOTALL)
            if match:
                text = match.group(1)

        groups = json.loads(text)
        if not isinstance(groups, list):
            return None

        # Validate: each group must be a list of valid indices
        valid_groups = []
        for group in groups:
            if isinstance(group, list) and len(group) >= 2:
                valid = [int(idx) for idx in group if 0 <= int(idx) < len(videos)]
                if len(valid) >= 2:
                    valid_groups.append(valid)

        if verbose and valid_groups:
            total_dupes = sum(len(g) - 1 for g in valid_groups)
            print(f"   Dedup (LLM): found {len(valid_groups)} duplicate groups ({total_dupes} redundant)")

        return valid_groups

    except Exception as e:
        if verbose:
            print(f"   Dedup: LLM failed ({e}), using rule-based fallback")
        return None


def _rule_based_dedup_groups(
    videos: List[VideoResult],
    speaker: str = "",
    verbose: bool = True,
) -> List[List[int]]:
    """
    Group videos by title keyword overlap and similar duration.

    Two videos are considered duplicates if:
    - Duration is within 30% of each other
    - Title keywords overlap >= 50% of the smaller keyword set (min 3 shared words)

    Speaker name words are excluded from overlap calculation to avoid
    false positives when all videos feature the same speaker.
    """
    NOISE_WORDS = {
        'full', 'live', 'watch', 'breaking', 'exclusive', 'video', 'official',
        'hd', '4k', 'the', 'a', 'an', 'and', 'or', 'of', 'in', 'on', 'at',
        'to', 'for', 'is', 'it', 'with', 'from', 'by', 'this', 'that', 'new',
        'latest', 'today', 'now', '|', '-', ':', '/',
    }

    # Exclude speaker name words from keyword matching
    speaker_words = set(re.findall(r'[a-z0-9]+', speaker.lower())) if speaker else set()

    def _title_keywords(title: str) -> set:
        words = set(re.findall(r'[a-z0-9]+', title.lower()))
        return words - NOISE_WORDS - speaker_words

    def _duration_ratio(a: int, b: int) -> float:
        if a == 0 or b == 0:
            return 0.0
        return min(a, b) / max(a, b)

    def _should_group(kw_a: set, kw_b: set, dur_a: int, dur_b: int) -> bool:
        if not kw_a or not kw_b:
            return False
        ratio = _duration_ratio(dur_a, dur_b)
        shared = kw_a & kw_b
        smaller = min(len(kw_a), len(kw_b))
        # Tight duration match (within 5%): only need 2 shared content words
        if ratio >= 0.95 and len(shared) >= 2:
            return True
        # Normal match (within 30%): need 3+ shared words at 50%+ overlap
        if ratio >= 0.7 and len(shared) >= 3 and len(shared) / smaller >= 0.5:
            return True
        return False

    # Pre-compute keywords
    keywords = [_title_keywords(v.title) for v in videos]

    # Union-find for grouping
    parent = list(range(len(videos)))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for i in range(len(videos)):
        for j in range(i + 1, len(videos)):
            if _should_group(keywords[i], keywords[j],
                             videos[i].duration_seconds, videos[j].duration_seconds):
                union(i, j)

    # Collect groups
    from collections import defaultdict
    group_map: Dict[int, List[int]] = defaultdict(list)
    for i in range(len(videos)):
        group_map[find(i)].append(i)

    groups = [g for g in group_map.values() if len(g) >= 2]

    if verbose and groups:
        total_dupes = sum(len(g) - 1 for g in groups)
        print(f"   Dedup (rule-based): found {len(groups)} duplicate groups ({total_dupes} redundant)")

    return groups


def collect_transcripts(
    videos: List[VideoResult],
    verbose: bool = True,
    use_fallback: bool = True,
) -> List[Dict[str, Any]]:
    """
    Collect transcripts for a list of videos using the transcript API.
    Falls back to direct YouTube transcript download if API fails.
    """
    if not videos:
        return []
    
    api = YouTubeTranscriptAPI()
    video_ids = [v.video_id for v in videos]
    
    if verbose:
        print(f"  Fetching transcripts for {len(video_ids)} videos...")
    
    # Get transcripts in batch via API
    results = api.get_transcripts(video_ids)
    
    # Track which videos need fallback
    videos_needing_fallback = []
    
    # Combine with video metadata
    transcripts = []
    for video in videos:
        if video.video_id in results:
            transcript_data = results[video.video_id]
            if transcript_data.get('text'):
                transcripts.append({
                    'video_id': video.video_id,
                    'url': video.url,
                    'title': video.title,
                    'channel': video.channel,
                    'duration': video.duration_formatted,
                    'duration_seconds': video.duration_seconds,
                    'published': video.published_text,
                    'text': transcript_data['text'],
                    'word_count': len(transcript_data['text'].split()),
                    'source': 'youtube-transcript-api',
                })
            else:
                videos_needing_fallback.append(video)
        else:
            videos_needing_fallback.append(video)
    
    # Try fallback for videos without transcripts
    if use_fallback and videos_needing_fallback:
        if verbose:
            print(f"  Trying fallback for {len(videos_needing_fallback)} videos...")
        
        try:
            from src.scrapers.youtube.youtube_transcript_downloader import YouTubeTranscriptDownloader
            downloader = YouTubeTranscriptDownloader(use_proxy=True)
            
            for video in videos_needing_fallback:
                try:
                    transcript = downloader.download_transcript(video.video_id)
                    if transcript:
                        full_text = ' '.join([s.text for s in transcript])
                        transcripts.append({
                            'video_id': video.video_id,
                            'url': video.url,
                            'title': video.title,
                            'channel': video.channel,
                            'duration': video.duration_formatted,
                            'duration_seconds': video.duration_seconds,
                            'published': video.published_text,
                            'text': full_text,
                            'word_count': len(full_text.split()),
                            'source': 'youtube-dl-fallback',
                        })
                except Exception:
                    pass  # Silently skip failed fallbacks
        except ImportError:
            pass  # Fallback not available
    
    if verbose:
        print(f"  Got {len(transcripts)} transcripts")
    
    return transcripts


def save_transcripts(
    transcripts: List[Dict[str, Any]],
    context: EventContext,
    output_dir: Optional[str] = None,
) -> List[str]:
    """
    Save transcripts to files.
    
    Directory structure:
        data/mentions/{event_type}/{speaker}/
    """
    if not transcripts:
        return []
    
    # Determine output directory
    if output_dir is None:
        if context.participants:
            safe_name = '_'.join([re.sub(r'[^\w\s-]', '', p).replace(' ', '_').lower() for p in context.participants[:2]])
            output_dir = f"data/mentions/{context.event_type}/{safe_name}"
        else:
            safe_speaker = re.sub(r'[^\w\s-]', '', context.speaker).replace(' ', '_').lower()
            output_dir = f"data/mentions/{context.event_type}/{safe_speaker}"
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    saved_files = []
    
    for t in transcripts:
        # Generate filename
        safe_title = re.sub(r'[^\w\s-]', '', t['title'])[:50].replace(' ', '_')
        filename = f"{t['video_id']}_{safe_title}.txt"
        filepath = output_path / filename
        
        # Skip if already exists
        if filepath.exists():
            saved_files.append(str(filepath))
            continue
        
        # Write file
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"Video ID: {t['video_id']}\n")
            f.write(f"URL: {t['url']}\n")
            f.write(f"Title: {t['title']}\n")
            f.write(f"Channel: {t['channel']}\n")
            f.write(f"Duration: {t['duration']}\n")
            f.write(f"Published: {t['published']}\n")
            f.write(f"Word Count: {t['word_count']}\n")
            f.write("=" * 80 + "\n\n")
            f.write(t['text'])
        
        saved_files.append(str(filepath))
    
    return saved_files


def preview_videos(
    event_title: str,
    event_ticker: str = "",
    event_type: Optional[str] = None,
    max_videos: int = 20,
    verbose: bool = True,
) -> VideoPreview:
    """
    Search and preview videos WITHOUT downloading.
    
    Returns a VideoPreview object for interactive selection.
    
    Args:
        event_title: The event title
        event_ticker: Optional Kalshi ticker
        event_type: Optional manual override for event type (e.g., 'fight', 'mma', 'interview')
        max_videos: Maximum videos to show
        verbose: Print progress
        
    Returns:
        VideoPreview object for interactive selection
        
    Usage:
        preview = preview_videos("What will Will Smith say...")
        preview = preview_videos("What will announcers say during Gaethje vs Pimblett fight?", event_type='fight')
        preview.show()           # See all found videos
        preview.remove([3, 7])   # Remove irrelevant videos
        preview.select([0,1,2])  # Select only these
        result = preview.collect()  # Download selected
    """
    if verbose:
        print("🔍 Searching for relevant videos...")
    
    # Parse context
    context = parse_event_context(event_title, event_ticker, event_type=event_type)
    
    if verbose:
        if context.participants:
            print(f"   Participants: {', '.join(context.participants)}")
        else:
            print(f"   Speaker: {context.speaker}")
        print(f"   Event Type: {context.event_type}")
        if context.search_queries:
            print(f"   Search queries: {context.search_queries}")
    
    # Search
    all_videos = search_relevant_videos(context, verbose=False)
    
    # Filter
    filtered = filter_relevant_videos(all_videos, context, max_videos=max_videos)
    
    if verbose:
        print(f"   Found {len(all_videos)} videos, kept {len(filtered)} relevant")
    
    # Create preview
    preview = VideoPreview(context=context, videos=filtered)
    
    return preview


def collect_for_event(
    event_title: str,
    event_ticker: str = "",
    phrases: Optional[List[str]] = None,
    event_type: Optional[str] = None,
    max_videos: int = 15,
    output_dir: Optional[str] = None,
    process_with_llm: bool = False,
    model: str = "gemini-2.0-flash",
    verbose: bool = True,
) -> Dict[str, Any]:
    """
    Full pipeline: collect relevant transcripts for a mention market event.
    
    Args:
        event_title: The event title (e.g., "What will Bernie Sanders say during...")
        event_ticker: Optional Kalshi event ticker
        phrases: Optional list of phrases being tracked
        event_type: Optional manual override for event type (e.g., 'fight', 'mma', 'interview')
        max_videos: Maximum number of videos to collect
        output_dir: Optional output directory override
        process_with_llm: If True, extract just the speaker's dialogue
        model: LLM model for processing
        verbose: Print progress
        
    Returns:
        Dict with 'context', 'videos', 'transcripts', 'saved_files'
    """
    if verbose:
        print("=" * 60)
        print("Mention Market Transcript Collector")
        print("=" * 60)
        print(f"Event: {event_title[:70]}...")
        print()
    
    # Step 1: Parse event context
    if verbose:
        print("Step 1: Parsing event context...")
    
    context = parse_event_context(event_title, event_ticker, event_type=event_type)
    context.phrases = phrases or []
    
    if verbose:
        if context.participants:
            print(f"  Participants: {', '.join(context.participants)}")
        else:
            print(f"  Speaker: {context.speaker}")
        print(f"  Event Type: {context.event_type}")
        if context.show_name:
            print(f"  Show: {context.show_name}")
        print(f"  Search queries: {context.search_queries}")
        print()
    
    # Step 2: Search for relevant videos
    if verbose:
        print("Step 2: Searching for relevant videos...")
    
    all_videos = search_relevant_videos(context, verbose=verbose)
    
    # Step 3: Filter to most relevant
    if verbose:
        print()
        print("Step 3: Filtering to most relevant videos...")
    
    filtered_videos = filter_relevant_videos(all_videos, context, max_videos=max_videos)
    
    if verbose:
        print(f"  Kept {len(filtered_videos)} most relevant videos")
        for i, v in enumerate(filtered_videos[:5], 1):
            print(f"    {i}. {v.title[:50]}... ({v.duration_formatted})")
        if len(filtered_videos) > 5:
            print(f"    ... and {len(filtered_videos) - 5} more")
        print()
    
    # Step 4: Collect transcripts
    if verbose:
        print("Step 4: Collecting transcripts...")
    
    transcripts = collect_transcripts(filtered_videos, verbose=verbose)
    
    if verbose:
        total_words = sum(t['word_count'] for t in transcripts)
        print(f"  Total words collected: {total_words:,}")
        print()
    
    # Step 5: Save to files
    if verbose:
        print("Step 5: Saving transcripts...")
    
    # Determine output directory
    if output_dir is None:
        if context.participants:
            safe_name = '_'.join([re.sub(r'[^\w\s-]', '', p).replace(' ', '_').lower() for p in context.participants[:2]])
            output_dir = f"data/mentions/{context.event_type}/{safe_name}"
        else:
            safe_speaker = re.sub(r'[^\w\s-]', '', context.speaker).replace(' ', '_').lower()
            output_dir = f"data/mentions/{context.event_type}/{safe_speaker}"
    
    saved_files = save_transcripts(transcripts, context, output_dir)
    
    if verbose:
        print(f"  Saved {len(saved_files)} files to {output_dir}")
        print()
    
    # Step 6: Optional LLM processing
    if process_with_llm and saved_files:
        if verbose:
            print("Step 6: Extracting speaker dialogue with LLM...")
        
        from .transcript_processor import process_batch
        process_batch(output_dir, context.speaker, model=model, skip_existing=True)
        print()
    
    if verbose:
        print("=" * 60)
        print("Collection Complete!")
        print("=" * 60)
    
    return {
        'context': {
            'event_ticker': context.event_ticker,
            'title': context.title,
            'speaker': context.speaker,
            'event_type': context.event_type,
            'show_name': context.show_name,
            'phrases': context.phrases,
        },
        'videos_found': len(all_videos),
        'videos_filtered': len(filtered_videos),
        'transcripts': transcripts,
        'saved_files': saved_files,
        'output_dir': output_dir,
    }


def collect_for_market(event_ticker: str, verbose: bool = True) -> Dict[str, Any]:
    """
    Collect transcripts for a Kalshi market by ticker.
    
    Fetches event details from Kalshi API, then runs collection pipeline.
    """
    from .market_discovery import get_event_details
    
    event = get_event_details(event_ticker)
    if not event:
        raise ValueError(f"Event not found: {event_ticker}")
    
    return collect_for_event(
        event_title=event.title,
        event_ticker=event.event_ticker,
        phrases=event.phrases,
        verbose=verbose,
    )


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1:
        # Collect for specific event title
        title = " ".join(sys.argv[1:])
        result = collect_for_event(title)
    else:
        # Demo with Will Smith
        result = collect_for_event(
            "What will Will Smith say during The Tonight Show Starring Jimmy Fallon?",
            event_ticker="KXLATENIGHTMENTION-26JAN21",
            phrases=["Travel", "Slap", "Pole to Pole", "National Geographic", "Jada"],
        )
    
    print(f"\nResult: {len(result['transcripts'])} transcripts collected")
