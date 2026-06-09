#!/usr/bin/env python3
"""
Transcript Collector for Mention Markets

Orchestrates transcript collection from various sources based on speaker registry
and event type. Supports both automated strategies and manual agent mode.

Usage:
    from src.auto_collect.transcript_collector import TranscriptCollector
    
    collector = TranscriptCollector()
    
    # Collect for a specific event
    result = collector.collect_for_event(mention_event)
    
    # Or generate agent instructions for manual collection
    instructions = collector.generate_agent_instructions(mention_event)
"""

import os
import re
import yaml
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass

from .market_discovery import MentionEvent
from .strategies.base import CollectionStrategy, CollectionResult, get_strategy


@dataclass
class SpeakerMatch:
    """Result of matching an event to a speaker in the registry."""
    speaker_key: str
    speaker_config: Dict[str, Any]
    confidence: float
    match_reason: str


class TranscriptCollector:
    """
    Main orchestrator for transcript collection.
    
    Handles:
    1. Matching events to speakers in the registry
    2. Selecting appropriate collection strategy
    3. Running automated collection or generating agent instructions
    """
    
    def __init__(
        self,
        registry_path: Optional[str] = None,
        recipes_path: Optional[str] = None
    ):
        """
        Initialize the collector.
        
        Args:
            registry_path: Path to speakers.yaml
            recipes_path: Path to recipe templates directory
        """
        base_dir = Path(__file__).parent
        
        self.registry_path = registry_path or str(base_dir / "speakers.yaml")
        self.recipes_path = recipes_path or str(base_dir / "recipes")
        
        self.registry = self._load_registry()
        self._strategies: Dict[str, CollectionStrategy] = {}
    
    def _load_registry(self) -> Dict[str, Any]:
        """Load the speaker registry from YAML."""
        try:
            with open(self.registry_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            print(f"Warning: Registry not found at {self.registry_path}")
            return {'speakers': {}, 'event_types': {}, 'defaults': {}}
    
    def _get_strategy(self, strategy_name: str) -> CollectionStrategy:
        """Get or create a collection strategy."""
        if strategy_name not in self._strategies:
            self._strategies[strategy_name] = get_strategy(strategy_name)
        return self._strategies[strategy_name]
    
    def match_speaker(self, event: MentionEvent) -> Optional[SpeakerMatch]:
        """
        Match an event to a speaker in the registry.
        
        Args:
            event: MentionEvent to match
            
        Returns:
            SpeakerMatch if found, None otherwise
        """
        title_lower = event.title.lower()
        
        speakers = self.registry.get('speakers', {})
        
        for speaker_key, config in speakers.items():
            name = config.get('name', '').lower()
            aliases = [a.lower() for a in config.get('aliases', [])]
            
            # Check for name/alias match in title
            all_names = [name] + aliases
            
            for check_name in all_names:
                if check_name and check_name in title_lower:
                    return SpeakerMatch(
                        speaker_key=speaker_key,
                        speaker_config=config,
                        confidence=0.9,
                        match_reason=f"name_match:{check_name}"
                    )
        
        # Check for show matches (late night, etc.)
        for speaker_key, config in speakers.items():
            if config.get('type') == 'show':
                show_name = config.get('name', '').lower()
                aliases = [a.lower() for a in config.get('aliases', [])]
                
                for check_name in [show_name] + aliases:
                    if check_name and check_name in title_lower:
                        return SpeakerMatch(
                            speaker_key=speaker_key,
                            speaker_config=config,
                            confidence=0.85,
                            match_reason=f"show_match:{check_name}"
                        )
        
        return None
    
    def classify_event_type(self, event: MentionEvent) -> str:
        """
        Classify the type of event from its title.
        
        Returns:
            Event type string (press_briefing, interview, speech, late_night, etc.)
        """
        title_lower = event.title.lower()
        
        if 'press briefing' in title_lower or 'press conference' in title_lower:
            return 'press_briefing'
        elif 'interview' in title_lower:
            return 'interview'
        elif any(x in title_lower for x in ['late show', 'tonight show', 'kimmel', 'late night']):
            return 'late_night'
        elif 'speech' in title_lower or 'remarks' in title_lower or 'address' in title_lower:
            return 'speech'
        elif 'podcast' in title_lower:
            return 'podcast'
        elif 'hearing' in title_lower or 'testimony' in title_lower:
            return 'congressional_hearing'
        elif 'wef' in title_lower or 'davos' in title_lower or 'world economic' in title_lower:
            return 'wef_panel'
        else:
            return 'unknown'
    
    def extract_speaker_from_title(self, title: str) -> Optional[str]:
        """
        Try to extract speaker name from event title.
        
        Patterns:
        - "What will X say during..."
        - "What will X mention..."
        - "Will X say..."
        """
        patterns = [
            r'[Ww]hat\s+will\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+say',
            r'[Ww]ill\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:say|mention)',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:speech|remarks|interview|briefing)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, title)
            if match:
                name = match.group(1).strip()
                # Filter out common false positives
                if name.lower() not in ['the', 'what', 'will', 'who']:
                    return name
        
        return None
    
    def collect_for_event(
        self,
        event: MentionEvent,
        force_agent_mode: bool = False
    ) -> Tuple[Optional[CollectionResult], Optional[str]]:
        """
        Collect transcripts for an event.
        
        Args:
            event: MentionEvent to collect for
            force_agent_mode: If True, always return agent instructions
            
        Returns:
            Tuple of (CollectionResult or None, agent_instructions or None)
        """
        # Match speaker
        speaker_match = self.match_speaker(event)
        event_type = self.classify_event_type(event)
        
        if force_agent_mode or speaker_match is None:
            # Generate agent instructions
            instructions = self.generate_agent_instructions(event, speaker_match, event_type)
            return None, instructions
        
        # Try automated collection
        config = speaker_match.speaker_config
        strategy_name = config.get('strategy', 'youtube_search')
        
        try:
            strategy = self._get_strategy(strategy_name)
            result = strategy.collect(
                speaker_config=config,
                target_date=event.close_time,
            )
            
            if result.success:
                # Save transcripts
                data_dir = config.get('data_dir', f'data/mentions/{speaker_match.speaker_key}')
                saved = strategy.save_transcripts(result, data_dir)
                result.metadata['saved_files'] = saved
            
            return result, None
            
        except Exception as e:
            # Fall back to agent mode
            instructions = self.generate_agent_instructions(
                event, speaker_match, event_type,
                error_context=str(e)
            )
            return None, instructions
    
    def generate_agent_instructions(
        self,
        event: MentionEvent,
        speaker_match: Optional[SpeakerMatch] = None,
        event_type: Optional[str] = None,
        error_context: Optional[str] = None
    ) -> str:
        """
        Generate instructions for manual agent execution (Claude Code).
        
        Args:
            event: MentionEvent to collect for
            speaker_match: Optional matched speaker from registry
            event_type: Classified event type
            error_context: Optional error message from failed automation
            
        Returns:
            Markdown instructions for the agent
        """
        if event_type is None:
            event_type = self.classify_event_type(event)
        
        # Extract speaker name from title if not matched
        speaker_name = None
        if speaker_match:
            speaker_name = speaker_match.speaker_config.get('name')
        else:
            speaker_name = self.extract_speaker_from_title(event.title) or "Unknown Speaker"
        
        # Build search queries
        search_queries = self._build_search_queries(event, speaker_name, event_type)
        
        # Determine data directory
        if speaker_match:
            data_dir = speaker_match.speaker_config.get('data_dir', f'data/mentions/{speaker_match.speaker_key}')
        else:
            # Create a safe directory name
            safe_speaker = re.sub(r'[^\w\s-]', '', speaker_name).replace(' ', '_').lower()
            data_dir = f'data/mentions/new/{safe_speaker}'
        
        # Load recipe template
        recipe = self._load_recipe(event_type)
        
        # Build instructions
        instructions = f"""# Agent Task: Collect Transcripts for Mention Market

## Market Information
- **Event Ticker:** {event.event_ticker}
- **Title:** {event.title}
- **Speaker:** {speaker_name}
- **Event Type:** {event_type}
- **Phrases to Track:** {', '.join(event.phrases[:10])}{'...' if len(event.phrases) > 10 else ''}
- **Target Date:** {event.close_time.strftime('%Y-%m-%d') if event.close_time else 'Unknown'}

## Speaker Registry Status
{"✅ **Found in registry:** " + speaker_match.speaker_key if speaker_match else "❌ **Not in registry** - Consider adding after collection"}

{f"## Previous Error\n```\n{error_context}\n```\n" if error_context else ""}

## Your Task
Find and collect transcript(s) for the event described above.

### Step 1: Search for Video/Transcript Sources

Try these search queries on YouTube:
"""
        
        for i, query in enumerate(search_queries[:5], 1):
            search_url = f"https://www.youtube.com/results?search_query={query.replace(' ', '+')}"
            instructions += f"{i}. [{query}]({search_url})\n"
        
        instructions += f"""
Also check these sources:
- C-SPAN: https://www.c-span.org/search/?query={speaker_name.replace(' ', '+')}
- Google News: https://news.google.com/search?q={event.title.replace(' ', '+')}+transcript

### Step 2: Collect Transcript

**Option A: YouTube Video Found**
If you find the video on YouTube, use the transcript downloader:

```bash
cd /Users/jstenger/Documents/repos/kalshi-research
source venv/bin/activate

# Create a CSV with the video URL
echo "url" > /tmp/videos.csv
echo "VIDEO_URL_HERE" >> /tmp/videos.csv

# Download transcript
python src/youtube/youtube_transcript_downloader.py /tmp/videos.csv {data_dir}
```

**Option B: Let Firecrawl Agent Find It Automatically**
If you can't find the exact URL, let Firecrawl's AI agent search for it:

```python
from src.auto_collect.strategies.firecrawl import agent_find_transcript

result = agent_find_transcript(
    speaker="{speaker_name}",
    event_description="{event.title}",
    target_date="{date_str}",
    hint_urls=["youtube.com", "c-span.org"],
    max_credits=50,  # Cost control
    verbose=True,
)

if result and result.get('transcripts'):
    for t in result['transcripts']:
        print(f"Found: {{t.get('title')}}")
        print(f"Text: {{t.get('full_text', '')[:500]}}...")
```

**Option C: Web Transcript Found (Direct URL)**
If you find a transcript on a website, use Firecrawl to extract:

```python
from src.auto_collect.strategies.firecrawl import extract_transcript_from_page

result = extract_transcript_from_page("TRANSCRIPT_URL_HERE")
print(result)
```

### Step 3: Save Transcript

Save the transcript to: `{data_dir}/`

Filename format: `YYYY-MM-DD_title.txt`

Include header:
```
Source: [URL]
Title: [Event Title]
Date: [YYYY-MM-DD]
Speaker: {speaker_name}
================================================================================

[Transcript content]
```

### Step 4: Update Registry (if new speaker)
"""
        
        if not speaker_match:
            instructions += f"""
Since this speaker isn't in the registry, add them to `src/mentions/speakers.yaml`:

```yaml
  {safe_speaker}:
    name: "{speaker_name}"
    aliases: ["{speaker_name}"]
    data_dir: "{data_dir}"
    strategy: youtube_search  # or youtube_channel if you found their channel
    youtube_config:
      search_queries:
        - "{speaker_name} interview"
        - "{speaker_name} speech"
```
"""
        else:
            instructions += "\nSpeaker already in registry - no update needed.\n"
        
        instructions += """
### Step 5: Verify Collection

After saving, verify the transcript was collected:

```bash
ls -la {data_dir}/
head -50 {data_dir}/*.txt
```

## Notes
""".format(data_dir=data_dir)
        
        if recipe:
            instructions += f"\n{recipe}\n"
        
        return instructions
    
    def _build_search_queries(
        self,
        event: MentionEvent,
        speaker_name: str,
        event_type: str
    ) -> List[str]:
        """Build search queries for finding transcripts."""
        queries = []
        
        # Use event title directly
        queries.append(event.title)
        
        # Event type specific queries
        type_suffixes = {
            'press_briefing': ['press briefing full', 'press conference'],
            'interview': ['full interview', 'interview'],
            'late_night': ['full interview', 'appearance'],
            'speech': ['full speech', 'remarks', 'address'],
            'wef_panel': ['WEF', 'Davos', 'full conversation'],
        }
        
        suffixes = type_suffixes.get(event_type, [''])
        
        for suffix in suffixes:
            queries.append(f"{speaker_name} {suffix}".strip())
        
        # Add date if available
        if event.close_time:
            date_str = event.close_time.strftime('%B %Y')
            queries = [f"{q} {date_str}" for q in queries[:3]] + queries
        
        return queries[:10]  # Limit to 10 queries
    
    def _load_recipe(self, event_type: str) -> Optional[str]:
        """Load recipe template for event type."""
        recipe_file = Path(self.recipes_path) / f"{event_type}.md"
        
        if recipe_file.exists():
            with open(recipe_file, 'r') as f:
                return f.read()
        
        return None
    
    def get_collection_status(self, event: MentionEvent) -> Dict[str, Any]:
        """
        Check if data already exists for an event.
        
        Returns:
            Dictionary with status information
        """
        speaker_match = self.match_speaker(event)
        
        if speaker_match:
            data_dir = speaker_match.speaker_config.get('data_dir')
            if data_dir and os.path.exists(data_dir):
                files = list(Path(data_dir).glob('*.txt'))
                return {
                    'has_data': len(files) > 0,
                    'file_count': len(files),
                    'data_dir': data_dir,
                    'speaker_matched': True,
                    'speaker_key': speaker_match.speaker_key,
                }
        
        return {
            'has_data': False,
            'file_count': 0,
            'data_dir': None,
            'speaker_matched': speaker_match is not None,
            'speaker_key': speaker_match.speaker_key if speaker_match else None,
        }


# Convenience functions for CLI usage

def collect_for_ticker(event_ticker: str, agent_mode: bool = True) -> str:
    """
    Collect transcripts for an event by ticker.
    
    Args:
        event_ticker: Kalshi event ticker
        agent_mode: If True, return agent instructions
        
    Returns:
        Agent instructions or collection result summary
    """
    from .market_discovery import get_event_details
    
    event = get_event_details(event_ticker)
    if not event:
        return f"Event not found: {event_ticker}"
    
    collector = TranscriptCollector()
    result, instructions = collector.collect_for_event(event, force_agent_mode=agent_mode)
    
    if instructions:
        return instructions
    elif result:
        return f"Collected {result.transcript_count} transcripts. Saved to: {result.metadata.get('saved_files', [])}"
    else:
        return "Collection failed with no instructions generated"


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Collect transcripts for mention markets')
    parser.add_argument('event_ticker', help='Kalshi event ticker')
    parser.add_argument('--auto', action='store_true', help='Try automated collection first')
    parser.add_argument('--output', '-o', help='Output file for agent instructions')
    
    args = parser.parse_args()
    
    result = collect_for_ticker(args.event_ticker, agent_mode=not args.auto)
    
    if args.output:
        with open(args.output, 'w') as f:
            f.write(result)
        print(f"Instructions saved to: {args.output}")
    else:
        print(result)
