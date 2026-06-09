"""
Base classes for transcript collection strategies.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path


@dataclass
class CollectionResult:
    """Result from a transcript collection attempt."""
    success: bool
    transcripts: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Tracking
    source_type: str = ""  # youtube, web, factbase, etc.
    collection_time: datetime = field(default_factory=datetime.now)
    
    @property
    def transcript_count(self) -> int:
        return len(self.transcripts)
    
    def add_transcript(
        self,
        text: str,
        date: Optional[datetime] = None,
        title: str = "",
        source_url: str = "",
        speaker: str = "",
        **extra_metadata
    ):
        """Add a transcript to the results."""
        self.transcripts.append({
            'text': text,
            'date': date,
            'title': title,
            'source_url': source_url,
            'speaker': speaker,
            'metadata': extra_metadata,
        })
    
    def add_error(self, error: str):
        """Add an error message."""
        self.errors.append(error)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'success': self.success,
            'transcript_count': self.transcript_count,
            'source_type': self.source_type,
            'collection_time': self.collection_time.isoformat(),
            'errors': self.errors,
            'metadata': self.metadata,
            'transcripts': [
                {
                    **t,
                    'date': t['date'].isoformat() if t.get('date') else None
                }
                for t in self.transcripts
            ]
        }


class CollectionStrategy(ABC):
    """
    Abstract base class for transcript collection strategies.
    
    Each strategy knows how to collect transcripts from a specific source type
    (YouTube, web scraping, APIs, etc.)
    """
    
    name: str = "base"
    
    @abstractmethod
    def collect(
        self,
        speaker_config: Dict[str, Any],
        target_date: Optional[datetime] = None,
        **kwargs
    ) -> CollectionResult:
        """
        Collect transcripts for a speaker.
        
        Args:
            speaker_config: Configuration from speakers.yaml
            target_date: Optional specific date to target
            **kwargs: Additional strategy-specific arguments
            
        Returns:
            CollectionResult with transcripts and metadata
        """
        pass
    
    def save_transcripts(
        self,
        result: CollectionResult,
        output_dir: str,
        skip_existing: bool = True
    ) -> List[str]:
        """
        Save collected transcripts to files.
        
        Args:
            result: CollectionResult to save
            output_dir: Directory to save transcripts
            skip_existing: Skip files that already exist
            
        Returns:
            List of saved file paths
        """
        saved_files = []
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        for transcript in result.transcripts:
            # Generate filename
            date = transcript.get('date')
            title = transcript.get('title', 'untitled')
            
            # Clean title for filename
            clean_title = "".join(c if c.isalnum() or c in ' -_' else '' for c in title)
            clean_title = clean_title.replace(' ', '_')[:100]
            
            if date:
                filename = f"{date.strftime('%Y-%m-%d')}_{clean_title}.txt"
            else:
                filename = f"{clean_title}.txt"
            
            filepath = output_path / filename
            
            if skip_existing and filepath.exists():
                continue
            
            # Write transcript
            with open(filepath, 'w', encoding='utf-8') as f:
                # Write metadata header
                f.write(f"Source: {transcript.get('source_url', 'N/A')}\n")
                f.write(f"Title: {transcript.get('title', 'N/A')}\n")
                f.write(f"Date: {date.strftime('%Y-%m-%d') if date else 'N/A'}\n")
                f.write(f"Speaker: {transcript.get('speaker', 'N/A')}\n")
                f.write("=" * 80 + "\n\n")
                
                # Write transcript text
                f.write(transcript.get('text', ''))
            
            saved_files.append(str(filepath))
        
        return saved_files


def get_strategy(strategy_name: str) -> CollectionStrategy:
    """
    Get a collection strategy by name.
    
    Args:
        strategy_name: Name of strategy (youtube_channel, youtube_search, firecrawl, firecrawl_agent)
        
    Returns:
        CollectionStrategy instance
    """
    from .youtube import YouTubeChannelStrategy, YouTubeSearchStrategy
    from .firecrawl import FirecrawlStrategy
    
    strategies = {
        'youtube_channel': YouTubeChannelStrategy(),
        'youtube_search': YouTubeSearchStrategy(),
        'web_scrape': FirecrawlStrategy(),
        'firecrawl': FirecrawlStrategy(),
        'firecrawl_agent': FirecrawlStrategy(),  # Same class, different method used
    }
    
    if strategy_name not in strategies:
        raise ValueError(f"Unknown strategy: {strategy_name}. Available: {list(strategies.keys())}")
    
    return strategies[strategy_name]
