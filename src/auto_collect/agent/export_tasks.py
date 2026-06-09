#!/usr/bin/env python3
"""
Export Agent Tasks for Mention Markets

Generates agent instruction files for each mention market event
that needs transcript collection.

Usage:
    python src/mentions/agent/export_tasks.py
    
    # Only export specific categories
    python src/mentions/agent/export_tasks.py --category late_night
    
    # Export single event
    python src/mentions/agent/export_tasks.py --event KXCOLBERTMENTION-26JAN21
"""

import os
import json
import argparse
from datetime import datetime
from pathlib import Path

# Add project root to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from src.auto_collect import (
    MentionMarketDiscovery,
    TranscriptCollector,
    get_event_details,
)
from src.auto_collect.market_discovery import discover_all_mention_events


def export_all_tasks(
    output_dir: str = "data/mentions/agent-tasks",
    categories: list = None,
    skip_existing: bool = True
):
    """
    Export agent task files for all open mention events.
    
    Args:
        output_dir: Directory to save task files
        categories: Filter to specific event categories
        skip_existing: Skip events that already have data
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    print("Discovering open mention events...")
    events_raw, by_series = discover_all_mention_events(delay_seconds=0.3, verbose=True)
    
    collector = TranscriptCollector()
    
    # Track stats
    exported = 0
    skipped_existing = 0
    skipped_category = 0
    
    print(f"\nProcessing {len(events_raw)} events...")
    
    for event_data in events_raw:
        event_ticker = event_data.get('event_ticker')
        title = event_data.get('title', '')
        
        # Get full event details
        event = get_event_details(event_ticker, delay_seconds=0.3)
        if not event:
            print(f"  ⚠ Could not get details for {event_ticker}")
            continue
        
        # Classify event type
        event_type = collector.classify_event_type(event)
        
        # Filter by category if specified
        if categories and event_type not in categories:
            skipped_category += 1
            continue
        
        # Check if we already have data
        if skip_existing:
            status = collector.get_collection_status(event)
            if status.get('has_data'):
                print(f"  ✓ {event_ticker} - already has {status['file_count']} files")
                skipped_existing += 1
                continue
        
        # Generate instructions
        result, instructions = collector.collect_for_event(event, force_agent_mode=True)
        
        if instructions:
            # Save to file
            safe_ticker = event_ticker.replace('/', '_')
            filename = f"{safe_ticker}.md"
            filepath = output_path / filename
            
            with open(filepath, 'w') as f:
                f.write(instructions)
            
            print(f"  📝 {event_ticker} -> {filename}")
            exported += 1
    
    print(f"\n=== Export Summary ===")
    print(f"Exported: {exported}")
    print(f"Skipped (existing data): {skipped_existing}")
    print(f"Skipped (category filter): {skipped_category}")
    print(f"Output directory: {output_path}")
    
    return exported


def export_single_task(event_ticker: str, output_file: str = None):
    """
    Export agent task for a single event.
    
    Args:
        event_ticker: Kalshi event ticker
        output_file: Optional output file path
    """
    print(f"Fetching event: {event_ticker}")
    event = get_event_details(event_ticker, delay_seconds=0.3)
    
    if not event:
        print(f"Event not found: {event_ticker}")
        return None
    
    collector = TranscriptCollector()
    result, instructions = collector.collect_for_event(event, force_agent_mode=True)
    
    if output_file:
        with open(output_file, 'w') as f:
            f.write(instructions)
        print(f"Saved to: {output_file}")
    else:
        print(instructions)
    
    return instructions


def create_summary_json(
    output_file: str = "data/mentions/events_summary.json",
    include_instructions: bool = False
):
    """
    Create a JSON summary of all mention events.
    
    Args:
        output_file: Output JSON file path
        include_instructions: Include full agent instructions in JSON
    """
    print("Discovering events...")
    events_raw, by_series = discover_all_mention_events(delay_seconds=0.3, verbose=True)
    
    collector = TranscriptCollector()
    
    events_data = []
    
    for event_data in events_raw:
        event_ticker = event_data.get('event_ticker')
        
        event = get_event_details(event_ticker, delay_seconds=0.3)
        if not event:
            continue
        
        event_type = collector.classify_event_type(event)
        match = collector.match_speaker(event)
        status = collector.get_collection_status(event)
        
        entry = {
            'event_ticker': event_ticker,
            'title': event.title,
            'event_type': event_type,
            'speaker_key': match.speaker_key if match else None,
            'speaker_name': match.speaker_config.get('name') if match else collector.extract_speaker_from_title(event.title),
            'has_data': status.get('has_data', False),
            'file_count': status.get('file_count', 0),
            'phrases': event.phrases[:10],
            'close_time': event.close_time.isoformat() if event.close_time else None,
        }
        
        if include_instructions:
            _, instructions = collector.collect_for_event(event, force_agent_mode=True)
            entry['agent_instructions'] = instructions
        
        events_data.append(entry)
    
    # Sort by has_data (False first), then by event type
    events_data.sort(key=lambda x: (x.get('has_data', False), x.get('event_type', '')))
    
    # Save JSON
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump({
            'generated_at': datetime.now().isoformat(),
            'total_events': len(events_data),
            'needs_collection': sum(1 for e in events_data if not e.get('has_data')),
            'events': events_data
        }, f, indent=2)
    
    print(f"\nSaved summary to: {output_path}")
    print(f"Total events: {len(events_data)}")
    print(f"Need collection: {sum(1 for e in events_data if not e.get('has_data'))}")
    
    return events_data


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Export agent tasks for mention markets')
    parser.add_argument('--event', '-e', help='Export single event by ticker')
    parser.add_argument('--category', '-c', action='append', help='Filter by event type (can specify multiple)')
    parser.add_argument('--output', '-o', help='Output file or directory')
    parser.add_argument('--summary', action='store_true', help='Create JSON summary only')
    parser.add_argument('--include-existing', action='store_true', help='Include events that already have data')
    
    args = parser.parse_args()
    
    if args.event:
        export_single_task(args.event, args.output)
    elif args.summary:
        create_summary_json(
            output_file=args.output or "data/mentions/events_summary.json"
        )
    else:
        export_all_tasks(
            output_dir=args.output or "data/mentions/agent-tasks",
            categories=args.category,
            skip_existing=not args.include_existing
        )
