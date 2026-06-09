#!/usr/bin/env python3
"""
Mention Market Transcript Collection CLI

File-based workflow for collecting transcripts:

    # Step 1: Discover videos and save to CSV
    python -m src.auto_collect discover "What will Will Smith say during The Tonight Show?"
    
    # Step 2: Edit the CSV to remove unwanted videos (optional)
    # Open the CSV, change 'selected' column to 'no' for videos you don't want
    
    # Step 3: Download transcripts for videos in CSV (saves to raw/ folder)
    python -m src.auto_collect collect data/mentions/will_smith_videos.csv
    
    # Step 4: Extract just the speaker's dialogue (saves to processed/ folder)
    python -m src.auto_collect process data/mentions/will_smith/raw/ --speaker "Will Smith"

Output structure:
    data/mentions/<speaker>/
        raw/                # Raw transcripts with YYYYMMDD prefix
            20240115_abc123_Interview_Title.txt
        processed/          # LLM-processed speaker dialogue
            20240115_abc123_Interview_Title.txt

Commands:
    discover  - Search YouTube and save found videos to CSV
    collect   - Download transcripts for videos in a CSV
    process   - Extract speaker dialogue from transcripts using LLM
    pipeline  - Run full pipeline (discover → collect → process)
"""

import argparse
import csv
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def _safe_speaker_key(name: str) -> str:
    return re.sub(r'[^\w\s-]', '', name).replace(' ', '_').lower()


def _read_context_file(context_file: str) -> Dict[str, str]:
    data: Dict[str, str] = {}
    if not os.path.exists(context_file):
        return data
    with open(context_file, 'r', encoding='utf-8') as f:
        for line in f:
            if ':' not in line:
                continue
            key, value = line.split(':', 1)
            data[key.strip()] = value.strip()
    return data


def _extract_event_ticker(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    match = re.search(r'([A-Za-z0-9]+-\d{2}[A-Za-z]{3}\d{2}[A-Za-z0-9]*)', value)
    return match.group(1).upper() if match else None


def _prompt_default(label: str, default: str, non_interactive: bool = False) -> str:
    if non_interactive:
        return default
    raw = input(f"{label} [{default}]: ").strip()
    return raw or default


def _prompt_bool(label: str, default: bool, non_interactive: bool = False) -> bool:
    if non_interactive:
        return default
    suffix = "Y/n" if default else "y/N"
    raw = input(f"{label} ({suffix}): ").strip().lower()
    if not raw:
        return default
    return raw in ('y', 'yes', '1', 'true')


def _prompt_int(label: str, default: int, min_value: int = 1, non_interactive: bool = False) -> int:
    if non_interactive:
        return default
    while True:
        raw = input(f"{label} [{default}]: ").strip()
        if not raw:
            return default
        try:
            value = int(raw)
            if value < min_value:
                print(f"   Enter a number >= {min_value}")
                continue
            return value
        except ValueError:
            print("   Enter a valid integer")


def _prompt_choice(
    label: str,
    default: str,
    options: List[str],
    non_interactive: bool = False,
) -> str:
    if non_interactive:
        return default
    print(f"{label}:")
    for idx, option in enumerate(options, 1):
        marker = " (default)" if option == default else ""
        print(f"   {idx}. {option}{marker}")
    raw = input(f"Select 1-{len(options)} or type value [{default}]: ").strip()
    if not raw:
        return default
    if raw.isdigit():
        index = int(raw)
        if 1 <= index <= len(options):
            return options[index - 1]
    return raw


def _select_market_interactive(
    events: List[Dict],
    limit: int = 60,
    non_interactive: bool = False,
) -> Optional[str]:
    if not events:
        return None
    capped = events[:limit]
    print()
    print(f"Open mention markets ({len(capped)} shown):")
    for idx, event in enumerate(capped, 1):
        ticker = event.get('event_ticker', '')
        title = event.get('title', '')
        print(f"  [{idx:>2}] {ticker} | {title[:110]}")

    if non_interactive:
        return capped[0].get('event_ticker')

    while True:
        raw = input("Select market # or paste ticker/URL: ").strip()
        ticker = _extract_event_ticker(raw)
        if ticker:
            return ticker
        if raw.isdigit():
            index = int(raw)
            if 1 <= index <= len(capped):
                return capped[index - 1].get('event_ticker')
        print("   Invalid selection. Try again.")


def cmd_discover(args):
    """Search for videos and save to CSV."""
    from .collector import parse_event_context, search_relevant_videos, filter_relevant_videos, deduplicate_videos
    
    print(f"🔍 Discovering videos for: {args.event[:60]}...")
    
    # Parse context
    event_type = getattr(args, 'event_type', None)
    venue = getattr(args, 'venue', None)
    context = parse_event_context(args.event, args.ticker or "", event_type=event_type, venue=venue)
    
    # Override speaker if explicitly provided
    if getattr(args, 'speaker', None):
        context.speaker = args.speaker
        print(f"   Speaker (override): {context.speaker}")
    elif context.participants:
        print(f"   Participants: {', '.join(context.participants)}")
    else:
        print(f"   Speaker: {context.speaker}")
    print(f"   Event Type: {context.event_type}")
    if getattr(args, 'venue', None):
        context.venue = args.venue.strip() or None
    if context.venue:
        print(f"   Venue: {context.venue}")
    
    # Get min duration
    min_duration = getattr(args, 'min_duration', 1) or 1  # Default 1 minute
    
    # Get min_transcripts - if set, use it as the floor for max_videos
    min_transcripts = getattr(args, 'min_transcripts', None)
    max_videos = args.max
    extended_search = getattr(args, 'extended', False)
    earliest_date = getattr(args, 'earliest_date', None)
    
    if min_transcripts:
        # Always use max of the two values
        max_videos = max(max_videos, min_transcripts)
        # Enable extended search for higher targets
        if min_transcripts > 15:
            extended_search = True
        print(f"   Min transcripts: {min_transcripts} (max_videos set to {max_videos})")
    
    if extended_search:
        print(f"   Extended search: enabled (more query variations)")
    
    if earliest_date:
        print(f"   Earliest date: {earliest_date}")
    
    # Search with min duration filter
    all_videos = search_relevant_videos(
        context, 
        verbose=True,
        min_duration_minutes=min_duration,
        extended_search=extended_search,
        min_videos_target=min_transcripts or 0,
    )
    print(f"   Found {len(all_videos)} total videos (min {min_duration}m)")
    
    # Filter with stricter speaker matching
    filtered = filter_relevant_videos(
        all_videos, 
        context, 
        max_videos=max_videos,
        min_duration_seconds=min_duration * 60,
        strict_speaker_match=getattr(args, 'strict', False),
        strict_venue_match=getattr(args, 'strict_venue', False),
        earliest_date=earliest_date,
    )
    print(f"   Kept {len(filtered)} relevant videos")

    # Content deduplication (remove same-event-different-channel duplicates)
    if not getattr(args, 'no_dedup', False) and len(filtered) > 1:
        filtered = deduplicate_videos(filtered, speaker=context.speaker or "", verbose=True)
        print(f"   After dedup: {len(filtered)} unique videos")

    # Determine output file - always in data/mentions/
    os.makedirs("data/mentions", exist_ok=True)
    
    if args.output:
        output_file = args.output
    else:
        # For fight events, use participants; otherwise use speaker
        if context.participants:
            safe_name = '_'.join([re.sub(r'[^\w\s-]', '', p).replace(' ', '_').lower() for p in context.participants[:2]])
            output_file = f"data/mentions/{safe_name}_videos.csv"
        else:
            safe_speaker = re.sub(r'[^\w\s-]', '', context.speaker).replace(' ', '_').lower()
            output_file = f"data/mentions/{safe_speaker}_videos.csv"
    
    # Write CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['index', 'selected', 'video_id', 'title', 'duration', 'channel', 'url', 'published'])
        
        for i, v in enumerate(filtered):
            writer.writerow([
                i,
                'yes',  # Selected by default
                v.video_id,
                v.title,
                v.duration_formatted,
                v.channel,
                v.url,
                v.published_text,
            ])
    
    print()
    print(f"✅ Saved {len(filtered)} videos to: {output_file}")
    print()
    print("📋 Next steps:")
    print(f"   1. Edit {output_file} - change 'selected' to 'no' for videos you don't want")
    print(f"   2. Run: python -m src.auto_collect collect {output_file}")
    
    # Also save context for later
    context_file = output_file.replace('.csv', '_context.txt')
    with open(context_file, 'w') as f:
        f.write(f"event: {args.event}\n")
        f.write(f"speaker: {context.speaker}\n")
        f.write(f"event_type: {context.event_type}\n")
        f.write(f"venue: {context.venue or ''}\n")
        f.write(f"show_name: {context.show_name or ''}\n")
        f.write(f"ticker: {args.ticker or ''}\n")
        if context.participants:
            f.write(f"participants: {', '.join(context.participants)}\n")
    
    return output_file


def parse_published_date(published_text: str) -> str:
    """Parse 'X years ago' style text to YYYYMMDD date prefix."""
    import re
    from datetime import datetime, timedelta
    
    if not published_text:
        return datetime.now().strftime('%Y%m%d')
    
    published_text = published_text.lower().strip()
    now = datetime.now()
    
    # Try to match relative time patterns
    patterns = [
        (r'(\d+)\s*year', 365),
        (r'(\d+)\s*month', 30),
        (r'(\d+)\s*week', 7),
        (r'(\d+)\s*day', 1),
        (r'(\d+)\s*hour', 0),
    ]
    
    for pattern, days_per_unit in patterns:
        match = re.search(pattern, published_text)
        if match:
            num = int(match.group(1))
            approx_date = now - timedelta(days=num * days_per_unit)
            return approx_date.strftime('%Y%m%d')
    
    # Default to today
    return now.strftime('%Y%m%d')


def cmd_collect(args):
    """Download transcripts for videos in CSV."""
    from .transcript_api import YouTubeTranscriptAPI
    
    # Read CSV
    if not os.path.exists(args.csv):
        print(f"❌ File not found: {args.csv}")
        return 1
    
    videos = []
    with open(args.csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Check if selected
            selected = row.get('selected', 'yes').lower().strip()
            if selected in ('yes', 'true', '1', 'x', '✓', '✅'):
                videos.append(row)
    
    if not videos:
        print("❌ No videos selected in CSV")
        return 1
    
    print(f"📥 Collecting transcripts for {len(videos)} videos...")
    
    # Determine base directory and speaker
    speaker = None
    context_file = args.csv.replace('.csv', '_context.txt')
    context_data = _read_context_file(context_file)
    speaker = context_data.get('speaker')
    
    if args.output:
        base_dir = args.output
    elif speaker:
        safe_speaker = re.sub(r'[^\w\s-]', '', speaker).replace(' ', '_').lower()
        base_dir = f"data/mentions/{safe_speaker}"
    else:
        base_dir = "data/mentions/transcripts"
    
    # Raw transcripts go in /raw/ subfolder
    output_dir = os.path.join(base_dir, 'raw')
    os.makedirs(output_dir, exist_ok=True)
    print(f"   Output: {output_dir}")
    
    # Save transcripts
    saved = 0
    failed = 0
    
    def generate_filename(video_data: dict) -> str:
        """Generate filename for a video based on date, channel, and title."""
        date_prefix = parse_published_date(video_data.get('published', ''))
        channel = video_data.get('channel', 'unknown')
        safe_channel = re.sub(r'[^\w\s-]', '', channel).replace(' ', '_').lower()
        safe_channel = re.sub(r'_+', '_', safe_channel)
        safe_channel = safe_channel.strip('_')
        if not safe_channel or safe_channel == 'unknown':
            safe_channel = 'unknown_channel'
        
        title = video_data.get('title', '')
        if title:
            safe_title = re.sub(r'[^\w\s-]', '', title).replace(' ', '_').lower()
            safe_title = re.sub(r'_+', '_', safe_title)
            safe_title = safe_title.strip('_')[:40]
            if safe_title:
                return f"{date_prefix}_{safe_channel}_{safe_title}.txt"
        return f"{date_prefix}_{safe_channel}.txt"
    
    def is_video_already_downloaded(video_id: str, directory: str) -> bool:
        """Check if a video is already downloaded by checking video ID in existing files."""
        if not os.path.exists(directory):
            return False
        
        existing_files = [f for f in os.listdir(directory) if f.endswith('.txt')]
        for existing_file in existing_files:
            existing_path = os.path.join(directory, existing_file)
            try:
                with open(existing_path, 'r', encoding='utf-8') as f:
                    first_lines = ''.join(f.readlines()[:10])
                    if f'Video ID: {video_id}' in first_lines:
                        return True
            except:
                continue
        return False
    
    def try_fallback_transcript(video_id: str) -> Optional[str]:
        """Try to get transcript using youtube-transcript-api library as fallback."""
        try:
            from youtube_transcript_api import YouTubeTranscriptApi
            transcript_list = YouTubeTranscriptApi.get_transcript(video_id)
            
            # Combine all transcript segments into full text
            full_text = ' '.join(item['text'] for item in transcript_list)
            return full_text
        except Exception as e:
            # Silently fail - fallback didn't work either
            return None
    
    # Check for existing files first to skip duplicates
    skipped = 0
    videos_to_process = []
    for v in videos:
        vid = v['video_id']
        if is_video_already_downloaded(vid, output_dir):
            skipped += 1
            title = v.get('title', vid)
            print(f"   ⏭️  {title[:50]}... (already downloaded)")
        else:
            videos_to_process.append(v)
    
    if skipped > 0:
        print(f"   Skipped {skipped} already downloaded video(s)")
    if not videos_to_process:
        print("   All videos already downloaded!")
        return 0
    
    print(f"   Processing {len(videos_to_process)} new video(s)...")
    
    # Get transcripts only for videos that need processing
    api = YouTubeTranscriptAPI()
    video_ids = [v['video_id'] for v in videos_to_process]
    
    print(f"   Fetching {len(video_ids)} transcripts...")
    results = api.get_transcripts(video_ids)
    
    for v in videos_to_process:
        vid = v['video_id']
        text = None
        
        # Try primary API first
        if vid in results and results[vid].get('text'):
            text = results[vid]['text']
        
        # Fallback to youtube-transcript-api if primary failed
        if not text:
            print(f"   ⚠️  Primary API failed for {vid}, trying fallback...")
            text = try_fallback_transcript(vid)
            if text:
                print(f"   ✅ Got transcript via fallback")
        
        if text:
            # Generate filename using helper function
            filename = generate_filename(v)
            filepath = os.path.join(output_dir, filename)
            
            # Parse date for file header
            date_prefix = parse_published_date(v.get('published', ''))
            
            # Handle duplicate filenames (same date + channel)
            if os.path.exists(filepath):
                counter = 1
                base_name = filename.replace('.txt', '')
                while os.path.exists(filepath):
                    filename = f"{base_name}_{counter}.txt"
                    filepath = os.path.join(output_dir, filename)
                    counter += 1
            
            # Write file
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(f"Video ID: {vid}\n")
                f.write(f"URL: {v.get('url', '')}\n")
                f.write(f"Title: {v.get('title', '')}\n")
                f.write(f"Channel: {v.get('channel', '')}\n")
                f.write(f"Duration: {v.get('duration', '')}\n")
                f.write(f"Published: {v.get('published', '')}\n")
                f.write(f"Date: {date_prefix}\n")
                f.write("=" * 80 + "\n\n")
                f.write(text)
            
            saved += 1
            title = v.get('title', '')
            channel = v.get('channel', 'unknown')
            title_display = title[:40] if title else filename
            print(f"   ✅ {date_prefix} {channel} - {title_display}")
        else:
            failed += 1
            title = v.get('title', vid)
            print(f"   ❌ {title[:50]}... (no transcript)")
    
    print()
    print(f"✅ Saved {saved} transcripts to: {output_dir}")
    if skipped > 0:
        print(f"⏭️  Skipped: {skipped} (already downloaded)")
    if failed:
        print(f"❌ Failed: {failed}")
    
    if saved > 0:
        print()
        print("📋 Next step (optional):")
        speaker_name = speaker or "SPEAKER_NAME"
        print(f"   python -m src.auto_collect process {output_dir} --speaker \"{speaker_name}\"")
    
    return 0


def cmd_process(args):
    """Extract speaker dialogue from transcripts using LLM."""
    from .transcript_processor import process_batch
    
    if not os.path.isdir(args.directory):
        print(f"❌ Directory not found: {args.directory}")
        return 1
    
    if not args.speaker:
        # Try to find speaker from context file in parent directories
        search_dirs = [args.directory, str(Path(args.directory).parent), str(Path(args.directory).parent.parent)]
        for search_dir in search_dirs:
            if not os.path.isdir(search_dir):
                continue
            for f in os.listdir(search_dir):
                if f.endswith('_context.txt'):
                    with open(os.path.join(search_dir, f)) as fp:
                        for line in fp:
                            if line.startswith('speaker:'):
                                args.speaker = line.split(':', 1)[1].strip()
                                break
                    if args.speaker:
                        break
            if args.speaker:
                break
    
    if not args.speaker:
        print("❌ Speaker name required. Use --speaker \"Name\"")
        return 1
    
    # Determine output directory: sibling /processed/ folder
    input_dir = Path(args.directory)
    if input_dir.name == 'raw':
        output_dir = str(input_dir.parent / 'processed')
    else:
        output_dir = str(input_dir.parent / 'processed')
    
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"🤖 Processing transcripts to extract {args.speaker}'s dialogue")
    print(f"   Input: {args.directory}")
    print(f"   Output: {output_dir}")
    print(f"   Model: {args.model}")
    
    results = process_batch(
        args.directory,
        args.speaker,
        output_dir=output_dir,
        model=args.model,
        skip_existing=not args.force,
        max_workers=getattr(args, 'workers', 5),
    )
    
    return 0 if results['processed'] > 0 or results['skipped'] > 0 else 1


def cmd_pipeline(args):
    """Run end-to-end market pipeline: select market -> discover -> collect -> process -> report."""
    from .collector import parse_event_context
    from .market_discovery import discover_all_mention_events, get_event_details
    from .report import generate_report

    print("=" * 60)
    print("🚀 Mention Market Pipeline")
    print("=" * 60)

    event_ticker = (
        _extract_event_ticker(getattr(args, 'ticker', None))
        or _extract_event_ticker(getattr(args, 'market_url', None))
        or _extract_event_ticker(getattr(args, 'event', None))
    )
    event_title = None

    if event_ticker:
        event = get_event_details(event_ticker, verbose=False)
        if not event:
            print(f"❌ Could not load event: {event_ticker}")
            return 1
        event_ticker = event.event_ticker
        event_title = event.title
    elif args.event:
        event_title = args.event
        event_ticker = getattr(args, 'ticker', None)
    else:
        print("🔍 Fetching open mention markets...")
        events, _ = discover_all_mention_events(delay_seconds=0.25, verbose=False)
        events = sorted(events, key=lambda e: (e.get('series_ticker', ''), e.get('event_ticker', '')))
        selected_ticker = _select_market_interactive(
            events,
            limit=getattr(args, 'market_limit', 60),
            non_interactive=getattr(args, 'non_interactive', False),
        )
        if not selected_ticker:
            print("❌ No markets found")
            return 1
        event = get_event_details(selected_ticker, verbose=False)
        if not event:
            print(f"❌ Could not load event: {selected_ticker}")
            return 1
        event_ticker = event.event_ticker
        event_title = event.title

    if not event_title:
        print("❌ Event title is required")
        return 1

    context = parse_event_context(
        event_title,
        event_ticker or "",
        event_type=getattr(args, 'event_type', None),
        venue=getattr(args, 'venue', None),
    )

    if getattr(args, 'speaker', None):
        context.speaker = args.speaker.strip()
    if getattr(args, 'event_type', None):
        context.event_type = args.event_type.strip()
    if getattr(args, 'venue', None):
        context.venue = args.venue.strip() or None

    max_videos = args.max if args.max is not None else 25
    min_duration = args.min_duration if args.min_duration is not None else 5
    min_transcripts = args.min_transcripts if args.min_transcripts is not None else 20
    strict_match = args.strict if args.strict is not None else True
    strict_venue_match = args.strict_venue if args.strict_venue is not None else False
    extended = args.extended if args.extended is not None else True
    process_with_llm = args.process if args.process is not None else True
    review_csv = args.review_csv if args.review_csv is not None else True
    open_report = args.open_report if args.open_report is not None else True
    non_interactive = getattr(args, 'non_interactive', False)

    print()
    print("Selected Market")
    print(f"  Ticker: {event_ticker or '-'}")
    print(f"  Title: {event_title}")

    default_speaker = context.speaker or "Unknown Speaker"
    speaker = _prompt_default("Speaker", default_speaker, non_interactive=non_interactive).strip()
    event_type_options = [
        "interview", "late_night", "press_briefing", "speech", "podcast",
        "congressional_hearing", "wef_panel", "fight", "mma", "boxing",
    ]
    if context.event_type and context.event_type not in event_type_options:
        event_type_options.insert(0, context.event_type)
    event_type = _prompt_choice(
        "Event type",
        context.event_type or "interview",
        event_type_options,
        non_interactive=non_interactive,
    ).strip()
    venue = _prompt_default("Venue (optional)", context.venue or "", non_interactive=non_interactive).strip() or None
    max_videos = _prompt_int("Max videos", max_videos, min_value=1, non_interactive=non_interactive)
    min_duration = _prompt_int("Min duration (minutes)", min_duration, min_value=1, non_interactive=non_interactive)
    min_transcripts = _prompt_int("Min transcripts target", min_transcripts, min_value=1, non_interactive=non_interactive)
    strict_match = _prompt_bool("Strict speaker match", strict_match, non_interactive=non_interactive)
    strict_venue_match = _prompt_bool("Strict venue match", strict_venue_match, non_interactive=non_interactive)
    extended = _prompt_bool("Extended search", extended, non_interactive=non_interactive)
    process_with_llm = _prompt_bool("Run LLM processing", process_with_llm, non_interactive=non_interactive)
    review_csv = _prompt_bool("Pause to review CSV selections", review_csv, non_interactive=non_interactive)
    open_report = _prompt_bool("Open report in browser", open_report, non_interactive=non_interactive)

    if not non_interactive:
        proceed = input("Press Enter to run pipeline (or type 'q' to cancel): ").strip().lower()
        if proceed.startswith('q'):
            print("Cancelled.")
            return 1

    print()
    print("Step 1: Discover Videos")
    print("-" * 40)
    discover_args = argparse.Namespace(
        event=event_title,
        output=args.output or None,
        ticker=event_ticker,
        max=max_videos,
        speaker=speaker,
        min_duration=min_duration,
        min_transcripts=min_transcripts,
        extended=extended,
        strict=strict_match,
        strict_venue=strict_venue_match,
        earliest_date=getattr(args, 'earliest_date', None),
        event_type=event_type,
        venue=venue,
        no_dedup=getattr(args, 'no_dedup', False),
    )
    csv_file = cmd_discover(discover_args)
    if not csv_file:
        return 1

    if review_csv and not non_interactive:
        print()
        input(f"📝 Edit {csv_file} (set selected=no to skip rows), then press Enter to continue...")

    print()
    print("Step 2: Collect Transcripts")
    print("-" * 40)
    collect_args = argparse.Namespace(csv=csv_file, output=None)
    collect_rc = cmd_collect(collect_args)
    if collect_rc != 0:
        return collect_rc

    context_data = _read_context_file(csv_file.replace('.csv', '_context.txt'))
    speaker = context_data.get('speaker', speaker)
    venue_for_report = (context_data.get('venue') or venue or "").strip() or None
    safe_speaker = _safe_speaker_key(speaker) if speaker else ""
    base_dir = f"data/mentions/{safe_speaker}" if safe_speaker else "data/mentions/transcripts"
    raw_dir = os.path.join(base_dir, 'raw')
    processed_dir = os.path.join(base_dir, 'processed')

    if process_with_llm and speaker and os.path.isdir(raw_dir):
        print()
        print("Step 3: Extract Speaker Dialogue")
        print("-" * 40)
        process_args = argparse.Namespace(
            directory=raw_dir,
            speaker=speaker,
            model=args.model,
            force=False,
            workers=getattr(args, 'workers', 5),
        )
        process_rc = cmd_process(process_args)
        if process_rc != 0:
            print("⚠️  Processing returned no outputs; continuing to report generation.")

    print()
    print("Step 4: Generate Report")
    print("-" * 40)
    report_directory = base_dir if os.path.isdir(base_dir) else raw_dir
    report_status, report_path = generate_report(
        directory=report_directory,
        event_ticker=event_ticker,
        phrases=None,
        venue=venue_for_report,
        file_filter=None,
        output_path=getattr(args, 'report_output', None),
        title=getattr(args, 'report_title', None),
        compare_dirs=None,
        open_browser=open_report,
    )
    if report_status != 0:
        return report_status

    if report_path:
        print(f"📄 Report: {report_path}")

    print()
    print("=" * 60)
    print("✅ Pipeline Complete!")
    print("=" * 60)
    return 0


def cmd_markets(args):
    """List open mention markets from Kalshi."""
    from .market_discovery import discover_all_mention_events
    
    print("🔍 Fetching open mention markets from Kalshi...")
    
    events, _ = discover_all_mention_events(delay_seconds=0.3, verbose=False)
    
    # Filter out earnings/sports if requested
    if args.filter:
        filters = args.filter.lower().split(',')
        filtered = []
        for e in events:
            title_lower = e.get('title', '').lower()
            series_lower = e.get('series_ticker', '').lower()
            
            skip = False
            for f in filters:
                if f in title_lower or f in series_lower:
                    skip = True
                    break
            
            if not skip:
                filtered.append(e)
        events = filtered
    
    print(f"Found {len(events)} mention markets")
    print()
    
    # Output as CSV if requested
    if args.output:
        with open(args.output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['ticker', 'title', 'series'])
            for e in events:
                writer.writerow([
                    e.get('event_ticker', ''),
                    e.get('title', ''),
                    e.get('series_ticker', ''),
                ])
        print(f"✅ Saved to: {args.output}")
    else:
        # Print to console
        for e in events[:args.limit]:
            print(f"  {e.get('event_ticker', '')}")
            print(f"    {e.get('title', '')}")
            print()


def main():
    parser = argparse.ArgumentParser(
        prog='python -m src.auto_collect',
        description='Mention Market Transcript Collection CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Discover videos for an event
  python -m src.auto_collect discover "What will Will Smith say during The Tonight Show?"
  
  # Discover with explicit speaker (prevents matching wrong person)
  python -m src.auto_collect discover "What will Ben Shapiro say during Katie Pavlich Tonight?" -s "Ben Shapiro"
  
  # Discover with minimum duration filter (videos >= 5 minutes)
  python -m src.auto_collect discover "What will Ben Shapiro say..." --min-duration 5
  
  # Strict matching (requires exact full name in title)
  python -m src.auto_collect discover "..." -s "Ben Shapiro" --strict --min-duration 10
  
  # Ensure at least 20 videos are collected (enables extended search automatically)
  python -m src.auto_collect discover "..." -s "Tim Walz" --strict --min-transcripts 20
  
  # Extended search - tries many query variations (CNN, Fox, press conference, podcast, etc.)
  python -m src.auto_collect discover "What will Tim Walz say?" -s "Tim Walz" --extended --min-duration 5
  
  # Discover fight videos (auto-detects fight event)
  python -m src.auto_collect discover "What will announcers say during Gaethje vs Pimblett fight?"
  
  # Manually specify event type
  python -m src.auto_collect discover "What will announcers say..." --event-type fight
  
  # Discover with custom output file
  python -m src.auto_collect discover "What will Bernie Sanders say..." -o data/mentions/bernie.csv
  
  # Collect transcripts from CSV
  python -m src.auto_collect collect data/mentions/will_smith_videos.csv
  
  # Collect to specific directory
  python -m src.auto_collect collect data/mentions/will_smith_videos.csv -o data/mentions/will_smith/
  
  # Extract speaker dialogue with LLM
  python -m src.auto_collect process data/mentions/will_smith/ --speaker "Will Smith"
  
  # Full pipeline (discover → collect → process)
  python -m src.auto_collect pipeline "What will Will Smith say..." --process
  
  # List open mention markets
  python -m src.auto_collect markets
  python -m src.auto_collect markets --filter earnings,nfl,nba
  
  # Generate analysis report (separate command)
  python -m src.auto_collect.report data/mentions/will_smith/ --phrases "Oscar,slap,Chris Rock"
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # discover command
    p_discover = subparsers.add_parser('discover', help='Search for videos and save to CSV')
    p_discover.add_argument('event', help='Event title (e.g., "What will Will Smith say...")')
    p_discover.add_argument('-o', '--output', help='Output CSV file')
    p_discover.add_argument('-t', '--ticker', help='Kalshi event ticker')
    p_discover.add_argument('-m', '--max', type=int, default=20, help='Max videos (default: 20)')
    p_discover.add_argument('-s', '--speaker', help='Explicit speaker name (overrides auto-parsing)')
    p_discover.add_argument('--venue', help='Optional venue/show hint (e.g., "MS NOW")')
    p_discover.add_argument('--min-duration', type=int, default=1, help='Minimum video duration in minutes (default: 1)')
    p_discover.add_argument('--min-transcripts', type=int, default=None, help='Minimum videos to collect (increases max if needed)')
    p_discover.add_argument('--extended', action='store_true', help='Extended search with many more query variations')
    p_discover.add_argument('--strict', action='store_true', help='Strict speaker name matching (require full name)')
    p_discover.add_argument('--strict-venue', action='store_true', help='Require venue match in title/channel metadata')
    p_discover.add_argument('--earliest-date', help='Earliest publish date to include (e.g., "2023-01-01" or "2023")')
    p_discover.add_argument('--event-type', help='Manual event type override (e.g., fight, mma, interview, late_night)')
    p_discover.add_argument('--no-dedup', action='store_true', help='Skip content deduplication (keep all videos including same-event duplicates)')
    
    # collect command
    p_collect = subparsers.add_parser('collect', help='Download transcripts for videos in CSV')
    p_collect.add_argument('csv', help='CSV file with video list')
    p_collect.add_argument('-o', '--output', help='Output directory')
    
    # process command
    p_process = subparsers.add_parser('process', help='Extract speaker dialogue using LLM')
    p_process.add_argument('directory', help='Directory with transcript files')
    p_process.add_argument('-s', '--speaker', help='Speaker name to extract')
    p_process.add_argument('-m', '--model', default='gemini-2.5-flash', help='LLM model (default: gemini-2.5-flash)')
    p_process.add_argument('-f', '--force', action='store_true', help='Reprocess existing files')
    p_process.add_argument('-w', '--workers', type=int, default=5, help='Parallel workers (default: 5)')
    
    # pipeline command
    p_pipeline = subparsers.add_parser('pipeline', help='Run full pipeline')
    p_pipeline.add_argument('event', nargs='?', help='Event title, ticker, or Kalshi URL (optional; prompts selection when omitted)')
    p_pipeline.add_argument('-o', '--output', help='Output CSV file')
    p_pipeline.add_argument('-t', '--ticker', help='Kalshi event ticker')
    p_pipeline.add_argument('--market-url', help='Kalshi market URL (alternative to --ticker)')
    p_pipeline.add_argument('-m', '--max', type=int, default=None, help='Max videos (default: 25)')
    p_pipeline.add_argument('-s', '--speaker', help='Explicit speaker name (overrides auto-parsing)')
    p_pipeline.add_argument('--venue', help='Optional venue/show hint')
    p_pipeline.add_argument('--min-duration', type=int, default=None, help='Minimum video duration in minutes (default: 5)')
    p_pipeline.add_argument('--min-transcripts', type=int, default=None, help='Minimum videos to collect (default: 20)')
    p_pipeline.add_argument('--extended', action=argparse.BooleanOptionalAction, default=None, help='Extended search with many more query variations')
    p_pipeline.add_argument('--strict', action=argparse.BooleanOptionalAction, default=None, help='Strict speaker name matching')
    p_pipeline.add_argument('--strict-venue', action=argparse.BooleanOptionalAction, default=None, help='Strict venue matching')
    p_pipeline.add_argument('--earliest-date', help='Earliest publish date to include (e.g., "2023-01-01" or "2023")')
    p_pipeline.add_argument('--event-type', help='Manual event type override (e.g., fight, mma, interview, late_night)')
    p_pipeline.add_argument('--process', action=argparse.BooleanOptionalAction, default=None, help='Run LLM processing step')
    p_pipeline.add_argument('--model', default='gemini-2.5-flash', help='LLM model for processing')
    p_pipeline.add_argument('-w', '--workers', type=int, default=5, help='Parallel workers (default: 5)')
    p_pipeline.add_argument('--review-csv', action=argparse.BooleanOptionalAction, default=None, help='Pause to edit CSV selections before collect')
    p_pipeline.add_argument('--open-report', action=argparse.BooleanOptionalAction, default=None, help='Open generated report in browser')
    p_pipeline.add_argument('--report-output', help='Output path for HTML report')
    p_pipeline.add_argument('--report-title', help='Custom report title')
    p_pipeline.add_argument('--market-limit', type=int, default=60, help='How many open markets to list in interactive picker')
    p_pipeline.add_argument('--non-interactive', action='store_true', help='Skip prompts and accept defaults/CLI overrides')
    p_pipeline.add_argument('--no-dedup', action='store_true', help='Skip content deduplication')
    
    # markets command
    p_markets = subparsers.add_parser('markets', help='List open mention markets')
    p_markets.add_argument('-o', '--output', help='Output CSV file')
    p_markets.add_argument('-f', '--filter', help='Filter out keywords (comma-separated)')
    p_markets.add_argument('-l', '--limit', type=int, default=50, help='Max markets to show')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    commands = {
        'discover': cmd_discover,
        'collect': cmd_collect,
        'process': cmd_process,
        'pipeline': cmd_pipeline,
        'markets': cmd_markets,
    }
    
    return commands[args.command](args)


if __name__ == '__main__':
    sys.exit(main() or 0)
