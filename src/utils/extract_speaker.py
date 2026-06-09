#!/usr/bin/env python3
"""
Extract text for specific speakers from diarized transcripts.

Usage:
    # List all speakers in a transcript
    python extract_speaker.py <transcript_file> --list
    
    # Extract speaker to stdout
    python extract_speaker.py <transcript_file> <speaker_id>
    
    # Extract speaker to file
    python extract_speaker.py <transcript_file> <speaker_id> -o <output_file>
"""

import sys
import re
from pathlib import Path
from collections import Counter


def list_speakers(transcript_path: str) -> dict:
    """Find all unique speakers in a diarized transcript."""
    with open(transcript_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Matches "Speaker SPEAKER_##: text"
    pattern = r'^Speaker (SPEAKER_\d+):'
    speaker_ids = re.findall(pattern, content, re.MULTILINE)
    speaker_counts = Counter(speaker_ids)
    
    return dict(sorted(speaker_counts.items()))


def extract_speaker_text(transcript_path: str, speaker_id: str) -> str:
    """Extract all text for a specific speaker from a diarized transcript."""
    with open(transcript_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Pattern to match speaker lines and capture text until next speaker
    # Matches "Speaker SPEAKER_00: text..." until next "Speaker SPEAKER_##:"
    pattern = rf'^Speaker {speaker_id}: (.*?)(?=^Speaker SPEAKER_\d+:|\Z)'
    matches = re.finditer(pattern, content, re.MULTILINE | re.DOTALL)
    
    speaker_segments = []
    for match in matches:
        text = match.group(1).strip()
        if text:
            speaker_segments.append(text)
    
    return '\n\n'.join(speaker_segments)


def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  # List all speakers")
        print("  python extract_speaker.py <transcript_file> --list")
        print("\n  # Extract speaker to stdout")
        print("  python extract_speaker.py <transcript_file> <speaker_id>")
        print("\n  # Extract speaker to file")
        print("  python extract_speaker.py <transcript_file> <speaker_id> -o <output_file>")
        sys.exit(1)
    
    transcript_path = sys.argv[1]
    
    # Check if file exists
    if not Path(transcript_path).exists():
        print(f"Error: File not found: {transcript_path}")
        sys.exit(1)
    
    # Handle --list mode
    if len(sys.argv) >= 3 and sys.argv[2] == '--list':
        try:
            speaker_counts = list_speakers(transcript_path)
            
            if not speaker_counts:
                print("No speakers found in transcript")
                sys.exit(1)
            
            print(f"Speakers found in {Path(transcript_path).name}:\n")
            print(f"{'Speaker':<15} {'# Segments':<15}")
            print("-" * 30)
            
            for speaker_id, count in speaker_counts.items():
                print(f"{speaker_id:<15} {count:<15}")
            
            print(f"\nTotal speakers: {len(speaker_counts)}")
            
        except Exception as e:
            print(f"Error processing transcript: {e}")
            sys.exit(1)
        
        sys.exit(0)
    
    # Handle extract mode
    if len(sys.argv) < 3:
        print("Error: Speaker ID required")
        print("Usage: python extract_speaker.py <transcript_file> <speaker_id> [-o output_file]")
        sys.exit(1)
    
    speaker_id = sys.argv[2]
    # Simple validation - if it's just an int, prepend SPEAKER_ if needed, 
    # or just trust the user input. Let's trust input but maybe auto-fix integers
    if re.match(r'^\d+$', speaker_id):
        speaker_id = f"SPEAKER_{int(speaker_id):02d}"
    
    # Check for output file option
    output_file = None
    if len(sys.argv) >= 5 and sys.argv[3] == '-o':
        output_file = sys.argv[4]
    else:
        # Auto-generate output path in processed-transcripts subdirectory
        transcript_path_obj = Path(transcript_path)
        output_dir = transcript_path_obj.parent.parent / "processed-transcripts"
        output_dir.mkdir(exist_ok=True)
        output_file = output_dir / transcript_path_obj.name
    
    # Extract speaker text
    try:
        speaker_text = extract_speaker_text(transcript_path, speaker_id)
        
        if not speaker_text:
            print(f"No text found for speaker {speaker_id}")
            print("\nTo see which speakers are in the transcript, run:")
            print(f"  python extract_speaker.py '{transcript_path}' --list")
            sys.exit(1)
        
        # Prepare output - just the text, no header, for cleaner analysis
        full_output = speaker_text
        
        if output_file:
            # Save to file
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(full_output)
            
            word_count = len(speaker_text.split())
            char_count = len(speaker_text)
            print(f"✓ Extracted speaker {speaker_id} to: {output_file}")
            print(f"  Words: {word_count:,}")
            print(f"  Characters: {char_count:,}")
        else:
            # Print to stdout
            print(full_output)
        
    except Exception as e:
        print(f"Error processing transcript: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
