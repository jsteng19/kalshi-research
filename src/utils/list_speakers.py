#!/usr/bin/env python3
"""
List all speakers in a diarized transcript with their occurrence counts.
Usage: python list_speakers.py <transcript_file>
"""

import sys
import re
from pathlib import Path
from collections import Counter


def list_speakers(transcript_path: str) -> dict:
    """
    Find all unique speakers in a diarized transcript.
    
    Args:
        transcript_path: Path to the transcript file
    
    Returns:
        Dictionary mapping speaker numbers to their occurrence count
    """
    with open(transcript_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Pattern to match speaker lines: "NUMBER (timestamp):"
    pattern = r'^(\d+) \([^)]+\):'
    
    # Find all speaker numbers
    speaker_numbers = re.findall(pattern, content, re.MULTILINE)
    
    # Convert to integers and count occurrences
    speaker_counts = Counter(int(num) for num in speaker_numbers)
    
    return dict(sorted(speaker_counts.items()))


def main():
    if len(sys.argv) != 2:
        print("Usage: python list_speakers.py <transcript_file>")
        print("\nExample:")
        print("  python list_speakers.py data/warren/raw-transcripts/transcript.txt")
        sys.exit(1)
    
    transcript_path = sys.argv[1]
    
    # Check if file exists
    if not Path(transcript_path).exists():
        print(f"Error: File not found: {transcript_path}")
        sys.exit(1)
    
    # List speakers
    try:
        speaker_counts = list_speakers(transcript_path)
        
        if not speaker_counts:
            print("No speakers found in transcript")
            sys.exit(1)
        
        print(f"Speakers found in {Path(transcript_path).name}:\n")
        print(f"{'Speaker':<10} {'# Segments':<15}")
        print("-" * 25)
        
        for speaker_num, count in speaker_counts.items():
            print(f"{speaker_num:<10} {count:<15}")
        
        print(f"\nTotal speakers: {len(speaker_counts)}")
        print(f"\nTo extract text for a specific speaker, use:")
        print(f"  python extract_speaker.py '{transcript_path}' <speaker_number>")
        
    except Exception as e:
        print(f"Error processing transcript: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

