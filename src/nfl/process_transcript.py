#!/usr/bin/env python3
"""
Script to process transcripts by removing timestamps and line breaks.
"""

import re
import sys
import argparse
from pathlib import Path


def process_transcript(text):
    """
    Process transcript text by removing timestamps and line breaks.
    
    Args:
        text (str): Raw transcript text with timestamps and line breaks
        
    Returns:
        str: Cleaned transcript text
    """
    # Remove timestamps like [ct=104.439]
    text = re.sub(r'\[ct=\d+\.\d+\]\s*', '', text)
    
    # Remove line breaks and extra whitespace
    text = re.sub(r'\n+', ' ', text)
    
    # Clean up multiple spaces
    text = re.sub(r'\s+', ' ', text)
    
    # Strip leading/trailing whitespace
    text = text.strip()
    
    return text


def process_file(input_file, output_file):
    """Process a single transcript file."""
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            raw_text = f.read()
        
        cleaned_text = process_transcript(raw_text)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(cleaned_text)
        
        print(f"Processed: {input_file.name} -> {output_file.name}")
        return True
    except Exception as e:
        print(f"Error processing {input_file.name}: {e}")
        return False


def process_directory(input_dir, output_dir):
    """Process all .txt files in input directory and save to output directory."""
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    
    if not input_path.exists():
        print(f"Error: Input directory '{input_path}' not found.")
        return False
    
    # Create output directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Find all .txt files in input directory
    txt_files = list(input_path.glob("*.txt"))
    
    if not txt_files:
        print(f"No .txt files found in '{input_path}'")
        return False
    
    print(f"Found {len(txt_files)} .txt files to process...")
    
    processed_count = 0
    for txt_file in txt_files:
        output_file = output_path / txt_file.name
        if process_file(txt_file, output_file):
            processed_count += 1
    
    print(f"Successfully processed {processed_count}/{len(txt_files)} files")
    return processed_count > 0


def main():
    """Main function to process transcripts."""
    parser = argparse.ArgumentParser(
        description="Process transcripts by removing timestamps and line breaks"
    )
    parser.add_argument(
        "input", 
        help="Input file or directory containing .txt files"
    )
    parser.add_argument(
        "--out", 
        help="Output file or directory (required for directory input)"
    )
    
    args = parser.parse_args()
    
    input_path = Path(args.input)
    
    if input_path.is_file():
        # Single file processing
        if not args.out:
            print("Error: --out required when processing a single file")
            sys.exit(1)
        
        output_path = Path(args.out)
        if process_file(input_path, output_path):
            print(f"Processed transcript saved to '{output_path}'")
        else:
            sys.exit(1)
    
    elif input_path.is_dir():
        # Directory processing
        if not args.out:
            print("Error: --out required when processing a directory")
            sys.exit(1)
        
        if not process_directory(input_path, args.out):
            sys.exit(1)
    
    else:
        print(f"Error: '{input_path}' is not a valid file or directory")
        sys.exit(1)


if __name__ == "__main__":
    main()
