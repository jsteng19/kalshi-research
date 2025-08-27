#!/usr/bin/env python3
"""
Preprocess YouTube auto-transcription files by removing line breaks.

This script handles the line break issues in YouTube auto-transcriptions where
phrases are often split across multiple lines. It simply removes all line breaks
and replaces them with spaces.
"""

import re
import argparse
from pathlib import Path


def preprocess_transcript(text):
    """
    Simple preprocessing for YouTube auto-transcription.
    
    YouTube auto-transcription has no meaningful line breaks - they're just
    for display formatting. Replace all line breaks with spaces.
    
    Args:
        text (str): Raw transcript text
        
    Returns:
        str: Cleaned text with line breaks replaced by spaces
    """
    # Replace all line breaks with spaces
    text = re.sub(r'\n', ' ', text)
    
    # Clean up multiple spaces
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()


def process_file(file_path, save_dir=None):
    """
    Process a single transcript file.
    
    Args:
        file_path (str): Path to the transcript file
        save_dir (str): Directory to save preprocessed file to
        
    Returns:
        dict: Processing results
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            raw_text = f.read()
        
        preprocessed_text = preprocess_transcript(raw_text)
        
        results = {
            'file': file_path,
            'raw_length': len(raw_text),
            'processed_length': len(preprocessed_text),
            'preprocessed_text': preprocessed_text
        }
        
        # Save preprocessed file if directory provided
        if save_dir:
            save_dir = Path(save_dir)
            save_dir.mkdir(exist_ok=True)
            original_path = Path(file_path)
            save_path = save_dir / f"{original_path.name}"
            
            with open(save_path, 'w', encoding='utf-8') as f:
                f.write(preprocessed_text)
            results['saved_to'] = str(save_path)
        
        return results
        
    except Exception as e:
        return {'file': file_path, 'error': str(e)}


def process_directory(dir_path, pattern="*.txt", save_dir=None):
    """
    Process all files matching pattern in a directory.
    
    Args:
        dir_path (str): Directory path
        pattern (str): File pattern to match
        save_dir (str): Directory to save preprocessed files to
        
    Returns:
        list: List of processing results for each file
    """
    results = []
    dir_path = Path(dir_path)
    
    for file_path in dir_path.glob(pattern):
        if file_path.is_file():
            result = process_file(str(file_path), save_dir)
            results.append(result)
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Preprocess YouTube auto-transcription files by removing line breaks"
    )
    parser.add_argument(
        'path',
        help="File or directory path to process"
    )
    parser.add_argument(
        '--save-to',
        help="Directory to save preprocessed files to"
    )
    parser.add_argument(
        '--pattern',
        default="*.txt",
        help="File pattern to match when processing directory (default: *.txt)"
    )
    parser.add_argument(
        '--preview',
        action='store_true',
        help="Show preview of preprocessed text (first 500 characters)"
    )
    
    args = parser.parse_args()
    
    path = Path(args.path)
    
    if path.is_file():
        # Process single file
        result = process_file(str(path), args.save_to)
        
        if 'error' in result:
            print(f"Error processing {path}: {result['error']}")
            return
        
        print(f"File: {result['file']}")
        print(f"Original length: {result['raw_length']} chars")
        print(f"Processed length: {result['processed_length']} chars")
        
        if 'saved_to' in result:
            print(f"Preprocessed file saved to: {result['saved_to']}")
        
        if args.preview:
            print(f"\nPreprocessed text preview (first 500 chars):")
            print(result['preprocessed_text'][:500] + "...")
    
    elif path.is_dir():
        # Process directory
        results = process_directory(str(path), args.pattern, args.save_to)
        
        successful_results = [r for r in results if 'error' not in r]
        error_results = [r for r in results if 'error' in r]
        
        print(f"Successfully processed {len(successful_results)} files from {path}")
        
        if error_results:
            print(f"Failed to process {len(error_results)} files:")
            for result in error_results:
                print(f"  {result['file']}: {result['error']}")
        
        if successful_results:
            total_raw_chars = sum(r['raw_length'] for r in successful_results)
            total_processed_chars = sum(r['processed_length'] for r in successful_results)
            print(f"\nTotal characters: {total_raw_chars} -> {total_processed_chars}")
            
            if args.save_to:
                print(f"Preprocessed files saved to: {args.save_to}")
    
    else:
        print(f"Error: {path} is not a valid file or directory")


if __name__ == "__main__":
    main() 