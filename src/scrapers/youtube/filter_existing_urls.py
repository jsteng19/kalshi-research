#!/usr/bin/env python3
"""
Script to filter out transcript URLs that have already been scraped.
Compares URLs against existing files in the raw-transcripts directory.
"""

import os
import re
from datetime import datetime
from urllib.parse import urlparse
import argparse


def get_existing_urls(data_dir):
    """
    Get set of URLs that have already been scraped by reading the Source: line from existing files
    Args:
        data_dir (str): Path to data directory
    Returns:
        set: Set of URLs that have already been scraped
    """
    existing_urls = set()
    raw_transcripts_dir = os.path.join(data_dir, 'raw-transcripts')
    
    if not os.path.exists(raw_transcripts_dir):
        return existing_urls
        
    for root, dirs, files in os.walk(raw_transcripts_dir):
        for file in files:
            if file.endswith('.txt'):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        # Read first few lines to find Source: line
                        for i, line in enumerate(f):
                            if i > 5:  # Source should be in first few lines
                                break
                            if line.startswith('Source: '):
                                url = line[8:].strip()  # Remove 'Source: ' prefix
                                existing_urls.add(url)
                                break
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")
                    continue
                
    return existing_urls

def filter_new_urls(url_file, data_dir="data"):
    """
    Filter URLs to only include those not already scraped
    Args:
        url_file (str): Path to file containing URLs
        data_dir (str): Path to data directory
    Returns:
        tuple: (new_urls, existing_urls)
    """
    # Read URLs
    with open(url_file, 'r') as f:
        urls = [line.strip() for line in f if line.strip()]
    
    print(f"Loaded {len(urls)} URLs from {url_file}")
    
    # Get existing URLs by reading Source: lines from files
    existing_urls_set = get_existing_urls(data_dir)
    print(f"Found {len(existing_urls_set)} existing scraped URLs")
    
    new_urls = []
    existing_urls = []
    
    for url in urls:
        if url in existing_urls_set:
            existing_urls.append(url)
        else:
            new_urls.append(url)
    
    return new_urls, existing_urls

def main():
    parser = argparse.ArgumentParser(description='Filter transcript URLs against existing files')
    parser.add_argument('url_file', help='Path to file containing URLs')
    parser.add_argument('--data-dir', default='data', help='Path to data directory (default: data)')
    parser.add_argument('--output', help='Output file for new URLs (default: print to stdout)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    # Filter URLs
    new_urls, existing_urls = filter_new_urls(args.url_file, args.data_dir)
    
    print(f"\nResults:")
    print(f"  New URLs to scrape: {len(new_urls)}")
    print(f"  Already scraped: {len(existing_urls)}")
    
    if args.verbose and existing_urls:
        print(f"\nSample of already scraped URLs:")
        for url in existing_urls[:5]:  # Show first 5
            print(f"  {url}")
        if len(existing_urls) > 5:
            print(f"  ... and {len(existing_urls) - 5} more")
    
    # Output new URLs
    if args.output:
        with open(args.output, 'w') as f:
            for url in new_urls:
                f.write(f"{url}\n")
        print(f"\nNew URLs saved to {args.output}")
    else:
        if new_urls:
            print(f"\nNew URLs to scrape:")
            for url in new_urls[:20]:  # Show first 20
                print(url)
            if len(new_urls) > 20:
                print(f"... and {len(new_urls) - 20} more")

if __name__ == "__main__":
    main()
