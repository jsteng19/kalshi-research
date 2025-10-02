#!/usr/bin/env python3
"""
Amazon Prime Video Caption Extractor with Deduplication

Extracts captions from Amazon Prime Video using authenticated URLs and removes duplicates.
Usage: python amazon_caption_extractor_with_dedup.py <subtitle_playlist_url> [output_file] [max_segments]
"""

import re
import requests
import sys
import os
from typing import List, Tuple, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed


class AmazonCaptionExtractorWithDedup:
    """Amazon Prime Video caption extractor with deduplication."""
    
    def __init__(self):
        self.session = requests.Session()
        # Set headers for Amazon Prime Video
        self.session.headers.update({
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Priority': 'u=3, i',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15',
            'Referer': 'https://www.amazon.com/',
        })
    
    def set_session_id(self, session_id: str):
        """Set the X-Playback-Session-Id header."""
        self.session.headers['X-Playback-Session-Id'] = session_id
    
    def download_subtitle_playlist(self, playlist_url: str) -> List[str]:
        """Download the subtitle playlist and extract VTT URLs."""
        print(f"📥 Downloading subtitle playlist...")
        
        try:
            response = self.session.get(playlist_url, timeout=15)
            response.raise_for_status()
            playlist_content = response.text
            
            # Parse the playlist to extract VTT URLs
            vtt_urls = []
            for line in playlist_content.strip().split('\n'):
                line = line.strip()
                if '.vtt' in line and not line.startswith('#'):
                    # Construct full URL for VTT segment
                    base_url = playlist_url.rsplit('/', 1)[0]
                    full_vtt_url = f'{base_url}/{line}'
                    vtt_urls.append(full_vtt_url)
            
            print(f"📝 Found {len(vtt_urls)} VTT segments")
            return vtt_urls
            
        except requests.RequestException as e:
            print(f"❌ Error downloading subtitle playlist: {e}")
            return []
    
    def download_vtt_segment(self, index_url_tuple: Tuple[int, str]) -> Tuple[int, str]:
        """Download a single VTT segment and return (index, content)."""
        index, vtt_url = index_url_tuple
        try:
            response = self.session.get(vtt_url, timeout=10)
            response.raise_for_status()
            return (index, response.text)
        except requests.RequestException as e:
            return (index, "")
    
    def parse_vtt_content(self, vtt_content: str) -> List[str]:
        """Parse WebVTT content and extract text lines."""
        if not vtt_content.strip():
            return []
        
        # Remove WebVTT header if present
        content = re.sub(r'^WEBVTT.*?\n\n', '', vtt_content, flags=re.MULTILINE | re.DOTALL)
        
        # Split into blocks (separated by double newlines)
        blocks = re.split(r'\n\s*\n', content.strip())
        
        text_lines = []
        for block in blocks:
            if not block.strip():
                continue
            
            lines = block.strip().split('\n')
            if len(lines) < 2:
                continue
            
            # Look for timing line (contains -->)
            for i, line in enumerate(lines):
                if '-->' in line:
                    # Get text lines after timing
                    text_lines.extend(lines[i+1:])
                    break
        
        return [line.strip() for line in text_lines if line.strip()]
    
    def deduplicate_captions(self, captions: List[str]) -> List[str]:
        """Remove duplicate captions while preserving order."""
        seen = set()
        unique_captions = []
        
        for caption in captions:
            # Normalize caption for comparison (lowercase, strip whitespace)
            normalized = caption.lower().strip()
            if normalized and normalized not in seen:
                seen.add(normalized)
                unique_captions.append(caption)
        
        return unique_captions
    
    def extract_captions_from_playlist(self, vtt_urls: List[str], output_file: str, max_segments: int = None):
        """Extract captions from VTT URLs with deduplication."""
        if max_segments:
            vtt_urls = vtt_urls[:max_segments]
            print(f"📄 Processing {len(vtt_urls)} VTT segments (limited to {max_segments})")
        else:
            print(f"📄 Processing {len(vtt_urls)} VTT segments")
        
        # Download all segments concurrently
        all_captions = []
        completed = 0
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            indexed_urls = [(i, url) for i, url in enumerate(vtt_urls)]
            future_to_index = {executor.submit(self.download_vtt_segment, indexed_url): indexed_url[0] 
                              for indexed_url in indexed_urls}
            
            for future in as_completed(future_to_index):
                completed += 1
                if completed % 100 == 0:
                    print(f"   Downloaded {completed}/{len(vtt_urls)} segments...")
                
                try:
                    index, segment_content = future.result()
                    if segment_content:
                        captions = self.parse_vtt_content(segment_content)
                        all_captions.extend(captions)
                            
                except Exception as e:
                    print(f"   Error processing segment: {e}")
        
        print(f"📊 Before deduplication: {len(all_captions)} lines")
        
        # Deduplicate captions
        unique_captions = self.deduplicate_captions(all_captions)
        
        print(f"📊 After deduplication: {len(unique_captions)} lines")
        print(f"📊 Removed {len(all_captions) - len(unique_captions)} duplicate lines")
        
        # Save to file
        final_text = '\n'.join(unique_captions)
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(final_text)
        
        print(f"   ✅ Extracted {len(unique_captions)} unique text lines → {output_file}")
    
    def extract_captions(self, playlist_url: str, output_file: str = None, session_id: str = None, max_segments: int = 500):
        """Main method to extract captions with deduplication."""
        print("🎯 Amazon Prime Video Caption Extractor with Deduplication")
        print("=" * 60)
        
        # Set session ID if provided
        if session_id:
            self.set_session_id(session_id)
            print(f"🔑 Using session ID: {session_id}")
        
        # Download subtitle playlist
        vtt_urls = self.download_subtitle_playlist(playlist_url)
        if not vtt_urls:
            print("❌ No VTT segments found")
            return False
        
        # Generate output filename
        if not output_file:
            output_file = "amazon_captions_deduped.txt"
        
        # Extract captions with deduplication
        self.extract_captions_from_playlist(vtt_urls, output_file, max_segments)
        
        print("🎉 Caption extraction completed successfully!")
        return True


def main():
    if len(sys.argv) < 2:
        print("Usage: python amazon_caption_extractor_with_dedup.py <subtitle_playlist_url> [output_file] [max_segments] [session_id]")
        print("")
        print("Examples:")
        print("  python amazon_caption_extractor_with_dedup.py 'https://abewj5raaaaaaaambvweo5c2id722.otte.live.cf.ww.aiv-cdn.net/.../fp_91_0-55c51e.m3u8'")
        print("  python amazon_caption_extractor_with_dedup.py 'URL' 'captions.txt' 500 '2F99BE6D-F541-4A58-A0CB-2B3C5C11B40D'")
        sys.exit(1)
    
    playlist_url = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    max_segments = int(sys.argv[3]) if len(sys.argv) > 3 else 500
    session_id = sys.argv[4] if len(sys.argv) > 4 else None
    
    extractor = AmazonCaptionExtractorWithDedup()
    
    try:
        success = extractor.extract_captions(playlist_url, output_file, session_id, max_segments)
        if not success:
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n⚠️  Extraction interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
