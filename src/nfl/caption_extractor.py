#!/usr/bin/env python3
"""
NFL Caption Extractor - Batch Processing Version

Extracts clean text from NFL.com HLS caption streams.
Usage: python nfl_caption_extractor.py <directory_with_m3u8_files>
"""

import re
import requests
import sys
import os
import glob
from typing import List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


class VTTSegment:
    """Represents a single WebVTT segment with text content."""
    
    def __init__(self, text: str):
        self.text = text.strip()


class NFLCaptionExtractor:
    """Extracts clean text captions from NFL.com HLS streams."""
    
    def __init__(self):
        self.session = requests.Session()
        # Set headers to mimic a browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/vtt,text/plain,*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.nfl.com/',
        })
    
    def parse_m3u8_file(self, m3u8_path: str) -> Tuple[List[str], str]:
        """Parse M3U8 file and extract VTT URLs, TS URLs, or analyze master playlist. Returns (urls, type)."""
        with open(m3u8_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        vtt_urls = []
        ts_urls = []
        subtitle_media_urls = []
        
        # Check if this is a master playlist
        if '#EXT-X-STREAM-INF' in content or '#EXT-X-MEDIA' in content:
            # This is a master playlist - look for subtitle references
            for line in content.strip().split('\n'):
                line = line.strip()
                # Look for subtitle media declarations
                if line.startswith('#EXT-X-MEDIA:') and 'TYPE=SUBTITLES' in line:
                    # Extract URI from the media line
                    uri_match = re.search(r'URI="([^"]+)"', line)
                    if uri_match:
                        subtitle_media_urls.append(uri_match.group(1))
            
            if subtitle_media_urls:
                return subtitle_media_urls, 'master_subs'
            else:
                return [], 'master_no_subs'
        
        # Regular playlist - look for segment URLs
        for line in content.strip().split('\n'):
            line = line.strip()
            if '.vtt' in line and line.startswith('http'):
                vtt_urls.append(line)
            elif '.ts' in line and line.startswith('http'):
                ts_urls.append(line)
        
        # Return VTT URLs if available (subtitle stream), otherwise TS URLs (video stream)
        if vtt_urls:
            return vtt_urls, 'vtt'
        elif ts_urls:
            return ts_urls, 'ts'
        else:
            return [], 'unknown'
    
    def extract_date_from_urls(self, vtt_urls: List[str]) -> Optional[str]:
        """Extract date from VTT URLs in format /1/YY/YY/MM/DD/ and convert to YYYY-MM-DD."""
        if not vtt_urls:
            return None
        
        # Look for pattern like /1/73/25/09/23/ in the URL
        # The pattern seems to be /1/XX/YY/MM/DD/ where YY/MM/DD is the date
        for url in vtt_urls[:5]:  # Check first few URLs
            match = re.search(r'/1/\d{2}/(\d{2})/(\d{2})/(\d{2})/', url)
            if match:
                yy, mm, dd = match.groups()
                # Convert YY to YYYY (assuming 20XX for years 00-99)
                yyyy = f"20{yy}"
                return f"{yyyy}-{mm}-{dd}"
        
        return None
    
    def download_vtt_segment(self, index_url_tuple: Tuple[int, str]) -> Tuple[int, str]:
        """Download a single VTT segment and return (index, content)."""
        index, vtt_url = index_url_tuple
        try:
            response = self.session.get(vtt_url, timeout=15)
            response.raise_for_status()
            return (index, response.text)
        except requests.RequestException:
            return (index, "")
    
    def download_ts_segment(self, index_url_tuple: Tuple[int, str]) -> Tuple[int, str]:
        """Download a TS segment and try to extract embedded captions."""
        index, ts_url = index_url_tuple
        try:
            response = self.session.get(ts_url, timeout=15)
            response.raise_for_status()
            
            # TS segments are binary video data, not text captions
            # We can't extract meaningful text from them directly
            # Return empty string to indicate no captions found
            return (index, "")
        except requests.RequestException:
            return (index, "")
    
    def parse_vtt_content(self, vtt_content: str) -> List[VTTSegment]:
        """Parse WebVTT content and extract just the text."""
        segments = []
        if not vtt_content.strip():
            return segments
        
        # Remove WebVTT header if present
        content = re.sub(r'^WEBVTT.*?\n\n', '', vtt_content, flags=re.MULTILINE | re.DOTALL)
        
        # Split into blocks (separated by double newlines)
        blocks = re.split(r'\n\s*\n', content.strip())
        
        for block in blocks:
            if not block.strip():
                continue
            
            lines = block.strip().split('\n')
            if len(lines) < 2:
                continue
            
            # Look for timing line (contains -->)
            text_lines = []
            for i, line in enumerate(lines):
                if '-->' in line:
                    text_lines = lines[i+1:]
                    break
            
            if text_lines:
                text = '\n'.join(text_lines).strip()
                if text:  # Only add non-empty segments
                    segments.append(VTTSegment(text))
        
        return segments
    
    def extract_captions(self, m3u8_path: str, output_file: str):
        """Extract all captions and save as clean text."""
        print(f"📄 Processing: {os.path.basename(m3u8_path)}")
        
        segment_urls, segment_type = self.parse_m3u8_file(m3u8_path)
        
        if segment_type == 'vtt':
            print(f"   Found {len(segment_urls)} VTT caption segments")
        elif segment_type == 'ts':
            print(f"   Found {len(segment_urls)} TS video segments (checking for embedded captions)")
        elif segment_type == 'master_subs':
            print(f"   📋 Master playlist with {len(segment_urls)} subtitle stream references!")
            print(f"   🔄 Subtitle streams found - you may need to download these separately")
            # For now, just report the subtitle URLs found
            for i, sub_url in enumerate(segment_urls):
                print(f"      📝 Subtitle stream {i+1}: {sub_url}")
            return
        elif segment_type == 'master_no_subs':
            print(f"   📋 Master playlist detected - no subtitle streams referenced")
            print(f"   💡 Captions may be embedded in video streams or available via separate API")
            return
        else:
            print(f"   ❌ No recognizable segments found in M3U8 file")
            return
        
        # Download all segments concurrently but maintain order
        segment_results = {}  # index -> list of text lines
        completed = 0
        
        # Use ThreadPoolExecutor for concurrent downloads
        with ThreadPoolExecutor(max_workers=10) as executor:  # Reduced workers for better order control
            # Submit all download tasks with index
            indexed_urls = [(i, url) for i, url in enumerate(segment_urls)]
            
            # Choose appropriate download method based on segment type
            if segment_type == 'vtt':
                future_to_index = {executor.submit(self.download_vtt_segment, indexed_url): indexed_url[0] 
                                  for indexed_url in indexed_urls}
            else:  # segment_type == 'ts'
                future_to_index = {executor.submit(self.download_ts_segment, indexed_url): indexed_url[0] 
                                  for indexed_url in indexed_urls}
            
            # Process completed downloads
            for future in as_completed(future_to_index):
                completed += 1
                if completed % 200 == 0:  # Less frequent updates for batch processing
                    print(f"   Downloaded {completed}/{len(segment_urls)} segments...")
                
                try:
                    index, segment_content = future.result()
                    if not segment_content:
                        segment_results[index] = []
                        continue
                    
                    # Only parse VTT content for VTT segments
                    if segment_type == 'vtt':
                        segments = self.parse_vtt_content(segment_content)
                        
                        # Store text lines for this segment index
                        text_lines = []
                        for segment in segments:
                            if segment.text.strip():
                                text_lines.append(segment.text.strip())
                        
                        segment_results[index] = text_lines
                    else:
                        # TS segments don't contain extractable captions
                        segment_results[index] = []
                            
                except Exception as e:
                    print(f"   Error processing segment: {e}")
                    segment_results[future_to_index[future]] = []
        
        # Reconstruct text in correct order
        all_text_lines = []
        for i in range(len(segment_urls)):
            if i in segment_results:
                all_text_lines.extend(segment_results[i])
        
        # Join all text and clean up
        final_text = '\n'.join(all_text_lines)
        
        # Clean up extra whitespace and empty lines
        lines = [line.strip() for line in final_text.split('\n') if line.strip()]
        final_text = '\n'.join(lines)
        
        # Save to file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(final_text)
        
        print(f"   ✅ Extracted {len(lines)} text lines → {output_file}")
    
    def process_directory(self, directory: str):
        """Process all M3U8 files in a directory."""
        # Find all .m3u8 files in the directory
        m3u8_pattern = os.path.join(directory, "*.m3u8")
        m3u8_files = glob.glob(m3u8_pattern)
        
        if not m3u8_files:
            print(f"❌ No .m3u8 files found in {directory}")
            return
        
        print(f"🎯 Found {len(m3u8_files)} M3U8 files in {directory}")
        print("=" * 60)
        
        for i, m3u8_file in enumerate(m3u8_files, 1):
            print(f"\n[{i}/{len(m3u8_files)}] Processing {os.path.basename(m3u8_file)}")
            
            try:
                # Use M3U8 filename for output text file naming
                base_name = os.path.splitext(os.path.basename(m3u8_file))[0]
                
                # Parse the M3U8 file to get segment URLs
                segment_urls, segment_type = self.parse_m3u8_file(m3u8_file)
                
                # Warn if this is a TS file (likely no captions)
                if segment_type == 'ts':
                    print(f"   ⚠️  This appears to be a video stream (TS segments), not a caption stream")
                    print(f"   ⚠️  Captions are unlikely to be extracted from video segments")
                elif segment_type == 'unknown':
                    print(f"   ❌ No recognizable segments found - skipping")
                    continue
                
                # Check if filename is in new format (yyyy-mm-dd_team1-at-team2)
                if re.match(r'\d{4}-\d{2}-\d{2}_', base_name):
                    # New format: use filename directly
                    output_filename = f"{base_name}.txt"
                else:
                    # Legacy format: try to extract date from URLs
                    date_str = self.extract_date_from_urls(segment_urls)
                    
                    if date_str:
                        output_filename = f"{date_str}_captions.txt"
                    else:
                        output_filename = f"{base_name}_captions.txt"
                
                output_path = os.path.join([directory, "captions" output_filename])
                
                # Skip if output file already exists
                if os.path.exists(output_path):
                    print(f"   ⏭️  Skipping - {output_filename} already exists")
                    continue
                
                # Extract captions
                self.extract_captions(m3u8_file, output_path)
                
            except Exception as e:
                print(f"   ❌ Error processing {m3u8_file}: {e}")
                continue
        
        print(f"\n🎉 Batch processing complete!")
        print(f"📁 Check {directory} for caption files")


def main():
    if len(sys.argv) != 2:
        print("Usage: python nfl_caption_extractor.py <directory_with_m3u8_files>")
        print("Example: python nfl_caption_extractor.py data-football/")
        sys.exit(1)
    
    directory = sys.argv[1]
    
    if not os.path.isdir(directory):
        print(f"❌ Error: {directory} is not a valid directory")
        sys.exit(1)
    
    extractor = NFLCaptionExtractor()
    
    try:
        extractor.process_directory(directory)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()