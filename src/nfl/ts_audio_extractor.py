#!/usr/bin/env python3
"""
Fast TS Audio Extractor

Optimized version of TS Audio Extractor with performance improvements:
- Parallel segment downloads
- Direct FFmpeg streaming (no intermediate files)
- Optimized FFmpeg parameters
- Enhanced retry logic with exponential backoff
- Intelligent error classification (retryable vs non-retryable)
- Second-pass retry mechanism for failed segments
- Configurable retry parameters and detailed statistics
- Better error handling and progress tracking
"""

import os
import sys
import subprocess
import requests
from typing import List, Dict, Optional, Tuple
import re
import tempfile
import argparse
from pathlib import Path
import concurrent.futures
import threading
from urllib.parse import urlparse
import glob
import time
import random
from requests.exceptions import RequestException, Timeout, ConnectionError, HTTPError


class FastTSAudioExtractor:
    """Fast audio extractor with parallel downloads and optimized processing."""
    
    def __init__(self, max_workers: int = 8, segments_per_worker: int = 10, 
                 max_retries: int = 5, backoff_factor: float = 1.0, 
                 base_timeout: int = 15, enable_second_pass_retry: bool = True):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        self.max_workers = max_workers
        self.segments_per_worker = segments_per_worker
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.base_timeout = base_timeout
        self.enable_second_pass_retry = enable_second_pass_retry
        self.download_lock = threading.Lock()
        self.downloaded_count = 0
        self.retry_count = 0
        self.failed_segments = []
    
    def _is_retryable_error(self, error: Exception) -> bool:
        """Determine if an error is retryable."""
        # Network-related errors that are worth retrying
        retryable_errors = (
            ConnectionError,
            Timeout,
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ContentDecodingError,
        )
        
        if isinstance(error, retryable_errors):
            return True
        
        # HTTP errors - retry on server errors (5xx) and some client errors
        if isinstance(error, HTTPError):
            status_code = error.response.status_code if error.response else 0
            # Retry on server errors (5xx) and rate limiting (429)
            if status_code >= 500 or status_code == 429:
                return True
            # Don't retry on client errors (4xx) except 429
            if 400 <= status_code < 500:
                return False
        
        # Generic request exceptions might be retryable
        if isinstance(error, RequestException):
            return True
        
        # File I/O errors and other exceptions are generally not retryable
        return False

    def download_ts_segment_fast(self, url: str, output_path: str, segment_index: int = None) -> Dict:
        """Fast download with enhanced retry logic and exponential backoff."""
        for attempt in range(self.max_retries):
            try:
                # Calculate timeout with some jitter for this attempt
                timeout = self.base_timeout + (attempt * 5) + random.uniform(0, 2)
                
                response = self.session.get(url, timeout=timeout, stream=True)
                response.raise_for_status()
                
                # Download and write file
                with open(output_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                # Verify file was written successfully
                if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
                    raise Exception("Downloaded file is empty or missing")
                
                with self.download_lock:
                    self.downloaded_count += 1
                    if self.downloaded_count % 10 == 0:
                        print(f"   📥 Downloaded {self.downloaded_count} segments...")
                
                return {'success': True, 'attempts': attempt + 1}
                
            except Exception as e:
                is_retryable = self._is_retryable_error(e)
                
                # Clean up partial file on failure
                if os.path.exists(output_path):
                    try:
                        os.remove(output_path)
                    except:
                        pass
                
                if attempt == self.max_retries - 1 or not is_retryable:
                    error_type = "non-retryable" if not is_retryable else "max retries exceeded"
                    with self.download_lock:
                        self.failed_segments.append({
                            'url': url, 
                            'segment_index': segment_index,
                            'error': str(e),
                            'error_type': error_type,
                            'attempts': attempt + 1
                        })
                    return {
                        'success': False, 
                        'error': str(e), 
                        'error_type': error_type,
                        'attempts': attempt + 1,
                        'retryable': is_retryable
                    }
                
                # Exponential backoff with jitter
                if is_retryable and attempt < self.max_retries - 1:
                    backoff_time = (self.backoff_factor * (2 ** attempt)) + random.uniform(0, 1)
                    with self.download_lock:
                        self.retry_count += 1
                    time.sleep(backoff_time)
                    continue
        
        return {'success': False, 'error': 'Unexpected error', 'attempts': self.max_retries}
    
    def download_segments_parallel(self, segments: List[Dict], temp_dir: str) -> List[str]:
        """Download segments in parallel while preserving chronological order."""
        print(f"📥 Downloading {len(segments)} segments in parallel...")
        print(f"   Workers: {self.max_workers}, Max retries per segment: {self.max_retries}")
        
        # Reset counters for this batch
        self.downloaded_count = 0
        self.retry_count = 0
        self.failed_segments = []
        
        # Pre-allocate segment files list to maintain order
        segment_files = [None] * len(segments)
        first_pass_failures = []
        
        def download_single_segment(segment_data):
            segment_index, segment = segment_data
            segment_path = os.path.join(temp_dir, f"segment_{segment_index:04d}.ts")
            
            result = self.download_ts_segment_fast(segment['url'], segment_path, segment_index)
            
            if result['success']:
                return (segment_index, segment_path, result)
            else:
                return (segment_index, None, result)
        
        # Create list of (index, segment) tuples for parallel processing
        segment_tasks = [(i, segment) for i, segment in enumerate(segments)]
        
        print(f"   📦 Processing {len(segment_tasks)} segments across {self.max_workers} workers")
        
        # First pass: parallel downloads
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all segment download tasks
            future_to_index = {
                executor.submit(download_single_segment, task): task[0]
                for task in segment_tasks
            }
            
            # Collect results as they complete, maintaining order
            for future in concurrent.futures.as_completed(future_to_index):
                segment_index, segment_path, result = future.result()
                
                if segment_path:
                    segment_files[segment_index] = segment_path
                else:
                    # Only retry if the error was retryable
                    if result.get('retryable', False):
                        first_pass_failures.append((segment_index, segments[segment_index]))
        
        # Second pass: retry failed segments sequentially if enabled
        if first_pass_failures and self.enable_second_pass_retry:
            print(f"   🔄 Second pass: retrying {len(first_pass_failures)} failed segments...")
            
            for segment_index, segment in first_pass_failures:
                segment_path = os.path.join(temp_dir, f"segment_{segment_index:04d}.ts")
                print(f"   🔄 Retrying segment {segment_index}...")
                
                result = self.download_ts_segment_fast(segment['url'], segment_path, segment_index)
                
                if result['success']:
                    segment_files[segment_index] = segment_path
                    print(f"   ✅ Segment {segment_index} recovered on second pass")
        
        # Remove None entries (failed downloads) and maintain chronological order
        successful_segment_files = [path for path in segment_files if path is not None]
        
        # Report results
        total_failed = len([f for f in segment_files if f is None])
        retryable_failed = len([f for f in self.failed_segments if f.get('error_type') != 'non-retryable'])
        non_retryable_failed = total_failed - retryable_failed
        
        if total_failed > 0:
            print(f"   ⚠️  Failed to download {total_failed} segments:")
            print(f"      - Retryable failures: {retryable_failed}")
            print(f"      - Non-retryable failures: {non_retryable_failed}")
            if self.retry_count > 0:
                print(f"      - Total retry attempts: {self.retry_count}")
        
        print(f"✅ Downloaded {len(successful_segment_files)}/{len(segments)} segments successfully")
        
        # Show detailed retry statistics if requested
        if hasattr(self, '_show_retry_stats') and self._show_retry_stats:
            self._print_retry_statistics()
        
        return successful_segment_files
    
    def _print_retry_statistics(self):
        """Print detailed retry statistics."""
        if not self.failed_segments and self.retry_count == 0:
            print("   📊 No retries needed - all segments downloaded successfully on first attempt!")
            return
        
        print(f"\n📊 RETRY STATISTICS:")
        print(f"   Total retry attempts: {self.retry_count}")
        
        if self.failed_segments:
            print(f"   Failed segments: {len(self.failed_segments)}")
            
            # Group failures by error type
            error_types = {}
            for failure in self.failed_segments:
                error_type = failure.get('error_type', 'unknown')
                if error_type not in error_types:
                    error_types[error_type] = []
                error_types[error_type].append(failure)
            
            for error_type, failures in error_types.items():
                print(f"   - {error_type}: {len(failures)} segments")
                
                # Show sample errors for each type
                if len(failures) <= 3:
                    for failure in failures:
                        print(f"     • Segment {failure.get('segment_index', '?')}: {failure.get('error', 'Unknown error')}")
                else:
                    for failure in failures[:2]:
                        print(f"     • Segment {failure.get('segment_index', '?')}: {failure.get('error', 'Unknown error')}")
                    print(f"     • ... and {len(failures) - 2} more")
        
        # Calculate success rate
        total_attempts = self.downloaded_count + len(self.failed_segments)
        if total_attempts > 0:
            success_rate = (self.downloaded_count / total_attempts) * 100
            print(f"   Success rate: {success_rate:.1f}% ({self.downloaded_count}/{total_attempts})")
        
        if self.retry_count > 0:
            avg_retries = self.retry_count / max(1, self.downloaded_count + len(self.failed_segments))
            print(f"   Average retries per segment: {avg_retries:.1f}")
    
    
    def extract_audio_parallel_download(self, segments: List[Dict], output_path: str,
                                      audio_format: str = 'wav', 
                                      sample_rate: int = 44100,
                                      channels: int = 2) -> Dict:
        """Extract audio using parallel downloads + concat (faster for small files)."""
        print("🎵 Extracting audio using parallel download method...")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Download segments in parallel
            segment_files = self.download_segments_parallel(segments, temp_dir)
            
            if not segment_files:
                return {'success': False, 'error': 'No segments downloaded successfully'}
            
            # Create concatenation file for FFmpeg
            concat_file = os.path.join(temp_dir, 'concat.txt')
            with open(concat_file, 'w') as f:
                for segment_file in segment_files:
                    f.write(f"file '{segment_file}'\n")
            
            # Extract audio from concatenated segments
            cmd = [
                'ffmpeg',
                '-f', 'concat',
                '-safe', '0',
                '-i', concat_file,
                '-vn',  # No video
                '-acodec', 'pcm_s16le' if audio_format == 'wav' else 'libmp3lame',
                '-ar', str(sample_rate),
                '-ac', str(channels),
                '-threads', '0',  # Use all available CPU cores
                '-y',  # Overwrite output
                output_path
            ]
            
            # Add quality optimizations
            if audio_format == 'mp3':
                cmd.extend(['-b:a', '128k'])
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0 and os.path.exists(output_path):
                file_size = os.path.getsize(output_path)
                print(f"✅ Audio extracted successfully!")
                print(f"   Output: {output_path}")
                print(f"   Size: {file_size / (1024*1024):.1f} MB")
                
                return {
                    'success': True,
                    'output_file': output_path,
                    'file_size': file_size,
                    'segments_processed': len(segment_files)
                }
            else:
                print(f"❌ Audio extraction failed: {result.stderr}")
                return {
                    'success': False,
                    'error': result.stderr,
                    'segments_processed': len(segment_files)
                }
    
    # Include the same parsing methods from the original extractor
    def parse_m3u8_streams(self, m3u8_path: str) -> List[Dict]:
        """Parse M3U8 file to extract stream information."""
        streams = []
        
        with open(m3u8_path, 'r') as f:
            content = f.read()
        
        lines = content.strip().split('\n')
        current_stream = {}
        
        for line in lines:
            line = line.strip()
            
            if line.startswith('#EXT-X-STREAM-INF:'):
                # Parse stream info
                current_stream = {}
                # Extract bandwidth, resolution, codecs
                bandwidth_match = re.search(r'BANDWIDTH=(\d+)', line)
                if bandwidth_match:
                    current_stream['bandwidth'] = int(bandwidth_match.group(1))
                
                resolution_match = re.search(r'RESOLUTION=(\d+x\d+)', line)
                if resolution_match:
                    current_stream['resolution'] = resolution_match.group(1)
                
                codecs_match = re.search(r'CODECS="([^"]+)"', line)
                if codecs_match:
                    current_stream['codecs'] = codecs_match.group(1)
                    
            elif line.startswith('http') and current_stream:
                current_stream['url'] = line
                streams.append(current_stream.copy())
                current_stream = {}
        
        return streams
    
    def parse_m3u8_segments(self, m3u8_path: str) -> List[Dict]:
        """Parse M3U8 file to extract TS segment URLs."""
        segments = []
        
        with open(m3u8_path, 'r') as f:
            content = f.read()
        
        lines = content.strip().split('\n')
        current_segment = {}
        
        for line in lines:
            line = line.strip()
            
            if line.startswith('#EXTINF:'):
                # Extract duration
                duration_match = re.search(r'#EXTINF:([\d.]+)', line)
                if duration_match:
                    current_segment['duration'] = float(duration_match.group(1))
                    
            elif line.startswith('http') and '.ts' in line:
                current_segment['url'] = line
                segments.append(current_segment.copy())
                current_segment = {}
        
        return segments
    
    def extract_audio_from_stream(self, m3u8_path: str, output_path: str,
                                max_segments: int = None,
                                audio_format: str = 'wav',
                                sample_rate: int = 44100,
                                channels: int = 2,
                                stream_index: int = None) -> Dict:
        """Extract audio from an entire M3U8 stream using optimized methods."""
        print(f"🎵 Fast extracting audio from {m3u8_path}")
        
        # Check if this is a master playlist and we need to select a stream
        if stream_index is not None:
            streams = self.parse_m3u8_streams(m3u8_path)
            if streams and stream_index < len(streams):
                selected_stream = streams[stream_index]
                print(f"📺 Selected stream {stream_index}: {selected_stream.get('bandwidth', 'N/A')} bps, {selected_stream.get('resolution', 'N/A')}")
                
                # Download the selected stream's M3U8 file
                stream_url = selected_stream['url']
                print(f"📥 Downloading stream playlist: {stream_url}")
                
                try:
                    response = self.session.get(stream_url, timeout=30)
                    response.raise_for_status()
                    
                    # Create temporary M3U8 file for the selected stream
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.m3u8', delete=False) as f:
                        f.write(response.text)
                        temp_m3u8_path = f.name
                    
                    # Parse segments from the selected stream
                    segments = self.parse_m3u8_segments(temp_m3u8_path)
                    print(f"📁 Found {len(segments)} segments in selected stream")
                    
                except Exception as e:
                    return {'success': False, 'error': f'Failed to download stream playlist: {e}'}
            else:
                return {'success': False, 'error': f'Stream index {stream_index} out of range (max: {len(streams)-1 if streams else 0})'}
        else:
            # Parse segments from current file
            segments = self.parse_m3u8_segments(m3u8_path)
        
        if not segments:
            return {'success': False, 'error': 'No TS segments found in M3U8 file'}
        
        if max_segments:
            segments = segments[:max_segments]
            print(f"📁 Processing first {len(segments)} segments")
        else:
            print(f"📁 Processing all {len(segments)} segments")
        
        # Use parallel download approach for all cases
        return self.extract_audio_parallel_download(segments, output_path, audio_format, sample_rate, channels)
    
    def list_available_streams(self, m3u8_path: str) -> List[Dict]:
        """List all available streams from a master playlist."""
        streams = self.parse_m3u8_streams(m3u8_path)
        
        if not streams:
            return []
        
        print(f"📺 Available streams in {m3u8_path}:")
        print("=" * 60)
        
        for i, stream in enumerate(streams):
            bandwidth = stream.get('bandwidth', 'N/A')
            resolution = stream.get('resolution', 'N/A')
            codecs = stream.get('codecs', 'N/A')
            
            print(f"   Stream {i}: {bandwidth} bps, {resolution}, {codecs}")
            
            # Add recommendation for transcription
            if bandwidth and isinstance(bandwidth, int):
                if bandwidth <= 500000:  # 500k bps or less
                    print(f"      🎯 RECOMMENDED for transcription (low bitrate)")
                elif bandwidth <= 1000000:  # 1M bps or less
                    print(f"      ✅ Good for transcription")
                else:
                    print(f"      ⚠️  High bitrate - consider lower for transcription")
        
        return streams
    
    def process_batch(self, input_dir: str, output_dir: str = None,
                     audio_format: str = 'wav', 
                     sample_rate: int = 44100,
                     channels: int = 2,
                     stream_index: int = None,
                     max_segments: int = None,
                     force: bool = False,
                     max_stream_workers: int = None) -> Dict:
        """Process all M3U8 files in a directory with parallel stream processing."""
        print(f"🔄 Batch processing M3U8 files in: {input_dir}")
        
        # Find all M3U8 files
        m3u8_pattern = os.path.join(input_dir, "*.m3u8")
        m3u8_files = glob.glob(m3u8_pattern)
        
        if not m3u8_files:
            return {
                'success': False,
                'error': f'No M3U8 files found in {input_dir}',
                'processed': 0,
                'failed': 0
            }
        
        print(f"📁 Found {len(m3u8_files)} M3U8 files")
        
        # Set up output directory
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        
        # Determine number of stream workers
        if max_stream_workers is None:
            # Use fewer workers for streams to avoid overwhelming the system
            max_stream_workers = min(4, len(m3u8_files))
        
        print(f"⚡ Processing {len(m3u8_files)} streams with {max_stream_workers} parallel workers")
        
        results = {
            'success': True,
            'processed': 0,
            'failed': 0,
            'skipped': 0,
            'files': [],
            'total_time': 0,
            'errors': []
        }
        
        start_time = time.time()
        
        def process_single_stream(m3u8_file):
            """Process a single M3U8 file - designed for parallel execution."""
            print(f"\n🎵 Processing: {os.path.basename(m3u8_file)}")
            
            # Determine output path
            base_name = Path(m3u8_file).stem
            if output_dir:
                output_path = os.path.join(output_dir, f"{base_name}.{audio_format}")
            else:
                output_path = f"{base_name}.{audio_format}"
            
            # Check if output file already exists
            if os.path.exists(output_path) and not force:
                file_size = os.path.getsize(output_path)
                print(f"   ⏭️  Output file already exists: {os.path.basename(output_path)}")
                print(f"      File size: {file_size / (1024*1024):.1f} MB")
                print(f"      Skipping extraction")
                
                return {
                    'input_file': m3u8_file,
                    'output_file': output_path,
                    'success': True,
                    'processing_time': 0,
                    'skipped': True,
                    'error': None
                }
            elif os.path.exists(output_path) and force:
                print(f"   🔄 Overwriting existing file: {os.path.basename(output_path)}")
            
            # Process the file
            file_start_time = time.time()
            result = self.extract_audio_from_stream(
                m3u8_file, output_path, max_segments,
                audio_format, sample_rate, channels, stream_index
            )
            file_time = time.time() - file_start_time
            
            if result['success']:
                print(f"   ✅ Success ({file_time:.1f}s)")
                return {
                    'input_file': m3u8_file,
                    'output_file': output_path,
                    'success': True,
                    'processing_time': file_time,
                    'skipped': False,
                    'error': None
                }
            else:
                error_msg = f"Failed to process {os.path.basename(m3u8_file)}: {result.get('error', 'Unknown error')}"
                print(f"   ❌ Failed ({file_time:.1f}s): {result.get('error', 'Unknown error')}")
                return {
                    'input_file': m3u8_file,
                    'output_file': None,
                    'success': False,
                    'processing_time': file_time,
                    'skipped': False,
                    'error': error_msg
                }
        
        # Process streams in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_stream_workers) as executor:
            # Submit all stream processing tasks
            future_to_file = {
                executor.submit(process_single_stream, m3u8_file): m3u8_file
                for m3u8_file in m3u8_files
            }
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_file):
                result = future.result()
                results['files'].append(result)
                
                if result['skipped']:
                    results['skipped'] += 1
                elif result['success']:
                    results['processed'] += 1
                else:
                    results['failed'] += 1
                    results['errors'].append(result['error'])
        
        results['total_time'] = time.time() - start_time
        
        # Print summary
        print(f"\n📊 BATCH PROCESSING SUMMARY")
        print(f"   Total files: {len(m3u8_files)}")
        print(f"   Successfully processed: {results['processed']}")
        print(f"   Skipped (already exist): {results['skipped']}")
        print(f"   Failed: {results['failed']}")
        print(f"   Total time: {results['total_time']:.1f}s")
        if results['processed'] > 0:
            print(f"   Average time per processed file: {results['total_time']/results['processed']:.1f}s")
        
        if results['errors']:
            print(f"\n❌ Errors:")
            for error in results['errors']:
                print(f"   - {error}")
        
        return results


def main():
    parser = argparse.ArgumentParser(description='Fast extract audio from TS streams and M3U8 playlists')
    parser.add_argument('input_file', nargs='?', help='Path to M3U8 file or directory for batch processing')
    parser.add_argument('-o', '--output-dir', help='Output directory for audio files')
    parser.add_argument('--batch', action='store_true', help='Process all M3U8 files in the input directory')
    parser.add_argument('-f', '--format', choices=['wav', 'mp3'], default='mp3', 
                       help='Audio output format (default: mp3)')
    parser.add_argument('-r', '--sample-rate', type=int, default=16000,
                       help='Sample rate in Hz (default: 16000)')
    parser.add_argument('-c', '--channels', type=int, default=1,
                       help='Number of audio channels (default: 1)')
    parser.add_argument('-s', '--segments', type=int, help='Maximum number of segments to process')
    parser.add_argument('--segment', type=int, help='Extract audio from specific segment index only')
    parser.add_argument('--stream', type=int, default=0, help='Select specific stream from master.m3u8 (0-based index)')
    parser.add_argument('--list-streams', action='store_true', help='List available streams from master playlist')
    parser.add_argument('--analyze', action='store_true', help='Analyze stream audio properties only')
    parser.add_argument('--workers', type=int, default=8, help='Number of parallel download workers (default: 8)')
    parser.add_argument('--segments-per-worker', type=int, default=10, help='Number of segments per worker batch (default: 10)')
    parser.add_argument('--stream-workers', type=int, default=4, help='Number of parallel stream workers for batch processing (default: 4)')
    parser.add_argument('--force', action='store_true', help='Overwrite existing output files')
    parser.add_argument('--max-retries', type=int, default=5, help='Maximum number of retry attempts per segment (default: 5)')
    parser.add_argument('--backoff-factor', type=float, default=1.0, help='Exponential backoff factor for retries (default: 1.0)')
    parser.add_argument('--base-timeout', type=int, default=15, help='Base timeout in seconds for downloads (default: 15)')
    parser.add_argument('--disable-second-pass', action='store_true', help='Disable second-pass retry for failed segments')
    parser.add_argument('--retry-stats', action='store_true', help='Show detailed retry statistics')
    parser.add_argument('--all-prime-time', action='store_true', 
                       help='[DEPRECATED] Use nfl_pipeline.py instead. Process all m3u8 dirs in data/football/')
    parser.add_argument('--data-dir', default='data/football',
                       help='Base data directory for --all-prime-time (default: data/football)')
    
    args = parser.parse_args()
    
    # Handle --all-prime-time flag (now dynamic)
    if args.all_prime_time:
        print("🏈 Processing all M3U8 directories for audio extraction...")
        print(f"📁 Data directory: {args.data_dir}")
        
        # Dynamically discover subdirectories with m3u8 folders
        from pathlib import Path
        
        data_path = Path(args.data_dir)
        prime_time_configs = []
        
        # Find all directories that have an m3u8 subdirectory
        for subdir in sorted(data_path.iterdir()):
            if subdir.is_dir() and subdir.name != 'csvs':
                m3u8_dir = subdir / 'm3u8'
                if m3u8_dir.exists() and m3u8_dir.is_dir():
                    # Check if there are any m3u8 files
                    m3u8_files = list(m3u8_dir.glob('*.m3u8'))
                    if m3u8_files:
                        stem = subdir.name
                        name_parts = stem.replace('_', '-').split('-')
                        name = ' '.join(part.upper() if len(part) <= 3 else part.title() for part in name_parts)
                        abbrev = ''.join(part[0].upper() for part in name_parts if part)[:4]
                        if len(abbrev) < 2:
                            abbrev = stem[:4].upper()
                        
                        prime_time_configs.append({
                            'name': name,
                            'abbrev': abbrev,
                            'input_dir': str(m3u8_dir),
                            'output_dir': str(subdir / 'mp3')
                        })
        
        if not prime_time_configs:
            print(f"\n❌ No m3u8 directories found in: {args.data_dir}")
            print(f"   Expected structure: {args.data_dir}/<name>/m3u8/*.m3u8")
            sys.exit(1)
        
        print(f"\n📋 Found {len(prime_time_configs)} directories with M3U8 files:")
        for config in prime_time_configs:
            m3u8_count = len(list(Path(config['input_dir']).glob('*.m3u8')))
            print(f"   • {config['name']}: {m3u8_count} files")
        
        all_results = []
        
        for config in prime_time_configs:
            print(f"\n{'='*70}")
            print(f"🏈 Processing {config['name']} ({config['abbrev']})")
            print(f"{'='*70}")
            print(f"📁 Input: {config['input_dir']}")
            print(f"📁 Output: {config['output_dir']}")
            
            # Check if input directory exists and has M3U8 files
            if not os.path.exists(config['input_dir']):
                print(f"   ⚠️  Input directory not found, skipping...")
                continue
            
            m3u8_files = glob.glob(os.path.join(config['input_dir'], "*.m3u8"))
            if not m3u8_files:
                print(f"   ⚠️  No M3U8 files found, skipping...")
                continue
            
            print(f"   📦 Found {len(m3u8_files)} M3U8 files")
            
            # Create output directory
            os.makedirs(config['output_dir'], exist_ok=True)
            
            # Create extractor for this batch
            extractor = FastTSAudioExtractor(
                max_workers=args.workers,
                segments_per_worker=args.segments_per_worker,
                max_retries=args.max_retries,
                backoff_factor=args.backoff_factor,
                base_timeout=args.base_timeout,
                enable_second_pass_retry=not args.disable_second_pass
            )
            extractor._show_retry_stats = args.retry_stats
            
            # Process this league's files
            result = extractor.process_batch(
                config['input_dir'],
                config['output_dir'],
                args.format,
                args.sample_rate,
                args.channels,
                args.stream,
                args.segments,
                args.force,
                args.stream_workers
            )
            
            all_results.append({
                'league': config['abbrev'],
                'result': result
            })
        
        # Print overall summary
        print(f"\n{'='*70}")
        print(f"🎉 All Prime-Time Audio Extraction Complete!")
        print(f"{'='*70}")
        
        total_processed = sum(r['result']['processed'] for r in all_results)
        total_skipped = sum(r['result']['skipped'] for r in all_results)
        total_failed = sum(r['result']['failed'] for r in all_results)
        
        print(f"📊 Overall Summary:")
        print(f"   Successfully processed: {total_processed}")
        print(f"   Skipped (already exist): {total_skipped}")
        print(f"   Failed: {total_failed}")
        
        for result_info in all_results:
            league = result_info['league']
            result = result_info['result']
            print(f"\n   {league}: {result['processed']} processed, {result['skipped']} skipped, {result['failed']} failed")
        
        return
    
    if not args.input_file:
        print("❌ No input file provided. Use --help for usage information.")
        sys.exit(1)
    
    if not os.path.exists(args.input_file):
        print(f"❌ File or directory not found: {args.input_file}")
        sys.exit(1)
    
    extractor = FastTSAudioExtractor(
        max_workers=args.workers, 
        segments_per_worker=args.segments_per_worker,
        max_retries=args.max_retries,
        backoff_factor=args.backoff_factor,
        base_timeout=args.base_timeout,
        enable_second_pass_retry=not args.disable_second_pass
    )
    
    # Set retry stats flag
    extractor._show_retry_stats = args.retry_stats
    
    # Handle batch processing
    if args.batch:
        if not os.path.isdir(args.input_file):
            print(f"❌ Batch processing requires a directory: {args.input_file}")
            sys.exit(1)
        
        # Process all M3U8 files in the directory
        result = extractor.process_batch(
            args.input_file, args.output_dir, args.format,
            args.sample_rate, args.channels, args.stream, args.segments, args.force, args.stream_workers
        )
        
        if result['success']:
            print(f"\n✅ Batch processing completed!")
            print(f"   Processed: {result['processed']} files")
            print(f"   Skipped: {result['skipped']} files")
            print(f"   Failed: {result['failed']} files")
            print(f"   Total time: {result['total_time']:.1f}s")
        else:
            print(f"\n❌ Batch processing failed: {result.get('error', 'Unknown error')}")
            sys.exit(1)
        
        return
    
    if args.list_streams:
        # List available streams
        streams = extractor.list_available_streams(args.input_file)
        
        if not streams:
            print("❌ No streams found in master playlist")
            sys.exit(1)
        
        print(f"\n💡 Usage examples:")
        print(f"   # Fast extraction from lowest bitrate stream")
        print(f"   python ts_audio_extractor.py {args.input_file} --stream 0 -r 16000")
        print(f"   # Fast extraction to specific directory")
        print(f"   python ts_audio_extractor.py {args.input_file} --stream 0 -r 16000 -o audio_output/")
        print(f"   # Batch process all M3U8 files in a directory (parallel streams)")
        print(f"   python ts_audio_extractor.py /path/to/m3u8/files --batch --stream 0 -r 16000 -o audio_output/")
        print(f"   # Batch process with more parallel streams")
        print(f"   python ts_audio_extractor.py /path/to/m3u8/files --batch --stream 0 -r 16000 --stream-workers 8")
        print(f"   # Overwrite existing files")
        print(f"   python ts_audio_extractor.py {args.input_file} --stream 0 -r 16000 --force")
        print(f"   # Fast extraction with more workers and segments per worker")
        print(f"   python ts_audio_extractor.py {args.input_file} --stream 0 -r 16000 --workers 16 --segments-per-worker 20")
        print(f"   # Robust extraction with enhanced retry logic")
        print(f"   python ts_audio_extractor.py {args.input_file} --stream 0 -r 16000 --max-retries 8 --backoff-factor 1.5 --retry-stats")
        print(f"   # Fast extraction with disabled second-pass retry")
        print(f"   python ts_audio_extractor.py {args.input_file} --stream 0 -r 16000 --disable-second-pass")
        return
    
    if args.analyze:
        print("❌ Analysis not implemented in fast version - use original ts_audio_extractor.py")
        return
    
    # Determine output path
    base_name = Path(args.input_file).stem
    if args.output_dir:
        # Use specified output directory
        os.makedirs(args.output_dir, exist_ok=True)
        output_path = os.path.join(args.output_dir, f"{base_name}.{args.format}")
    else:
        # Use current directory
        output_path = f"{base_name}.{args.format}"
    
    # Check if output file already exists
    if os.path.exists(output_path) and not args.force:
        file_size = os.path.getsize(output_path)
        print(f"⏭️  Output file already exists: {output_path}")
        print(f"   File size: {file_size / (1024*1024):.1f} MB")
        print(f"   Skipping extraction to avoid overwriting existing file")
        print(f"   Use --force to overwrite existing files")
        sys.exit(0)
    elif os.path.exists(output_path) and args.force:
        print(f"🔄 Overwriting existing file: {output_path}")
    
    # Extract audio
    if args.segment is not None:
        print("❌ Single segment extraction not implemented in fast version - use original ts_audio_extractor.py")
        sys.exit(1)
    else:
        # Extract from entire stream
        result = extractor.extract_audio_from_stream(
            args.input_file, output_path, args.segments,
            args.format, args.sample_rate, args.channels, args.stream
        )
    
    if result['success']:
        print(f"\n✅ Fast audio extraction completed successfully!")
        print(f"   Output file: {result['output_file']}")
        if 'file_size' in result:
            print(f"   File size: {result['file_size'] / (1024*1024):.1f} MB")
        if 'segments_processed' in result:
            print(f"   Segments processed: {result['segments_processed']}")
    else:
        print(f"\n❌ Audio extraction failed: {result.get('error', 'Unknown error')}")
        sys.exit(1)


if __name__ == '__main__':
    main()
