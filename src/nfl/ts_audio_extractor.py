#!/usr/bin/env python3
"""
Fast TS Audio Extractor

Optimized version of TS Audio Extractor with performance improvements:
- Parallel segment downloads
- Direct FFmpeg streaming (no intermediate files)
- Optimized FFmpeg parameters
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


class FastTSAudioExtractor:
    """Fast audio extractor with parallel downloads and optimized processing."""
    
    def __init__(self, max_workers: int = 8, segments_per_worker: int = 10):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        self.max_workers = max_workers
        self.segments_per_worker = segments_per_worker
        self.download_lock = threading.Lock()
        self.downloaded_count = 0
    
    def download_ts_segment_fast(self, url: str, output_path: str) -> bool:
        """Fast download with retry logic."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=15, stream=True)
                response.raise_for_status()
                
                with open(output_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                with self.download_lock:
                    self.downloaded_count += 1
                    if self.downloaded_count % 10 == 0:
                        print(f"   📥 Downloaded {self.downloaded_count} segments...")
                
                return True
            except Exception as e:
                if attempt == max_retries - 1:
                    print(f"   ❌ Failed to download {url} after {max_retries} attempts: {e}")
                    return False
                continue
        return False
    
    def download_segments_parallel(self, segments: List[Dict], temp_dir: str) -> List[str]:
        """Download segments in parallel with batching for optimal performance."""
        print(f"📥 Downloading {len(segments)} segments in parallel...")
        print(f"   Workers: {self.max_workers}, Segments per worker: {self.segments_per_worker}")
        
        segment_files = []
        failed_downloads = []
        
        def download_segment_batch(batch_data):
            batch_index, batch_segments = batch_data
            batch_results = []
            
            for i, segment in enumerate(batch_segments):
                segment_path = os.path.join(temp_dir, f"segment_{batch_index * self.segments_per_worker + i:04d}.ts")
                
                if self.download_ts_segment_fast(segment['url'], segment_path):
                    batch_results.append(segment_path)
                else:
                    failed_downloads.append(batch_index * self.segments_per_worker + i)
            
            return batch_results
        
        # Split segments into batches
        segment_batches = []
        for i in range(0, len(segments), self.segments_per_worker):
            batch = segments[i:i + self.segments_per_worker]
            segment_batches.append((i // self.segments_per_worker, batch))
        
        print(f"   📦 Split into {len(segment_batches)} batches")
        
        # Use ThreadPoolExecutor for parallel batch downloads
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all batch download tasks
            future_to_batch = {
                executor.submit(download_segment_batch, batch_data): batch_data[0]
                for batch_data in segment_batches
            }
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_batch):
                batch_results = future.result()
                segment_files.extend(batch_results)
        
        if failed_downloads:
            print(f"   ⚠️  Failed to download {len(failed_downloads)} segments: {failed_downloads}")
        
        print(f"✅ Downloaded {len(segment_files)}/{len(segments)} segments successfully")
        return segment_files
    
    
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
                     force: bool = False) -> Dict:
        """Process all M3U8 files in a directory."""
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
        
        for i, m3u8_file in enumerate(m3u8_files):
            print(f"\n🎵 Processing file {i+1}/{len(m3u8_files)}: {os.path.basename(m3u8_file)}")
            
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
                
                results['skipped'] += 1
                results['files'].append({
                    'input_file': m3u8_file,
                    'output_file': output_path,
                    'success': True,
                    'processing_time': 0,
                    'skipped': True,
                    'error': None
                })
                continue
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
                results['processed'] += 1
                print(f"   ✅ Success ({file_time:.1f}s)")
            else:
                results['failed'] += 1
                error_msg = f"Failed to process {os.path.basename(m3u8_file)}: {result.get('error', 'Unknown error')}"
                results['errors'].append(error_msg)
                print(f"   ❌ Failed ({file_time:.1f}s): {result.get('error', 'Unknown error')}")
            
            results['files'].append({
                'input_file': m3u8_file,
                'output_file': output_path if result['success'] else None,
                'success': result['success'],
                'processing_time': file_time,
                'error': result.get('error') if not result['success'] else None
            })
        
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
    parser.add_argument('input_file', help='Path to M3U8 file or directory for batch processing')
    parser.add_argument('-o', '--output-dir', help='Output directory for audio files')
    parser.add_argument('--batch', action='store_true', help='Process all M3U8 files in the input directory')
    parser.add_argument('-f', '--format', choices=['wav', 'mp3'], default='wav', 
                       help='Audio output format (default: wav)')
    parser.add_argument('-r', '--sample-rate', type=int, default=44100,
                       help='Sample rate in Hz (default: 44100)')
    parser.add_argument('-c', '--channels', type=int, default=1,
                       help='Number of audio channels (default: 1)')
    parser.add_argument('-s', '--segments', type=int, help='Maximum number of segments to process')
    parser.add_argument('--segment', type=int, help='Extract audio from specific segment index only')
    parser.add_argument('--stream', type=int, help='Select specific stream from master.m3u8 (0-based index)')
    parser.add_argument('--list-streams', action='store_true', help='List available streams from master playlist')
    parser.add_argument('--analyze', action='store_true', help='Analyze stream audio properties only')
    parser.add_argument('--workers', type=int, default=8, help='Number of parallel download workers (default: 8)')
    parser.add_argument('--segments-per-worker', type=int, default=10, help='Number of segments per worker batch (default: 10)')
    parser.add_argument('--force', action='store_true', help='Overwrite existing output files')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input_file):
        print(f"❌ File or directory not found: {args.input_file}")
        sys.exit(1)
    
    extractor = FastTSAudioExtractor(max_workers=args.workers, segments_per_worker=args.segments_per_worker)
    
    # Handle batch processing
    if args.batch:
        if not os.path.isdir(args.input_file):
            print(f"❌ Batch processing requires a directory: {args.input_file}")
            sys.exit(1)
        
        # Process all M3U8 files in the directory
        result = extractor.process_batch(
            args.input_file, args.output_dir, args.format,
            args.sample_rate, args.channels, args.stream, args.segments, args.force
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
        print(f"   # Batch process all M3U8 files in a directory")
        print(f"   python ts_audio_extractor.py /path/to/m3u8/files --batch --stream 0 -r 16000 -o audio_output/")
        print(f"   # Overwrite existing files")
        print(f"   python ts_audio_extractor.py {args.input_file} --stream 0 -r 16000 --force")
        print(f"   # Fast extraction with more workers and segments per worker")
        print(f"   python ts_audio_extractor.py {args.input_file} --stream 0 -r 16000 --workers 16 --segments-per-worker 20")
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
