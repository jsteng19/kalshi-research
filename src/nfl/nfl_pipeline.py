#!/usr/bin/env python3
"""
NFL Transcript Pipeline

Unified pipeline that dynamically discovers CSVs and processes them through:
1. M3U8 download from NFL+
2. Audio extraction from streams
3. Whisper transcription

Usage:
    # Process all CSVs in data/football/csvs/
    python src/nfl/nfl_pipeline.py

    # Process specific CSV
    python src/nfl/nfl_pipeline.py --csv data/football/csvs/monday-night.csv

    # Skip stages
    python src/nfl/nfl_pipeline.py --skip-download --skip-audio

    # Force overwrite
    python src/nfl/nfl_pipeline.py --force

CSV files should have format: date,url
Output directories are derived from CSV filename:
    data/football/csvs/monday-night.csv -> data/football/monday-night/{m3u8,mp3,transcripts,diarized}
"""

import os
import sys
import argparse
import glob
import subprocess
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional


def get_csv_configs(csv_dir: str = "data/football/csvs", specific_csv: str = None) -> List[Dict]:
    """
    Discover CSVs and generate pipeline configurations.
    
    Each CSV generates a config with:
        - name: Human-readable name derived from filename
        - abbrev: Short name for logging
        - csv: Path to CSV file
        - output_base: Base output directory (data/football/<csv-stem>/)
        - m3u8_dir: Directory for M3U8 files
        - mp3_dir: Directory for MP3 files
        - transcript_dir: Base directory for transcripts
    """
    configs = []
    
    if specific_csv:
        # Process single CSV
        csv_files = [specific_csv] if os.path.exists(specific_csv) else []
    else:
        # Discover all CSVs in the directory
        csv_pattern = os.path.join(csv_dir, "*.csv")
        csv_files = sorted(glob.glob(csv_pattern))
    
    for csv_path in csv_files:
        stem = Path(csv_path).stem
        
        # Generate human-readable name from filename
        # e.g., "monday-night" -> "Monday Night", "cbs-eagle" -> "CBS Eagle"
        name_parts = stem.replace('_', '-').split('-')
        name = ' '.join(part.upper() if len(part) <= 3 else part.title() for part in name_parts)
        
        # Short abbreviation for logging
        abbrev = ''.join(part[0].upper() for part in name_parts if part)[:4]
        if len(abbrev) < 2:
            abbrev = stem[:4].upper()
        
        # Output directory structure
        output_base = f"data/football/nfl/{stem}"
        
        config = {
            'name': name,
            'abbrev': abbrev,
            'csv': csv_path,
            'output_base': output_base,
            'm3u8_dir': f"{output_base}/m3u8",
            'mp3_dir': f"{output_base}/mp3",
            'transcript_dir': output_base  # transcripts/ and diarized/ created inside
        }
        
        configs.append(config)
    
    return configs


def parse_csv_urls(csv_path: str) -> tuple:
    """
    Parse a CSV file and return (urls, url_dates, url_output_dirs).
    
    Expects CSV format: date,url (with optional header)
    """
    urls = []
    url_dates = {}
    
    try:
        with open(csv_path, 'r') as f:
            lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        
        # Check if this is a CSV with date,url format
        if lines and ',' in lines[0] and lines[0].lower().startswith('date'):
            # Skip header and parse CSV
            for line in lines[1:]:
                if ',' in line:
                    parts = [part.strip() for part in line.split(',')]
                    if len(parts) >= 2:
                        date_str = parts[0]
                        url_str = parts[1]
                        urls.append(url_str)
                        url_dates[url_str] = date_str
        else:
            # Plain URL list format
            urls.extend(lines)
            
    except FileNotFoundError:
        print(f"   ❌ CSV file not found: {csv_path}")
    except Exception as e:
        print(f"   ❌ Error reading CSV: {e}")
    
    return urls, url_dates


def run_download_stage(configs: List[Dict], force: bool = False, 
                       wait_time: int = 20, max_retries: int = 3) -> bool:
    """Run M3U8 download stage for all configs."""
    print("\n" + "=" * 70)
    print("📥 STAGE 1: Downloading M3U8 Files from NFL+")
    print("=" * 70)
    
    # Collect all URLs with their output directories
    all_urls = []
    url_dates = {}
    url_output_dirs = {}
    
    print("\n📋 Reading CSV files...")
    for config in configs:
        print(f"   📄 {config['abbrev']}: {config['csv']}")
        
        urls, dates = parse_csv_urls(config['csv'])
        
        if not urls:
            print(f"      ⚠️  No URLs found, skipping...")
            continue
        
        for url in urls:
            all_urls.append(url)
            if url in dates:
                url_dates[url] = dates[url]
            url_output_dirs[url] = config['m3u8_dir']
        
        print(f"      ✅ Found {len(urls)} games")
        
        # Create output directory
        os.makedirs(config['m3u8_dir'], exist_ok=True)
    
    if not all_urls:
        print("\n❌ No URLs found in any CSV files!")
        return False
    
    print(f"\n📊 Total games to process: {len(all_urls)}")
    
    # Import and run downloader
    try:
        from auto_audio_m3u8_downloader import AutoAudioM3U8Downloader
    except ImportError:
        from src.nfl.auto_audio_m3u8_downloader import AutoAudioM3U8Downloader
    
    downloader = AutoAudioM3U8Downloader(
        download_dir='data/football',
        wait_time=wait_time,
        force=force,
        max_retries=max_retries
    )
    
    results = downloader.process_urls(all_urls, url_dates, url_output_dirs)
    
    print(f"\n✅ Download stage complete: {len(results)} files")
    return len(results) > 0


def run_audio_extraction_stage(configs: List[Dict], force: bool = False,
                               stream_index: int = 0, sample_rate: int = 16000,
                               channels: int = 1, workers: int = 8,
                               stream_workers: int = 4) -> bool:
    """Run audio extraction stage for all configs."""
    print("\n" + "=" * 70)
    print("🎵 STAGE 2: Extracting Audio from M3U8 Streams")
    print("=" * 70)
    
    try:
        from ts_audio_extractor import FastTSAudioExtractor
    except ImportError:
        from src.nfl.ts_audio_extractor import FastTSAudioExtractor
    
    all_results = []
    
    for config in configs:
        print(f"\n{'='*60}")
        print(f"🏈 Processing {config['name']} ({config['abbrev']})")
        print(f"{'='*60}")
        print(f"📁 Input: {config['m3u8_dir']}")
        print(f"📁 Output: {config['mp3_dir']}")
        
        # Check if input directory exists and has M3U8 files
        if not os.path.exists(config['m3u8_dir']):
            print(f"   ⚠️  Input directory not found, skipping...")
            continue
        
        m3u8_files = glob.glob(os.path.join(config['m3u8_dir'], "*.m3u8"))
        if not m3u8_files:
            print(f"   ⚠️  No M3U8 files found, skipping...")
            continue
        
        print(f"   📦 Found {len(m3u8_files)} M3U8 files")
        
        # Create output directory
        os.makedirs(config['mp3_dir'], exist_ok=True)
        
        # Create extractor and process
        extractor = FastTSAudioExtractor(
            max_workers=workers,
            segments_per_worker=10,
            max_retries=5,
            backoff_factor=1.0,
            base_timeout=15,
            enable_second_pass_retry=True
        )
        
        result = extractor.process_batch(
            config['m3u8_dir'],
            config['mp3_dir'],
            'mp3',
            sample_rate,
            channels,
            stream_index,
            None,  # max_segments
            force,
            stream_workers
        )
        
        all_results.append({
            'config': config,
            'result': result
        })
    
    # Print summary
    total_processed = sum(r['result']['processed'] for r in all_results)
    total_skipped = sum(r['result']['skipped'] for r in all_results)
    total_failed = sum(r['result']['failed'] for r in all_results)
    
    print(f"\n📊 Audio Extraction Summary:")
    print(f"   Successfully processed: {total_processed}")
    print(f"   Skipped (already exist): {total_skipped}")
    print(f"   Failed: {total_failed}")
    
    return total_processed > 0 or total_skipped > 0


def run_transcription_stage(configs: List[Dict], force: bool = False,
                           workers: int = 8, use_diarize: bool = True,
                           use_universal: bool = True, use_slam: bool = False,
                           use_elevenlabs: bool = False) -> bool:
    """Run transcription stage for all configs."""
    print("\n" + "=" * 70)
    print("🎤 STAGE 3: Transcribing Audio")
    print("=" * 70)
    
    try:
        from src.transcription.whisper_transcriber import transcribe_batch, get_audio_files
    except ImportError:
        from src.transcription.whisper_transcriber import transcribe_batch, get_audio_files
    
    all_results = []
    
    for config in configs:
        print(f"\n{'='*60}")
        print(f"🏈 Processing {config['name']} ({config['abbrev']})")
        print(f"{'='*60}")
        print(f"📁 Input: {config['mp3_dir']}")
        print(f"📁 Output: {config['transcript_dir']}")
        
        # Check if input directory exists
        if not os.path.exists(config['mp3_dir']):
            print(f"   ⚠️  Input directory not found, skipping...")
            continue
        
        audio_files = get_audio_files(config['mp3_dir'])
        if not audio_files:
            print(f"   ⚠️  No audio files found, skipping...")
            continue
        
        print(f"   🎵 Found {len(audio_files)} audio files")
        
        # Create output directory
        os.makedirs(config['transcript_dir'], exist_ok=True)
        
        # Run transcription
        result = transcribe_batch(
            config['mp3_dir'],
            config['transcript_dir'],
            'whisper-large-v3-turbo',
            workers,
            25,  # max_size_mb
            use_diarize,
            use_slam,
            use_universal,
            use_elevenlabs
        )
        
        all_results.append({
            'config': config,
            'result': result
        })
    
    # Print summary
    total_successful = sum(r['result']['successful'] for r in all_results)
    total_skipped = sum(r['result']['skipped'] for r in all_results)
    total_failed = sum(r['result']['failed'] for r in all_results)
    
    print(f"\n📊 Transcription Summary:")
    print(f"   Successfully transcribed: {total_successful}")
    print(f"   Skipped (already exist): {total_skipped}")
    print(f"   Failed: {total_failed}")
    
    return total_successful > 0 or total_skipped > 0


def main():
    parser = argparse.ArgumentParser(
        description='NFL Transcript Pipeline - Process CSVs through download, extraction, and transcription',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all CSVs in data/football/csvs/
  python src/nfl/nfl_pipeline.py

  # Process specific CSV
  python src/nfl/nfl_pipeline.py --csv data/football/csvs/monday-night.csv

  # Skip download stage (use existing M3U8 files)
  python src/nfl/nfl_pipeline.py --skip-download

  # Skip audio extraction (use existing MP3 files)
  python src/nfl/nfl_pipeline.py --skip-audio

  # Only transcribe (skip download and extraction)
  python src/nfl/nfl_pipeline.py --skip-download --skip-audio

  # Force overwrite all existing files
  python src/nfl/nfl_pipeline.py --force

  # Use custom CSV directory
  python src/nfl/nfl_pipeline.py --csv-dir data/football/my-csvs/

  # List discovered CSVs without processing
  python src/nfl/nfl_pipeline.py --list
        """
    )
    
    # Input options
    parser.add_argument('--csv-dir', default='data/football/csvs',
                       help='Directory containing CSV files (default: data/football/csvs)')
    parser.add_argument('--csv', help='Process specific CSV file instead of directory')
    parser.add_argument('--list', action='store_true',
                       help='List discovered CSVs and exit')
    
    # Stage control
    parser.add_argument('--skip-download', action='store_true',
                       help='Skip M3U8 download stage')
    parser.add_argument('--skip-audio', action='store_true',
                       help='Skip audio extraction stage')
    parser.add_argument('--skip-transcribe', action='store_true',
                       help='Skip transcription stage')
    parser.add_argument('--force', action='store_true',
                       help='Overwrite existing files at each stage')
    
    # Download options
    parser.add_argument('--wait-time', type=int, default=20,
                       help='Max wait time per game for M3U8 detection (default: 20)')
    parser.add_argument('--max-retries', type=int, default=3,
                       help='Maximum retries for M3U8 download (default: 3)')
    
    # Audio extraction options
    parser.add_argument('--stream', type=int, default=0,
                       help='Stream index to extract from M3U8 (default: 0)')
    parser.add_argument('--sample-rate', type=int, default=16000,
                       help='Audio sample rate in Hz (default: 16000)')
    parser.add_argument('--channels', type=int, default=1,
                       help='Number of audio channels (default: 1)')
    parser.add_argument('--workers', type=int, default=8,
                       help='Number of parallel download workers (default: 8)')
    parser.add_argument('--stream-workers', type=int, default=4,
                       help='Number of parallel stream workers (default: 4)')
    
    # Transcription options
    parser.add_argument('--transcribe-workers', type=int, default=8,
                       help='Number of concurrent transcription workers (default: 8)')
    parser.add_argument('--diarize', action='store_true', default=True,
                       help='Enable speaker diarization (default: True)')
    parser.add_argument('--no-diarize', action='store_true',
                       help='Disable speaker diarization')
    parser.add_argument('--universal', action='store_true', default=True,
                       help='Use AssemblyAI Universal model (default: True)')
    parser.add_argument('--slam', action='store_true',
                       help='Use AssemblyAI SLAM model instead of Universal')
    parser.add_argument('--elevenlabs', action='store_true',
                       help='Use ElevenLabs scribe_v1 model')
    parser.add_argument('--groq', action='store_true',
                       help='Use Groq Whisper instead of AssemblyAI')
    
    args = parser.parse_args()
    
    # Get CSV configurations
    configs = get_csv_configs(args.csv_dir, args.csv)
    
    if not configs:
        print(f"❌ No CSV files found in: {args.csv_dir}")
        print(f"   Create CSVs with format: date,url")
        print(f"   Example: 2024-01-01,https://www.nfl.com/plus/games/...")
        sys.exit(1)
    
    # List mode
    if args.list:
        print(f"📋 Discovered {len(configs)} CSV configurations:\n")
        for config in configs:
            print(f"   {config['abbrev']}: {config['name']}")
            print(f"      CSV: {config['csv']}")
            print(f"      Output: {config['output_base']}/")
            print()
        sys.exit(0)
    
    # Print pipeline header
    print("=" * 70)
    print("🏈 NFL TRANSCRIPT PIPELINE 🏈")
    print("=" * 70)
    print(f"\n📋 Processing {len(configs)} CSV configurations:")
    for config in configs:
        print(f"   • {config['name']} ({config['csv']})")
    print()
    
    # Track timing
    start_time = datetime.now()
    
    # Stage 1: Download M3U8 files
    if not args.skip_download:
        if not run_download_stage(configs, args.force, args.wait_time, args.max_retries):
            print("⚠️  Download stage had issues, but continuing...")
    else:
        print("\n⏭️  Skipping M3U8 download (--skip-download)")
    
    # Stage 2: Extract audio
    if not args.skip_audio:
        if not run_audio_extraction_stage(
            configs, args.force, args.stream, args.sample_rate,
            args.channels, args.workers, args.stream_workers
        ):
            print("⚠️  Audio extraction stage had issues, but continuing...")
    else:
        print("\n⏭️  Skipping audio extraction (--skip-audio)")
    
    # Stage 3: Transcribe
    if not args.skip_transcribe:
        use_diarize = args.diarize and not args.no_diarize
        use_elevenlabs = args.elevenlabs
        use_universal = args.universal and not args.slam and not args.groq and not use_elevenlabs
        use_slam = args.slam and not args.groq and not use_elevenlabs
        
        if not run_transcription_stage(
            configs, args.force, args.transcribe_workers,
            use_diarize, use_universal, use_slam, use_elevenlabs
        ):
            print("⚠️  Transcription stage had issues")
    else:
        print("\n⏭️  Skipping transcription (--skip-transcribe)")
    
    # Print final summary
    end_time = datetime.now()
    duration = end_time - start_time
    minutes = int(duration.total_seconds() // 60)
    seconds = int(duration.total_seconds() % 60)
    
    print("\n" + "=" * 70)
    print("🎉 PIPELINE COMPLETE! 🎉")
    print("=" * 70)
    print(f"Total processing time: {minutes}m {seconds}s")
    print(f"\nOutput directories:")
    for config in configs:
        print(f"   📁 {config['name']}:")
        print(f"      M3U8:        {config['m3u8_dir']}/")
        print(f"      MP3:         {config['mp3_dir']}/")
        print(f"      Transcripts: {config['transcript_dir']}/transcripts/")
        if args.diarize and not args.no_diarize:
            print(f"      Diarized:    {config['transcript_dir']}/diarized/")


if __name__ == '__main__':
    main()

