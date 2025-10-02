#!/usr/bin/env python3
"""
Whisper Transcriber

Transcribes audio files using Whisper Turbo on Groq.
Supports both single file and batch directory processing with concurrent transcription.
Saves transcripts to specified output directory.
"""

import os
import sys
import argparse
import glob
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from dotenv import load_dotenv
from groq import Groq


def load_environment():
    """Load environment variables from .env file."""
    env_path = Path(__file__).parent.parent.parent / '.env'
    if env_path.exists():
        load_dotenv(env_path)
        print(f"✅ Loaded environment from {env_path}")
    else:
        print(f"⚠️  No .env file found at {env_path}")
    
    # Check if API key is available
    api_key = os.getenv('GROQ_API_KEY')
    if not api_key:
        print("❌ GROQ_API_KEY not found in environment variables")
        print("   Please add GROQ_API_KEY=your_key_here to .env file")
        sys.exit(1)
    
    return api_key


def get_audio_files(directory: str) -> list:
    """Get all audio files from a directory."""
    audio_extensions = ['*.mp3', '*.wav', '*.m4a', '*.flac', '*.aac', '*.ogg', '*.wma']
    audio_files = []
    
    for ext in audio_extensions:
        pattern = os.path.join(directory, '**', ext)
        audio_files.extend(glob.glob(pattern, recursive=True))
    
    return sorted(audio_files)


def transcribe_single_audio(audio_file: str, output_dir: str, model: str, client: Groq, print_lock: Lock) -> dict:
    """Transcribe a single audio file using Whisper Turbo on Groq."""
    
    # Check if audio file exists
    if not os.path.exists(audio_file):
        with print_lock:
            print(f"❌ Audio file not found: {audio_file}")
        return {"success": False, "file": audio_file, "error": "File not found"}
    
    # Generate output filename (match input filename)
    input_filename = Path(audio_file).stem
    output_file = os.path.join(output_dir, f"{input_filename}.txt")
    
    # Check if output already exists
    if os.path.exists(output_file):
        with print_lock:
            print(f"⏭️  Skipping {audio_file} (output already exists)")
        return {"success": True, "file": audio_file, "output": output_file, "skipped": True}
    
    with print_lock:
        print(f"🎵 Transcribing: {os.path.basename(audio_file)}")
    
    try:
        # Get file info
        file_size = os.path.getsize(audio_file)
        
        with print_lock:
            print(f"   File size: {file_size / (1024*1024):.1f} MB")
        
        # Transcribe the audio
        with open(audio_file, "rb") as file:
            transcription = client.audio.transcriptions.create(
                file=(audio_file, file.read()),
                model=model,
                response_format="verbose_json",
            )
        
        # Save transcription to file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(transcription.text)
        
        with print_lock:
            print(f"✅ Completed: {os.path.basename(audio_file)}")
            print(f"   Output: {output_file}")
            print(f"   Length: {len(transcription.text)} characters")
        
        return {
            "success": True, 
            "file": audio_file, 
            "output": output_file, 
            "length": len(transcription.text),
            "skipped": False
        }
        
    except Exception as e:
        with print_lock:
            print(f"❌ Failed: {os.path.basename(audio_file)} - {e}")
        return {"success": False, "file": audio_file, "error": str(e)}


def transcribe_audio(audio_file: str, output_dir: str = None, model: str = "whisper-large-v3-turbo", output_filename: str = None) -> str:
    """Transcribe audio file using Whisper Turbo on Groq."""
    
    # Load environment and get API key
    api_key = load_environment()
    
    # Initialize Groq client
    try:
        client = Groq(api_key=api_key)
        print("✅ Connected to Groq API")
    except Exception as e:
        print(f"❌ Failed to connect to Groq: {e}")
        sys.exit(1)
    
    # Check if audio file exists
    if not os.path.exists(audio_file):
        print(f"❌ Audio file not found: {audio_file}")
        sys.exit(1)
    
    print(f"🎵 Transcribing: {audio_file}")
    print(f"   Model: {model}")
    
    # Get file info
    file_size = os.path.getsize(audio_file)
    print(f"   File size: {file_size / (1024*1024):.1f} MB")
    
    # Determine output path
    if output_dir is None:
        output_dir = "data-football/audio/whisper-transcripts"
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate output filename
    if output_filename:
        # Use provided filename
        if not output_filename.endswith('.txt'):
            output_filename += '.txt'
        output_file = os.path.join(output_dir, output_filename)
    else:
        # Use input filename
        input_filename = Path(audio_file).stem
        output_file = os.path.join(output_dir, f"{input_filename}.txt")
    
    try:
        # Transcribe the audio
        print(f"🔄 Transcribing with {model}...")
        
        with open(audio_file, "rb") as file:
            transcription = client.audio.transcriptions.create(
                file=(audio_file, file.read()),
                model=model,
                response_format="verbose_json",
            )
        
        # Save transcription to file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(transcription.text)
        
        print(f"✅ Transcription completed!")
        print(f"   Output: {output_file}")
        print(f"   Length: {len(transcription.text)} characters")
        
        # Show preview of transcription
        preview = transcription.text[:200].replace('\n', ' ')
        print(f"   Preview: {preview}...")
        
        return output_file
        
    except Exception as e:
        print(f"❌ Transcription failed: {e}")
        sys.exit(1)


def transcribe_batch(input_path: str, output_dir: str, model: str, max_workers: int = 4) -> dict:
    """Transcribe multiple audio files from a directory using concurrent processing."""
    
    # Load environment and get API key
    api_key = load_environment()
    
    # Initialize Groq client
    try:
        client = Groq(api_key=api_key)
        print("✅ Connected to Groq API")
    except Exception as e:
        print(f"❌ Failed to connect to Groq: {e}")
        sys.exit(1)
    
    # Check if input path exists
    if not os.path.exists(input_path):
        print(f"❌ Input path not found: {input_path}")
        sys.exit(1)
    
    # Get audio files
    if os.path.isfile(input_path):
        audio_files = [input_path]
        print(f"🎵 Single file mode: {os.path.basename(input_path)}")
    else:
        audio_files = get_audio_files(input_path)
        if not audio_files:
            print(f"❌ No audio files found in: {input_path}")
            sys.exit(1)
        print(f"🎵 Batch mode: Found {len(audio_files)} audio files")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"📁 Output directory: {output_dir}")
    print(f"🤖 Model: {model}")
    print(f"⚡ Max workers: {max_workers}")
    print("=" * 60)
    
    # Process files concurrently
    results = []
    print_lock = Lock()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all transcription tasks
        future_to_file = {
            executor.submit(transcribe_single_audio, audio_file, output_dir, model, client, print_lock): audio_file
            for audio_file in audio_files
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_file):
            result = future.result()
            results.append(result)
    
    # Summary
    successful = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]
    skipped = [r for r in results if r.get("skipped", False)]
    
    print("\n" + "=" * 60)
    print("📊 BATCH PROCESSING SUMMARY")
    print(f"✅ Successful: {len(successful)}")
    print(f"⏭️  Skipped: {len(skipped)}")
    print(f"❌ Failed: {len(failed)}")
    print(f"📁 Total: {len(results)}")
    
    if failed:
        print("\n❌ Failed files:")
        for result in failed:
            print(f"   - {os.path.basename(result['file'])}: {result['error']}")
    
    return {
        "total": len(results),
        "successful": len(successful),
        "failed": len(failed),
        "skipped": len(skipped),
        "results": results
    }


def main():
    """Main function to handle command line arguments."""
    parser = argparse.ArgumentParser(description='Transcribe audio files using Whisper Turbo on Groq')
    parser.add_argument('input_path', help='Path to audio file or directory containing audio files')
    parser.add_argument('-o', '--output-dir', default='data-football/audio/whisper-transcripts',
                       help='Output directory for transcripts (default: data-football/audio/whisper-transcripts)')
    parser.add_argument('-m', '--model', default='whisper-large-v3-turbo',
                       choices=['whisper-large-v3-turbo', 'whisper-large-v3',],
                       help='Whisper model to use (default: whisper-large-v3-turbo)')
    parser.add_argument('-f', '--output-filename', 
                       help='Output filename (without .txt extension, only for single file mode)')
    parser.add_argument('-w', '--workers', type=int, default=4,
                       help='Number of concurrent workers for batch processing (default: 4)')
    parser.add_argument('--preview', action='store_true', 
                       help='Show preview of transcription without saving (single file only)')
    parser.add_argument('--single', action='store_true',
                       help='Force single file mode even if input is a directory')
    
    args = parser.parse_args()
    
    # Convert to absolute path
    input_path = os.path.abspath(args.input_path)
    
    print("🎤 WHISPER TURBO TRANSCRIPTION")
    print("=" * 50)
    print(f"Input path: {input_path}")
    print(f"Output dir: {args.output_dir}")
    
    # Determine if we should use batch or single file mode
    use_batch = os.path.isdir(input_path) and not args.single
    
    if use_batch:
        print(f"\n🔄 BATCH MODE")
        if args.preview:
            print("⚠️  Preview mode not supported for batch processing")
            return
        
        # Batch processing
        results = transcribe_batch(input_path, args.output_dir, args.model, args.workers)
        
        print(f"\n📊 Final Summary:")
        print(f"   Total files processed: {results['total']}")
        print(f"   Successful transcriptions: {results['successful']}")
        print(f"   Files skipped (already exist): {results['skipped']}")
        print(f"   Failed transcriptions: {results['failed']}")
        
    else:
        # Single file mode
        if not os.path.exists(input_path):
            print(f"❌ Input file not found: {input_path}")
            return
        
        if args.preview:
            print("\n🔍 PREVIEW MODE - Transcription will not be saved")
        
        # Transcribe the audio
        output_file = transcribe_audio(input_path, args.output_dir, args.model, args.output_filename)
        
        if not args.preview:
            print(f"\n📄 Transcript saved to: {output_file}")
            
            # Show file size
            if os.path.exists(output_file):
                file_size = os.path.getsize(output_file)
                print(f"   File size: {file_size} bytes")
        else:
            print(f"\n🔍 Preview completed (not saved)")


if __name__ == '__main__':
    main()
