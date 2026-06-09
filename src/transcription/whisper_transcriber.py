#!/usr/bin/env python3
"""
Whisper Transcriber

Transcribes audio files using:
- Whisper Turbo on Groq (default)
- Fireworks AI with diarization (--diarize flag)
- AssemblyAI SLAM model with NFL-optimized keyterms (--slam flag, optionally with --diarize)
- AssemblyAI Universal model with NFL-optimized keyterms (--universal flag, optionally with --diarize)

Supports both single file and batch directory processing with concurrent transcription.
Automatically chunks large files (>25MB) to comply with Groq API limits.
Saves transcripts to specified output directory.
"""

import os
import sys
import argparse
import glob
import tempfile
import subprocess
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from collections import Counter
from dotenv import load_dotenv
# from groq import Groq

# Optional AssemblyAI import
try:
    import assemblyai as aai
    ASSEMBLYAI_AVAILABLE = True
except ImportError:
    ASSEMBLYAI_AVAILABLE = False

# Optional ElevenLabs import
try:
    from elevenlabs.client import ElevenLabs
    ELEVENLABS_AVAILABLE = True
except ImportError:
    ELEVENLABS_AVAILABLE = False

# Import keyterms for AssemblyAI
try:
    from src.transcription.assemblyai_keyterms import NFL_KEYTERMS, SPEAKER_ROLES
except ImportError:
    try:
        from src.transcription.assemblyai_keyterms import NFL_KEYTERMS, SPEAKER_ROLES
    except ImportError:
        NFL_KEYTERMS = []
        SPEAKER_ROLES = ["Play by play announcer", "Color Commentary announcer", "All other"]

def load_environment(use_diarization=False, use_slam=False, use_universal=False, use_elevenlabs=False):
    """Load environment variables from .env file."""
    env_path = Path(__file__).parent.parent.parent / '.env'
    if env_path.exists():
        load_dotenv(env_path)
        print(f"✅ Loaded environment from {env_path}")
    else:
        print(f"⚠️  No .env file found at {env_path}")
    
    # Check if API key is available based on mode
    if use_elevenlabs:
        api_key = os.getenv('ELEVEN_LABS_API_KEY')
        if not api_key:
            print("❌ ELEVEN_LABS_API_KEY not found in environment variables")
            print("   Please add ELEVEN_LABS_API_KEY=your_key_here to .env file")
            sys.exit(1)
    elif use_slam or use_universal:
        api_key = os.getenv('ASSEMBLYAI_API_KEY')
        if not api_key:
            print("❌ ASSEMBLYAI_API_KEY not found in environment variables")
            print("   Please add ASSEMBLYAI_API_KEY=your_key_here to .env file")
            sys.exit(1)
    elif use_diarization:
        api_key = os.getenv('FIREWORKS_API_KEY')
        if not api_key:
            print("❌ FIREWORKS_API_KEY not found in environment variables")
            print("   Please add FIREWORKS_API_KEY=your_key_here to .env file")
            sys.exit(1)
    else:
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

def create_retry_session(retries=5, backoff_factor=2, status_forcelist=(429, 500, 502, 503, 504)):
    """Create a requests session with retry logic."""
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["POST"],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=1, pool_maxsize=1)
    session.mount("https://", adapter)
    return session

def transcribe_with_fireworks(audio_file: str, api_key: str, max_retries: int = 5):
    """Transcribe audio file using Fireworks AI with diarization."""
    url = "https://audio-prod.api.fireworks.ai/v1/audio/transcriptions"
    session = create_retry_session(retries=max_retries)
    for attempt in range(max_retries):
        try:
            with open(audio_file, "rb") as f:
                response = session.post(
                    url,
                    headers={"Authorization": f"Bearer {api_key}"},
                    files={"file": f},
                    data={
                        "vad_model": "silero",
                        "alignment_model": "tdnn_ffn",
                        "response_format": "verbose_json",
                        "preprocessing": "none",
                        "language": "en",
                        "temperature": "0,0.2,0.4,0.6,0.8,1",
                        "timestamp_granularities": "word",
                        "diarize": "true"
                    },
                    timeout=600  # 10 minute timeout for large files
                )
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                wait_time = (2 ** attempt) * 5  # 5, 10, 20, 40, 80 seconds
                print(f"   ⏳ Rate limited, waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
            elif response.status_code >= 500:
                wait_time = (2 ** attempt) * 2  # 2, 4, 8, 16, 32 seconds
                print(f"   ⏳ Server error {response.status_code}, waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
            else:
                raise Exception(f"Fireworks API Error {response.status_code}: {response.text}")
        except requests.exceptions.ConnectionError as e:
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) * 3
                print(f"   ⏳ Connection error, waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
            else:
                raise Exception(f"Connection failed after {max_retries} retries: {e}")
        except requests.exceptions.Timeout as e:
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) * 2
                print(f"   ⏳ Timeout, waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
            else:
                raise Exception(f"Request timed out after {max_retries} retries: {e}")
    raise Exception(f"Fireworks API failed after {max_retries} retries")

def generate_diarized_text(transcription_data):
    """Convert raw transcription JSON into a readable diarized string."""
    words = transcription_data.get('words', [])
    words_sorted = sorted(words, key=lambda x: x['start'])
    transcript_lines = []
    current_speaker = None
    current_text = []
    for word_data in words_sorted:
        speaker_id = word_data.get('speaker_id')
        word = word_data['word']
        if speaker_id != current_speaker:
            if current_speaker is not None and current_text:
                transcript_lines.append(f"Speaker {current_speaker}: {' '.join(current_text)}")
            current_speaker = speaker_id
            current_text = [word]
        else:
            current_text.append(word)
    if current_speaker is not None and current_text:
        transcript_lines.append(f"Speaker {current_speaker}: {' '.join(current_text)}")
    return "\n\n".join(transcript_lines)

def get_top_speakers(transcription_data, top_n=2):
    """Get the top N most frequent speakers by word count."""
    words = transcription_data.get('words', [])
    speaker_word_counts = Counter()
    for word_data in words:
        speaker_id = word_data.get('speaker_id')
        if speaker_id is not None:
            speaker_word_counts[speaker_id] += 1
    top_speakers = [speaker for speaker, count in speaker_word_counts.most_common(top_n)]
    return top_speakers

def generate_filtered_transcript(transcription_data, allowed_speakers):
    """Generate transcript with only specified speakers (plain text, no labels or line breaks)."""
    words = transcription_data.get('words', [])
    words_sorted = sorted(words, key=lambda x: x['start'])
    filtered_words = [w['word'] for w in words_sorted if w.get('speaker_id') in allowed_speakers]
    return ' '.join(filtered_words)

def transcribe_with_assemblyai(
    audio_file: str,
    api_key: str,
    max_retries: int = 3,
    use_universal: bool = False,
    diarize: bool = True
):
    """
    Transcribe audio file using AssemblyAI's SLAM or Universal model.
    Diarization (speaker labels, role identification) is optional via the diarize flag.
    
    Fix: Use correct constructor arguments for aai.TranscriptionConfig.
    """
    if not ASSEMBLYAI_AVAILABLE:
        raise Exception("AssemblyAI package not installed. Run: pip install -U assemblyai")
    aai.settings.api_key = api_key
    config_kwargs = dict(
        format_text=True,
        punctuate=True,
        language_code="en_us",
    )
    # Build diarization/speech_understanding arguments
    if diarize:
        config_kwargs["speaker_labels"] = True
        config_kwargs["speech_understanding"] = {
            "request": {
                "speaker_identification": {
                    "speaker_type": "role",
                    "known_values": SPEAKER_ROLES
                }
            }
        }
    else:
        config_kwargs["speaker_labels"] = False
        config_kwargs["speech_understanding"] = None

    # Use SLAM model or Universal model (default)
    if not use_universal:
        config_kwargs["speech_models"] = ["slam-1"]
    # For Universal model, don't pass speech_models, will use default

    if NFL_KEYTERMS:
        config_kwargs["keyterms_prompt"] = NFL_KEYTERMS

    config = aai.TranscriptionConfig(**config_kwargs)

    for attempt in range(max_retries):
        try:
            transcriber = aai.Transcriber(config=config)
            transcript = transcriber.transcribe(audio_file)
            if transcript.status == aai.TranscriptStatus.error:
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) * 5
                    print(f"   ⏳ Transcription error, waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                    time.sleep(wait_time)
                    continue
                raise Exception(f"AssemblyAI transcription failed: {transcript.error}")
            return transcript
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) * 3
                print(f"   ⏳ Error: {e}, waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
            else:
                raise Exception(f"AssemblyAI failed after {max_retries} retries: {e}")
    raise Exception(f"AssemblyAI failed after {max_retries} retries")

def generate_assemblyai_diarized_text(transcript):
    """Convert AssemblyAI transcript into a readable diarized string."""
    if not getattr(transcript, 'utterances', None):
        return transcript.text or ""
    transcript_lines = []
    for utterance in transcript.utterances:
        speaker = utterance.speaker
        text = utterance.text
        transcript_lines.append(f"Speaker {speaker}: {text}")
    return "\n\n".join(transcript_lines)

def get_assemblyai_top_speakers(transcript, top_n=2):
    """Get the top N most frequent speakers by word count from AssemblyAI transcript."""
    if not getattr(transcript, 'utterances', None):
        return []
    speaker_word_counts = Counter()
    for utterance in transcript.utterances:
        speaker = utterance.speaker
        word_count = len(utterance.text.split())
        speaker_word_counts[speaker] += word_count
    top_speakers = [speaker for speaker, count in speaker_word_counts.most_common(top_n)]
    return top_speakers

def generate_assemblyai_filtered_transcript(transcript, allowed_speakers):
    """Generate transcript with only specified speakers from AssemblyAI."""
    if not getattr(transcript, 'utterances', None):
        return transcript.text or ""
    filtered_texts = []
    for utterance in transcript.utterances:
        if utterance.speaker in allowed_speakers:
            filtered_texts.append(utterance.text)
    return ' '.join(filtered_texts)

def transcribe_with_elevenlabs(
    audio_file: str,
    api_key: str,
    max_retries: int = 3,
    diarize: bool = True
):
    """
    Transcribe audio file using Eleven Labs scribe_v1 model.
    
    Args:
        audio_file: Path to audio file
        api_key: Eleven Labs API key
        max_retries: Number of retries on failure
        diarize: Whether to enable speaker diarization
    
    Returns:
        dict with 'text' and optionally 'words' with speaker info
    """
    if not ELEVENLABS_AVAILABLE:
        raise Exception("ElevenLabs package not installed. Run: pip install elevenlabs")
    
    client = ElevenLabs(api_key=api_key)
    
    for attempt in range(max_retries):
        try:
            with open(audio_file, "rb") as f:
                transcription = client.speech_to_text.convert(
                    model_id="scribe_v1",
                    file=f,
                    language_code="en",
                    tag_audio_events=False,
                    diarize=diarize
                )
            return transcription
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) * 3
                print(f"   ⏳ Error: {e}, waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
            else:
                raise Exception(f"ElevenLabs failed after {max_retries} retries: {e}")
    
    raise Exception(f"ElevenLabs failed after {max_retries} retries")

def generate_elevenlabs_diarized_text(transcription):
    """Convert ElevenLabs transcription into a readable diarized string."""
    words = getattr(transcription, 'words', None) or transcription.get('words', [])
    if not words:
        return getattr(transcription, 'text', None) or transcription.get('text', '')
    
    transcript_lines = []
    current_speaker = None
    current_text = []
    
    for word_data in words:
        # Handle both object and dict access
        if hasattr(word_data, 'speaker_id'):
            speaker_id = word_data.speaker_id
            word = word_data.text
        else:
            speaker_id = word_data.get('speaker_id')
            word = word_data.get('text', '')
        
        if speaker_id != current_speaker:
            if current_speaker is not None and current_text:
                transcript_lines.append(f"Speaker {current_speaker}: {' '.join(current_text)}")
            current_speaker = speaker_id
            current_text = [word]
        else:
            current_text.append(word)
    
    if current_speaker is not None and current_text:
        transcript_lines.append(f"Speaker {current_speaker}: {' '.join(current_text)}")
    
    return "\n\n".join(transcript_lines)

def get_elevenlabs_top_speakers(transcription, top_n=2):
    """Get the top N most frequent speakers by word count from ElevenLabs transcription."""
    words = getattr(transcription, 'words', None) or transcription.get('words', [])
    if not words:
        return []
    
    speaker_word_counts = Counter()
    for word_data in words:
        if hasattr(word_data, 'speaker_id'):
            speaker_id = word_data.speaker_id
        else:
            speaker_id = word_data.get('speaker_id')
        if speaker_id is not None:
            speaker_word_counts[speaker_id] += 1
    
    top_speakers = [speaker for speaker, count in speaker_word_counts.most_common(top_n)]
    return top_speakers

def generate_elevenlabs_filtered_transcript(transcription, allowed_speakers):
    """Generate transcript with only specified speakers from ElevenLabs."""
    words = getattr(transcription, 'words', None) or transcription.get('words', [])
    if not words:
        return getattr(transcription, 'text', None) or transcription.get('text', '')
    
    filtered_words = []
    for word_data in words:
        if hasattr(word_data, 'speaker_id'):
            speaker_id = word_data.speaker_id
            word = word_data.text
        else:
            speaker_id = word_data.get('speaker_id')
            word = word_data.get('text', '')
        
        if speaker_id in allowed_speakers:
            filtered_words.append(word)
    
    return ' '.join(filtered_words)


# Role labels used by AssemblyAI / NBA diarization (Play by play announcer, Color Commentary announcer)
import re as _re

def _has_role_labels(content: str) -> bool:
    """Return True if diarized content has Play by play or Color Commentary speaker labels."""
    return bool(
        _re.search(r"Speaker\s+Play\s+by\s+play\s+announcer", content, _re.IGNORECASE)
        or _re.search(r"Speaker\s+Color\s+Commentary\s+announcer", content, _re.IGNORECASE)
    )


def extract_play_by_play_and_color_from_diarized(diarized_text: str) -> tuple:
    """
    Parse diarized transcript text with role labels (e.g. Speaker Play by play announcer: ...)
    and return (play_by_play_text, color_commentary_text). Each is a single string; empty if none.
    Handles "Speaker ... - 1:", "Speaker ... - 2:", etc.
    """
    pbp_parts = []
    color_parts = []
    segments = _re.split(r"\n\s*Speaker\s+", diarized_text, flags=_re.IGNORECASE)
    for seg in segments:
        seg = seg.strip()
        if not seg:
            continue
        if seg.lower().startswith("speaker "):
            seg = seg[8:].strip()
        if not seg:
            continue
        if _re.match(r"Play\s+by\s+play\s+announcer(?:\s*-\s*\d+)?\s*:", seg, _re.IGNORECASE):
            text = _re.sub(
                r"^Play\s+by\s+play\s+announcer(?:\s*-\s*\d+)?\s*:\s*", "", seg, count=1, flags=_re.IGNORECASE
            )
            text = " ".join(text.split())
            if text:
                pbp_parts.append(text)
        elif _re.match(r"Color\s+Commentary\s+announcer(?:\s*-\s*\d+)?\s*:", seg, _re.IGNORECASE):
            text = _re.sub(
                r"^Color\s+Commentary\s+announcer(?:\s*-\s*\d+)?\s*:\s*", "", seg, count=1, flags=_re.IGNORECASE
            )
            text = " ".join(text.split())
            if text:
                color_parts.append(text)
    return (" ".join(pbp_parts) if pbp_parts else "", " ".join(color_parts) if color_parts else "")


def write_role_extracts_if_present(diarized_text: str, output_dir: str, input_filename: str, print_lock: Lock = None):
    """
    If diarized_text contains Play by play / Color Commentary role labels, extract and write
    play_by_play/<stem>.txt and color_commentary/<stem>.txt under output_dir.
    """
    if not diarized_text or not _has_role_labels(diarized_text):
        return
    pbp_text, color_text = extract_play_by_play_and_color_from_diarized(diarized_text)
    if not pbp_text and not color_text:
        return
    stem = Path(input_filename).stem
    pbp_dir = os.path.join(output_dir, "play_by_play")
    color_dir = os.path.join(output_dir, "color_commentary")
    os.makedirs(pbp_dir, exist_ok=True)
    os.makedirs(color_dir, exist_ok=True)
    if pbp_text:
        pbp_file = os.path.join(pbp_dir, f"{stem}.txt")
        with open(pbp_file, "w", encoding="utf-8") as f:
            f.write(pbp_text)
        if print_lock:
            with print_lock:
                print(f"   Play-by-play extract: {pbp_file}")
    if color_text:
        color_file = os.path.join(color_dir, f"{stem}.txt")
        with open(color_file, "w", encoding="utf-8") as f:
            f.write(color_text)
        if print_lock:
            with print_lock:
                print(f"   Color commentary extract: {color_file}")


def chunk_audio_file(audio_file: str, max_size_mb: int = 25) -> list:
    """
    Split a large audio file into 2 chunks if it exceeds max_size_mb.
    Returns a list of temporary chunk file paths.
    """
    file_size_mb = os.path.getsize(audio_file) / (1024 * 1024)
    if file_size_mb <= max_size_mb:
        return [audio_file]
    print(f"   📦 File size ({file_size_mb:.1f} MB) exceeds limit ({max_size_mb} MB), splitting into 2 chunks...")
    try:
        duration_cmd = [
            'ffprobe', '-v', 'quiet', '-show_entries', 'format=duration',
            '-of', 'csv=p=0', audio_file
        ]
        result = subprocess.run(duration_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"   ⚠️  Could not get audio duration, using original file")
            return [audio_file]
        total_duration_seconds = float(result.stdout.strip())
        midpoint_seconds = total_duration_seconds / 2
        chunks = []
        temp_dir = tempfile.mkdtemp(prefix="whisper_chunks_")
        original_ext = os.path.splitext(audio_file)[1]
        chunk1_path = os.path.join(temp_dir, f"chunk_001{original_ext}")
        ffmpeg_cmd1 = [
            'ffmpeg', '-y', '-i', audio_file,
            '-t', str(midpoint_seconds),
            '-c', 'copy',
            chunk1_path
        ]
        result1 = subprocess.run(ffmpeg_cmd1, capture_output=True, text=True)
        if result1.returncode == 0 and os.path.exists(chunk1_path):
            chunk1_size_mb = os.path.getsize(chunk1_path) / (1024 * 1024)
            chunks.append(chunk1_path)
            print(f"   📄 Created chunk 1: {chunk1_size_mb:.1f} MB")
        chunk2_path = os.path.join(temp_dir, f"chunk_002{original_ext}")
        ffmpeg_cmd2 = [
            'ffmpeg', '-y', '-i', audio_file,
            '-ss', str(midpoint_seconds),
            '-c', 'copy',
            chunk2_path
        ]
        result2 = subprocess.run(ffmpeg_cmd2, capture_output=True, text=True)
        if result2.returncode == 0 and os.path.exists(chunk2_path):
            chunk2_size_mb = os.path.getsize(chunk2_path) / (1024 * 1024)
            chunks.append(chunk2_path)
            print(f"   📄 Created chunk 2: {chunk2_size_mb:.1f} MB")
        if not chunks:
            print(f"   ⚠️  Failed to create chunks, using original file")
            return [audio_file]
        print(f"   ✅ Split into {len(chunks)} chunks")
        return chunks
    except Exception as e:
        print(f"   ❌ Failed to chunk audio file: {e}")
        return [audio_file]

def cleanup_temp_chunks(chunk_files: list, original_file: str):
    """Clean up temporary chunk files."""
    for chunk_file in chunk_files:
        if chunk_file != original_file and os.path.exists(chunk_file):
            try:
                os.remove(chunk_file)
            except Exception as e:
                print(f"   ⚠️  Could not remove temp file {chunk_file}: {e}")
    if chunk_files and chunk_files[0] != original_file:
        temp_dir = os.path.dirname(chunk_files[0])
        try:
            if os.path.exists(temp_dir) and not os.listdir(temp_dir):
                os.rmdir(temp_dir)
        except Exception as e:
            print(f"   ⚠️  Could not remove temp directory {temp_dir}: {e}")

def transcribe_single_audio(audio_file: str, output_dir: str, model: str, client, print_lock: Lock, max_size_mb: int = 25, use_diarization: bool = False, api_key: str = None, use_slam: bool = False, use_universal: bool = False, use_elevenlabs: bool = False) -> dict:
    """
    Transcribe a single audio file using Whisper Turbo on Groq,
    Fireworks AI with diarization,
    AssemblyAI SLAM/Universal (optionally with diarization),
    or ElevenLabs scribe_v1.
    """
    if not os.path.exists(audio_file):
        with print_lock:
            print(f"❌ Audio file not found: {audio_file}")
        return {"success": False, "file": audio_file, "error": "File not found"}
    input_filename = Path(audio_file).stem
    diarize_assemblyai = use_diarization if (use_slam or use_universal) else False
    if use_slam or use_universal or (use_diarization and not (use_slam or use_universal)):
        output_file = os.path.join(output_dir, "transcripts", f"{input_filename}.txt")
    else:
        output_file = os.path.join(output_dir, f"{input_filename}.txt")
    if os.path.exists(output_file):
        with print_lock:
            print(f"⏭️  Skipping {audio_file} (output already exists)")
        return {"success": True, "file": audio_file, "output": output_file, "skipped": True}
    with print_lock:
        print(f"🎵 Transcribing: {os.path.basename(audio_file)}")
    try:
        file_size = os.path.getsize(audio_file)
        with print_lock:
            print(f"   File size: {file_size / (1024*1024):.1f} MB")
        if use_elevenlabs:
            with print_lock:
                print(f"   Using ElevenLabs scribe_v1 model... (Diarization: {'enabled' if use_diarization else 'disabled'})")
            transcription = transcribe_with_elevenlabs(audio_file, api_key, diarize=use_diarization)
            diarized_dir = os.path.join(output_dir, "diarized")
            transcripts_dir = os.path.join(output_dir, "transcripts")
            os.makedirs(diarized_dir, exist_ok=True)
            os.makedirs(transcripts_dir, exist_ok=True)
            if use_diarization:
                diarized_text = generate_elevenlabs_diarized_text(transcription)
                diarized_file = os.path.join(diarized_dir, f"{input_filename}.txt")
                with open(diarized_file, 'w', encoding='utf-8') as f:
                    f.write(diarized_text)
                write_role_extracts_if_present(diarized_text, output_dir, input_filename, print_lock)
                top_speakers = get_elevenlabs_top_speakers(transcription, top_n=2)
                filtered_text = generate_elevenlabs_filtered_transcript(transcription, top_speakers)
                filtered_file = os.path.join(transcripts_dir, f"{input_filename}.txt")
                with open(filtered_file, 'w', encoding='utf-8') as f:
                    f.write(filtered_text)
                with print_lock:
                    print(f"✅ Completed: {os.path.basename(audio_file)}")
                    print(f"   Diarized output: {diarized_file}")
                    print(f"   Filtered output (top 2 speakers): {filtered_file}")
                    print(f"   Top speakers: {top_speakers}")
                    print(f"   Length: {len(filtered_text)} characters")
                return {
                    "success": True,
                    "file": audio_file,
                    "output": filtered_file,
                    "diarized_output": diarized_file,
                    "length": len(filtered_text),
                    "skipped": False,
                    "top_speakers": top_speakers,
                    "model": "elevenlabs"
                }
            else:
                plain_text = getattr(transcription, 'text', None) or transcription.get('text', '')
                filtered_file = os.path.join(transcripts_dir, f"{input_filename}.txt")
                with open(filtered_file, 'w', encoding='utf-8') as f:
                    f.write(plain_text)
                with print_lock:
                    print(f"✅ Completed: {os.path.basename(audio_file)}")
                    print(f"   Output: {filtered_file}")
                    print(f"   Length: {len(plain_text)} characters")
                return {
                    "success": True,
                    "file": audio_file,
                    "output": filtered_file,
                    "diarized_output": None,
                    "length": len(plain_text),
                    "skipped": False,
                    "top_speakers": [],
                    "model": "elevenlabs"
                }
        elif use_slam or use_universal:
            model_name = "Universal" if use_universal else "SLAM"
            with print_lock:
                print(f"   Using AssemblyAI {model_name} model... (Diarization: {'enabled' if diarize_assemblyai else 'disabled'})")
            transcript = transcribe_with_assemblyai(audio_file, api_key, use_universal=use_universal, diarize=diarize_assemblyai)
            diarized_dir = os.path.join(output_dir, "diarized")
            transcripts_dir = os.path.join(output_dir, "transcripts")
            os.makedirs(diarized_dir, exist_ok=True)
            os.makedirs(transcripts_dir, exist_ok=True)
            if diarize_assemblyai:
                diarized_text = generate_assemblyai_diarized_text(transcript)
                diarized_file = os.path.join(diarized_dir, f"{input_filename}.txt")
                with open(diarized_file, 'w', encoding='utf-8') as f:
                    f.write(diarized_text)
                write_role_extracts_if_present(diarized_text, output_dir, input_filename, print_lock)
                top_speakers = get_assemblyai_top_speakers(transcript, top_n=2)
                filtered_text = generate_assemblyai_filtered_transcript(transcript, top_speakers)
                filtered_file = os.path.join(transcripts_dir, f"{input_filename}.txt")
                with open(filtered_file, 'w', encoding='utf-8') as f:
                    f.write(filtered_text)
                with print_lock:
                    print(f"✅ Completed: {os.path.basename(audio_file)}")
                    print(f"   Diarized output: {diarized_file}")
                    print(f"   Filtered output (top 2 speakers): {filtered_file}")
                    print(f"   Top speakers: {top_speakers}")
                    print(f"   Length: {len(filtered_text)} characters")
                return {
                    "success": True,
                    "file": audio_file,
                    "output": filtered_file,
                    "diarized_output": diarized_file,
                    "length": len(filtered_text),
                    "skipped": False,
                    "top_speakers": top_speakers,
                    "model": "universal" if use_universal else "slam"
                }
            else:
                filtered_text = transcript.text or ""
                filtered_file = os.path.join(transcripts_dir, f"{input_filename}.txt")
                with open(filtered_file, 'w', encoding='utf-8') as f:
                    f.write(filtered_text)
                with print_lock:
                    print(f"✅ Completed: {os.path.basename(audio_file)}")
                    print(f"   Output: {filtered_file}")
                    print(f"   Length: {len(filtered_text)} characters")
                return {
                    "success": True,
                    "file": audio_file,
                    "output": filtered_file,
                    "diarized_output": None,
                    "length": len(filtered_text),
                    "skipped": False,
                    "top_speakers": [],
                    "model": "universal" if use_universal else "slam"
                }
        elif use_diarization:
            with print_lock:
                print(f"   Using Fireworks AI with diarization...")
            transcription_data = transcribe_with_fireworks(audio_file, api_key)
            diarized_dir = os.path.join(output_dir, "diarized")
            transcripts_dir = os.path.join(output_dir, "transcripts")
            os.makedirs(diarized_dir, exist_ok=True)
            os.makedirs(transcripts_dir, exist_ok=True)
            diarized_text = generate_diarized_text(transcription_data)
            diarized_file = os.path.join(diarized_dir, f"{input_filename}.txt")
            with open(diarized_file, 'w', encoding='utf-8') as f:
                f.write(diarized_text)
            write_role_extracts_if_present(diarized_text, output_dir, input_filename, print_lock)
            top_speakers = get_top_speakers(transcription_data, top_n=2)
            filtered_text = generate_filtered_transcript(transcription_data, top_speakers)
            filtered_file = os.path.join(transcripts_dir, f"{input_filename}.txt")
            with open(filtered_file, 'w', encoding='utf-8') as f:
                f.write(filtered_text)
            with print_lock:
                print(f"✅ Completed: {os.path.basename(audio_file)}")
                print(f"   Diarized output: {diarized_file}")
                print(f"   Filtered output (top 2 speakers): {filtered_file}")
                print(f"   Top speakers: {top_speakers}")
                print(f"   Length: {len(filtered_text)} characters")
            return {
                "success": True,
                "file": audio_file,
                "output": filtered_file,
                "diarized_output": diarized_file,
                "length": len(filtered_text),
                "skipped": False,
                "top_speakers": top_speakers
            }
        else:
            chunk_files = chunk_audio_file(audio_file, max_size_mb)
            all_transcripts = []
            chunk_count = len(chunk_files)
            for i, chunk_file in enumerate(chunk_files):
                if chunk_count > 1:
                    with print_lock:
                        print(f"   📄 Processing chunk {i+1}/{chunk_count}: {os.path.basename(chunk_file)}")
                with open(chunk_file, "rb") as file:
                    transcription = client.audio.transcriptions.create(
                        file=(chunk_file, file.read()),
                        model=model,
                        response_format="verbose_json",
                    )
                all_transcripts.append(transcription.text)
            combined_transcript = "\n\n".join(all_transcripts)
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(combined_transcript)
            cleanup_temp_chunks(chunk_files, audio_file)
            with print_lock:
                print(f"✅ Completed: {os.path.basename(audio_file)}")
                print(f"   Output: {output_file}")
                print(f"   Length: {len(combined_transcript)} characters")
                if chunk_count > 1:
                    print(f"   Chunks processed: {chunk_count}")
            return {
                "success": True, 
                "file": audio_file, 
                "output": output_file, 
                "length": len(combined_transcript),
                "skipped": False,
                "chunks": chunk_count
            }
    except Exception as e:
        if 'chunk_files' in locals():
            cleanup_temp_chunks(chunk_files, audio_file)
        with print_lock:
            print(f"❌ Failed: {os.path.basename(audio_file)} - {e}")
        return {"success": False, "file": audio_file, "error": str(e)}

def transcribe_audio(audio_file: str, output_dir: str = None, model: str = "whisper-large-v3-turbo", output_filename: str = None, max_size_mb: int = 25, use_diarization: bool = False, use_slam: bool = False, use_universal: bool = False, use_elevenlabs: bool = False) -> str:
    """
    Transcribe audio file using Whisper Turbo on Groq,
    Fireworks AI with diarization,
    AssemblyAI SLAM/Universal (optionally with diarization),
    or ElevenLabs scribe_v1.
    """
    api_key = load_environment(use_diarization, use_slam, use_universal, use_elevenlabs)
    client = None
    if use_elevenlabs:
        if not ELEVENLABS_AVAILABLE:
            print("❌ ElevenLabs package not installed. Run: pip install elevenlabs")
            sys.exit(1)
        print("✅ Using ElevenLabs scribe_v1 model")
    elif use_slam or use_universal:
        if not ASSEMBLYAI_AVAILABLE:
            print("❌ AssemblyAI package not installed. Run: pip install -U assemblyai")
            sys.exit(1)
        model_name = "Universal" if use_universal else "SLAM"
        print(f"✅ Using AssemblyAI {model_name} model")
    elif not use_diarization:
        try:
            client = Groq(api_key=api_key)
            print("✅ Connected to Groq API")
        except Exception as e:
            print(f"❌ Failed to connect to Groq: {e}")
            sys.exit(1)
    else:
        print("✅ Using Fireworks AI with diarization")
    if not os.path.exists(audio_file):
        print(f"❌ Audio file not found: {audio_file}")
        sys.exit(1)
    print(f"🎵 Transcribing: {audio_file}")
    if use_elevenlabs:
        print(f"   Model: ElevenLabs scribe_v1")
        print(f"   Diarization: {'enabled' if use_diarization else 'disabled'}")
    elif use_slam or use_universal:
        model_name = "Universal" if use_universal else "SLAM-1"
        print(f"   Model: AssemblyAI {model_name}")
        print(f"   Diarization: {'enabled' if use_diarization else 'disabled'}")
    elif not use_diarization:
        print(f"   Model: {model}")
        print(f"   Max chunk size: {max_size_mb} MB")
    file_size = os.path.getsize(audio_file)
    print(f"   File size: {file_size / (1024*1024):.1f} MB")
    if output_dir is None:
        output_dir = "data/football/audio/whisper-transcripts"
    os.makedirs(output_dir, exist_ok=True)
    input_filename = Path(audio_file).stem
    diarize_assemblyai = use_diarization if (use_slam or use_universal) else False
    if use_elevenlabs or use_slam or use_universal or use_diarization:
        diarized_dir = os.path.join(output_dir, "diarized")
        transcripts_dir = os.path.join(output_dir, "transcripts")
        os.makedirs(diarized_dir, exist_ok=True)
        os.makedirs(transcripts_dir, exist_ok=True)
        output_file = os.path.join(transcripts_dir, f"{input_filename}.txt")
    else:
        if output_filename:
            if not output_filename.endswith('.txt'):
                output_filename += '.txt'
            output_file = os.path.join(output_dir, output_filename)
        else:
            output_file = os.path.join(output_dir, f"{input_filename}.txt")
    try:
        if use_elevenlabs:
            print(f"🔄 Transcribing with ElevenLabs scribe_v1 model... (Diarization: {'enabled' if use_diarization else 'disabled'})")
            transcription = transcribe_with_elevenlabs(audio_file, api_key, diarize=use_diarization)
            if use_diarization:
                diarized_text = generate_elevenlabs_diarized_text(transcription)
                diarized_file = os.path.join(diarized_dir, f"{input_filename}.txt")
                with open(diarized_file, 'w', encoding='utf-8') as f:
                    f.write(diarized_text)
                write_role_extracts_if_present(diarized_text, output_dir, input_filename)
                top_speakers = get_elevenlabs_top_speakers(transcription, top_n=2)
                filtered_text = generate_elevenlabs_filtered_transcript(transcription, top_speakers)
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(filtered_text)
                print(f"✅ Transcription completed!")
                print(f"   Diarized output (all speakers): {diarized_file}")
                print(f"   Filtered output (top 2 speakers): {output_file}")
                print(f"   Top speakers: {top_speakers}")
                print(f"   Filtered length: {len(filtered_text)} characters")
                preview = filtered_text[:200].replace('\n', ' ')
                print(f"   Preview: {preview}...")
            else:
                plain_text = getattr(transcription, 'text', None) or transcription.get('text', '')
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(plain_text)
                print(f"✅ Transcription completed!")
                print(f"   Output: {output_file}")
                print(f"   Length: {len(plain_text)} characters")
                preview = plain_text[:200].replace('\n', ' ')
                print(f"   Preview: {preview}...")
        elif use_slam or use_universal:
            model_name = "Universal" if use_universal else "SLAM"
            print(f"🔄 Transcribing with AssemblyAI {model_name} model... (Diarization: {'enabled' if diarize_assemblyai else 'disabled'})")
            transcript = transcribe_with_assemblyai(audio_file, api_key, use_universal=use_universal, diarize=diarize_assemblyai)
            if diarize_assemblyai:
                diarized_text = generate_assemblyai_diarized_text(transcript)
                diarized_file = os.path.join(diarized_dir, f"{input_filename}.txt")
                with open(diarized_file, 'w', encoding='utf-8') as f:
                    f.write(diarized_text)
                write_role_extracts_if_present(diarized_text, output_dir, input_filename)
                top_speakers = get_assemblyai_top_speakers(transcript, top_n=2)
                filtered_text = generate_assemblyai_filtered_transcript(transcript, top_speakers)
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(filtered_text)
                print(f"✅ Transcription completed!")
                print(f"   Diarized output (all speakers): {diarized_file}")
                print(f"   Filtered output (top 2 speakers): {output_file}")
                print(f"   Top speakers: {top_speakers}")
                print(f"   Filtered length: {len(filtered_text)} characters")
                preview = filtered_text[:200].replace('\n', ' ')
                print(f"   Preview: {preview}...")
            else:
                filtered_text = transcript.text or ""
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(filtered_text)
                print(f"✅ Transcription completed!")
                print(f"   Output: {output_file}")
                print(f"   Length: {len(filtered_text)} characters")
                preview = filtered_text[:200].replace('\n', ' ')
                print(f"   Preview: {preview}...")
        elif use_diarization:
            print(f"🔄 Transcribing with Fireworks AI (diarization enabled)...")
            transcription_data = transcribe_with_fireworks(audio_file, api_key)
            diarized_text = generate_diarized_text(transcription_data)
            diarized_file = os.path.join(diarized_dir, f"{input_filename}.txt")
            with open(diarized_file, 'w', encoding='utf-8') as f:
                f.write(diarized_text)
            write_role_extracts_if_present(diarized_text, output_dir, input_filename)
            top_speakers = get_top_speakers(transcription_data, top_n=2)
            filtered_text = generate_filtered_transcript(transcription_data, top_speakers)
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(filtered_text)
            print(f"✅ Transcription completed!")
            print(f"   Diarized output (all speakers): {diarized_file}")
            print(f"   Filtered output (top 2 speakers): {output_file}")
            print(f"   Top speakers: {top_speakers}")
            print(f"   Filtered length: {len(filtered_text)} characters")
            preview = filtered_text[:200].replace('\n', ' ')
            print(f"   Preview: {preview}...")
        else:
            chunk_files = chunk_audio_file(audio_file, max_size_mb)
            all_transcripts = []
            chunk_count = len(chunk_files)
            print(f"🔄 Transcribing with {model}...")
            if chunk_count > 1:
                print(f"   Processing {chunk_count} chunks...")
            for i, chunk_file in enumerate(chunk_files):
                if chunk_count > 1:
                    print(f"   📄 Processing chunk {i+1}/{chunk_count}: {os.path.basename(chunk_file)}")
                with open(chunk_file, "rb") as file:
                    transcription = client.audio.transcriptions.create(
                        file=(chunk_file, file.read()),
                        model=model,
                        response_format="verbose_json",
                    )
                all_transcripts.append(transcription.text)
            combined_transcript = "\n\n".join(all_transcripts)
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(combined_transcript)
            cleanup_temp_chunks(chunk_files, audio_file)
            print(f"✅ Transcription completed!")
            print(f"   Output: {output_file}")
            print(f"   Length: {len(combined_transcript)} characters")
            if chunk_count > 1:
                print(f"   Chunks processed: {chunk_count}")
            preview = combined_transcript[:200].replace('\n', ' ')
            print(f"   Preview: {preview}...")
        return output_file
    except Exception as e:
        if 'chunk_files' in locals():
            cleanup_temp_chunks(chunk_files, audio_file)
        print(f"❌ Transcription failed: {e}")
        sys.exit(1)

def transcribe_batch(
    input_path: str,
    output_dir: str,
    model: str,
    max_workers: int = 4,
    max_size_mb: int = 25,
    use_diarization: bool = False,
    use_slam: bool = False,
    use_universal: bool = False,
    use_elevenlabs: bool = False
) -> dict:
    """
    Transcribe multiple audio files from a directory using concurrent processing.
    Diarization is now optional for AssemblyAI-based models (SLAM/Universal) and ElevenLabs.
    """
    api_key = load_environment(use_diarization, use_slam, use_universal, use_elevenlabs)
    client = None
    if use_elevenlabs:
        if not ELEVENLABS_AVAILABLE:
            print("❌ ElevenLabs package not installed. Run: pip install elevenlabs")
            sys.exit(1)
        print("✅ Using ElevenLabs scribe_v1 model")
        print(f"🎙️  Diarization: {'enabled' if use_diarization else 'disabled'} (for ElevenLabs)")
    elif use_slam or use_universal:
        if not ASSEMBLYAI_AVAILABLE:
            print("❌ AssemblyAI package not installed. Run: pip install -U assemblyai")
            sys.exit(1)
        model_name = "Universal" if use_universal else "SLAM"
        print(f"✅ Using AssemblyAI {model_name} model")
        print(f"🎙️  Diarization: {'enabled' if use_diarization else 'disabled'} (for AssemblyAI)")
    elif not use_diarization:
        try:
            client = Groq(api_key=api_key)
            print("✅ Connected to Groq API")
        except Exception as e:
            print(f"❌ Failed to connect to Groq: {e}")
            sys.exit(1)
    else:
        print("✅ Using Fireworks AI with diarization")
    if not os.path.exists(input_path):
        print(f"❌ Input path not found: {input_path}")
        sys.exit(1)
    if os.path.isfile(input_path):
        audio_files = [input_path]
        print(f"🎵 Single file mode: {os.path.basename(input_path)}")
    else:
        audio_files = get_audio_files(input_path)
        if not audio_files:
            print(f"❌ No audio files found in: {input_path}")
            sys.exit(1)
        print(f"🎵 Batch mode: Found {len(audio_files)} audio files")
    os.makedirs(output_dir, exist_ok=True)
    print(f"📁 Output directory: {output_dir}")
    if use_elevenlabs:
        print(f"🤖 Model: ElevenLabs scribe_v1")
        print(f"🎙️  Diarization: {'enabled' if use_diarization else 'disabled'} (top 2 speakers will be extracted if enabled)")
        if max_workers > 1:
            print(f"⚠️  Note: ElevenLabs may rate-limit with multiple workers. Consider --workers 1 if you see failures.")
    elif use_slam or use_universal:
        model_name = "Universal" if use_universal else "SLAM-1"
        print(f"🤖 Model: AssemblyAI {model_name}")
        print(f"🎙️  Diarization: {'enabled' if use_diarization else 'disabled'} (top 2 speakers will be extracted if enabled)")
        if max_workers > 1:
            print(f"⚠️  Note: AssemblyAI may rate-limit with multiple workers. Consider --workers 1 if you see failures.")
    elif not use_diarization:
        print(f"🤖 Model: {model}")
        print(f"📦 Max chunk size: {max_size_mb} MB")
    else:
        print(f"🎙️  Diarization: enabled (top 2 speakers will be extracted)")
        if max_workers > 1:
            print(f"⚠️  Note: Fireworks API may rate-limit with multiple workers. Consider --workers 1 if you see failures.")
    print(f"⚡ Max workers: {max_workers}")
    print("=" * 60)
    results = []
    print_lock = Lock()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(
                transcribe_single_audio,
                audio_file,
                output_dir,
                model,
                client,
                print_lock,
                max_size_mb,
                use_diarization,
                api_key,
                use_slam,
                use_universal,
                use_elevenlabs
            ): audio_file
            for audio_file in audio_files
        }
        for future in as_completed(future_to_file):
            result = future.result()
            results.append(result)
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
    parser = argparse.ArgumentParser(description='Transcribe audio files using Whisper Turbo on Groq, Fireworks AI with diarization, AssemblyAI, or ElevenLabs')
    parser.add_argument('input_path', nargs='?', help='Path to audio file or directory containing audio files')
    parser.add_argument('-o', '--output-dir', default='data/football/audio/whisper-transcripts',
                       help='Output directory for transcripts (default: data/football/audio/whisper-transcripts)')
    parser.add_argument('-m', '--model', default='whisper-large-v3-turbo',
                       choices=['whisper-large-v3-turbo', 'whisper-large-v3',],
                       help='Whisper model to use (default: whisper-large-v3-turbo)')
    parser.add_argument('-f', '--output-filename', 
                       help='Output filename (without .txt extension, only for single file mode)')
    parser.add_argument('-w', '--workers', type=int, default=4,
                       help='Number of concurrent workers for batch processing (default: 4)')
    parser.add_argument('-s', '--max-size', type=int, default=25,
                       help='Maximum file size in MB before chunking (default: 25, only for Groq)')
    parser.add_argument('--diarize', action='store_true',
                       help='Use speaker diarization (for FireworksAI, and for AssemblyAI with --slam or --universal)')
    parser.add_argument('--slam', action='store_true',
                       help='Use AssemblyAI SLAM model with NFL keyterms (optionally with --diarize to enable diarization)')
    parser.add_argument('--universal', action='store_true',
                       help='Use AssemblyAI Universal model with NFL keyterms (optionally with --diarize to enable diarization)')
    parser.add_argument('--elevenlabs', action='store_true',
                       help='Use ElevenLabs scribe_v1 model (optionally with --diarize to enable diarization)')
    parser.add_argument('--preview', action='store_true', 
                       help='Show preview of transcription without saving (single file only)')
    parser.add_argument('--single', action='store_true',
                       help='Force single file mode even if input is a directory')
    parser.add_argument('--all-prime-time', action='store_true',
                       help='[DEPRECATED] Use nfl_pipeline.py instead. Process all mp3 dirs in data/football/')
    parser.add_argument('--data-dir', default='data/football',
                       help='Base data directory for --all-prime-time (default: data/football)')
    args = parser.parse_args()
    if args.all_prime_time:
        print("🏈 Processing all MP3 directories for transcription...")
        print(f"📁 Data directory: {args.data_dir}")
        
        # Dynamically discover subdirectories with mp3 folders
        from pathlib import Path
        
        data_path = Path(args.data_dir)
        prime_time_configs = []
        
        # Find all directories that have an mp3 subdirectory
        for subdir in sorted(data_path.iterdir()):
            if subdir.is_dir() and subdir.name != 'csvs':
                mp3_dir = subdir / 'mp3'
                if mp3_dir.exists() and mp3_dir.is_dir():
                    # Check if there are any audio files
                    audio_files = get_audio_files(str(mp3_dir))
                    if audio_files:
                        stem = subdir.name
                        name_parts = stem.replace('_', '-').split('-')
                        name = ' '.join(part.upper() if len(part) <= 3 else part.title() for part in name_parts)
                        abbrev = ''.join(part[0].upper() for part in name_parts if part)[:4]
                        if len(abbrev) < 2:
                            abbrev = stem[:4].upper()
                        
                        prime_time_configs.append({
                            'name': name,
                            'abbrev': abbrev,
                            'input_dir': str(mp3_dir),
                            'output_dir': str(subdir)
                        })
        
        if not prime_time_configs:
            print(f"\n❌ No mp3 directories found in: {args.data_dir}")
            print(f"   Expected structure: {args.data_dir}/<name>/mp3/*.mp3")
            return
        
        print(f"\n📋 Found {len(prime_time_configs)} directories with audio files:")
        for config in prime_time_configs:
            audio_count = len(get_audio_files(config['input_dir']))
            print(f"   • {config['name']}: {audio_count} files")
        all_results = []
        for config in prime_time_configs:
            print(f"\n{'='*70}")
            print(f"🏈 Processing {config['name']} ({config['abbrev']})")
            print(f"{'='*70}")
            print(f"📁 Input: {config['input_dir']}")
            print(f"📁 Output: {config['output_dir']}")
            if not os.path.exists(config['input_dir']):
                print(f"   ⚠️  Input directory not found, skipping...")
                continue
            audio_files = get_audio_files(config['input_dir'])
            if not audio_files:
                print(f"   ⚠️  No audio files found, skipping...")
                continue
            print(f"   🎵 Found {len(audio_files)} audio files")
            os.makedirs(config['output_dir'], exist_ok=True)
            result = transcribe_batch(
                config['input_dir'],
                config['output_dir'],
                args.model,
                args.workers,
                args.max_size,
                args.diarize,
                args.slam,
                args.universal,
                args.elevenlabs
            )
            all_results.append({
                'league': config['abbrev'],
                'result': result
            })
        print(f"\n{'='*70}")
        print(f"🎉 All Prime-Time Transcription Complete!")
        print(f"{'='*70}")
        total_successful = sum(r['result']['successful'] for r in all_results)
        total_skipped = sum(r['result']['skipped'] for r in all_results)
        total_failed = sum(r['result']['failed'] for r in all_results)
        print(f"📊 Overall Summary:")
        print(f"   Successfully transcribed: {total_successful}")
        print(f"   Skipped (already exist): {total_skipped}")
        print(f"   Failed: {total_failed}")
        for result_info in all_results:
            league = result_info['league']
            result = result_info['result']
            print(f"\n   {league}: {result['successful']} successful, {result['skipped']} skipped, {result['failed']} failed")
        return
    if not args.input_path:
        print("❌ No input path provided. Use --help for usage information.")
        return
    input_path = os.path.abspath(args.input_path)
    print("🎤 WHISPER TURBO TRANSCRIPTION")
    print("=" * 50)
    print(f"Input path: {input_path}")
    print(f"Output dir: {args.output_dir}")
    print(f"Max file size: {args.max_size} MB")
    use_batch = os.path.isdir(input_path) and not args.single
    if use_batch:
        print(f"\n🔄 BATCH MODE")
        if args.preview:
            print("⚠️  Preview mode not supported for batch processing")
            return
        results = transcribe_batch(input_path, args.output_dir, args.model, args.workers, args.max_size, args.diarize, args.slam, args.universal, args.elevenlabs)
        print(f"\n📊 Final Summary:")
        print(f"   Total files processed: {results['total']}")
        print(f"   Successful transcriptions: {results['successful']}")
        print(f"   Files skipped (already exist): {results['skipped']}")
        print(f"   Failed transcriptions: {results['failed']}")
    else:
        if not os.path.exists(input_path):
            print(f"❌ Input file not found: {input_path}")
            return
        if args.preview:
            print("\n🔍 PREVIEW MODE - Transcription will not be saved")
        output_file = transcribe_audio(input_path, args.output_dir, args.model, args.output_filename, args.max_size, args.diarize, args.slam, args.universal, args.elevenlabs)
        if not args.preview:
            print(f"\n📄 Transcript saved to: {output_file}")
            if os.path.exists(output_file):
                file_size = os.path.getsize(output_file)
                print(f"   File size: {file_size} bytes")
        else:
            print(f"\n🔍 Preview completed (not saved)")


if __name__ == '__main__':
    main()
