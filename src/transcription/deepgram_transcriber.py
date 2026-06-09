#!/usr/bin/env python3
"""
Deepgram Nova-3 Transcriber

Lightweight transcription CLI for local audio files with:
- Deepgram Nova-3
- Speaker diarization enabled
- Single-file and batch-directory modes

Outputs:
- diarized/<name>.txt   (all speakers)
- transcripts/<name>.txt (top 2 speakers by word count)
"""

import argparse
import glob
import mimetypes
import os
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

DEEPGRAM_LISTEN_URL = "https://api.deepgram.com/v1/listen"
MAX_FILE_SIZE_BYTES = 2 * 1024 * 1024 * 1024  # 2 GB


def load_environment() -> str:
    """Load .env and return DEEPGRAM_API_KEY."""
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)

    api_key = os.getenv("DEEPGRAM_API_KEY")
    if not api_key:
        print("ERROR: DEEPGRAM_API_KEY not found in environment variables.")
        print("Add DEEPGRAM_API_KEY to your .env file.")
        sys.exit(1)

    return api_key


def get_audio_files(directory: str) -> List[str]:
    """Return all supported audio files in a directory recursively."""
    audio_extensions = ["*.mp3", "*.wav", "*.m4a", "*.flac", "*.aac", "*.ogg", "*.wma", "*.mp4", "*.webm", "*.mkv"]
    audio_files: List[str] = []
    for ext in audio_extensions:
        audio_files.extend(glob.glob(os.path.join(directory, "**", ext), recursive=True))
    return sorted(audio_files)


def _word_text(word_obj: Dict[str, Any]) -> str:
    return (word_obj.get("punctuated_word") or word_obj.get("word") or "").strip()


def parse_deepgram_response(response_data: Dict[str, Any]) -> Tuple[str, str, List[int]]:
    """
    Return:
    - diarized text (all speakers)
    - filtered transcript text (top 2 speakers by word count)
    - top speaker ids
    """
    channels = response_data.get("results", {}).get("channels", [])
    alternatives = channels[0].get("alternatives", []) if channels else []
    alt = alternatives[0] if alternatives else {}
    words = alt.get("words", []) or []
    utterances = response_data.get("results", {}).get("utterances", []) or []

    diarized_lines: List[str] = []
    if utterances:
        for utt in utterances:
            speaker = utt.get("speaker")
            transcript = (utt.get("transcript") or "").strip()
            if transcript:
                diarized_lines.append(f"Speaker {speaker}: {transcript}")
    else:
        current_speaker: Optional[int] = None
        current_words: List[str] = []
        words_sorted = sorted(words, key=lambda w: float(w.get("start", 0.0)))
        for w in words_sorted:
            speaker = w.get("speaker")
            token = _word_text(w)
            if not token:
                continue
            if speaker != current_speaker:
                if current_words:
                    diarized_lines.append(f"Speaker {current_speaker}: {' '.join(current_words)}")
                current_speaker = speaker
                current_words = [token]
            else:
                current_words.append(token)
        if current_words:
            diarized_lines.append(f"Speaker {current_speaker}: {' '.join(current_words)}")

    speaker_counts: Counter[int] = Counter()
    for w in words:
        speaker = w.get("speaker")
        if speaker is not None:
            speaker_counts[int(speaker)] += 1
    top_speakers = [speaker for speaker, _count in speaker_counts.most_common(2)]

    filtered_text = ""
    if top_speakers and words:
        words_sorted = sorted(words, key=lambda w: float(w.get("start", 0.0)))
        filtered_tokens = [
            _word_text(w)
            for w in words_sorted
            if w.get("speaker") is not None and int(w.get("speaker")) in top_speakers
        ]
        filtered_text = " ".join(token for token in filtered_tokens if token).strip()
    elif utterances:
        filtered_text = " ".join((u.get("transcript") or "").strip() for u in utterances if u.get("transcript")).strip()
    else:
        filtered_text = (alt.get("transcript") or "").strip()

    diarized_text = "\n\n".join(diarized_lines).strip()
    if not diarized_text:
        diarized_text = filtered_text

    return diarized_text, filtered_text, top_speakers


def transcribe_with_deepgram(
    audio_file: str,
    api_key: str,
    model: str = "nova-3",
    max_retries: int = 5,
    timeout_seconds: int = 900,
    keyterms: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Transcribe local audio file with Deepgram Nova-3 using diarization.
    Docs:
    - https://developers.deepgram.com/docs/pre-recorded-audio
    - https://developers.deepgram.com/docs/diarization
    """
    params: List[Tuple[str, str]] = [
        ("model", model),
        ("smart_format", "true"),
        ("punctuate", "true"),
        ("diarize", "true"),
        ("utterances", "true"),
    ]
    if keyterms:
        for term in keyterms:
            cleaned = term.strip()
            if cleaned:
                params.append(("keyterm", cleaned))
    content_type = mimetypes.guess_type(audio_file)[0] or "audio/*"
    headers = {
        "Authorization": f"Token {api_key}",
        "Content-Type": content_type,
    }

    for attempt in range(max_retries):
        try:
            with open(audio_file, "rb") as audio_data:
                response = requests.post(
                    DEEPGRAM_LISTEN_URL,
                    headers=headers,
                    params=params,
                    data=audio_data,
                    timeout=timeout_seconds,
                )
        except requests.RequestException as exc:
            if attempt < max_retries - 1:
                wait_seconds = min((2**attempt) * 2, 60)
                time.sleep(wait_seconds)
                continue
            raise RuntimeError(f"Deepgram request failed: {exc}") from exc

        if response.status_code == 200:
            return response.json()

        if response.status_code in {429, 500, 502, 503, 504}:
            wait_seconds = min((2**attempt) * 2, 60)
            if attempt < max_retries - 1:
                time.sleep(wait_seconds)
                continue

        response_text = response.text[:500]
        raise RuntimeError(f"Deepgram API error {response.status_code}: {response_text}")

    raise RuntimeError(f"Deepgram API failed after {max_retries} retries")


def _default_output_paths(output_dir: str, input_stem: str) -> Tuple[str, str]:
    diarized_dir = os.path.join(output_dir, "diarized")
    transcripts_dir = os.path.join(output_dir, "transcripts")
    os.makedirs(diarized_dir, exist_ok=True)
    os.makedirs(transcripts_dir, exist_ok=True)
    return (
        os.path.join(diarized_dir, f"{input_stem}.txt"),
        os.path.join(transcripts_dir, f"{input_stem}.txt"),
    )


def transcribe_single_audio(
    audio_file: str,
    output_dir: str,
    api_key: str,
    print_lock: Lock,
    model: str = "nova-3",
    max_retries: int = 5,
    output_filename: Optional[str] = None,
) -> Dict[str, Any]:
    """Transcribe one file and write diarized + filtered outputs."""
    if not os.path.exists(audio_file):
        return {"success": False, "file": audio_file, "error": "File not found"}

    if os.path.getsize(audio_file) > MAX_FILE_SIZE_BYTES:
        return {"success": False, "file": audio_file, "error": "File exceeds Deepgram 2GB limit"}

    stem = Path(audio_file).stem
    diarized_file, transcript_file = _default_output_paths(output_dir, stem)

    if output_filename:
        name = output_filename if output_filename.endswith(".txt") else f"{output_filename}.txt"
        transcript_file = os.path.join(output_dir, "transcripts", name)

    if os.path.exists(diarized_file) and os.path.exists(transcript_file):
        with print_lock:
            print(f"Skipping {os.path.basename(audio_file)} (outputs already exist)")
        return {
            "success": True,
            "file": audio_file,
            "output": transcript_file,
            "diarized_output": diarized_file,
            "skipped": True,
        }

    with print_lock:
        print(f"Transcribing {os.path.basename(audio_file)}")

    try:
        response_data = transcribe_with_deepgram(
            audio_file=audio_file,
            api_key=api_key,
            model=model,
            max_retries=max_retries,
        )
        diarized_text, filtered_text, top_speakers = parse_deepgram_response(response_data)

        with open(diarized_file, "w", encoding="utf-8") as f:
            f.write(diarized_text)

        with open(transcript_file, "w", encoding="utf-8") as f:
            f.write(filtered_text)

        with print_lock:
            print(f"Completed {os.path.basename(audio_file)}")
            print(f"  Diarized: {diarized_file}")
            print(f"  Transcript: {transcript_file}")
            if top_speakers:
                print(f"  Top speakers: {top_speakers}")

        return {
            "success": True,
            "file": audio_file,
            "output": transcript_file,
            "diarized_output": diarized_file,
            "skipped": False,
            "top_speakers": top_speakers,
            "length": len(filtered_text),
        }
    except Exception as exc:
        with print_lock:
            print(f"Failed {os.path.basename(audio_file)}: {exc}")
        return {"success": False, "file": audio_file, "error": str(exc)}


def transcribe_batch(
    input_dir: str,
    output_dir: str,
    model: str = "nova-3",
    workers: int = 4,
    max_retries: int = 5,
) -> Dict[str, Any]:
    """Batch transcribe all audio files in a directory."""
    api_key = load_environment()
    audio_files = get_audio_files(input_dir)
    if not audio_files:
        print(f"No audio files found in: {input_dir}")
        return {"total": 0, "successful": 0, "failed": 0, "skipped": 0, "results": []}

    os.makedirs(output_dir, exist_ok=True)
    print_lock = Lock()
    results: List[Dict[str, Any]] = []

    print(f"Found {len(audio_files)} audio files")
    print(f"Using {workers} workers")

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                transcribe_single_audio,
                audio_file,
                output_dir,
                api_key,
                print_lock,
                model,
                max_retries,
            ): audio_file
            for audio_file in audio_files
        }
        for future in as_completed(futures):
            results.append(future.result())

    successful = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]
    skipped = [r for r in successful if r.get("skipped")]

    print("\nBatch Summary")
    print(f"  Successful: {len(successful)}")
    print(f"  Skipped: {len(skipped)}")
    print(f"  Failed: {len(failed)}")
    print(f"  Total: {len(results)}")

    if failed:
        print("Failed files:")
        for r in failed:
            print(f"  - {os.path.basename(r['file'])}: {r['error']}")

    return {
        "total": len(results),
        "successful": len(successful),
        "failed": len(failed),
        "skipped": len(skipped),
        "results": results,
    }


def preview_transcription(audio_file: str, model: str = "nova-3", max_retries: int = 5) -> None:
    """Run transcription and print preview without writing output files."""
    if not os.path.exists(audio_file):
        print(f"Input file not found: {audio_file}")
        return

    api_key = load_environment()
    response_data = transcribe_with_deepgram(
        audio_file=audio_file,
        api_key=api_key,
        model=model,
        max_retries=max_retries,
    )
    diarized_text, filtered_text, top_speakers = parse_deepgram_response(response_data)

    print("Preview (filtered transcript):")
    print((filtered_text[:500] + "...") if len(filtered_text) > 500 else filtered_text)
    if top_speakers:
        print(f"Top speakers: {top_speakers}")
    print("\nPreview (diarized):")
    print((diarized_text[:700] + "...") if len(diarized_text) > 700 else diarized_text)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Transcribe audio with Deepgram Nova-3 using diarization (single file or batch directory)."
    )
    parser.add_argument("input_path", nargs="?", help="Path to audio file or directory")
    parser.add_argument(
        "-o",
        "--output-dir",
        default="data/audio/deepgram-transcripts",
        help="Output directory (default: data/audio/deepgram-transcripts)",
    )
    parser.add_argument(
        "-m",
        "--model",
        default="nova-3",
        help="Deepgram model (default: nova-3)",
    )
    parser.add_argument(
        "-f",
        "--output-filename",
        help="Output filename for single-file transcript (without .txt is allowed)",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=4,
        help="Concurrent workers for batch mode (default: 4)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Max retry attempts on transient API failures (default: 5)",
    )
    parser.add_argument("--single", action="store_true", help="Force single-file mode")
    parser.add_argument("--preview", action="store_true", help="Preview transcription without saving (single file only)")
    args = parser.parse_args()

    if not args.input_path:
        print("No input path provided. Use --help for usage details.")
        return

    input_path = os.path.abspath(args.input_path)
    use_batch = os.path.isdir(input_path) and not args.single

    print("DEEPGRAM NOVA-3 TRANSCRIPTION")
    print(f"Input path: {input_path}")
    print(f"Output dir: {args.output_dir}")
    print(f"Model: {args.model}")

    if use_batch:
        if args.preview:
            print("Preview mode is only supported for single-file mode.")
            return
        results = transcribe_batch(
            input_dir=input_path,
            output_dir=args.output_dir,
            model=args.model,
            workers=args.workers,
            max_retries=args.max_retries,
        )
        print("\nFinal Summary")
        print(f"  Total: {results['total']}")
        print(f"  Successful: {results['successful']}")
        print(f"  Skipped: {results['skipped']}")
        print(f"  Failed: {results['failed']}")
        return

    if not os.path.exists(input_path):
        print(f"Input file not found: {input_path}")
        return

    if args.preview:
        preview_transcription(input_path, model=args.model, max_retries=args.max_retries)
        return

    os.makedirs(args.output_dir, exist_ok=True)
    api_key = load_environment()
    print_lock = Lock()
    result = transcribe_single_audio(
        audio_file=input_path,
        output_dir=args.output_dir,
        api_key=api_key,
        print_lock=print_lock,
        model=args.model,
        max_retries=args.max_retries,
        output_filename=args.output_filename,
    )

    if not result["success"]:
        print(f"Transcription failed: {result['error']}")
        sys.exit(1)

    print("\nSaved outputs")
    print(f"  Transcript: {result['output']}")
    print(f"  Diarized: {result['diarized_output']}")


if __name__ == "__main__":
    main()
