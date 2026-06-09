#!/usr/bin/env python3
"""
NBA Deepgram Wrapper

Wrapper around src/deepgram_transcriber.py for NBA announcer folders in data/nba.
It resolves audio/transcript directory structure by announcer name, filters short
audio files, transcribes with Deepgram Nova-3 + diarization, and writes:

- transcripts/<announcer-slug>/transcripts/<game>.txt
- transcripts/<announcer-slug>/diarized/<game>.txt
- transcripts/<announcer-slug>/play_by_play/<game>.txt
- transcripts/<announcer-slug>/color_commentary/<game>.txt
"""

import argparse
import csv
import os
import re
import subprocess
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Dict, List, Optional, Tuple

# Make project imports work when invoked as: python src/nba/deepgram_nba_transcriber.py
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.transcription.deepgram_transcriber import load_environment, transcribe_with_deepgram


GAME_STEM_PATTERN = re.compile(r"^(?P<date>\d{4}-\d{2}-\d{2})_(?P<away>[a-z]{2,3})-at-(?P<home>[a-z]{2,3})$")


@dataclass
class GameAnnouncerMeta:
    play_by_play: Optional[str]
    color_commentator: Optional[str]


@dataclass
class Segment:
    speaker: Optional[int]
    start: float
    text: str


def normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def announcer_slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def best_name_case(value: str) -> str:
    return " ".join(part.capitalize() for part in re.split(r"[_\-\s]+", value.strip()) if part)


def resolve_audio_dir(audio_base: Path, announcer_name: str) -> Path:
    if not audio_base.exists():
        raise FileNotFoundError(f"Audio base directory does not exist: {audio_base}")

    target = normalize_key(announcer_name)
    candidates: List[Tuple[int, Path]] = []
    for entry in audio_base.iterdir():
        if not entry.is_dir():
            continue
        if normalize_key(entry.name) == target:
            mp3_count = len(list(entry.glob("*.mp3")))
            candidates.append((mp3_count, entry))

    if not candidates:
        raise FileNotFoundError(
            f"Could not find announcer audio directory for '{announcer_name}' under {audio_base}"
        )

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def load_announcer_metadata(csv_path: Path) -> Dict[Tuple[str, str, str], GameAnnouncerMeta]:
    if not csv_path.exists():
        return {}

    index: Dict[Tuple[str, str, str], GameAnnouncerMeta] = {}
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            date = (row.get("date") or "").strip()
            away = (row.get("away_code") or "").strip().lower()
            home = (row.get("home_code") or "").strip().lower()
            if not date or not away or not home:
                continue
            index[(date, away, home)] = GameAnnouncerMeta(
                play_by_play=(row.get("play_by_play") or "").strip() or None,
                color_commentator=(row.get("color_commentator") or "").strip() or None,
            )
    return index


def parse_game_key_from_stem(stem: str) -> Optional[Tuple[str, str, str]]:
    match = GAME_STEM_PATTERN.match(stem)
    if not match:
        return None
    return (
        match.group("date"),
        match.group("away").lower(),
        match.group("home").lower(),
    )


def probe_duration_seconds(audio_path: Path) -> Optional[float]:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(audio_path),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        return None
    value = proc.stdout.strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def extract_segments(response_data: dict) -> List[Segment]:
    results = response_data.get("results", {})
    utterances = results.get("utterances", []) or []
    if utterances:
        segments: List[Segment] = []
        for utt in utterances:
            text = (utt.get("transcript") or "").strip()
            if not text:
                continue
            speaker = utt.get("speaker")
            speaker_int = int(speaker) if speaker is not None else None
            start = float(utt.get("start", 0.0) or 0.0)
            segments.append(Segment(speaker=speaker_int, start=start, text=text))
        return sorted(segments, key=lambda s: s.start)

    channels = results.get("channels", [])
    alternatives = channels[0].get("alternatives", []) if channels else []
    alt = alternatives[0] if alternatives else {}
    words = alt.get("words", []) or []
    if not words:
        transcript = (alt.get("transcript") or "").strip()
        return [Segment(speaker=None, start=0.0, text=transcript)] if transcript else []

    words_sorted = sorted(words, key=lambda w: float(w.get("start", 0.0) or 0.0))
    segments = []
    current_speaker: Optional[int] = None
    current_start = 0.0
    current_words: List[str] = []

    def flush():
        if current_words:
            segments.append(
                Segment(
                    speaker=current_speaker,
                    start=current_start,
                    text=" ".join(current_words).strip(),
                )
            )

    for word in words_sorted:
        token = (word.get("punctuated_word") or word.get("word") or "").strip()
        if not token:
            continue
        speaker_raw = word.get("speaker")
        speaker = int(speaker_raw) if speaker_raw is not None else None
        start = float(word.get("start", 0.0) or 0.0)
        if speaker != current_speaker:
            flush()
            current_speaker = speaker
            current_start = start
            current_words = [token]
        else:
            current_words.append(token)

    flush()
    return segments


def get_word_counts_by_speaker(response_data: dict) -> Counter:
    counts: Counter = Counter()
    results = response_data.get("results", {})
    channels = results.get("channels", [])
    alternatives = channels[0].get("alternatives", []) if channels else []
    alt = alternatives[0] if alternatives else {}
    words = alt.get("words", []) or []
    if words:
        for word in words:
            speaker = word.get("speaker")
            if speaker is not None:
                counts[int(speaker)] += 1
        return counts

    for seg in extract_segments(response_data):
        if seg.speaker is not None:
            counts[seg.speaker] += len(seg.text.split())
    return counts


def name_keyterms(play_by_play: Optional[str], color_commentator: Optional[str]) -> List[str]:
    terms: List[str] = []
    for value in (play_by_play, color_commentator):
        if not value:
            continue
        cleaned = value.strip()
        if cleaned:
            terms.append(cleaned)
            parts = cleaned.split()
            if len(parts) > 1:
                terms.append(parts[-1])  # last name
    deduped = []
    seen = set()
    for term in terms:
        key = term.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(term)
    return deduped


def choose_role_speakers(
    speaker_word_counts: Counter,
    fallback_pbp_name: Optional[str],
    fallback_color_name: Optional[str],
) -> Tuple[Optional[int], Optional[int], str]:
    sorted_speakers = [speaker for speaker, _ in speaker_word_counts.most_common()]
    if not sorted_speakers:
        return None, None, "No speaker IDs returned by Deepgram."

    pbp_speaker = sorted_speakers[0]
    color_speaker = sorted_speakers[1] if len(sorted_speakers) > 1 else None

    explanation = (
        "Mapped roles by word volume: highest-volume speaker -> Play by play announcer"
    )
    if color_speaker is not None:
        explanation += ", second-highest -> Color Commentary announcer"
    else:
        explanation += "; no second speaker available for color commentary"

    if fallback_pbp_name or fallback_color_name:
        explanation += " (announcer names were passed as Deepgram keyterms)"

    return pbp_speaker, color_speaker, explanation


def build_outputs(
    segments: List[Segment],
    pbp_speaker: Optional[int],
    color_speaker: Optional[int],
    pbp_name: Optional[str],
    color_name: Optional[str],
) -> Tuple[str, str, str, str]:
    diarized_lines: List[str] = []
    pbp_parts: List[str] = []
    color_parts: List[str] = []
    transcript_parts: List[str] = []

    for seg in segments:
        text = " ".join(seg.text.split()).strip()
        if not text:
            continue

        if seg.speaker == pbp_speaker and pbp_speaker is not None:
            role_label = "Play by play announcer"
            pbp_parts.append(text)
            transcript_parts.append(text)
        elif seg.speaker == color_speaker and color_speaker is not None:
            role_label = "Color Commentary announcer"
            color_parts.append(text)
            transcript_parts.append(text)
        elif seg.speaker is None:
            role_label = "All other"
        else:
            role_label = f"All other - {seg.speaker}"

        diarized_lines.append(f"Speaker {role_label}: {text}")

    map_lines: List[str] = []
    if pbp_speaker is not None:
        name = pbp_name or "Unknown"
        map_lines.append(f"# Speaker {pbp_speaker} => Play by play announcer ({name})")
    if color_speaker is not None:
        name = color_name or "Unknown"
        map_lines.append(f"# Speaker {color_speaker} => Color Commentary announcer ({name})")
    if not map_lines:
        map_lines.append("# No speaker-role mapping available")

    diarized_text = "\n".join(map_lines) + "\n\n" + ("\n\n".join(diarized_lines) if diarized_lines else "")
    transcript_text = " ".join(transcript_parts).strip()
    pbp_text = " ".join(pbp_parts).strip()
    color_text = " ".join(color_parts).strip()
    return diarized_text.strip(), transcript_text, pbp_text, color_text


def transcribe_one(
    audio_path: Path,
    output_dir: Path,
    api_key: str,
    model: str,
    max_retries: int,
    timeout_seconds: int,
    game_meta: Optional[GameAnnouncerMeta],
    default_pbp_name: str,
    default_color_name: Optional[str],
    use_keyterms: bool,
    force: bool,
    print_lock: Lock,
    file_index: int,
    total_files: int,
) -> dict:
    stem = audio_path.stem
    diarized_path = output_dir / "diarized" / f"{stem}.txt"
    transcript_path = output_dir / "transcripts" / f"{stem}.txt"
    pbp_path = output_dir / "play_by_play" / f"{stem}.txt"
    color_path = output_dir / "color_commentary" / f"{stem}.txt"

    pbp_name = (game_meta.play_by_play if game_meta and game_meta.play_by_play else default_pbp_name) or default_pbp_name
    color_name = (game_meta.color_commentator if game_meta and game_meta.color_commentator else default_color_name)

    required = [diarized_path, transcript_path, pbp_path]
    if color_name:
        required.append(color_path)
    if (not force) and all(path.exists() for path in required):
        with print_lock:
            print(f"[{file_index}/{total_files}] SKIP {audio_path.name} (outputs already exist)")
        return {"success": True, "file": str(audio_path), "skipped": True}

    keyterms = name_keyterms(pbp_name, color_name) if use_keyterms else []

    with print_lock:
        print(f"[{file_index}/{total_files}] START {audio_path.name}")
        print(f"  PBP target: {pbp_name}")
        print(f"  Color target: {color_name or 'N/A'}")
        if keyterms:
            print(f"  Deepgram keyterms: {keyterms}")

    try:
        response_data = transcribe_with_deepgram(
            audio_file=str(audio_path),
            api_key=api_key,
            model=model,
            max_retries=max_retries,
            timeout_seconds=timeout_seconds,
            keyterms=keyterms,
        )
        segments = extract_segments(response_data)
        speaker_counts = get_word_counts_by_speaker(response_data)
        pbp_speaker, color_speaker, strategy_note = choose_role_speakers(speaker_counts, pbp_name, color_name)
        diarized_text, transcript_text, pbp_text, color_text = build_outputs(
            segments=segments,
            pbp_speaker=pbp_speaker,
            color_speaker=color_speaker,
            pbp_name=pbp_name,
            color_name=color_name,
        )

        if not transcript_text and segments:
            transcript_text = " ".join(seg.text for seg in segments).strip()
        if not pbp_text and transcript_text:
            pbp_text = transcript_text

        diarized_path.write_text(diarized_text + "\n", encoding="utf-8")
        transcript_path.write_text(transcript_text + "\n", encoding="utf-8")
        pbp_path.write_text(pbp_text + "\n", encoding="utf-8")
        if color_text:
            color_path.write_text(color_text + "\n", encoding="utf-8")

        with print_lock:
            print(f"[{file_index}/{total_files}] DONE  {audio_path.name}")
            print(f"  Strategy: {strategy_note}")
            print(f"  Speaker word counts: {dict(speaker_counts)}")
            print(f"  Wrote: {transcript_path}")
            print(f"  Wrote: {diarized_path}")
            print(f"  Wrote: {pbp_path}")
            if color_text:
                print(f"  Wrote: {color_path}")
            else:
                print("  Color commentary text was empty; file not written.")

        return {
            "success": True,
            "file": str(audio_path),
            "skipped": False,
            "speakers": dict(speaker_counts),
            "pbp_speaker": pbp_speaker,
            "color_speaker": color_speaker,
        }
    except Exception as exc:
        with print_lock:
            print(f"[{file_index}/{total_files}] FAIL  {audio_path.name}: {exc}")
        return {"success": False, "file": str(audio_path), "error": str(exc)}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="NBA wrapper for Deepgram Nova-3 transcription with diarization and role outputs."
    )
    parser.add_argument("announcer", help="Announcer name or slug, e.g. 'mark_jones' or 'Mark Jones'")
    parser.add_argument("--audio-base", default="data/nba/audio", help="Base NBA audio directory")
    parser.add_argument("--output-base", default="data/nba/transcripts", help="Base NBA transcript directory")
    parser.add_argument(
        "--announcer-metadata-csv",
        default="data/nba/all_announcers_2024_2026.csv",
        help="CSV with game announcer assignments",
    )
    parser.add_argument("--model", default="nova-3", help="Deepgram model (default: nova-3)")
    parser.add_argument("--workers", type=int, default=2, help="Parallel transcription workers (default: 2)")
    parser.add_argument("--max-retries", type=int, default=5, help="Deepgram retry count (default: 5)")
    parser.add_argument("--timeout-seconds", type=int, default=1200, help="Request timeout seconds (default: 1200)")
    parser.add_argument("--min-minutes", type=float, default=60.0, help="Minimum audio duration in minutes (default: 60)")
    parser.add_argument("--max-files", type=int, default=0, help="Limit number of files to process (0 = no limit)")
    parser.add_argument("--force", action="store_true", help="Re-transcribe even if outputs already exist")
    parser.add_argument("--dry-run", action="store_true", help="Show selected files and exit")
    parser.add_argument("--use-keyterms", action="store_true", default=True, help="Pass announcer names as Deepgram keyterms")
    parser.add_argument("--no-keyterms", action="store_true", help="Disable passing announcer names as Deepgram keyterms")
    parser.add_argument("--play-by-play-name", default="", help="Override play-by-play name for all files")
    parser.add_argument("--color-name", default="", help="Override color commentator name for all files")
    args = parser.parse_args()

    if args.no_keyterms:
        args.use_keyterms = False

    audio_base = PROJECT_ROOT / args.audio_base
    output_base = PROJECT_ROOT / args.output_base
    metadata_csv = PROJECT_ROOT / args.announcer_metadata_csv

    audio_dir = resolve_audio_dir(audio_base, args.announcer)
    slug = announcer_slug(args.announcer)
    output_dir = output_base / slug
    for sub in ("transcripts", "diarized", "play_by_play", "color_commentary"):
        (output_dir / sub).mkdir(parents=True, exist_ok=True)

    print("NBA DEEPGRAM TRANSCRIBER")
    print("=" * 70)
    print(f"Announcer input: {args.announcer}")
    print(f"Resolved audio dir: {audio_dir}")
    print(f"Output dir: {output_dir}")
    print(f"Model: {args.model}")
    print(f"Workers: {args.workers}")
    print(f"Min duration: {args.min_minutes:.1f} minutes")
    print(f"Metadata CSV: {metadata_csv}")
    print(
        "Deepgram speaker naming note: API returns numeric speaker IDs; this wrapper maps IDs "
        "to Play by play / Color roles after transcription."
    )

    announcer_index = load_announcer_metadata(metadata_csv)
    default_pbp_name = args.play_by_play_name.strip() or best_name_case(args.announcer)
    default_color_name = args.color_name.strip() or None

    all_mp3s = sorted(audio_dir.glob("*.mp3"))
    if not all_mp3s:
        print("No MP3 files found.")
        return

    selected: List[Tuple[Path, float, Optional[GameAnnouncerMeta]]] = []
    skipped_short = 0
    skipped_probe_fail = 0

    for mp3 in all_mp3s:
        duration_seconds = probe_duration_seconds(mp3)
        if duration_seconds is None:
            skipped_probe_fail += 1
            print(f"SKIP duration probe failed: {mp3.name}")
            continue

        minutes = duration_seconds / 60.0
        if minutes < args.min_minutes:
            skipped_short += 1
            print(f"SKIP short ({minutes:.1f} min): {mp3.name}")
            continue

        game_key = parse_game_key_from_stem(mp3.stem)
        game_meta = announcer_index.get(game_key) if game_key else None
        selected.append((mp3, minutes, game_meta))

    if args.max_files and args.max_files > 0:
        selected = selected[: args.max_files]

    print("-" * 70)
    print(f"Total mp3 in folder: {len(all_mp3s)}")
    print(f"Selected for transcription: {len(selected)}")
    print(f"Skipped (<{args.min_minutes:.1f} min): {skipped_short}")
    print(f"Skipped (duration probe failed): {skipped_probe_fail}")
    if selected:
        print("Selected files:")
        for mp3, minutes, meta in selected:
            print(
                f"  - {mp3.name} ({minutes:.1f} min) | "
                f"pbp={meta.play_by_play if meta and meta.play_by_play else default_pbp_name} | "
                f"color={meta.color_commentator if meta and meta.color_commentator else (default_color_name or 'N/A')}"
            )

    if args.dry_run:
        print("Dry run complete. No transcription executed.")
        return

    if not selected:
        print("No files matched filters; exiting.")
        return

    api_key = load_environment()
    print_lock = Lock()

    futures = []
    results = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        for idx, (mp3, _minutes, meta) in enumerate(selected, start=1):
            futures.append(
                executor.submit(
                    transcribe_one,
                    audio_path=mp3,
                    output_dir=output_dir,
                    api_key=api_key,
                    model=args.model,
                    max_retries=args.max_retries,
                    timeout_seconds=args.timeout_seconds,
                    game_meta=meta,
                    default_pbp_name=default_pbp_name,
                    default_color_name=default_color_name,
                    use_keyterms=args.use_keyterms,
                    force=args.force,
                    print_lock=print_lock,
                    file_index=idx,
                    total_files=len(selected),
                )
            )

        for future in as_completed(futures):
            results.append(future.result())

    successful = [r for r in results if r.get("success")]
    failed = [r for r in results if not r.get("success")]
    skipped = [r for r in successful if r.get("skipped")]

    print("\n" + "=" * 70)
    print("RUN SUMMARY")
    print(f"Successful: {len(successful)}")
    print(f"Skipped existing: {len(skipped)}")
    print(f"Failed: {len(failed)}")
    print(f"Attempted: {len(results)}")
    if failed:
        print("Failed files:")
        for row in failed:
            print(f"  - {Path(row['file']).name}: {row.get('error')}")


if __name__ == "__main__":
    main()
