#!/usr/bin/env python3
"""
Validate and optionally clean NCAAB transcript files.

This script is designed for the YouTube transcript pipeline and performs:
1. Deterministic transcript QA:
   - word-count distribution / outlier detection
   - title/team/date matching checks
   - transcript quality heuristics (common-word ratio)
   - announcer first-name mention checks
   - lightweight announcer-intro mismatch checks
2. Optional LLM cleanup:
   - keep only play-by-play + analyst dialogue
   - remove pre/post-game, commercials, PA, sideline, coaches, etc.
   - write cleaned transcript files
   - recompute cleaned metrics

The validator aims to get transcript quality most of the way there rather than
to be perfect.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

from src.ncaab.video_finder import TEAM_ALIASES


COMMON_WORDS = {
    "a", "about", "all", "also", "an", "and", "are", "as", "at", "back",
    "ball", "be", "been", "big", "buzzer", "by", "call", "can", "chance",
    "clock", "coach", "come", "down", "drive", "for", "from", "foul", "game",
    "get", "good", "got", "great", "half", "has", "have", "he", "here", "his",
    "home", "i", "if", "in", "inside", "into", "is", "it", "just", "line",
    "look", "make", "man", "more", "move", "no", "not", "now", "of", "on",
    "one", "or", "out", "over", "pass", "play", "point", "possession",
    "rebound", "right", "score", "second", "see", "set", "shot", "so", "some",
    "start", "state", "take", "team", "that", "the", "their", "them", "there",
    "they", "this", "three", "time", "to", "tonight", "turnover", "two", "up",
    "was", "we", "well", "what", "when", "with", "would", "you", "your",
    "wcc", "byu", "tcu", "espn", "sec", "acc", "big", "pac", "ot", "ncaa",
    "ncaam", "wac", "mbb", "acu",
}

DEFAULT_MIN_FULL_GAME_WORDS = 8000

MONTH_ALIASES = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

STOP_PHRASES = [
    "SPEAKER_NOT_FOUND",
    "[music]",
]

SYSTEM_PROMPT = """You are cleaning a men's college basketball broadcast transcript.

Your task is to keep only the spoken dialogue from the play-by-play announcer
and color commentator(s) for the actual game.

RULES:
1. KEEP only dialogue from the play-by-play announcer and color commentator(s)
2. REMOVE stadium/public-address announcer lines, referee calls, coaches, players,
   sideline reporters, studio hosts, interviewers, and guests
3. START at opening tip / start of game action; REMOVE all pre-game content
4. END at the final buzzer / end of overtime; REMOVE all post-game content
5. REMOVE commercials, sponsor reads, promos, transitions, "we'll be right back",
   upcoming-game plugs, halftime/studio segments, and technical messages
6. KEEP normal in-game analyst discussion, replay breakdowns, and timeout talk if
   it is spoken by the play-by-play or analyst crew
7. PRESERVE the original wording as closely as possible; do not summarize
8. Output ONLY the cleaned transcript text, with no headers or explanations
9. If the chunk contains no usable play-by-play / analyst dialogue, output an empty string
"""

USER_PROMPT_TEMPLATE = """Clean this college basketball broadcast transcript chunk.

Primary play-by-play announcer: {play_by_play}
Color commentator(s): {analysts}
Chunk position: {chunk_position}

Transcript chunk:
{transcript}

Output only the cleaned transcript text."""


@dataclass
class ValidationMetrics:
    raw_word_count: int
    common_word_ratio: float
    pbp_first_name_mentions: int
    analyst_first_name_mentions: int
    title_team_match: bool
    title_date_status: str
    title_has_target_name: bool
    intro_mismatch_name: str
    wordcount_short_flag: bool
    wordcount_long_flag: bool
    low_quality_text_flag: bool
    status: str
    keep_for_dataset: bool
    reasons: str
    cleaned_word_count: int = 0
    cleaned_pbp_first_name_mentions: int = 0
    cleaned_analyst_first_name_mentions: int = 0
    cleaned_path: str = ""


def normalize_path(path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return (PROJECT_ROOT / path).resolve()


def resolve_existing_path(path_str: str) -> Path:
    path = normalize_path(path_str)
    if path.exists():
        return path

    candidate = Path(path_str)
    candidate_str = str(candidate)
    for marker in ("-v2", "_v2"):
        if marker in candidate_str:
            alt = normalize_path(candidate_str.replace(marker, ""))
            if alt.exists():
                return alt

    raise FileNotFoundError(path)


def strip_header(raw_text: str) -> str:
    if "=" * 80 in raw_text:
        return raw_text.split("=" * 80, 1)[-1].strip()
    return raw_text.strip()


def tokenize_words(text: str) -> list[str]:
    return re.findall(r"[A-Za-z']+", text)


def count_words(text: str) -> int:
    return len(tokenize_words(text))


def common_word_ratio(text: str) -> float:
    tokens = [token.lower() for token in tokenize_words(text)]
    if not tokens:
        return 0.0
    return sum(token in COMMON_WORDS for token in tokens) / len(tokens)


def normalize_text(text: str) -> str:
    text = text.lower()
    text = text.replace("&", " and ")
    text = text.replace("st.", "saint")
    text = text.replace("st ", "saint ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def clean_matchup_team_label(label: str) -> str:
    text = label.strip()
    text = re.sub(r"^(Champ\.|Final|Semifinal|Semi)\s*:\s*", "", text)
    text = re.sub(r"^[A-Za-z0-9 .()/&'-]+:\s*", "", text)
    text = re.sub(r"^\([^)]+\)\s*", "", text)
    text = re.sub(r"^[A-Z]{0,3}\d+\s+", "", text)
    text = re.sub(r"^\d+\s+", "", text)
    return text.strip()


def split_matchup(matchup: str) -> tuple[str, str]:
    away, home = matchup.split(" vs. ", 1)
    return clean_matchup_team_label(away), clean_matchup_team_label(home)


def build_team_aliases(team: str) -> list[str]:
    aliases = TEAM_ALIASES.get(team, [team])
    seen: set[str] = set()
    results: list[str] = []

    words = [word for word in re.findall(r"[A-Za-z]+", team) if word]
    if len(words) >= 2:
        base_acronym = "".join(word[0] for word in words).lower()
        aliases.append(base_acronym)
        if words[-1].lower() == "state":
            aliases.append("".join(word[0] for word in words[:-1]).lower() + "su")
        elif all(word.lower() not in {"saint", "st", "marys", "mary's"} for word in words):
            aliases.append(base_acronym + "u")

    for alias in aliases + [team]:
        normalized = normalize_text(alias)
        if normalized and normalized not in seen:
            seen.add(normalized)
            results.append(normalized)
        saint_variant = normalized.replace("saint", "st")
        saint_variant = re.sub(r"\s+", " ", saint_variant).strip()
        if saint_variant and saint_variant not in seen:
            seen.add(saint_variant)
            results.append(saint_variant)
    return results


def title_matches_team(title: str, team: str) -> bool:
    norm_title = normalize_text(title)
    return any(alias in norm_title for alias in build_team_aliases(team))


def parse_date_status_from_title(title: str, game_date: str) -> str:
    year, month, day = [int(part) for part in game_date.split("-")]
    title_lower = title.lower()

    # MM/DD/YY or MM/DD/YYYY
    date_matches = re.findall(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b", title_lower)
    for m_str, d_str, y_str in date_matches:
        y = int(y_str)
        if y < 100:
            y += 2000
        if (int(m_str), int(d_str), y) == (month, day, year):
            return "match"
        return "mismatch"

    # MM/DD with no year
    short_matches = re.findall(r"\b(\d{1,2})/(\d{1,2})\b", title_lower)
    for m_str, d_str in short_matches:
        if (int(m_str), int(d_str)) == (month, day):
            return "match"
        return "mismatch"

    # Month name Day, Year optional
    month_name_matches = re.findall(
        r"\b([a-z]{3,9})\.?\s+(\d{1,2})(?:,?\s*(\d{2,4}))?\b",
        title_lower,
    )
    for month_name, day_str, year_str in month_name_matches:
        if month_name not in MONTH_ALIASES:
            continue
        month_val = MONTH_ALIASES[month_name]
        if year_str:
            y = int(year_str)
            if y < 100:
                y += 2000
            if (month_val, int(day_str), y) == (month, day, year):
                return "match"
            return "mismatch"
        if (month_val, int(day_str)) == (month, day):
            return "match"
        return "mismatch"

    return "absent"


def first_names(full_names: str) -> list[str]:
    names: list[str] = []
    for person in [part.strip() for part in full_names.split(",") if part.strip()]:
        parts = person.split()
        if not parts:
            continue
        first = re.sub(r"[^A-Za-z'-]", "", parts[0]).lower()
        if first:
            names.append(first)
    return names


def count_name_mentions(text: str, names: list[str]) -> int:
    lowered = text.lower()
    total = 0
    for name in names:
        if len(name) < 2:
            continue
        total += len(re.findall(rf"\b{re.escape(name)}\b", lowered))
    return total


def intro_mismatch_name(text: str, valid_names: set[str]) -> str:
    intro = text[:2500].lower()
    patterns = [
        r"hi again everybody i'?m ([a-z]+(?: [a-z]+){0,2})",
        r"i'?m ([a-z]+(?: [a-z]+){0,2}) alongside ([a-z]+(?: [a-z]+){0,2})",
        r"([a-z]+(?: [a-z]+){0,2}) here with you",
        r"alongside ([a-z]+(?: [a-z]+){0,2})",
        r"my partner ([a-z]+(?: [a-z]+){0,2})",
    ]
    found: list[str] = []
    for pattern in patterns:
        for match in re.finditer(pattern, intro):
            for group in match.groups():
                if not group:
                    continue
                cleaned = re.sub(r"\s+", " ", group).strip()
                if cleaned and cleaned not in found:
                    found.append(cleaned)
    for candidate in found:
        tokens = set(candidate.split())
        if tokens and tokens.isdisjoint(valid_names):
            return candidate
    return ""


def quantile(series: pd.Series, q: float) -> float:
    if series.empty:
        return 0.0
    return float(series.quantile(q))


def chunk_text(text: str, chunk_size: int) -> list[str]:
    if len(text) <= chunk_size:
        return [text]

    paragraphs = text.split("\n\n")
    chunks: list[str] = []
    current = ""
    for paragraph in paragraphs:
        paragraph = paragraph.strip()
        if not paragraph:
            continue
        candidate = paragraph if not current else f"{current}\n\n{paragraph}"
        if len(candidate) <= chunk_size:
            current = candidate
            continue
        if current:
            chunks.append(current)
        if len(paragraph) <= chunk_size:
            current = paragraph
            continue
        sentences = re.split(r"(?<=[.!?])\s+", paragraph)
        current = ""
        for sentence in sentences:
            sentence = sentence.strip()
            if not sentence:
                continue
            candidate = sentence if not current else f"{current} {sentence}"
            if len(candidate) <= chunk_size:
                current = candidate
            else:
                if current:
                    chunks.append(current)
                current = sentence
        if current:
            chunks.append(current)
            current = ""
    if current:
        chunks.append(current)
    return chunks or [text]


def llm_cleanup_openai(
    text: str,
    play_by_play: str,
    analysts: str,
    model: str,
    chunk_size: int,
    request_timeout: int,
) -> str | None:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("❌ OPENAI_API_KEY not found")
        return None

    chunks = chunk_text(text, chunk_size=chunk_size)
    outputs: dict[int, str] = {}

    def process_one(idx: int, chunk: str) -> tuple[int, str | None]:
        if len(chunks) == 1:
            chunk_position = "full transcript"
        elif idx == 0:
            chunk_position = "first chunk"
        elif idx == len(chunks) - 1:
            chunk_position = "last chunk"
        else:
            chunk_position = "middle chunk"
        try:
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "messages": [
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {
                            "role": "user",
                            "content": USER_PROMPT_TEMPLATE.format(
                                play_by_play=play_by_play,
                                analysts=analysts or "None listed",
                                chunk_position=chunk_position,
                                transcript=chunk,
                            ),
                        },
                    ],
                    "temperature": 0.1,
                    "max_tokens": 16000,
                },
                timeout=request_timeout,
            )
        except requests.RequestException as exc:
            print(f"❌ OpenAI request error: {exc}")
            return idx, None
        if response.status_code != 200:
            print(f"❌ OpenAI API error: {response.status_code} {response.text[:200]}")
            return idx, None
        data = response.json()
        content = (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
        )
        return idx, content.strip()

    with ThreadPoolExecutor(max_workers=min(4, len(chunks))) as executor:
        futures = {
            executor.submit(process_one, idx, chunk): idx
            for idx, chunk in enumerate(chunks)
        }
        for future in as_completed(futures):
            idx, result = future.result()
            if result is None:
                return None
            outputs[idx] = result

    cleaned_parts = [
        outputs[idx].strip()
        for idx in sorted(outputs)
        if outputs[idx].strip() and outputs[idx].strip() not in STOP_PHRASES
    ]
    return "\n\n".join(cleaned_parts).strip()


def llm_cleanup_gemini(
    text: str,
    play_by_play: str,
    analysts: str,
    model: str,
    chunk_size: int,
) -> str | None:
    try:
        from google import genai
        from google.genai import types
    except ImportError:
        print("❌ google-genai not installed")
        return None

    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print("❌ GOOGLE_API_KEY not found")
        return None

    client = genai.Client(api_key=api_key)
    config = types.GenerateContentConfig(
        system_instruction=SYSTEM_PROMPT,
        temperature=0.1,
        top_p=0.95,
        max_output_tokens=20000,
    )
    chunks = chunk_text(text, chunk_size=chunk_size)
    outputs: dict[int, str] = {}

    def process_one(idx: int, chunk: str) -> tuple[int, str | None]:
        if len(chunks) == 1:
            chunk_position = "full transcript"
        elif idx == 0:
            chunk_position = "first chunk"
        elif idx == len(chunks) - 1:
            chunk_position = "last chunk"
        else:
            chunk_position = "middle chunk"
        response = client.models.generate_content(
            model=model,
            contents=USER_PROMPT_TEMPLATE.format(
                play_by_play=play_by_play,
                analysts=analysts or "None listed",
                chunk_position=chunk_position,
                transcript=chunk,
            ),
            config=config,
        )
        return idx, (response.text or "").strip()

    with ThreadPoolExecutor(max_workers=min(4, len(chunks))) as executor:
        futures = {
            executor.submit(process_one, idx, chunk): idx
            for idx, chunk in enumerate(chunks)
        }
        for future in as_completed(futures):
            idx, result = future.result()
            if result is None:
                return None
            outputs[idx] = result

    cleaned_parts = [
        outputs[idx].strip()
        for idx in sorted(outputs)
        if outputs[idx].strip() and outputs[idx].strip() not in STOP_PHRASES
    ]
    return "\n\n".join(cleaned_parts).strip()


def llm_cleanup(
    text: str,
    play_by_play: str,
    analysts: str,
    model: str,
    chunk_size: int,
    request_timeout: int,
) -> str | None:
    if model.startswith("gpt"):
        return llm_cleanup_openai(
            text,
            play_by_play,
            analysts,
            model,
            chunk_size,
            request_timeout,
        )
    if model.startswith("gemini"):
        return llm_cleanup_gemini(text, play_by_play, analysts, model, chunk_size)
    raise ValueError(f"Unsupported model: {model}")


def write_cleaned_transcript(
    output_path: Path,
    raw_path: Path,
    row: pd.Series,
    cleaned_text: str,
    model: str,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        handle.write(f"Source transcript: {raw_path.relative_to(PROJECT_ROOT)}\n")
        handle.write(f"Model: {model}\n")
        handle.write(f"URL: {row.get('youtube_url', '')}\n")
        handle.write(f"Title: {row.get('youtube_title', '')}\n")
        handle.write(f"Date: {row.get('game_date', '')}\n")
        handle.write(f"Matchup: {row.get('matchup', '')}\n")
        handle.write(f"Play-by-play: {row.get('play_by_play', '')}\n")
        handle.write(f"Analysts: {row.get('analysts', '')}\n")
        handle.write("=" * 80 + "\n\n")
        handle.write(cleaned_text.strip())
        handle.write("\n")


def cleanup_one_transcript(
    row: pd.Series,
    clean_dir: Path,
    model: str,
    overwrite_cleaned: bool,
    chunk_size: int,
    request_timeout: int,
) -> dict[str, Any]:
    transcript_name = str(row.get("transcript_path", ""))
    transcript_path_value = str(row.get("transcript_path", ""))
    try:
        transcript_path = resolve_existing_path(str(row["transcript_path"]))
        transcript_name = transcript_path.name
        out_path = cleaned_output_path(clean_dir, transcript_path)
        if out_path.exists() and not overwrite_cleaned:
            cleaned_text = strip_header(out_path.read_text(encoding="utf-8", errors="ignore"))
            elapsed = 0.0
            created = False
        else:
            raw_text = strip_header(transcript_path.read_text(encoding="utf-8", errors="ignore"))
            start = time.time()
            cleaned_text = llm_cleanup(
                raw_text,
                str(row.get("play_by_play", "")),
                str(row.get("analysts", "")),
                model,
                chunk_size,
                request_timeout,
            )
            elapsed = time.time() - start
            if cleaned_text is None:
                return {
                    "transcript_path": transcript_path_value,
                    "ok": False,
                    "error": "llm_cleanup_failed",
                    "elapsed": elapsed,
                    "transcript_name": transcript_name,
                }
            write_cleaned_transcript(out_path, transcript_path, row, cleaned_text, model)
            created = True

        cleaned_wc = count_words(cleaned_text)
        cleaned_common_ratio = common_word_ratio(cleaned_text)
        cleaned_pbp_mentions = count_name_mentions(cleaned_text, first_names(str(row.get("play_by_play", ""))))
        cleaned_analyst_mentions = count_name_mentions(cleaned_text, first_names(str(row.get("analysts", ""))))
        return {
            "transcript_path": transcript_path_value,
            "ok": True,
            "transcript_name": transcript_name,
            "cleaned_path": str(out_path.relative_to(PROJECT_ROOT)),
            "cleaned_word_count": cleaned_wc,
            "cleaned_common_word_ratio": round(cleaned_common_ratio, 4),
            "cleaned_pbp_first_name_mentions": cleaned_pbp_mentions,
            "cleaned_analyst_first_name_mentions": cleaned_analyst_mentions,
            "elapsed": elapsed,
            "created": created,
        }
    except Exception as exc:
        elapsed = 0.0
        if "start" in locals():
            elapsed = time.time() - start
        return {
            "transcript_path": transcript_path_value,
            "ok": False,
            "error": str(exc),
            "elapsed": elapsed,
            "transcript_name": transcript_name,
        }


def validate_row(
    row: pd.Series,
    wordcount_min: float,
    wordcount_max: float,
    min_full_game_words: int,
) -> ValidationMetrics:
    transcript_path = resolve_existing_path(str(row["transcript_path"]))
    raw_text = transcript_path.read_text(encoding="utf-8", errors="ignore")
    body = strip_header(raw_text)
    raw_word_count = count_words(body)
    quality_ratio = common_word_ratio(body)

    pbp_names = first_names(str(row.get("play_by_play", "")))
    analyst_names = first_names(str(row.get("analysts", "")))
    pbp_mentions = count_name_mentions(body, pbp_names)
    analyst_mentions = count_name_mentions(body, analyst_names)

    title = str(row.get("youtube_title", ""))
    matchup = str(row.get("matchup", ""))
    away, home = split_matchup(matchup)
    title_team_match = title_matches_team(title, away) and title_matches_team(title, home)
    title_date_status = parse_date_status_from_title(title, str(row.get("game_date", "")))

    target_names = pbp_names + analyst_names
    title_has_target_name = any(
        re.search(rf"\b{re.escape(name)}\b", title.lower())
        for name in target_names
        if len(name) >= 2
    )
    intro_mismatch = intro_mismatch_name(body, set(target_names))

    short_flag = raw_word_count < max(wordcount_min, float(min_full_game_words))
    long_flag = raw_word_count > wordcount_max
    low_quality_flag = quality_ratio < 0.22

    reasons: list[str] = []
    if not title_team_match:
        reasons.append("title_team_mismatch")
    if title_date_status == "mismatch":
        reasons.append("title_date_mismatch")
    if intro_mismatch:
        reasons.append(f"intro_announcer_mismatch:{intro_mismatch}")
    if short_flag:
        reasons.append("wordcount_too_short")
    if long_flag:
        reasons.append("wordcount_too_long")
    if low_quality_flag:
        reasons.append("low_common_word_ratio")
    if pbp_mentions == 0:
        reasons.append("no_pbp_first_name_mentions")

    if not title_team_match or title_date_status == "mismatch" or intro_mismatch:
        status = "invalid"
        keep = False
    elif short_flag or low_quality_flag:
        status = "low_quality"
        keep = False
    elif pbp_mentions >= 1 or analyst_mentions >= 1 or title_has_target_name:
        status = "confirmed_valid"
        keep = True
    else:
        status = "probable_valid"
        keep = True

    return ValidationMetrics(
        raw_word_count=raw_word_count,
        common_word_ratio=quality_ratio,
        pbp_first_name_mentions=pbp_mentions,
        analyst_first_name_mentions=analyst_mentions,
        title_team_match=title_team_match,
        title_date_status=title_date_status,
        title_has_target_name=title_has_target_name,
        intro_mismatch_name=intro_mismatch,
        wordcount_short_flag=short_flag,
        wordcount_long_flag=long_flag,
        low_quality_text_flag=low_quality_flag,
        status=status,
        keep_for_dataset=keep,
        reasons=";".join(reasons),
    )


def cleaned_output_path(clean_dir: Path, transcript_path: Path) -> Path:
    return clean_dir / transcript_path.name


def transcript_path_exists(path_str: str) -> bool:
    try:
        resolve_existing_path(path_str)
        return True
    except FileNotFoundError:
        return False


def assess_cleaned_output(
    row: pd.Series,
    clean_requested: bool,
    min_full_game_words: int,
) -> tuple[str, bool, str]:
    base_status = str(row.get("validation_status", ""))
    base_keep = str(row.get("keep_for_dataset", "")).lower() == "true"
    base_reasons = str(row.get("validation_reasons", ""))

    if not clean_requested:
        return base_status, base_keep, base_reasons

    if not base_keep:
        return base_status, False, base_reasons

    cleaned_path = str(row.get("cleaned_path", "")).strip()
    if not cleaned_path:
        reasons = [reason for reason in base_reasons.split(";") if reason]
        reasons.append("cleanup_failed")
        return "cleanup_failed", False, ";".join(reasons)

    cleaned_wc = int(pd.to_numeric(row.get("cleaned_word_count", 0), errors="coerce") or 0)
    cleaned_ratio = float(pd.to_numeric(row.get("cleaned_common_word_ratio", 0.0), errors="coerce") or 0.0)
    raw_wc = int(pd.to_numeric(row.get("raw_word_count", 0), errors="coerce") or 0)

    reasons = [reason for reason in base_reasons.split(";") if reason]
    retention_ratio = (cleaned_wc / raw_wc) if raw_wc else 0.0
    if cleaned_wc < min_full_game_words:
        reasons.append("cleaned_wordcount_too_short")
        return "low_quality", False, ";".join(reasons)
    if retention_ratio < 0.08:
        reasons.append("cleaned_retention_too_low")
        return "low_quality", False, ";".join(reasons)
    if cleaned_ratio < 0.2:
        reasons.append("cleaned_low_common_word_ratio")
        return "low_quality", False, ";".join(reasons)

    return base_status, True, ";".join(reasons)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate and optionally clean NCAAB transcript files")
    parser.add_argument("--results", required=True, help="Results CSV from youtube_bulk_extract")
    parser.add_argument("--output", default="", help="Validation CSV output path")
    parser.add_argument("--clean-with-llm", action="store_true", help="Run LLM cleanup on keepable transcripts")
    parser.add_argument("--clean-dir", default="", help="Directory for cleaned transcript outputs")
    parser.add_argument("--model", default="gpt-4o-mini", help="LLM model for cleanup")
    parser.add_argument("--clean-limit", type=int, default=None, help="Limit LLM cleanup to N files")
    parser.add_argument("--cleanup-workers", type=int, default=1, help="Concurrent transcript cleanup workers")
    parser.add_argument("--chunk-size", type=int, default=30000, help="Transcript chunk size for LLM cleanup")
    parser.add_argument("--request-timeout", type=int, default=180, help="Per-request timeout for OpenAI cleanup")
    parser.add_argument(
        "--min-full-game-words",
        type=int,
        default=DEFAULT_MIN_FULL_GAME_WORDS,
        help="Minimum word count for a transcript to count as a full game",
    )
    parser.add_argument("--overwrite-cleaned", action="store_true", help="Rewrite cleaned files if they already exist")
    args = parser.parse_args()

    results_path = normalize_path(args.results)
    output_path = (
        normalize_path(args.output)
        if args.output
        else results_path.with_name(f"{results_path.stem}-validation.csv")
    )
    clean_dir = (
        normalize_path(args.clean_dir)
        if args.clean_dir
        else results_path.parent.parent / "transcripts-cleaned"
    )

    df = pd.read_csv(results_path, dtype=str).fillna("")
    df = df[df["transcript_path"] != ""].copy().reset_index(drop=False)
    missing_mask = ~df["transcript_path"].apply(transcript_path_exists)
    if missing_mask.any():
        print(f"Skipping {int(missing_mask.sum())} rows with missing transcript files")
        df = df[~missing_mask].copy()
    df = df.rename(columns={"index": "source_index"})
    if df.empty:
        print("No transcript rows found.")
        return 1

    df["raw_word_count_temp"] = df["transcript_path"].apply(
        lambda path: count_words(strip_header(resolve_existing_path(path).read_text(encoding="utf-8", errors="ignore")))
    )
    q1 = quantile(df["raw_word_count_temp"], 0.25)
    q3 = quantile(df["raw_word_count_temp"], 0.75)
    median = quantile(df["raw_word_count_temp"], 0.5)
    iqr = q3 - q1
    wordcount_min = max(300.0, min(q1 - 1.5 * iqr, median * 0.35) if iqr else median * 0.35)
    wordcount_max = max(q3 + 1.5 * iqr, median * 2.75 if median else q3 + 1.5 * iqr)

    print(f"Word-count distribution: q1={q1:.0f} median={median:.0f} q3={q3:.0f}")
    print(f"Word-count thresholds: min={wordcount_min:.0f} max={wordcount_max:.0f}")

    rows: list[dict[str, Any]] = []
    cleanup_candidates: list[tuple[int, pd.Series, ValidationMetrics]] = []
    for idx, row in df.iterrows():
        metrics = validate_row(row, wordcount_min, wordcount_max, args.min_full_game_words)
        if metrics.keep_for_dataset and metrics.status in {"confirmed_valid", "probable_valid"}:
            cleanup_candidates.append((idx, row, metrics))
        row_dict = row.to_dict()
        row_dict.update(
            {
                "raw_word_count": metrics.raw_word_count,
                "common_word_ratio": round(metrics.common_word_ratio, 4),
                "pbp_first_name_mentions": metrics.pbp_first_name_mentions,
                "analyst_first_name_mentions": metrics.analyst_first_name_mentions,
                "title_team_match": metrics.title_team_match,
                "title_date_status": metrics.title_date_status,
                "title_has_target_name": metrics.title_has_target_name,
                "intro_mismatch_name": metrics.intro_mismatch_name,
                "wordcount_short_flag": metrics.wordcount_short_flag,
                "wordcount_long_flag": metrics.wordcount_long_flag,
                "low_quality_text_flag": metrics.low_quality_text_flag,
                "validation_status": metrics.status,
                "keep_for_dataset": metrics.keep_for_dataset,
                "validation_reasons": metrics.reasons,
                "cleaned_path": "",
                "cleaned_word_count": 0,
                "cleaned_common_word_ratio": 0.0,
                "cleaned_pbp_first_name_mentions": 0,
                "cleaned_analyst_first_name_mentions": 0,
                "final_validation_status": metrics.status,
                "final_keep_for_dataset": metrics.keep_for_dataset,
                "final_validation_reasons": metrics.reasons,
            }
        )
        rows.append(row_dict)

    output_df = pd.DataFrame(rows)
    cleanup_candidates.sort(key=lambda item: item[2].raw_word_count)

    if args.clean_with_llm:
        selected_candidates = cleanup_candidates
        if args.clean_limit is not None:
            selected_candidates = cleanup_candidates[:args.clean_limit]

        workers = max(1, args.cleanup_workers)
        print(f"Cleaning {len(selected_candidates)} transcripts with {args.model} using {workers} worker(s)...")
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = []
            for _, row, _ in selected_candidates:
                transcript_name = resolve_existing_path(str(row["transcript_path"])).name
                print(f"Queueing {transcript_name}...")
                futures.append(
                    executor.submit(
                        cleanup_one_transcript,
                        row,
                        clean_dir,
                        args.model,
                        args.overwrite_cleaned,
                        args.chunk_size,
                        args.request_timeout,
                    )
                )

            for future in as_completed(futures):
                try:
                    result = future.result()
                except Exception as exc:
                    print(f"  ❌ Cleanup worker crashed: {exc}")
                    continue
                if not result["ok"]:
                    print(f"  ❌ Cleanup failed for {result['transcript_name']} ({result['elapsed']:.1f}s): {result.get('error', 'unknown_error')}")
                    continue
                transcript_path_value = str(result["transcript_path"])
                source_mask = output_df["transcript_path"].astype(str) == transcript_path_value
                output_df.loc[source_mask, "cleaned_path"] = result["cleaned_path"]
                output_df.loc[source_mask, "cleaned_word_count"] = result["cleaned_word_count"]
                output_df.loc[source_mask, "cleaned_common_word_ratio"] = result["cleaned_common_word_ratio"]
                output_df.loc[source_mask, "cleaned_pbp_first_name_mentions"] = result["cleaned_pbp_first_name_mentions"]
                output_df.loc[source_mask, "cleaned_analyst_first_name_mentions"] = result["cleaned_analyst_first_name_mentions"]
                action = "Wrote" if result.get("created") else "Reused"
                print(f"  ✅ {action} {result['cleaned_path']} ({result['elapsed']:.1f}s)")

    final_statuses: list[str] = []
    final_keeps: list[bool] = []
    final_reasons: list[str] = []
    for _, row in output_df.iterrows():
        final_status, final_keep, final_reasons_value = assess_cleaned_output(
            row,
            clean_requested=args.clean_with_llm,
            min_full_game_words=args.min_full_game_words,
        )
        final_statuses.append(final_status)
        final_keeps.append(final_keep)
        final_reasons.append(final_reasons_value)
    output_df["final_validation_status"] = final_statuses
    output_df["final_keep_for_dataset"] = final_keeps
    output_df["final_validation_reasons"] = final_reasons

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_df = output_df.drop(columns=["raw_word_count_temp"], errors="ignore")
    output_df.sort_values(["priority_rank"], key=lambda s: pd.to_numeric(s, errors="coerce"), inplace=True)
    output_df.to_csv(output_path, index=False)

    print(f"\nWrote validation CSV to {output_path}")
    print("\nRaw validation status counts:")
    print(output_df["validation_status"].value_counts().to_string())
    print("\nFinal validation status counts:")
    print(output_df["final_validation_status"].value_counts().to_string())
    keep_mask = output_df["final_keep_for_dataset"].astype(str).str.lower() == "true"
    print(f"\nFinal keepable rows: {int(keep_mask.sum())}/{len(output_df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
