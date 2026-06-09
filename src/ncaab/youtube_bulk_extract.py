#!/usr/bin/env python3
"""
Search YouTube and bulk extract transcripts/audio for a target announcer list.

Primary path:
1. Search YouTube for each target game using NCAABVideoFinder.
2. Save a candidate/results CSV.
3. Try YouTube transcript extraction first.
4. Fall back to yt-dlp audio download if transcript extraction fails.

This is intentionally incremental and safe to rerun.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.proxies import WebshareProxyConfig
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

from src.auto_collect.transcript_api import YouTubeTranscriptAPI
from src.ncaab.video_finder import NCAABGame, NCAABVideoFinder


DEFAULT_TARGETS = PROJECT_ROOT / "data/ncaab" / "targets" / "ian-eagle.csv"
DEFAULT_RESULTS = PROJECT_ROOT / "data/ncaab" / "targets" / "ian-eagle-youtube.csv"
DEFAULT_BASE_DIR = PROJECT_ROOT / "data/ncaab" / "transcripts" / "ian-eagle"

MANUAL_YOUTUBE_CANDIDATES = {
    "2022-03-27|Saint Peter's|North Carolina": {
        "url": "https://www.youtube.com/watch?v=JT8Jnh6qx6Q",
        "title": "North Carolina vs. Saint Peter's: 2022 NCAA men's Elite Eight round | FULL REPLAY",
    },
    "2022-03-19|Saint Peter's|Murray State": {
        "url": "https://www.youtube.com/watch?v=pAUp7R-a_Jg",
        "title": "Saint Peter's vs. Murray State: 2022 NCAA men's second round | FULL REPLAY",
    },
    "2019-03-23|Wofford|Kentucky": {
        "url": "https://www.youtube.com/watch?v=HijvkdmF9WM",
        "title": "2018-2019 Kentucky vs Wofford (2nd Round - Game 35)",
    },
    "2018-03-25|Texas Tech|Villanova": {
        "url": "https://www.youtube.com/watch?v=dNncQer6LxA",
        "title": "Elite Eight - 2018 NCAA Championship - Villanova vs Texas Tech",
    },
    "2018-03-23|Texas Tech|Purdue": {
        "url": "https://www.youtube.com/watch?v=ratICWT_jRk",
        "title": "Sweet Sixteen - 2018 NCAA Championship - Purdue vs Texas Tech",
    },
    "2024-03-12|Gonzaga|Saint Mary's": {
        "url": "https://www.youtube.com/watch?v=gKXpSi6G7Nc",
        "title": "Gonzaga vs Saint Mary's 03/12/2024 (WCC Tournament Championship)",
    },
    "2024-03-11|San Francisco|Gonzaga": {
        "url": "https://www.youtube.com/watch?v=bUdGWaonZrQ",
        "title": "Gonzaga vs San Francisco 03/11/2024 (WCC Tournament Semifinals)",
    },
    "2024-02-20|San Francisco|Saint Mary's": {
        "url": "https://www.youtube.com/watch?v=ut3IAgA6AHE",
        "title": "San Francisco Vs Saint Mary's Gaels FULL GAME Today | Ncaam Full highlights Feb,20 2024",
    },
    "2024-02-03|Saint Mary's|Gonzaga": {
        "url": "https://www.youtube.com/watch?v=pZu2mXdjQR8",
        "title": "Gonzaga vs Saint Mary's 02/03/2024",
    },
    "2024-01-11|Gonzaga|Santa Clara": {
        "url": "https://www.youtube.com/watch?v=bb6gSBHmuGU",
        "title": "Gonzaga at Santa Clara 01/11/2024",
    },
    "2023-02-23|San Diego|Gonzaga": {
        "url": "https://www.youtube.com/watch?v=HVsSP7A28oA",
        "title": "Gonzaga vs San Diego 02/23/2023",
    },
    "2023-02-11|BYU|Gonzaga": {
        "url": "https://www.youtube.com/watch?v=fFOUmteq4ho",
        "title": "Gonzaga vs BYU 02/11/2023",
    },
    "2022-03-12|Abilene Christian|New Mexico State": {
        "url": "https://www.youtube.com/watch?v=QCDL6nj-yl8",
        "title": "WAC Championship NMSU MBB vs ACU 2022",
    },
    "2022-02-16|Gonzaga|Pepperdine": {
        "url": "https://www.youtube.com/watch?v=KgIl_h6PUAY",
        "title": "NCAAM - Gonzaga Bulldogs @ Pepperdine Waves",
    },
    "2022-02-03|Gonzaga|San Diego": {
        "url": "https://www.youtube.com/watch?v=EuDL1i-g2FM",
        "title": "NCAAM - San Diego Toreros @ Gonzaga Bulldogs",
    },
    "2021-03-09|BYU|Gonzaga": {
        "url": "https://www.youtube.com/watch?v=ILxCBSDT62g",
        "title": "Gonzaga vs BYU 03/09/2021 (WCC Tournament Championship)",
    },
    "2021-03-08|Saint Mary's|Gonzaga": {
        "url": "https://www.youtube.com/watch?v=w9YObzumgkg",
        "title": "Gonzaga vs Saint Mary's 03/08/2021 (WCC Tournament Semifinals)",
    },
}


@dataclass
class TargetGame:
    priority_rank: int
    play_by_play: str
    game_date: str
    season: str
    round_name: str
    priority_bucket: str
    matchup: str
    network: str
    analysts: str

    @property
    def cleaned_matchup(self) -> tuple[str, str]:
        away, home = self.matchup.split(" vs. ", 1)
        return clean_team_label(away), clean_team_label(home)

    @property
    def key(self) -> str:
        away, home = self.cleaned_matchup
        return f"{self.game_date}|{away}|{home}"


RESULT_COLUMNS = [
    "priority_rank",
    "play_by_play",
    "game_date",
    "season",
    "round_name",
    "priority_bucket",
    "matchup",
    "away_team",
    "home_team",
    "network",
    "analysts",
    "youtube_url",
    "youtube_title",
    "channel",
    "duration_seconds",
    "search_status",
    "extraction_status",
    "extraction_method",
    "transcript_path",
    "audio_path",
    "error",
]


def clean_team_label(label: str) -> str:
    text = label.strip()
    text = re.sub(r"^(Champ\.|Final|Semifinal|Semi)\s*:\s*", "", text)
    text = re.sub(r"^[A-Za-z0-9 .()/&'-]+:\s*", "", text)
    text = re.sub(r"^\([^)]+\)\s*", "", text)
    text = re.sub(r"^[A-Z]{0,3}\d+\s+", "", text)
    text = re.sub(r"^\d+\s+", "", text)
    return text.strip()


def slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def load_targets(path: Path) -> list[TargetGame]:
    df = pd.read_csv(path)
    targets: list[TargetGame] = []
    for row in df.to_dict(orient="records"):
        targets.append(
            TargetGame(
                priority_rank=int(row["priority_rank"]),
                play_by_play=str(row["play_by_play"]),
                game_date=str(row["game_date"]),
                season=str(row["season"]),
                round_name=str(row["round_name"]),
                priority_bucket=str(row["priority_bucket"]),
                matchup=str(row["matchup"]),
                network=str(row["network"]),
                analysts=str(row["analysts"]),
            )
        )
    return targets


def load_results(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=RESULT_COLUMNS)
    df = pd.read_csv(path, dtype=str).fillna("")
    for col in RESULT_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df[RESULT_COLUMNS].copy()


def save_results(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    for col in RESULT_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df[RESULT_COLUMNS].to_csv(path, index=False)


def extract_video_id(url: str) -> str | None:
    parsed = urlparse(url)
    if "youtu.be" in parsed.netloc:
        return parsed.path.lstrip("/")
    if "youtube.com" in parsed.netloc:
        return parse_qs(parsed.query).get("v", [None])[0]
    return None


def build_game(target: TargetGame) -> NCAABGame:
    away, home = target.cleaned_matchup
    return NCAABGame(
        date=target.game_date,
        away_team=away,
        home_team=home,
        network=target.network,
        play_by_play=target.play_by_play,
        color_commentator=target.analysts,
    )


def transcript_output_path(base_dir: Path, target: TargetGame) -> Path:
    away, home = target.cleaned_matchup
    stem = f"{target.game_date}_{slugify(away)}-vs-{slugify(home)}"
    return base_dir / "transcripts" / f"{stem}.txt"


def audio_output_path(base_dir: Path, target: TargetGame) -> Path:
    away, home = target.cleaned_matchup
    stem = f"{target.game_date}_{slugify(away)}-vs-{slugify(home)}"
    return base_dir / "audio" / f"{stem}.mp3"


def normalize_path(path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return (PROJECT_ROOT / path).resolve()


def fetch_transcript(video_id: str):
    try:
        paid_api = YouTubeTranscriptAPI()
        transcript = paid_api.get_transcript(video_id)
        if transcript and transcript.get("text", "").strip():
            return {
                "provider": "youtube_transcript_io",
                "language_code": "en",
                "is_generated": None,
                "text": transcript.get("text", "").strip(),
                "segments": transcript.get("segments", []),
            }
    except Exception:
        pass

    proxy_username = os.getenv("WEBSHARE_PROXY_USERNAME")
    proxy_password = os.getenv("WEBSHARE_PROXY_PASSWORD")
    if proxy_username and proxy_password:
        api = YouTubeTranscriptApi(
            proxy_config=WebshareProxyConfig(
                proxy_username=proxy_username,
                proxy_password=proxy_password,
            )
        )
    else:
        api = YouTubeTranscriptApi()
    for kwargs in (
        {"languages": ["en"]},
        {},
    ):
        try:
            transcript = api.fetch(video_id, **kwargs)
            return {
                "provider": "youtube_transcript_api",
                "language_code": transcript.language_code,
                "is_generated": transcript.is_generated,
                "text": " ".join(snippet.text for snippet in transcript.snippets).strip(),
                "segments": [
                    {
                        "text": snippet.text,
                        "start": snippet.start,
                        "duration": snippet.duration,
                    }
                    for snippet in transcript.snippets
                ],
            }
        except Exception:
            continue
    return None


def write_transcript(path: Path, target: TargetGame, url: str, title: str, transcript) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        handle.write(f"Video ID: {extract_video_id(url) or ''}\n")
        handle.write(f"URL: {url}\n")
        handle.write(f"Title: {title}\n")
        handle.write(f"Date: {target.game_date}\n")
        handle.write(f"Matchup: {target.matchup}\n")
        handle.write(f"Play-by-play: {target.play_by_play}\n")
        handle.write(f"Analysts: {target.analysts}\n")
        handle.write(f"Round: {target.round_name}\n")
        handle.write(f"Transcript source: {transcript.get('provider', '')}\n")
        handle.write(f"Language: {transcript.get('language_code', '')}\n")
        handle.write(f"Generated: {transcript.get('is_generated', '')}\n")
        handle.write("=" * 80 + "\n\n")
        handle.write(transcript.get("text", ""))
        handle.write("\n")


def download_audio(url: str, output_path: Path) -> tuple[bool, str]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_template = str(output_path.with_suffix(".%(ext)s"))
    cmd = [
        str(PROJECT_ROOT / "venv" / "bin" / "yt-dlp"),
        "-f",
        "bestaudio[ext=m4a]/bestaudio/best",
        "-x",
        "--audio-format",
        "mp3",
        "--audio-quality",
        "0",
        "--no-playlist",
        "--force-overwrites",
        "-o",
        temp_template,
        url,
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        return True, ""
    except subprocess.CalledProcessError as exc:
        err = exc.stderr.strip() or exc.stdout.strip() or str(exc)
        return False, err[:500]


def ensure_row(df: pd.DataFrame, target: TargetGame) -> tuple[pd.DataFrame, int]:
    away, home = target.cleaned_matchup
    mask = (
        (df["game_date"] == target.game_date)
        & (df["away_team"] == away)
        & (df["home_team"] == home)
        & (df["play_by_play"] == target.play_by_play)
    )
    matches = df.index[mask].tolist()
    if matches:
        idx = matches[0]
    else:
        idx = len(df)
        df.loc[idx] = {col: "" for col in RESULT_COLUMNS}
    df.at[idx, "priority_rank"] = str(target.priority_rank)
    df.at[idx, "play_by_play"] = target.play_by_play
    df.at[idx, "game_date"] = target.game_date
    df.at[idx, "season"] = target.season
    df.at[idx, "round_name"] = target.round_name
    df.at[idx, "priority_bucket"] = target.priority_bucket
    df.at[idx, "matchup"] = target.matchup
    df.at[idx, "away_team"] = away
    df.at[idx, "home_team"] = home
    df.at[idx, "network"] = target.network
    df.at[idx, "analysts"] = target.analysts
    return df, idx


def search_games(
    targets: list[TargetGame],
    results_df: pd.DataFrame,
    results_path: Path,
    max_games: int | None,
    max_results: int,
) -> pd.DataFrame:
    finder = NCAABVideoFinder(use_scraping=True)
    searched = 0

    for target in targets:
        if max_games is not None and searched >= max_games:
            break
        results_df, idx = ensure_row(results_df, target)
        if results_df.at[idx, "search_status"] == "found":
            continue

        manual_candidate = MANUAL_YOUTUBE_CANDIDATES.get(target.key)
        if manual_candidate:
            results_df.at[idx, "youtube_url"] = manual_candidate["url"]
            results_df.at[idx, "youtube_title"] = manual_candidate["title"]
            results_df.at[idx, "channel"] = "manual_candidate"
            results_df.at[idx, "duration_seconds"] = ""
            results_df.at[idx, "search_status"] = "found"
            results_df.at[idx, "error"] = ""
            print(f"FOUND  {target.priority_rank:02d} {target.matchup} -> {manual_candidate['url']} [manual]")
            searched += 1
            save_results(results_df, results_path)
            continue

        game = build_game(target)
        videos = finder.search_video(game, max_results=max_results)
        if videos:
            best = videos[0]
            results_df.at[idx, "youtube_url"] = best.url
            results_df.at[idx, "youtube_title"] = best.title
            results_df.at[idx, "channel"] = best.channel
            results_df.at[idx, "duration_seconds"] = str(best.duration_seconds)
            results_df.at[idx, "search_status"] = "found"
            results_df.at[idx, "error"] = ""
            print(f"FOUND  {target.priority_rank:02d} {target.matchup} -> {best.url}")
        else:
            results_df.at[idx, "search_status"] = "not_found"
            results_df.at[idx, "error"] = "no youtube candidate"
            print(f"MISS   {target.priority_rank:02d} {target.matchup}")
        searched += 1
        save_results(results_df, results_path)

    return results_df


def extract_games(
    targets: list[TargetGame],
    results_df: pd.DataFrame,
    results_path: Path,
    base_dir: Path,
    max_games: int | None,
) -> pd.DataFrame:
    processed = 0
    target_map = {target.key: target for target in targets}

    for idx, row in results_df.iterrows():
        if max_games is not None and processed >= max_games:
            break
        if row.get("search_status") != "found":
            continue
        if row.get("extraction_status") == "success":
            continue

        key = f"{row.get('game_date')}|{row.get('away_team')}|{row.get('home_team')}"
        target = target_map.get(key)
        if target is None:
            continue

        url = row.get("youtube_url", "")
        title = row.get("youtube_title", "")
        video_id = extract_video_id(url)
        if not video_id:
            results_df.at[idx, "extraction_status"] = "failed"
            results_df.at[idx, "error"] = "invalid youtube url"
            save_results(results_df, results_path)
            continue

        transcript_path = transcript_output_path(base_dir, target)
        audio_path = audio_output_path(base_dir, target)

        if transcript_path.exists():
            results_df.at[idx, "extraction_status"] = "success"
            existing_text = transcript_path.read_text(encoding="utf-8", errors="ignore")
            if "Transcript source: youtube_transcript_io" in existing_text:
                method = "youtube_transcript_io"
            else:
                method = "youtube_transcript_api"
            results_df.at[idx, "extraction_method"] = method
            results_df.at[idx, "transcript_path"] = str(transcript_path.relative_to(PROJECT_ROOT))
            results_df.at[idx, "audio_path"] = ""
            results_df.at[idx, "error"] = ""
            save_results(results_df, results_path)
            processed += 1
            continue

        if audio_path.exists():
            results_df.at[idx, "extraction_status"] = "success"
            results_df.at[idx, "extraction_method"] = "yt_dlp_audio"
            results_df.at[idx, "audio_path"] = str(audio_path.relative_to(PROJECT_ROOT))
            results_df.at[idx, "transcript_path"] = ""
            results_df.at[idx, "error"] = ""
            save_results(results_df, results_path)
            processed += 1
            continue

        transcript = fetch_transcript(video_id)
        if transcript:
            write_transcript(transcript_path, target, url, title, transcript)
            results_df.at[idx, "extraction_status"] = "success"
            results_df.at[idx, "extraction_method"] = transcript.get("provider", "youtube_transcript_api")
            results_df.at[idx, "transcript_path"] = str(transcript_path.relative_to(PROJECT_ROOT))
            results_df.at[idx, "audio_path"] = ""
            results_df.at[idx, "error"] = ""
            print(f"TRANSCRIPT {target.priority_rank:02d} {target.matchup}")
        else:
            ok, err = download_audio(url, audio_path)
            if ok and audio_path.exists():
                results_df.at[idx, "extraction_status"] = "success"
                results_df.at[idx, "extraction_method"] = "yt_dlp_audio"
                results_df.at[idx, "audio_path"] = str(audio_path.relative_to(PROJECT_ROOT))
                results_df.at[idx, "transcript_path"] = ""
                results_df.at[idx, "error"] = ""
                print(f"AUDIO      {target.priority_rank:02d} {target.matchup}")
            else:
                results_df.at[idx, "extraction_status"] = "failed"
                results_df.at[idx, "extraction_method"] = ""
                results_df.at[idx, "error"] = err or "audio download failed"
                print(f"FAILED     {target.priority_rank:02d} {target.matchup}")

        processed += 1
        save_results(results_df, results_path)

    return results_df


def print_summary(results_df: pd.DataFrame) -> None:
    search_counts = results_df["search_status"].value_counts(dropna=False).to_dict()
    extract_counts = results_df["extraction_status"].value_counts(dropna=False).to_dict()
    print("\nSummary")
    print(f"  Search: {search_counts}")
    print(f"  Extraction: {extract_counts}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Bulk YouTube extraction for target NCAAB announcer games")
    parser.add_argument("--targets", default=str(DEFAULT_TARGETS), help="Target CSV")
    parser.add_argument("--results", default=str(DEFAULT_RESULTS), help="Results CSV")
    parser.add_argument("--base-dir", default=str(DEFAULT_BASE_DIR), help="Base output directory")
    parser.add_argument("--search-only", action="store_true", help="Only search for videos")
    parser.add_argument("--extract-only", action="store_true", help="Only extract from existing results")
    parser.add_argument("--max-games", type=int, default=None, help="Limit number of games processed in this run")
    parser.add_argument("--max-results", type=int, default=3, help="Max YouTube candidates per search")
    args = parser.parse_args()

    targets = load_targets(normalize_path(args.targets))
    results_path = normalize_path(args.results)
    base_dir = normalize_path(args.base_dir)
    results_df = load_results(results_path)

    if not args.extract_only:
        results_df = search_games(targets, results_df, results_path, args.max_games, args.max_results)
    if not args.search_only:
        results_df = extract_games(targets, results_df, results_path, base_dir, args.max_games)

    save_results(results_df, results_path)
    print_summary(results_df)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
