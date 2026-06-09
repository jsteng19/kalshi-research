#!/usr/bin/env python3
"""
Build NCAAB target lists for a specific play-by-play announcer.

This script combines:
1. Current-season repo data in data/ncaab/game_announcers.csv
2. Historical 506 archive pages

Unlike the March Madness builder, this is announcer-specific and does not
impose tournament-vs-regular-season priority unless the caller wants to
filter after generation. Records are sorted by recency.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from src.ncaab.build_march_madness_targets import (
    fetch_text_items,
    parse_announcer_parts,
    parse_archive_date_line,
    slugify,
)


REGULAR_SEASON_CSV = PROJECT_ROOT / "data/ncaab" / "game_announcers.csv"
TARGETS_DIR = PROJECT_ROOT / "data/ncaab" / "targets"

ARCHIVE_SOURCES = [
    {
        "season": "2013-14",
        "url": "https://archive.506sports.com/wiki/2013-14_College_Basketball_Season",
    },
    {
        "season": "2014-15",
        "url": "https://archive.506sports.com/wiki/2014-15_College_Basketball_Season",
    },
    {
        "season": "2015-16",
        "url": "https://archive.506sports.com/wiki/2015-16_College_Basketball_Season",
    },
    {
        "season": "2016-17",
        "url": "https://archive.506sports.com/wiki/2016-17_College_Basketball_Season",
    },
    {
        "season": "2024-25",
        "url": "https://archive.506sports.com/wiki/2024-25_College_Basketball_Season",
    },
    {
        "season": "2023-24",
        "url": "https://archive.506sports.com/wiki/2023-24_College_Basketball_Season",
    },
    {
        "season": "2022-23",
        "url": "https://archive.506sports.com/wiki/2022-23_College_Basketball_Season",
    },
    {
        "season": "2021-22",
        "url": "https://archive.506sports.com/wiki/2021-22_College_Basketball_Season",
    },
    {
        "season": "2020-21",
        "url": "https://archive.506sports.com/wiki/2020-21_College_Basketball_Season",
    },
    {
        "season": "2019-20",
        "url": "https://archive.506sports.com/wiki/2019-20_College_Basketball_Season",
    },
    {
        "season": "2018-19",
        "url": "https://archive.506sports.com/wiki/2018-19_College_Basketball_Season",
    },
    {
        "season": "2017-18",
        "url": "https://archive.506sports.com/wiki/2017-18_College_Basketball_Season",
    },
]

@dataclass
class AnnouncerRecord:
    play_by_play: str
    game_date: str
    season: str
    round_name: str
    priority_bucket: str
    matchup: str
    network: str
    analysts: str
    sideline: str
    source_type: str
    source_url: str


SUPPLEMENTAL_ICDB_RECORDS = {
    "Brandon Gaudin": [
        AnnouncerRecord(
            play_by_play="Brandon Gaudin",
            game_date="2025-02-08",
            season="2024-25",
            round_name="Regular season",
            priority_bucket="mixed",
            matchup="Oregon vs. Michigan State",
            network="FOX",
            analysts="LaPhonso Ellis",
            sideline="",
            source_type="icdb_match_page",
            source_url="https://basketball.icdb.tv/match/14122-Michigan-State-v-Oregon",
        ),
        AnnouncerRecord(
            play_by_play="Brandon Gaudin",
            game_date="2024-01-18",
            season="2023-24",
            round_name="Regular season",
            priority_bucket="mixed",
            matchup="Maryland vs. Northwestern",
            network="Big Ten Network",
            analysts="Stephen Bardo",
            sideline="",
            source_type="icdb_profile_snippet",
            source_url="https://basketball.icdb.tv/stats/581/Brandon-Gaudin",
        ),
    ]
}


def clean_team_name(text: str) -> str:
    value = text.strip()
    value = re.sub(r"^\([A-Za-z0-9]+\)\s*", "", value)
    value = re.sub(r"^\^?\d+\s+", "", value)
    value = re.sub(r"^\[\d+\]\s*", "", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def normalize_matchup(raw_matchup: str) -> tuple[str, str]:
    parts = re.split(r"\s+vs\.?\s+|\s+@\s+|\s+at\s+", raw_matchup, maxsplit=1, flags=re.IGNORECASE)
    if len(parts) != 2:
        return "", ""
    return clean_team_name(parts[0]), clean_team_name(parts[1])


def parse_generic_game_line(
    line: str,
    season: str,
    current_date: str,
    current_section: str,
    source_url: str,
) -> AnnouncerRecord | None:
    if " - " not in line or ", " not in line:
        return None

    left, announcer_blob = line.rsplit(" - ", 1)
    pbp, analysts, sideline = parse_announcer_parts(announcer_blob)
    if not pbp:
        return None

    if left.startswith("Westwood One:") or left.startswith("ESPN International:"):
        return None

    parts = [part.strip() for part in left.split(",")]
    if len(parts) < 3:
        return None

    raw_matchup = ", ".join(parts[:-2]).strip()
    raw_matchup = re.sub(r"^[A-Za-z0-9 .()/&'^:-]+:\s*", "", raw_matchup)
    away, home = normalize_matchup(raw_matchup)
    if not away or not home:
        return None

    return AnnouncerRecord(
        play_by_play=pbp,
        game_date=current_date,
        season=season,
        round_name=current_section or "Season coverage",
        priority_bucket="mixed",
        matchup=f"{away} vs. {home}",
        network=parts[-1].strip(),
        analysts=analysts,
        sideline=sideline,
        source_type="506_archive",
        source_url=source_url,
    )


def load_current_records(announcer: str) -> list[AnnouncerRecord]:
    records: list[AnnouncerRecord] = []
    with REGULAR_SEASON_CSV.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            pbp = (row.get("play_by_play") or "").strip()
            if pbp != announcer:
                continue
            away = (row.get("away_team") or "").strip()
            home = (row.get("home_team") or "").strip()
            if not away or not home:
                continue
            records.append(
                AnnouncerRecord(
                    play_by_play=pbp,
                    game_date=(row.get("date") or "").strip(),
                    season="2025-26",
                    round_name="Regular season",
                    priority_bucket="mixed",
                    matchup=f"{away} vs. {home}",
                    network=(row.get("network") or "").strip(),
                    analysts=(row.get("color_commentator") or "").strip(),
                    sideline="",
                    source_type="repo_regular_season",
                    source_url=str(REGULAR_SEASON_CSV.relative_to(PROJECT_ROOT)),
                )
            )
    return records


def load_archive_records(announcer: str) -> list[AnnouncerRecord]:
    records: list[AnnouncerRecord] = []
    for source in ARCHIVE_SOURCES:
        try:
            items = fetch_text_items(source["url"])
        except Exception as exc:
            print(f"Skipping {source['season']} archive due to fetch error: {exc}")
            continue
        current_date = ""
        current_section = ""
        for tag, text in items:
            if tag in {"h2", "h3", "h4"}:
                current_section = text.strip()
                continue

            maybe_date = parse_archive_date_line(text, source["season"])
            if maybe_date:
                current_date = maybe_date
                continue

            if not current_date:
                continue

            record = parse_generic_game_line(
                text,
                season=source["season"],
                current_date=current_date,
                current_section=current_section,
                source_url=source["url"],
            )
            if record and record.play_by_play == announcer:
                records.append(record)
    return records


def dedupe_records(records: list[AnnouncerRecord]) -> list[AnnouncerRecord]:
    seen: set[tuple[str, str, str]] = set()
    deduped: list[AnnouncerRecord] = []
    for record in records:
        key = (record.play_by_play, record.game_date, record.matchup)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(record)
    return deduped


def write_targets(path: Path, records: list[AnnouncerRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "priority_rank",
                "play_by_play",
                "game_date",
                "season",
                "round_name",
                "priority_bucket",
                "matchup",
                "network",
                "analysts",
                "sideline",
                "source_type",
                "source_url",
            ]
        )
        for idx, record in enumerate(records, start=1):
            writer.writerow(
                [
                    idx,
                    record.play_by_play,
                    record.game_date,
                    record.season,
                    record.round_name,
                    record.priority_bucket,
                    record.matchup,
                    record.network,
                    record.analysts,
                    record.sideline,
                    record.source_type,
                    record.source_url,
                ]
            )


def main() -> int:
    parser = argparse.ArgumentParser(description="Build NCAAB targets for a single announcer")
    parser.add_argument("--announcer", required=True, help="Play-by-play announcer name")
    parser.add_argument("--limit", type=int, default=60, help="Max targets to write")
    parser.add_argument("--output", default="", help="Output CSV path")
    args = parser.parse_args()

    records = dedupe_records(
        load_current_records(args.announcer)
        + load_archive_records(args.announcer)
        + SUPPLEMENTAL_ICDB_RECORDS.get(args.announcer, [])
    )
    records.sort(key=lambda record: (record.game_date, record.season), reverse=True)
    limited = records[: args.limit]

    output_path = (
        Path(args.output)
        if args.output
        else TARGETS_DIR / f"{slugify(args.announcer)}.csv"
    )
    if not output_path.is_absolute():
        output_path = (PROJECT_ROOT / output_path).resolve()
    write_targets(output_path, limited)

    sections = Counter(record.round_name for record in limited)
    print(f"Wrote {len(limited)} targets for {args.announcer} to {output_path}")
    print(f"Date range: {limited[-1].game_date} -> {limited[0].game_date}" if limited else "No targets found")
    print(f"Sections: {dict(sections.most_common(10))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
