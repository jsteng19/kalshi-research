#!/usr/bin/env python3
"""
Build tournament-first target game lists for the upcoming men's tournament crews.

This script pulls historical NCAA tournament game-announcer mappings from
506 Sports archive pages using only the Python standard library, then
supplements shortfalls with the current 2025-26 regular season feed in
data/ncaab/game_announcers.csv.

Outputs:
  - data/ncaab/targets/2026-march-madness-targets.csv
  - data/ncaab/targets/<announcer-slug>.csv
  - data/ncaab/targets/README.md
"""

from __future__ import annotations

import csv
import re
import urllib.request
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data/ncaab" / "targets"
REGULAR_SEASON_CSV = PROJECT_ROOT / "data/ncaab" / "game_announcers.csv"

MIN_TARGET_GAMES = 12

TARGET_ANNOUNCERS = [
    "Ian Eagle",
    "Brian Anderson",
    "Andrew Catalon",
    "Kevin Harlan",
    "Brad Nessler",
    "Tom McCarthy",
    "Brandon Gaudin",
    "Jordan Kent",
]

TOURNAMENT_SOURCES = [
    {
        "season": "2024-25",
        "url": "https://archive.506sports.com/wiki/2024-25_College_Basketball_Season_%28NCAA_tournament%29",
        "page_type": "tournament_only",
    },
    {
        "season": "2023-24",
        "url": "https://archive.506sports.com/wiki/2023-24_College_Basketball_Season_%28NCAA_tournament%29",
        "page_type": "tournament_only",
    },
    {
        "season": "2022-23",
        "url": "https://archive.506sports.com/wiki/2022-23_College_Basketball_Season",
        "page_type": "full_season",
    },
    {
        "season": "2021-22",
        "url": "https://archive.506sports.com/wiki/2021-22_College_Basketball_Season",
        "page_type": "full_season",
    },
    {
        "season": "2020-21",
        "url": "https://archive.506sports.com/wiki/2020-21_College_Basketball_Season",
        "page_type": "full_season",
    },
    {
        "season": "2018-19",
        "url": "https://archive.506sports.com/wiki/2018-19_College_Basketball_Season",
        "page_type": "full_season",
    },
    {
        "season": "2017-18",
        "url": "https://archive.506sports.com/wiki/2017-18_College_Basketball_Season",
        "page_type": "full_season",
    },
]

REGULAR_SEASON_SOURCES = [
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
]

ROUND_ORDER = {
    "National Championship": 8,
    "Final Four": 7,
    "Elite 8": 6,
    "Sweet 16": 5,
    "Second round": 4,
    "First round": 3,
    "First Four": 2,
    "Regular season": 1,
}

ROUND_ALIASES = {
    "Final 4": "Final Four",
    "National Championship": "National Championship",
    "Round of 64": "First round",
    "Round of 32": "Second round",
    "Regional Semifinal": "Sweet 16",
    "Regional Semifinals": "Sweet 16",
    "Regionals": "Sweet 16",
    "Regional Final": "Elite 8",
    "Regional Finals": "Elite 8",
    "National Semifinal": "Final Four",
    "National Semifinals": "Final Four",
}


@dataclass
class GameRecord:
    play_by_play: str
    season: str
    game_date: str
    round_name: str
    matchup: str
    network: str
    analysts: str
    sideline: str
    source_type: str
    source_url: str
    priority_bucket: str
    source_rank: int


class ArchiveTextParser(HTMLParser):
    """Collect plain text from 506 archive paragraph and heading tags."""

    def __init__(self) -> None:
        super().__init__()
        self.current_tag: str | None = None
        self.buffer: list[str] = []
        self.items: list[tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag in {"p", "h2", "h3", "h4"}:
            self.current_tag = tag
            self.buffer = []

    def handle_data(self, data: str) -> None:
        if self.current_tag:
            self.buffer.append(data)

    def handle_endtag(self, tag: str) -> None:
        if self.current_tag == tag:
            text = " ".join("".join(self.buffer).split())
            if text:
                self.items.append((tag, text))
            self.current_tag = None
            self.buffer = []


def slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def fetch_text_items(url: str) -> list[tuple[str, str]]:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as response:
        html = response.read().decode("utf-8", "ignore")
    parser = ArchiveTextParser()
    parser.feed(html)
    return parser.items


def clean_team_name(name: str) -> str:
    s = name.strip()
    s = re.sub(r"^\(?[A-Z0-9]+\)?\s*", "", s)
    s = re.sub(r"^\d+\s+", "", s)
    s = re.sub(r"^\[\d+\]\s*", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_matchup(matchup: str) -> tuple[str, str]:
    parts = re.split(r"\s+vs\.?\s+|\s+@\s+|\s+at\s+", matchup, maxsplit=1, flags=re.IGNORECASE)
    if len(parts) == 2:
        return clean_team_name(parts[0]), clean_team_name(parts[1])
    return matchup.strip(), ""


def parse_announcer_parts(raw_announcers: str) -> tuple[str, str, str]:
    people = [p.strip() for p in raw_announcers.split(",") if p.strip()]
    pbp = people[0] if people else ""
    analysts = ", ".join(people[1:-1]) if len(people) > 2 else (people[1] if len(people) == 2 else "")
    sideline = people[-1] if len(people) > 2 else ""
    return pbp, analysts, sideline


def parse_tournament_game_line(
    line: str,
    season: str,
    round_name: str,
    current_date: str,
    source_url: str,
) -> GameRecord | None:
    if " - " not in line or ", " not in line:
        return None

    left, announcer_blob = line.rsplit(" - ", 1)
    pbp, analysts, sideline = parse_announcer_parts(announcer_blob)
    if not pbp:
        return None

    # Remove site/region prefixes such as "(L)South:" or "Semi.:"
    left = re.sub(r"^\([^)]+\)", "", left).strip()
    left = re.sub(r"^(East|West|South|Midwest|MW|Semi\.)\s*:\s*", "", left)

    m = re.match(r"^(?P<matchup>.+?),\s*(?P<time>\d{1,2}:\d{2}),\s*(?P<network>[^,]+)$", left)
    if not m:
        return None

    matchup = m.group("matchup").strip()
    network = m.group("network").strip()
    matchup = re.sub(r"^[A-Za-z0-9 .()/&'-]+:\s*", "", matchup)
    round_override = ""
    for prefix, override in [
        ("Champ.:", "National Championship"),
        ("Final:", "National Championship"),
        ("Semifinal:", "Final Four"),
        ("Semi.:", "Final Four"),
    ]:
        if matchup.startswith(prefix):
            matchup = matchup[len(prefix):].strip()
            round_override = override
            break

    return GameRecord(
        play_by_play=pbp,
        season=season,
        game_date=current_date,
        round_name=round_override or round_name,
        matchup=matchup,
        network=network,
        analysts=analysts,
        sideline=sideline,
        source_type="506_tournament",
        source_url=source_url,
        priority_bucket="tournament",
        source_rank=0,
    )


def parse_archive_date_line(text: str, season: str) -> str | None:
    text = text.strip()
    month_map = {
        "january": 1,
        "jan": 1,
        "february": 2,
        "feb": 2,
        "march": 3,
        "mar": 3,
        "april": 4,
        "apr": 4,
        "may": 5,
        "june": 6,
        "jun": 6,
        "july": 7,
        "jul": 7,
        "august": 8,
        "aug": 8,
        "september": 9,
        "sep": 9,
        "sept": 9,
        "october": 10,
        "oct": 10,
        "november": 11,
        "nov": 11,
        "december": 12,
        "dec": 12,
    }
    m = re.search(
        r"(?:Mon(?:day)?|Tue(?:s|sday)?|Wed(?:nesday)?|Thu(?:rs|rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)\.?,?\s+([A-Za-z]+)\s+(\d{1,2})",
        text,
        re.IGNORECASE,
    )
    if m:
        month = month_map.get(m.group(1).lower())
        if month is None:
            return None
        day = int(m.group(2))
    else:
        m = re.search(
            r"(?:Mon(?:day)?|Tue(?:s|sday)?|Wed(?:nesday)?|Thu(?:rs|rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)\.?,?\s+(\d{1,2})/(\d{1,2})",
            text,
            re.IGNORECASE,
        )
        if m:
            month = int(m.group(1))
            day = int(m.group(2))
        else:
            m = re.search(r"^([A-Za-z]+)\s+(\d{1,2})$", text, re.IGNORECASE)
            if m:
                month = month_map.get(m.group(1).lower())
                if month is None:
                    return None
                day = int(m.group(2))
            else:
                m = re.search(r"^(\d{1,2})/(\d{1,2})$", text)
                if not m:
                    return None
                month = int(m.group(1))
                day = int(m.group(2))
    season_start_year = int(season[:4])
    year = season_start_year if month >= 10 else season_start_year + 1
    return f"{year:04d}-{month:02d}-{day:02d}"


def parse_regular_game_line(
    line: str,
    season: str,
    current_date: str,
    source_url: str,
) -> GameRecord | None:
    if " - " not in line or ", " not in line:
        return None

    left, announcer_blob = line.rsplit(" - ", 1)
    pbp, analysts, sideline = parse_announcer_parts(announcer_blob)
    if pbp not in TARGET_ANNOUNCERS:
        return None

    if left.startswith("Westwood One:") or left.startswith("ESPN International:"):
        return None

    parts = [part.strip() for part in left.split(",")]
    if len(parts) < 3:
        return None

    matchup = ", ".join(parts[:-2]).strip()
    network = parts[-1].strip()

    return GameRecord(
        play_by_play=pbp,
        season=season,
        game_date=current_date,
        round_name="Regular season",
        matchup=matchup,
        network=network,
        analysts=analysts,
        sideline=sideline,
        source_type="506_regular_season",
        source_url=source_url,
        priority_bucket="regular_season",
        source_rank=1,
    )


def iter_tournament_records(source: dict[str, str]) -> list[GameRecord]:
    items = fetch_text_items(source["url"])
    results: list[GameRecord] = []
    active = source["page_type"] == "tournament_only"
    round_name = ""
    current_date = ""

    for _tag, text in items:
        normalized = ROUND_ALIASES.get(text, text)

        if not active and normalized.startswith("First Four"):
            active = True
            round_name = "First Four"
            current_date = ""
            continue

        if not active:
            continue

        if normalized in {"NIT", "National Invitation Tournament"}:
            break

        if normalized in {
            "First Four",
            "First round",
            "Second round",
            "Sweet 16",
            "Elite 8",
            "Final Four",
            "National Championship",
        }:
            round_name = normalized
            continue

        maybe_date = parse_archive_date_line(text, source["season"])
        if maybe_date:
            current_date = maybe_date
            continue

        if not current_date:
            continue

        record = parse_tournament_game_line(
            text,
            season=source["season"],
            round_name=round_name,
            current_date=current_date,
            source_url=source["url"],
        )
        if record and record.play_by_play in TARGET_ANNOUNCERS:
            results.append(record)

    return results


def load_regular_season_records() -> list[GameRecord]:
    records: list[GameRecord] = []
    with REGULAR_SEASON_CSV.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            pbp = (row.get("play_by_play") or "").strip()
            if pbp not in TARGET_ANNOUNCERS:
                continue
            date = (row.get("date") or "").strip()
            away = (row.get("away_team") or "").strip()
            home = (row.get("home_team") or "").strip()
            color = (row.get("color_commentator") or "").strip()
            matchup = f"{away} vs. {home}"
            records.append(
                GameRecord(
                    play_by_play=pbp,
                    season="2025-26",
                    game_date=date,
                    round_name="Regular season",
                    matchup=matchup,
                    network=(row.get("network") or "").strip(),
                    analysts=color,
                    sideline="",
                    source_type="repo_regular_season",
                    source_url=str(REGULAR_SEASON_CSV.relative_to(PROJECT_ROOT)),
                    priority_bucket="regular_season",
                    source_rank=1,
                )
            )
    return records


def load_archive_regular_season_records() -> list[GameRecord]:
    records: list[GameRecord] = []
    for source in REGULAR_SEASON_SOURCES:
        items = fetch_text_items(source["url"])
        current_date = ""
        for _tag, text in items:
            if text.startswith("First Four"):
                break

            maybe_date = parse_archive_date_line(text, source["season"])
            if maybe_date:
                current_date = maybe_date
                continue

            if not current_date:
                continue

            record = parse_regular_game_line(
                text,
                season=source["season"],
                current_date=current_date,
                source_url=source["url"],
            )
            if record:
                records.append(record)
    return records


def dedupe_records(records: list[GameRecord]) -> list[GameRecord]:
    seen: set[tuple[str, str, str, str]] = set()
    deduped: list[GameRecord] = []
    for record in records:
        key = (
            record.play_by_play,
            record.game_date,
            record.matchup,
            record.round_name,
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(record)
    return deduped


def sort_key(record: GameRecord) -> tuple[int, str]:
    return (
        ROUND_ORDER.get(record.round_name, 0),
        record.game_date,
    )


def build_target_lists() -> dict[str, list[GameRecord]]:
    tournament_records: list[GameRecord] = []
    for source in TOURNAMENT_SOURCES:
        tournament_records.extend(iter_tournament_records(source))
    tournament_records = dedupe_records(tournament_records)

    regular_records = dedupe_records(
        load_regular_season_records() + load_archive_regular_season_records()
    )

    by_announcer: dict[str, list[GameRecord]] = defaultdict(list)
    for record in tournament_records:
        by_announcer[record.play_by_play].append(record)

    regular_by_announcer: dict[str, list[GameRecord]] = defaultdict(list)
    for record in regular_records:
        regular_by_announcer[record.play_by_play].append(record)

    final_lists: dict[str, list[GameRecord]] = {}
    for announcer in TARGET_ANNOUNCERS:
        tournament_list = sorted(
            by_announcer.get(announcer, []),
            key=sort_key,
            reverse=True,
        )
        regular_list = sorted(
            regular_by_announcer.get(announcer, []),
            key=lambda record: record.game_date,
            reverse=True,
        )

        selected = list(tournament_list)
        if len(selected) < MIN_TARGET_GAMES:
            for record in regular_list:
                selected.append(record)
                if len(selected) >= MIN_TARGET_GAMES:
                    break

        final_lists[announcer] = selected

    return final_lists


def write_csv(path: Path, records: list[GameRecord]) -> None:
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


def build_summary(final_lists: dict[str, list[GameRecord]]) -> str:
    lines = [
        "# March Madness 2026 target game lists",
        "",
        f"Generated on {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}.",
        "",
        "Priority rule:",
        f"- Tournament games are always ranked ahead of regular season games. Minimum target per announcer is {MIN_TARGET_GAMES} when available.",
        "- The 2020 NCAA men's tournament was canceled, so there is no 2020 tournament inventory to backfill.",
        "",
        "Source pages:",
    ]
    for source in TOURNAMENT_SOURCES:
        lines.append(f"- {source['season']}: {source['url']}")
    lines.append(f"- Regular season fallback: `{REGULAR_SEASON_CSV.relative_to(PROJECT_ROOT)}`")
    for source in REGULAR_SEASON_SOURCES:
        lines.append(f"- Regular season archive fallback {source['season']}: {source['url']}")
    lines.append("")
    lines.append("Coverage summary:")

    for announcer in TARGET_ANNOUNCERS:
        records = final_lists.get(announcer, [])
        bucket_counts = Counter(record.priority_bucket for record in records)
        round_counts = Counter(record.round_name for record in records if record.priority_bucket == "tournament")
        lines.append(
            f"- {announcer}: {len(records)} targets "
            f"({bucket_counts.get('tournament', 0)} tournament, {bucket_counts.get('regular_season', 0)} regular-season fallback)"
        )
        if round_counts:
            round_bits = ", ".join(
                f"{name}={round_counts[name]}"
                for name in [
                    "National Championship",
                    "Final Four",
                    "Elite 8",
                    "Sweet 16",
                    "Second round",
                    "First round",
                    "First Four",
                ]
                if round_counts.get(name)
            )
            lines.append(f"  Tournament rounds: {round_bits}")
        else:
            lines.append("  Tournament rounds: none found in 2021-2025 archive pages")
            lines.append("  Status: fallback-only target; no 2021-2025 men's tournament TV inventory found")
        if len(records) < MIN_TARGET_GAMES:
            lines.append("  Status: exhausted current tournament archive + 2025-26 regular-season backfill")

    return "\n".join(lines) + "\n"


def main() -> None:
    final_lists = build_target_lists()

    master_records: list[GameRecord] = []
    for announcer in TARGET_ANNOUNCERS:
        records = final_lists[announcer]
        master_records.extend(records)
        write_csv(DATA_DIR / f"{slugify(announcer)}.csv", records)

    master_records.sort(key=lambda record: (record.play_by_play, record.priority_bucket, record.game_date), reverse=False)
    write_csv(DATA_DIR / "2026-march-madness-targets.csv", master_records)
    (DATA_DIR / "README.md").write_text(build_summary(final_lists), encoding="utf-8")

    print(f"Wrote target files to {DATA_DIR}")
    for announcer in TARGET_ANNOUNCERS:
        print(f"{announcer}: {len(final_lists[announcer])} games")


if __name__ == "__main__":
    main()
