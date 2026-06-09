#!/usr/bin/env python3
"""Find and extract audio from basketball-video.com NCAA replay pages."""

from __future__ import annotations

import argparse
import csv
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from src.nba.filemoon_extractor import extract_audio_from_hls_parallel, extract_hls_url_from_filemoon
from src.nba.okru_extractor import extract_audio as extract_okru_audio

BASE = "https://basketball-video.com"


@dataclass
class ReplayHit:
    page_url: str
    title: str
    source_url: str
    source_kind: str


def _headers() -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    }


def _slug(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text


def date_page_url(date: str) -> str:
    dt = datetime.strptime(date, "%Y-%m-%d")
    return f"{BASE}/ncaa-college-basketball-full-game-replays-{dt.strftime('%B').lower()}-{dt.day}-{dt.year}"


def search_url(query: str) -> str:
    return f"{BASE}/search/?q={quote_plus(query)}"


def _get_html(url: str) -> Optional[str]:
    try:
        resp = requests.get(url, headers=_headers(), timeout=25)
        if resp.status_code != 200:
            return None
        return resp.text
    except Exception:
        return None


def _candidate_anchors(html: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    hits: list[tuple[str, str]] = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = " ".join(a.get_text(" ", strip=True).split())
        if not text:
            continue
        if "/ncaa_video/" in href or "/cbb" in href:
            hits.append((text, href))
    return hits


def _match_score(title: str, away: str, home: str, date: str) -> int:
    title_l = title.lower()
    dt = datetime.strptime(date, "%Y-%m-%d")
    date_tokens = {
        date.lower(),
        dt.strftime("%b %d, %Y").replace(" 0", " ").lower(),
        dt.strftime("%B %d, %Y").replace(" 0", " ").lower(),
    }
    score = 0
    if away.lower() in title_l:
        score += 2
    if home.lower() in title_l:
        score += 2
    if any(token in title_l for token in date_tokens):
        score += 2
    if "basketball full game replay" in title_l:
        score += 1
    return score


def find_replay_page(away: str, home: str, date: str, allow_older_matchups: bool = False) -> Optional[ReplayHit]:
    dt = datetime.strptime(date, "%Y-%m-%d")
    date_queries = [
        f"{away} {home} {dt.strftime('%B')} {dt.day} {dt.year}",
        f"{away} {home} {dt.strftime('%b')} {dt.day} {dt.year}",
        f"{away} {home} {date}",
    ]
    matchup_queries = [f"{away} {home}", f"{home} {away}"]

    date_html = _get_html(date_page_url(date))
    if date_html:
        candidates = _candidate_anchors(date_html)
        best: Optional[tuple[int, str, str]] = None
        for title, href in candidates:
            score = _match_score(title, away, home, date)
            if score >= 4 and (best is None or score > best[0]):
                best = (score, title, href)
        if best:
            href = best[2]
            if href.startswith("/"):
                href = f"{BASE}{href}"
            return ReplayHit(page_url=href, title=best[1], source_url="", source_kind="")

    for query in date_queries:
        html = _get_html(search_url(query))
        if not html:
            continue
        candidates = _candidate_anchors(html)
        scored = []
        for title, href in candidates:
            score = _match_score(title, away, home, date)
            if score >= 5 or (allow_older_matchups and score >= 2):
                scored.append((score, title, href))
        if scored:
            scored.sort(key=lambda x: (x[0], len(x[1])), reverse=True)
            _, title, href = scored[0]
            if href.startswith("/"):
                href = f"{BASE}{href}"
            return ReplayHit(page_url=href, title=title, source_url="", source_kind="")

    if not allow_older_matchups:
        return None

    for query in matchup_queries:
        html = _get_html(search_url(query))
        if not html:
            continue
        candidates = _candidate_anchors(html)
        scored = []
        for title, href in candidates:
            score = _match_score(title, away, home, date)
            if score >= 2:
                scored.append((score, title, href))
        if scored:
            scored.sort(key=lambda x: (x[0], len(x[1])), reverse=True)
            _, title, href = scored[0]
            if href.startswith("/"):
                href = f"{BASE}{href}"
            return ReplayHit(page_url=href, title=title, source_url="", source_kind="")

    return None


def detect_source(page_url: str) -> Optional[ReplayHit]:
    html = _get_html(page_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    page_title = soup.title.get_text(" ", strip=True) if soup.title else page_url

    for tag in soup.find_all(["iframe", "a"], href=True):
        src = tag.get("src") or tag.get("href") or ""
        src = src.strip()
        if src.startswith("//"):
            src = "https:" + src
        if "ok.ru/videoembed/" in src:
            return ReplayHit(page_url=page_url, title=page_title, source_url=src, source_kind="okru")
        if any(d in src for d in ("filemoon.sx", "filemoon.to", "luluvdo.com", "fmembed.cc")):
            return ReplayHit(page_url=page_url, title=page_title, source_url=src, source_kind="filemoon")

    text = html
    okru = re.search(r"(https?:)?//(?:www\.)?ok\.ru/videoembed/\d+", text)
    if okru:
        src = okru.group(0)
        if src.startswith("//"):
            src = "https:" + src
        return ReplayHit(page_url=page_url, title=page_title, source_url=src, source_kind="okru")

    filemoon = re.search(r"https?://(?:www\.)?(?:filemoon\.sx|filemoon\.to|luluvdo\.com|fmembed\.cc)/e/[^\s\"']+", text)
    if filemoon:
        return ReplayHit(page_url=page_url, title=page_title, source_url=filemoon.group(0), source_kind="filemoon")

    return None


def extract_audio_from_replay(hit: ReplayHit, output_dir: str, filename: str) -> bool:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    output_path = out / f"{filename}.mp3"
    if hit.source_kind == "okru":
        return extract_okru_audio(
            hit.source_url,
            str(out),
            filename=filename,
            sample_rate=16000,
            channels=1,
            bitrate="48k",
        )
    if hit.source_kind == "filemoon":
        hls_url = extract_hls_url_from_filemoon(hit.source_url)
        if not hls_url:
            return False
        return extract_audio_from_hls_parallel(
            hls_url=hls_url,
            output_path=str(output_path),
            sample_rate=16000,
            channels=1,
            bitrate="48k",
        )
    return False


def process_game(away: str, home: str, date: str, output_dir: str, allow_older_matchups: bool = False) -> int:
    page = find_replay_page(away, home, date, allow_older_matchups=allow_older_matchups)
    if not page:
        print(f"MISS  {date} {away} at {home}")
        return 1
    print(f"FOUND {date} {away} at {home}")
    print(f"  page: {page.page_url}")
    print(f"  title: {page.title}")
    source = detect_source(page.page_url)
    if not source:
        print("  no embed source found")
        return 1
    print(f"  source: {source.source_kind} -> {source.source_url}")
    filename = f"{date}_{_slug(away)}-vs-{_slug(home)}"
    ok = extract_audio_from_replay(source, output_dir, filename)
    print("  extracted" if ok else "  extraction failed")
    return 0 if ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Basketball-video NCAA audio extractor")
    parser.add_argument("--away", help="Away team")
    parser.add_argument("--home", help="Home team")
    parser.add_argument("--date", help="Game date YYYY-MM-DD")
    parser.add_argument("--output-dir", default="data/ncaab/audio/basketball-video")
    parser.add_argument("--allow-older-matchups", action="store_true")
    parser.add_argument("--targets", help="CSV with game_date,away_team,home_team")
    args = parser.parse_args()

    if args.targets:
        with open(args.targets, newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        failures = 0
        for row in rows:
            failures += process_game(
                row["away_team"],
                row["home_team"],
                row["game_date"],
                args.output_dir,
                allow_older_matchups=args.allow_older_matchups,
            )
        return 1 if failures else 0

    if not (args.away and args.home and args.date):
        parser.error("provide either --targets or --away/--home/--date")

    return process_game(
        args.away,
        args.home,
        args.date,
        args.output_dir,
        allow_older_matchups=args.allow_older_matchups,
    )


if __name__ == "__main__":
    raise SystemExit(main())
