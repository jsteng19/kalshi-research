#!/usr/bin/env python3
import argparse
import csv
import html
import os
import re
from typing import Iterable, List, Tuple
from urllib.parse import urlparse


ANCHOR_H3_PATTERN = re.compile(
    r"<a[^>]*?href=\"(?P<href>[^\"]+)\"[^>]*>\s*<h3[^>]*>(?P<title>.*?)</h3>",
    re.IGNORECASE | re.DOTALL,
)


def normalize_url(href: str, base: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    # Ensure single slash between base and path
    if not base.endswith("/") and not href.startswith("/"):
        return f"{base}/{href}"
    if base.endswith("/") and href.startswith("/"):
        return f"{base[:-1]}{href}"
    return f"{base}{href}"


DATE_SUFFIX_8 = re.compile(r"(\d{8})$")
DATE_SUFFIX_6 = re.compile(r"(\d{6})$")


def extract_date_from_url(url: str) -> str:
    """Extract date as yyyy-mm-dd from the URL's last path segment.

    Supports:
    - 8-digit yyyymmdd suffix
    - 6-digit ddmmyy suffix (mapped to 20yy)
    Returns empty string if no date suffix is found.
    """
    try:
        path = urlparse(url).path
        last_segment = path.rstrip("/").split("/")[-1]
    except Exception:
        return ""

    m8 = DATE_SUFFIX_8.search(last_segment)
    if m8:
        yyyymmdd = m8.group(1)
        year = yyyymmdd[0:4]
        month = yyyymmdd[4:6]
        day = yyyymmdd[6:8]
        return f"{year}-{month}-{day}"

    m6 = DATE_SUFFIX_6.search(last_segment)
    if m6:
        ddmmyy = m6.group(1)
        day = ddmmyy[0:2]
        month = ddmmyy[2:4]
        year = f"20{ddmmyy[4:6]}"
        return f"{year}-{month}-{day}"

    return ""


def extract_title_url_pairs(text: str, base_url: str) -> List[Tuple[str, str, str]]:
    pairs: List[Tuple[str, str, str]] = []
    seen_urls = set()

    for match in ANCHOR_H3_PATTERN.finditer(text):
        raw_href = match.group("href").strip()
        raw_title = match.group("title").strip()

        # Clean HTML entities and whitespace
        title = html.unescape(re.sub(r"\s+", " ", raw_title)).strip()
        url = normalize_url(raw_href, base_url)

        if not title or not url:
            continue
        if url in seen_urls:
            continue
        seen_urls.add(url)
        date_str = extract_date_from_url(url)
        pairs.append((title, url, date_str))

    return pairs


def write_csv(rows: Iterable[Tuple[str, str, str]], output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["title", "url", "date"])
        for title, url, date_str in rows:
            writer.writerow([title, url, date_str])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract titles and URLs from gov.il News webarchive/text and save as CSV."
    )
    parser.add_argument(
        "--input",
        default="News.txt",
        help="Path to input file (raw .webarchive text or HTML). Default: News.txt",
    )
    parser.add_argument(
        "--output",
        default="news_urls.csv",
        help="Path to output CSV file. Default: news_urls.csv",
    )
    parser.add_argument(
        "--base-url",
        default="https://www.gov.il",
        help="Base URL to prepend to relative paths. Default: https://www.gov.il",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    with open(args.input, "r", encoding="utf-8", errors="replace") as f:
        text = f.read()

    pairs = extract_title_url_pairs(text, args.base_url)
    write_csv(pairs, args.output)

    print(f"Extracted {len(pairs)} entries to {args.output}")


if __name__ == "__main__":
    main()


