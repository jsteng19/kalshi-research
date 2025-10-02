#!/usr/bin/env python3
import argparse
import csv
import os
import re
from datetime import datetime
from typing import List, Optional, Tuple


def sanitize_filename(filename: str) -> str:
    filename = re.sub(r'[<>:"/\\|?*]', '', filename)
    filename = re.sub(r'\s+', '_', filename)
    filename = filename.strip('._')
    return filename[:200]


def parse_iso(date_str: str) -> Optional[datetime]:
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d"):  # be tolerant
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None


def extract_date_from_url(url: str) -> Tuple[Optional[datetime], str]:
    try:
        last = url.rstrip('/').split('/')[-1]
    except Exception:
        return None, ""
    # ddmmyy anywhere in last segment
    m6 = re.search(r"(\d{6})(?:$|[^0-9])", last)
    if m6:
        ddmmyy = m6.group(1)
        day = int(ddmmyy[0:2])
        month = int(ddmmyy[2:4])
        year = 2000 + int(ddmmyy[4:6])
        try:
            dt = datetime(year, month, day)
            return dt, dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    # yyyymmdd
    m8 = re.search(r"(\d{8})(?:$|[^0-9])", last)
    if m8:
        yyyymmdd = m8.group(1)
        year = int(yyyymmdd[0:4])
        month = int(yyyymmdd[4:6])
        day = int(yyyymmdd[6:8])
        try:
            dt = datetime(year, month, day)
            return dt, dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None, ""


def expected_filename(title: str, url: str, date_str: str) -> Optional[str]:
    dt = parse_iso(date_str)
    iso = date_str
    if dt is None:
        dt2, iso2 = extract_date_from_url(url)
        if dt2 is None:
            return None
        dt, iso = dt2, iso2
    base = f"{iso}_{sanitize_filename(title)}.txt"
    return base


def filter_missing(input_csv: str, output_csv: str, transcripts_dir: str, delete_existing: bool) -> Tuple[int, int]:
    os.makedirs(os.path.dirname(output_csv) or '.', exist_ok=True)

    kept_rows: List[dict] = []
    total = 0
    deleted_count = 0
    missing_count = 0

    with open(input_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or ['title', 'url', 'date']
        for row in reader:
            total += 1
            title = (row.get('title') or '').strip()
            url = (row.get('url') or '').strip()
            date_str = (row.get('date') or '').strip()

            if not title or not url:
                # Keep malformed rows out of the missing list
                continue

            fname = expected_filename(title, url, date_str)
            if not fname:
                # If we cannot form a filename (no date), treat as missing to review
                kept_rows.append(row)
                missing_count += 1
                continue

            path = os.path.join(transcripts_dir, fname)
            if os.path.exists(path):
                if delete_existing:
                    try:
                        os.remove(path)
                        deleted_count += 1
                    except Exception:
                        pass
                # Do not include in output (already scraped)
            else:
                kept_rows.append(row)
                missing_count += 1

    with open(output_csv, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(kept_rows)

    return deleted_count, missing_count


def main():
    parser = argparse.ArgumentParser(description='Filter CSV to only include items without an existing transcript file')
    parser.add_argument('--input', required=True, help='Input CSV (e.g., data-netanyahu/unga_included_2004_2025.csv)')
    parser.add_argument('--output', required=True, help='Output CSV for missing items')
    parser.add_argument('--transcripts-dir', default='data-netanyahu/raw-transcripts', help='Directory containing transcript .txt files')
    parser.add_argument('--delete-existing', action='store_true', help='Also delete the already-scraped transcript files from disk (optional)')
    args = parser.parse_args()

    deleted, missing = filter_missing(args.input, args.output, args.transcripts_dir, args.delete_existing)
    print(f"Missing items written to: {args.output}")
    print(f"Deleted existing transcripts: {deleted}")
    print(f"Missing count: {missing}")


if __name__ == '__main__':
    main()





