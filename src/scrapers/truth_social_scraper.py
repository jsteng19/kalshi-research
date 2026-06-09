"""Truth Social scraper backed by the Rollcall / Factbase JSON API.

Rollcall hosts a Factbase index of Trump's social posts at
`https://rollcall.com/factbase/trump/topic/social/`. The page is hydrated by
this REST endpoint:

    GET https://rollcall.com/wp-json/factbase/v1/twitter
        ?person=trump&platform=truth+social&sort=date&sort_order=desc&page=N

Each page returns 50 records of:
    {date, text, platform, post_url, handle, document_id, ...}

Why use this instead of truthsocial.com directly?
    Truth Social's own API recently locked the OAuth password-grant behind
    Cloudflare (403), so we can't get a bearer token. Anonymous access still
    works but is rate-limited to ~4-6 pages of 20 posts before a 60s cooldown,
    which makes a multi-month backfill take 30+ minutes. The Rollcall index
    is unrate-limited, returns 50 posts/page in ~20ms, has full text in the
    JSON response, and goes back to 2017+.

CLI
---
    python -m src.truth_social_scraper                      # incremental update
    python -m src.truth_social_scraper --since 2025-01-01   # explicit cutoff
    python -m src.truth_social_scraper --full               # rebuild from 2024-01-01
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests


API_URL = "https://rollcall.com/wp-json/factbase/v1/twitter"
DEFAULT_PROFILE = "trump"
DEFAULT_PLATFORM = "truth social"
DEFAULT_CSV = "data/trump/truth-social/trump_truths_full.csv"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


class TruthSocialScraper:
    def __init__(
        self,
        person: str = DEFAULT_PROFILE,
        platform: str = DEFAULT_PLATFORM,
        csv_path: str = DEFAULT_CSV,
        request_timeout: int = 30,
        sleep_between_pages: float = 0.1,
        max_retries: int = 5,
    ) -> None:
        self.person = person
        self.platform = platform
        self.csv_path = Path(csv_path)
        self.request_timeout = request_timeout
        self.sleep_between_pages = sleep_between_pages
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ API
    def _get_page(self, page: int) -> dict:
        params = {
            "person": self.person,
            "platform": self.platform,
            "sort": "date",
            "sort_order": "desc",
            "page": page,
        }
        backoff = 5.0
        for attempt in range(1, self.max_retries + 1):
            resp = self.session.get(API_URL, params=params, timeout=self.request_timeout)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code in (429, 502, 503, 504):
                retry_after = resp.headers.get("retry-after")
                wait = float(retry_after) if retry_after and retry_after.isdigit() else backoff
                print(f"  HTTP {resp.status_code} on page {page} attempt {attempt}; sleeping {wait:.1f}s")
                time.sleep(wait)
                backoff = min(backoff * 2, 60.0)
                continue
            resp.raise_for_status()
        raise RuntimeError(f"Exhausted retries on page {page}")

    def fetch_since(self, since: datetime, verbose: bool = True) -> list[dict]:
        """Page through the API (newest first) until we cross `since`."""
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)

        collected: list[dict] = []
        page = 1
        page_count: int | None = None
        while True:
            payload = self._get_page(page)
            data = payload.get("data", [])
            meta = payload.get("meta", {})
            if page_count is None:
                page_count = meta.get("page_count")
                if verbose:
                    print(
                        f"  total_hits={meta.get('total_hits')}, page_count={page_count}, page_size={meta.get('page_size')}"
                    )
            if not data:
                break
            stop = False
            for item in data:
                created = self._parse_date(item.get("date"))
                if created is None:
                    continue
                if created >= since:
                    collected.append(item)
                else:
                    stop = True
                    break
            if verbose:
                last = data[-1].get("date") if data else "?"
                print(f"  page {page}: +{len(data)} (total kept: {len(collected)}, last_date={last})")
            if stop:
                break
            if page_count and page >= page_count:
                break
            page += 1
            if self.sleep_between_pages:
                time.sleep(self.sleep_between_pages)
        return collected

    @staticmethod
    def _parse_date(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    # -------------------------------------------------------------- cleaning
    _MEDIA_PLACEHOLDER_RE = re.compile(
        r"^\[?(image|images|photo|photos|video|videos|gif|audio)\]?$",
        re.IGNORECASE,
    )

    @classmethod
    def _clean_post_text(cls, text: str) -> tuple[str, bool]:
        """Return (cleaned_text, should_drop). Mirrors the notebook's rules,
        plus drops Rollcall's media-only placeholders ([Image], [Video], ...)."""
        if not text:
            return "", True
        if cls._MEDIA_PLACEHOLDER_RE.match(text.strip()):
            return text, True
        text = re.sub(r"https://\S+", "", text)
        trimmed = text.lstrip().upper()
        if trimmed.startswith("RT") or trimmed.startswith('"RT'):
            return text, True
        if not text.strip():
            return text, True
        return text, False

    def posts_to_dataframe(self, posts: list[dict]) -> pd.DataFrame:
        records = []
        for post in posts:
            raw_text = (post.get("text") or "").strip()
            cleaned, drop = self._clean_post_text(raw_text)
            if drop:
                continue
            cleaned = re.sub(r"\s+", " ", cleaned).strip()
            records.append({"post_date": post.get("date"), "status_text": cleaned})
        df = pd.DataFrame(records, columns=["post_date", "status_text"])
        if not df.empty:
            df["post_date"] = pd.to_datetime(df["post_date"], errors="coerce", utc=True)
            df = df.dropna(subset=["post_date"])
            df["post_date"] = df["post_date"].dt.tz_convert(None).dt.strftime("%Y-%m-%d %H:%M:%S")
        return df

    # ----------------------------------------------------------------- merge
    def _load_existing(self) -> pd.DataFrame:
        if not self.csv_path.exists():
            return pd.DataFrame(columns=["post_date", "status_text"])
        df = pd.read_csv(self.csv_path, dtype=str)
        return df.dropna(subset=["post_date"])

    def latest_post_date(self) -> datetime | None:
        df = self._load_existing()
        if df.empty:
            return None
        ts = pd.to_datetime(df["post_date"], errors="coerce").dropna()
        if ts.empty:
            return None
        return ts.max().to_pydatetime().replace(tzinfo=timezone.utc)

    def update(self, since: datetime | None = None, verbose: bool = True) -> dict:
        existing = self._load_existing()
        if since is None:
            since = self.latest_post_date() or datetime(2024, 1, 1, tzinfo=timezone.utc)
        if verbose:
            print(f"Fetching posts on or after {since.isoformat()}")

        new_posts = self.fetch_since(since, verbose=verbose)
        new_df = self.posts_to_dataframe(new_posts)

        if verbose:
            print(f"Fetched {len(new_posts)} raw posts → {len(new_df)} kept after cleaning")

        if new_df.empty:
            return {
                "existing": len(existing),
                "fetched_raw": len(new_posts),
                "kept": 0,
                "added": 0,
                "total": len(existing),
            }

        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["post_date", "status_text"])
        combined = combined.sort_values("post_date").reset_index(drop=True)
        added = len(combined) - len(existing)

        combined.to_csv(self.csv_path, index=False)
        if verbose:
            print(
                f"Existing {len(existing)} + new {len(new_df)} → "
                f"{len(combined)} after dedup (+{added} net new). "
                f"Saved to {self.csv_path}"
            )

        return {
            "existing": len(existing),
            "fetched_raw": len(new_posts),
            "kept": len(new_df),
            "added": added,
            "total": len(combined),
        }


# ---------------------------------------------------------------------- CLI
def _parse_since(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Update Truth Social CSV via Rollcall Factbase API.")
    parser.add_argument("--person", default=DEFAULT_PROFILE)
    parser.add_argument("--platform", default=DEFAULT_PLATFORM)
    parser.add_argument("--csv", default=DEFAULT_CSV, help="CSV path to read/write.")
    parser.add_argument("--since", type=_parse_since, default=None,
                        help="Earliest date (YYYY-MM-DD); defaults to latest in CSV.")
    parser.add_argument("--full", action="store_true",
                        help="Rebuild from 2024-01-01 (overrides --since when --since is unset).")
    args = parser.parse_args()

    scraper = TruthSocialScraper(person=args.person, platform=args.platform, csv_path=args.csv)
    since = args.since
    if args.full and since is None:
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
    summary = scraper.update(since=since)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
