import os
import time
import json
import requests
from datetime import datetime


class TruthSocialScraper:
    """Fetch Truth Social posts through the Apify `easyapi/truth-social-scraper` actor.

    Example
    -------
    >>> scraper = TruthSocialScraper(
    ...     since="2024-01-01",
    ...     max_items=5000,
    ...     output_path="data/truth-social/realDonaldTrump_posts.json",
    ... )
    >>> scraper.scrape()
    Saved 1234 posts to data/truth-social/realDonaldTrump_posts.json
    """

    APIFY_BASE_URL = "https://api.apify.com/v2"
    ACTOR_ID = "easyapi~truth-social-scraper"  # Note the tilde separator

    def __init__(
        self,
        profile_url: str = "https://truthsocial.com/@realDonaldTrump",
        since: str | datetime = "2024-01-01",
        max_items: int = 5000,
        token: str | None = None,
        output_path: str = "data/truth-social/realDonaldTrump_posts.json",
        poll_interval: int = 10,
        _timeout_ms: int = 60 * 60 * 1000,  # 1 h
    ) -> None:
        """Parameters
        ----------
        profile_url : str
            Truth Social profile to scrape.
        since : str | datetime
            Earliest post date to keep (inclusive). If string, expects YYYY-MM-DD.
        max_items : int
            Max posts to request from the actor.
        token : str | None
            Apify API token. If *None*, will read from the ``APIFY_API_TOKEN`` env var.
        output_path : str
            Where to save the filtered posts as JSON.
        poll_interval : int
            Seconds between status checks while the actor is running.
        _timeout_ms : int
            Maximum actor run time in milliseconds.
        """
        self.profile_url = profile_url
        self.since = (
            datetime.strptime(since, "%Y-%m-%d") if isinstance(since, str) else since
        )
        self.max_items = max_items
        self.token = token or os.getenv("APIFY_API_TOKEN")
        if not self.token:
            raise EnvironmentError(
                "Apify API token not provided. Set APIFY_API_TOKEN env variable or pass via the 'token' argument."
            )
        self.output_path = output_path
        self.poll_interval = poll_interval
        self.timeout_ms = _timeout_ms

        # Ensure output directory exists
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)

    # ---------------------------------------------------------------------
    # Internal helpers
    # ---------------------------------------------------------------------
    def _run_actor(self) -> str:
        """Start the actor run and return the resulting dataset ID."""
        run_url = (
            f"{self.APIFY_BASE_URL}/acts/{self.ACTOR_ID}/run?token={self.token}&timeout={self.timeout_ms}"
        )
        payload = {
            "profileUrls": [self.profile_url],
            "maxItems": self.max_items,
        }
        response = requests.post(run_url, json=payload, timeout=60)
        response.raise_for_status()
        run = response.json()["data"]
        run_id = run["id"]

        # Poll until completion
        status_url = f"{self.APIFY_BASE_URL}/actor-runs/{run_id}?token={self.token}"
        while True:
            status_resp = requests.get(status_url, timeout=30)
            status_resp.raise_for_status()
            status = status_resp.json()["data"]
            if status["status"] in {"SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"}:
                if status["status"] != "SUCCEEDED":
                    raise RuntimeError(
                        f"Actor run finished with status {status['status']}: {status.get('statusMessage', '')}"
                    )
                return status["defaultDatasetId"]
            time.sleep(self.poll_interval)

    def _fetch_dataset_items(self, dataset_id: str) -> list[dict]:
        """Download all items from the dataset as JSON."""
        # Using limit=0 to retrieve all items in one go (Apify convention)
        items_url = (
            f"{self.APIFY_BASE_URL}/datasets/{dataset_id}/items?token={self.token}&clean=true&format=json&limit=0"
        )
        resp = requests.get(items_url, timeout=120)
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def scrape(self) -> None:
        """Run the actor, filter posts, and save them to disk."""
        print(
            f"Starting scrape for {self.profile_url}. This may take several minutes depending on the number of posts…"
        )
        dataset_id = self._run_actor()
        print(f"Actor run succeeded. Dataset ID: {dataset_id}. Downloading items…")
        items = self._fetch_dataset_items(dataset_id)

        # Filter by `created_at` timestamp
        filtered = []
        for entry in items:
            created_at = entry.get("post", {}).get("created_at")
            if not created_at:
                continue
            try:
                created_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            except ValueError:
                # Fallback to dateutil if format differs
                from dateutil.parser import parse as parse_date  # lazy import

                created_dt = parse_date(created_at)
            if created_dt >= self.since:
                filtered.append(entry)

        # Save
        with open(self.output_path, "w", encoding="utf-8") as f:
            json.dump(filtered, f, ensure_ascii=False, indent=2)
        print(f"Saved {len(filtered)} posts to {self.output_path}")


# -------------------------------------------------------------------------
# CLI helper
# -------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch Truth Social posts via the Apify easyapi/truth-social-scraper actor."
    )
    parser.add_argument(
        "--profile-url",
        default="https://truthsocial.com/@realDonaldTrump",
        help="Truth Social profile URL to scrape.",
    )
    parser.add_argument(
        "--since",
        default="2024-01-01",
        help="Earliest post date to include (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=5000,
        help="Maximum number of posts to request from the actor.",
    )
    parser.add_argument(
        "--output",
        default="data/truth-social/realDonaldTrump_posts.json",
        help="Output JSON file path.",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="Apify API token. If omitted, reads from APIFY_API_TOKEN env variable.",
    )
    args = parser.parse_args()

    scraper = TruthSocialScraper(
        profile_url=args.profile_url,
        since=args.since,
        max_items=args.max_items,
        token=args.token,
        output_path=args.output,
    )
    scraper.scrape() 