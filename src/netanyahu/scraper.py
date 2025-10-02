#!/usr/bin/env python3
"""
Playwright-based Scraper for Netanyahu URLs

This script uses Playwright to render JavaScript and save pages, then extracts transcript content.

Usage:
    python src/playwright_scraper.py --input data-netanyahu/filtered_netanyahu_news.csv --output-dir data-netanyahu/raw-transcripts
"""

import argparse
import asyncio
import csv
import os
import re
import time
import random
from pathlib import Path
from typing import List, Tuple, Optional
from datetime import datetime
from bs4 import BeautifulSoup
import html


def sanitize_filename(filename: str) -> str:
    """Sanitize filename for filesystem compatibility."""
    filename = re.sub(r'[<>:"/\\|?*]', '', filename)
    filename = re.sub(r'\s+', '_', filename)
    filename = filename.strip('._')
    return filename[:200]


def extract_transcript_from_html(html_content: str, url: str) -> Optional[str]:
    """Extract transcript content from HTML content.

    Strategy:
    1) Prefer container with id starting with htmlContent (e.g., htmlContent_0)
    2) Else fall back to common article containers
    3) Prefer <p> tags; if too few, fall back to container.get_text with newlines
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')

        # 1) Prefer htmlContent*
        container = soup.find(id=re.compile(r'^htmlContent', re.I))
        
        # 2) Fallbacks
        if not container:
            for sel in ['main', 'article', 'div[class*="content"]', 'div[class*="article"]']:
                el = soup.select_one(sel)
                if el and len(el.get_text(strip=True)) > 100:
                    container = el
                    break

        if not container:
            print(f"Warning: No suitable content container found in {url}")
            return None

        # Prefer paragraphs first
        p_texts = [p.get_text(separator=' ', strip=True) for p in container.find_all('p')]
        p_texts = [t for t in p_texts if t and len(t) > 2]

        # If too few paragraphs, fallback to full container text with line breaks
        if len(p_texts) < 5:
            full_text = container.get_text(separator='\n', strip=True)
            # Normalize whitespace and split into lines
            lines = [re.sub(r'\s+', ' ', ln).strip() for ln in full_text.split('\n')]
            # Filter lines: non-empty and not pure boilerplate
            keep = []
            for ln in lines:
                if not ln:
                    continue
                # Skip very short nav-like lines
                if len(ln) < 3:
                    continue
                keep.append(ln)
            # Group lines into paragraphs by blank line separation heuristic
            # Here we simply join with blank lines for readability
            if keep:
                transcript_lines = [f"Source: {url}", "", *keep]
                return '\n\n'.join(transcript_lines)

        # Use paragraph-based transcript
        transcript_lines = [f"Source: {url}", ""]
        for t in p_texts:
            transcript_lines.append(t)
            transcript_lines.append("")
        if len(transcript_lines) > 3:
            return '\n'.join(transcript_lines)
        return None
    except Exception as e:
        print(f"Error extracting transcript from {url}: {str(e)}")
        return None


async def _create_context(p, timeout_ms: int, browser_name: str, headless: bool, storage_state_path: Optional[str]):
    # Choose browser engine
    if browser_name == 'webkit':
        browser = await p.webkit.launch(headless=headless)
        # iOS/macOS Safari-like UA
        user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 15_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15'
    elif browser_name == 'firefox':
        browser = await p.firefox.launch(headless=headless)
        user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 15.0; rv:141.0) Gecko/20100101 Firefox/141.0'
    else:
        browser = await p.chromium.launch(headless=headless)
        user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'

    context_kwargs = dict(
        user_agent=user_agent,
        viewport={"width": 1366, "height": 900},
        java_script_enabled=True,
        ignore_https_errors=True,
        timezone_id='America/New_York',
        locale='en-US',
        extra_http_headers={
            'Accept-Language': 'en-US,en;q=0.9'
        }
    )

    if storage_state_path and os.path.exists(storage_state_path):
        context = await browser.new_context(storage_state=storage_state_path, **context_kwargs)
    else:
        context = await browser.new_context(**context_kwargs)

    # Reduce bot detection surface
    await context.add_init_script("Object.defineProperty(navigator, 'webdriver', { get: () => undefined })")

    # Block heavy resources to speed up loads
    async def _route_handler(route, request):
        rtype = request.resource_type
        # Keep stylesheets to avoid anti-bot quirks; block heavy media
        if rtype in {"image", "media", "font"}:
            return await route.abort()
        # Optionally block common analytics
        url = request.url
        if any(host in url for host in [
            'googletagmanager.com', 'google-analytics.com', 'facebook.net', 'cdn.gbqofs.com'
        ]):
            return await route.abort()
        return await route.continue_()

    await context.route("**/*", _route_handler)
    # Set default timeouts
    context.set_default_timeout(timeout_ms)
    context.set_default_navigation_timeout(timeout_ms)
    return browser, context


async def _fetch_page_html(context, url: str, timeout_ms: int, wait_after_load_ms: int) -> Optional[str]:
    page = await context.new_page()
    try:
        # Navigate and wait for DOM readiness
        await page.goto(url, wait_until='domcontentloaded', timeout=timeout_ms)
        # Wait for any meaningful content to appear if possible
        try:
            await page.wait_for_selector('#htmlContent_0, div[id*="htmlContent"], main, article, p', timeout=timeout_ms // 3)
        except:
            pass
        # Small additional wait for dynamic inits
        await page.wait_for_timeout(wait_after_load_ms)
        return await page.content()
    finally:
        await page.close()


def process_url_sync(url: str, title: str, date_str: str, output_dir: str) -> bool:
    """Legacy sync wrapper (not used in concurrent mode)."""
    return asyncio.run(process_single_with_new_browser(url, title, date_str, output_dir))


async def _process_with_context(context, url: str, title: str, date_str: str, output_dir: str, timeout_ms: int, wait_after_load_ms: int, max_retries: int) -> bool:
    try:
        # Create filename
        if date_str:
            filename_base = f"{date_str}_{sanitize_filename(title)}"
        else:
            filename_base = sanitize_filename(title)

        transcript_path = os.path.join(output_dir, f"{filename_base}.txt")

        # Skip if already exists
        if os.path.exists(transcript_path):
            print(f"Skipping existing: {transcript_path}")
            return True

        print(f"Fetching with Playwright: {url}")

        html_content: Optional[str] = None
        for attempt in range(1, max_retries + 1):
            try:
                html_content = await _fetch_page_html(context, url, timeout_ms, wait_after_load_ms)
                if html_content:
                    break
            except Exception as e:
                print(f"Attempt {attempt}/{max_retries} failed for {url}: {e}")
            # backoff with jitter
            if attempt < max_retries:
                backoff = min(2 ** attempt, 8) + random.uniform(0, 0.5)
                print(f"Retrying in {backoff:.1f}s...")
                await asyncio.sleep(backoff)

        if not html_content:
            print(f"Failed to fetch content from: {url}")
            return False

        # Extract transcript
        transcript = extract_transcript_from_html(html_content, url)
        if not transcript:
            print(f"Failed to extract transcript from: {url}")
            # Save HTML content for debugging
            os.makedirs(output_dir, exist_ok=True)
            debug_path = os.path.join(output_dir, f"DEBUG_{filename_base}.html")
            with open(debug_path, 'w', encoding='utf-8') as f:
                f.write(f"<!-- DEBUG HTML CONTENT FOR: {url} -->\n\n")
                f.write(html_content)
            print(f"Saved debug HTML to: {debug_path}")
            return False

        # Save transcript
        os.makedirs(output_dir, exist_ok=True)
        with open(transcript_path, 'w', encoding='utf-8') as f:
            f.write(transcript)

        print(f"Saved transcript: {transcript_path}")
        return True

    except Exception as e:
        print(f"Error processing {url}: {str(e)}")
        return False


async def process_single_with_new_browser(url: str, title: str, date_str: str, output_dir: str) -> bool:
    # Fallback single processing (not concurrent). Keeps previous behavior when called directly.
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("Playwright not installed. Please run: pip install playwright && playwright install")
        return False

    async with async_playwright() as p:
        browser, context = await _create_context(p, 60000)
        try:
            return await _process_with_context(context, url, title, date_str, output_dir, 60000, 3500, 3)
        finally:
            await context.close()
            await browser.close()


def load_urls_from_csv(csv_path: str) -> List[Tuple[str, str, str]]:
    """Load URLs from CSV file and sort by date descending.

    Tries, in order:
    - Use 'date' column if parseable (YYYY-MM-DD)
    - Extract ddmmyy or yyyymmdd from URL last segment
    - Otherwise, leave date empty and sort those last
    """
    def parse_iso(date_str: str) -> Optional[datetime]:
        if not date_str:
            return None
        for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
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

    items = []  # (title, url, iso_date, dt, idx)
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader):
                title = (row.get('title') or '').strip()
                url = (row.get('url') or '').strip()
                raw_date = (row.get('date') or '').strip()

                if not title or not url:
                    continue

                dt = parse_iso(raw_date)
                iso = raw_date

                if dt is None:
                    dt2, iso2 = extract_date_from_url(url)
                    if dt2 is not None:
                        dt, iso = dt2, iso2

                items.append((title, url, iso, dt, idx))

        # Sort: valid dates first (desc), then undated by original order
        items.sort(key=lambda x: (x[3] is None, -(x[3].timestamp()) if x[3] else 0, x[4]))

        urls = [(t, u, iso) for (t, u, iso, _dt, _idx) in items]
        print(f"Loaded {len(urls)} URLs from {csv_path} (sorted by date desc)")
        return urls

    except Exception as e:
        print(f"Error loading CSV {csv_path}: {str(e)}")
        return []


async def _async_main(args):
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("Playwright not installed. Please run: pip install playwright && playwright install")
        return

    # Load URLs
    urls = load_urls_from_csv(args.input)
    if not urls:
        print("No URLs to process")
        return

    # Apply start-from and limit
    if args.start_from > 0:
        urls = urls[args.start_from:]
        print(f"Starting from index {args.start_from}")

    if args.limit:
        urls = urls[:args.limit]
        print(f"Processing {len(urls)} URLs")

    concurrency = max(1, args.concurrency)
    max_retries = max(1, args.max_retries)
    timeout_ms = args.timeout_ms
    wait_after_load_ms = args.wait_after_load_ms
    browser_name = args.browser
    headless = not args.headful
    storage_state_path = args.storage_state

    success_count = 0
    total = len(urls)

    async with async_playwright() as p:
        # Optional interactive step to capture Cloudflare cookies
        if args.interactive_login:
            print("Opening interactive browser to establish session. Complete any checks, then press Enter here to continue...")
            ibrowser, icontext = await _create_context(p, timeout_ms, browser_name, False, storage_state_path)
            ipage = await icontext.new_page()
            await ipage.goto('https://www.gov.il/en', wait_until='domcontentloaded', timeout=timeout_ms)
            input()
            if storage_state_path:
                await icontext.storage_state(path=storage_state_path)
            await ipage.close()
            await icontext.close()
            await ibrowser.close()

        browser, context = await _create_context(p, timeout_ms, browser_name, headless, storage_state_path)
        sem = asyncio.Semaphore(concurrency)

        async def _worker(idx: int, title: str, url: str, date_str: str):
            nonlocal success_count
            async with sem:
                print(f"\n[{idx}/{total}] Processing: {title}")
                print(f"URL: {url}")
                ok = await _process_with_context(context, url, title, date_str, args.output_dir, timeout_ms, wait_after_load_ms, max_retries)
                if ok:
                    success_count += 1
                    print(f"✓ Success ({success_count}/{idx})")
                else:
                    print("✗ Failed")
                # small jitter between tasks to be polite
                await asyncio.sleep(random.uniform(0.1, 0.6))

        tasks = [
            asyncio.create_task(_worker(i, title, url, date_str))
            for i, (title, url, date_str) in enumerate(urls, 1)
        ]

        await asyncio.gather(*tasks)

        await context.close()
        await browser.close()

    print(f"\nCompleted: {success_count}/{total} URLs processed successfully")


def main():
    parser = argparse.ArgumentParser(description='Playwright-based scraper for Netanyahu URLs')
    parser.add_argument('--input', default='data-netanyahu/filtered_netanyahu_news.csv',
                       help='Input CSV file with URLs')
    parser.add_argument('--output-dir', default='data-netanyahu/raw-transcripts',
                       help='Output directory for transcripts')
    parser.add_argument('--limit', type=int, help='Limit number of URLs to process (for testing)')
    parser.add_argument('--start-from', type=int, default=0,
                       help='Start processing from this index (for resuming)')
    parser.add_argument('--concurrency', type=int, default=3,
                       help='Number of concurrent pages to process (default: 3)')
    parser.add_argument('--max-retries', type=int, default=3,
                       help='Max retries per URL on failure (default: 3)')
    parser.add_argument('--timeout-ms', type=int, default=60000,
                       help='Navigation and selector timeout in milliseconds (default: 60000)')
    parser.add_argument('--wait-after-load-ms', type=int, default=3500,
                       help='Extra wait after load for dynamic content (default: 3500)')
    parser.add_argument('--browser', choices=['chromium','webkit','firefox'], default='webkit',
                       help='Browser engine to use (default: webkit for Safari-like)')
    parser.add_argument('--headful', action='store_true',
                       help='Run browser in headful (visible) mode')
    parser.add_argument('--storage-state', default='playwright_storage.json',
                       help='Path to persist/reuse cookies and local storage (default: playwright_storage.json)')
    parser.add_argument('--interactive-login', action='store_true',
                       help='Open an interactive browser first to complete any anti-bot checks; saves cookies to storage-state')

    args = parser.parse_args()
    asyncio.run(_async_main(args))


if __name__ == '__main__':
    main()
