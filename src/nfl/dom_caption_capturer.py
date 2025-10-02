#!/usr/bin/env python3
"""
DOM Caption Capturer

Injects a robust "stable capture" JavaScript observer into a page to collect
closed captions that are rendered in the DOM (e.g., live subtitles/CC overlays).

The script polls the captured buffer periodically and writes unique, stabilized
caption lines to an output file. It also records the associated HTML5 video
currentTime where available, allowing later alignment with the media timeline.

Supports both single URL processing and batch processing from CSV files.

Usage (Single URL):
  python src/nfl/dom_caption_capturer.py \
    "https://example.com/player" \
    --out captions.txt \
    --duration 600 \
    --selector '[aria-live="polite"]' \
    --headless

Usage (Batch CSV):
  python src/nfl/dom_caption_capturer.py \
    data-football/2024-thursday-night.csv \
    --out data-football/captions/ \
    --duration 600 \
    --wait-for-enter

CSV Format:
  date,url
  2024-09-23,https://www.nfl.com/plus/games/seahawks-at-cardinals-2025-reg-4
  2024-09-24,https://www.nfl.com/plus/games/patriots-at-bills-2025-reg-4

Notes:
- For batch processing, authentication is handled once at the beginning
- Output files are named: yyyy-mm-dd_Team1-at-Team2.txt
- If output files already exist, they are skipped
- If the page requires authentication, run without --headless and use --wait-for-enter
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Default caption selectors to try (in order)
DEFAULT_CAPTION_SELECTORS: List[str] = [
    '.theoplayer-cea608-texttrack-CC1',
    '.theoplayer-texttracks',
]


def parse_team_names_from_url(url: str) -> Tuple[str, str]:
    """Parse team names from NFL URL like 'seahawks-at-cardinals-2025-reg-4'.
    
    Returns:
        Tuple of (team1, team2) with proper capitalization.
    """
    # Extract the game part from URL
    match = re.search(r'/games/([^/]+)', url)
    if not match:
        return "unknown", "unknown"
    
    game_part = match.group(1)
    
    # Split by '-at-' to get teams
    parts = game_part.split('-at-')
    if len(parts) != 2:
        return "unknown", "unknown"
    
    team1_raw = parts[0]
    team2_raw = parts[1]
    
    # Remove year and other suffixes from team2
    team2_raw = re.sub(r'-\d{4}-.*$', '', team2_raw)
    
    # Capitalize team names properly
    team1 = team1_raw.replace('-', ' ').title()
    team2 = team2_raw.replace('-', ' ').title()
    
    return team1, team2


def load_urls_from_csv(csv_path: str) -> List[Dict[str, str]]:
    """Load URLs from CSV file with date and url columns.
    
    Returns:
        List of dicts with 'date' and 'url' keys.
    """
    urls = []
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            print(f"CSV columns found: {reader.fieldnames}")
            
            for i, row in enumerate(reader, 1):
                print(f"Row {i}: {row}")
                
                # Try different column name variations
                date_key = None
                url_key = None
                
                for key in row.keys():
                    key_lower = key.lower().strip()
                    if key_lower in ['date', 'game_date', 'game date']:
                        date_key = key
                    elif key_lower in ['url', 'link', 'game_url', 'game url']:
                        url_key = key
                
                if date_key and url_key:
                    date_val = row[date_key].strip()
                    url_val = row[url_key].strip()
                    
                    if url_val and url_val != 'data:':
                        urls.append({
                            'date': date_val,
                            'url': url_val
                        })
                        print(f"  -> Added: {date_val} | {url_val}")
                    else:
                        print(f"  -> Skipped empty/invalid URL: '{url_val}'")
                else:
                    print(f"  -> Skipped: missing date/url columns")
                    
    except Exception as e:
        print(f"Error reading CSV file {csv_path}: {e}")
        sys.exit(1)
    
    if not urls:
        print(f"No valid URLs found in {csv_path}")
        print("Expected CSV format:")
        print("date,url")
        print("2024-09-23,https://www.nfl.com/plus/games/seahawks-at-cardinals-2025-reg-4")
        sys.exit(1)
    
    print(f"Loaded {len(urls)} URLs from {csv_path}")
    return urls


def generate_output_filename(date: str, team1: str, team2: str, output_dir: str) -> str:
    """Generate output filename in format: yyyy-mm-dd_team1-at-team2.txt"""
    # Replace spaces and special chars with underscores for filename safety
    team1_clean = team1.replace(' ', '-').replace('_', '-')
    team2_clean = team2.replace(' ', '-').replace('_', '-')
    filename = f"{date}_{team1_clean}-at-{team2_clean}.txt"
    return os.path.join(output_dir, filename)


STABLE_CAPTURE_JS: str = r"""
(function(){
  try {
    if (window.__ccArm && window.__ccArm.started) {
      return { status: 'already_started', roots: (window.__ccArm.roots||[]).length };
    }

    function uniq(arr) {
      return Array.from(new Set(arr));
    }

    function getSpeed(){
      try {
        if (window.__ccArm && typeof window.__ccArm.playbackSpeed === 'number') return Math.max(0.5, Math.min(32, window.__ccArm.playbackSpeed));
        if (typeof window.__ccPlaybackSpeed === 'number') return Math.max(0.5, Math.min(32, window.__ccPlaybackSpeed));
        const v = document.querySelector('video');
        return v && typeof v.playbackRate === 'number' ? Math.max(0.5, Math.min(32, v.playbackRate)) : 1;
      } catch(e) { return 1; }
    }

    // Heuristics to find likely caption containers
    function findCaptionRoots(userSelector) {
      // If user provided a selector, try it first without visibility checks
      if (userSelector) {
        try {
          const userNodes = Array.from(document.querySelectorAll(userSelector));
          if (userNodes.length > 0) {
            return userNodes; // Trust the user selector, return immediately
          }
        } catch (e) { /* invalid selector */ }
      }
      
      // Fallback: try heuristic selectors
      const selectors = uniq([
        '[aria-live="assertive"]',
        '[aria-live="polite"]',
        '[role="alert"]',
        '[role="log"]',
        '[class*="caption"]',
        '[class*="subtitl"]',
        '[class*="cc"]',
        '.ytp-caption-segment',
        '.vjs-text-track-display',
        '.shaka-text-container'
      ].filter(Boolean));

      const nodes = new Set();
      for (const sel of selectors) {
        try {
          document.querySelectorAll(sel).forEach(el => nodes.add(el));
        } catch (e) { /* ignore invalid selectors */ }
      }

      // Filter nodes that are invisible or detached
      const visibleNodes = Array.from(nodes).filter((el) => {
        if (!el || !el.isConnected) return false;
        const style = window.getComputedStyle(el);
        if (!style) return false;
        if (style.display === 'none' || style.visibility === 'hidden' || parseFloat(style.opacity||'1') < 0.05) return false;
        const rect = el.getBoundingClientRect();
        if ((rect.width === 0 && rect.height === 0)) return false;
        return true;
      });

      return visibleNodes;
    }

    function getVideoCurrentTime() {
      try {
        const videos = Array.from(document.getElementsByTagName('video'));
        if (!videos.length) return null;
        let chosen = videos.find(v => !v.paused && !v.ended && v.readyState >= 2) || videos[0];
        return chosen ? Number(chosen.currentTime || 0) : null;
      } catch (e) {
        return null;
      }
    }

    function isJunk(text) {
      if (!text) return true;
      const t = String(text).trim();
      if (t.length < 2) return true;
      // Avoid UI labels that are common junk
      if (/^(cc|subtitles|captions|closed captions)$/i.test(t)) return true;
      // CSS-ish (has many property:value; patterns)
      if ((/[{};]/.test(t) && /:\s*[^:]+;/.test(t)) || /\b(display|position|color|background|width|height)\b\s*:/.test(t)) return true;
      // JS-ish keywords
      if (/\b(function|var|let|const|return|class|=>)\b/.test(t)) return true;
      // Pure time strings (UI clock)
      if (/^\d{1,2}:\d{2}(?::\d{2})?$/.test(t)) return true;
      // Extreme length safety valve
      if (t.length > 5000) return true;
      return false;
    }

    function collectLinesWithPos(root) {
      // For CEA-608 style captions: each direct child div is a separate caption line
      // Collect each child's text and its vertical position to distinguish top/bottom lines
      const children = Array.from(root.children || []);
      const rootRect = root.getBoundingClientRect();
      const results = [];

      for (const child of children) {
        const walker = document.createTreeWalker(child, NodeFilter.SHOW_TEXT, {
          acceptNode(node) {
            try {
              const parent = node.parentElement;
              if (!parent) return NodeFilter.FILTER_REJECT;
              const tag = parent.tagName;
              if (tag === 'SCRIPT' || tag === 'STYLE') return NodeFilter.FILTER_REJECT;
              const style = window.getComputedStyle(parent);
              if (!style || style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity||'1') < 0.05) return NodeFilter.FILTER_REJECT;
              const text = node.nodeValue.replace(/\s+/g, ' ').trim();
              if (!text) return NodeFilter.FILTER_REJECT;
              return NodeFilter.FILTER_ACCEPT;
            } catch (e) {
              return NodeFilter.FILTER_REJECT;
            }
          }
        });
        const parts = [];
        let n;
        while ((n = walker.nextNode())) parts.push(n.nodeValue);
        const joined = parts.join(' ').replace(/\s+/g, ' ').trim();
        if (joined) {
          const cr = child.getBoundingClientRect();
          const y = rootRect.height > 0 ? (cr.top - rootRect.top) / rootRect.height : 0;
          results.push({ text: joined, y: Math.max(0, Math.min(1, y)) });
        }
      }
      return results;
    }

    function commitLines(lines) {
      if (!lines || !lines.length) return;
      const ct = getVideoCurrentTime();
      const now = Date.now();
      window.__ccStable = window.__ccStable || [];
      window.__ccArm = window.__ccArm || {};
      // Track by text + approx position so we can capture when lines move
      window.__ccArm.seenKeys = window.__ccArm.seenKeys || new Set();

      for (const line of lines) {
        const t = String(line.text || '').trim();
        if (!t || isJunk(t)) continue;
        const y = typeof line.y === 'number' ? line.y : null;
        const posBucket = y === null ? 'na' : String(Math.round(y * 100));
        const key = `${t}@@${posBucket}`;
        if (!window.__ccArm.seenKeys.has(key)) {
          window.__ccArm.seenKeys.add(key);
          window.__ccStable.push({ t: t, ct: ct, ts: now, y: y });
        }
      }
    }

    function createObserver(root) {
      let buffer = '';
      let lastChange = 0;
      const sp = getSpeed();
      const stableMsBase = 150;
      const checkEveryMsBase = 50;
      const trailingGraceBase = 250;
      const stableMs = Math.max(60, Math.round(stableMsBase / sp));
      const checkEveryMs = Math.max(20, Math.round(checkEveryMsBase / sp));
      const trailingGraceMs = Math.max(80, Math.round(trailingGraceBase / sp));
      let graceTimer = null;

      function refresh() {
        const linesNow = collectLinesWithPos(root);
        const t = linesNow.map(x => x.text).join('\n');
        if (t && t !== buffer) {
          buffer = t;
          lastChange = performance.now();
        }
      }

      const obs = new MutationObserver(() => {
        refresh();
        // Also commit immediately on change to catch fleeting text
        const linesNow = collectLinesWithPos(root);
        if (linesNow && linesNow.length) {
          commitLines(linesNow);
        }
        // Commit on next paint as an additional safety
        try { requestAnimationFrame(() => { try { commitLines(collectLinesWithPos(root)); } catch(e){} }); } catch(e){}
        // Trailing grace commit to pick up end-of-line appended words
        try { if (graceTimer) clearTimeout(graceTimer); } catch(e){}
        try {
          graceTimer = setTimeout(() => {
            try { commitLines(collectLinesWithPos(root)); } catch(e){}
          }, trailingGraceMs);
        } catch(e){}
      });

      obs.observe(root, { characterData: true, childList: true, subtree: true, attributes: true });

      // Periodic stability check - check every 50ms to catch brief flashes
      const interval = setInterval(() => {
        const now = performance.now();
        if (buffer && (now - lastChange) >= stableMs) {
          const linesNow = collectLinesWithPos(root);
          commitLines(linesNow);
          buffer = '';
        }
      }, checkEveryMs); // scaled by playback speed

      // Initial prime
      refresh();

      return { obs, interval, root };
    }

    function setupTextTrackCapture(){
      try {
        const videos = Array.from(document.getElementsByTagName('video'));
        const controllers = [];
        const sp = getSpeed();
        const pollMs = Math.max(100, Math.round(300 / sp));
        for (const v of videos) {
          try {
            const tracks = Array.from(v.textTracks || []);
            for (const tr of tracks) {
              try {
                const kind = (tr.kind || '').toLowerCase();
                if (kind !== 'subtitles' && kind !== 'captions') continue;
                try { if (tr.mode === 'disabled') tr.mode = 'hidden'; } catch(e){}
                const onCueChange = () => {
                  try {
                    const cues = Array.from(tr.activeCues || []);
                    if (cues && cues.length) {
                      const lines = [];
                      for (const cue of cues) {
                        let text = '';
                        try {
                          if (cue && typeof cue.text === 'string') {
                            text = cue.text;
                          } else if (cue && typeof cue.getCueAsHTML === 'function') {
                            const tmp = document.createElement('div');
                            tmp.appendChild(cue.getCueAsHTML());
                            text = tmp.textContent || '';
                          }
                        } catch(e){}
                        text = (text || '').replace(/\s+/g, ' ').trim();
                        if (text) lines.push({ text, y: null });
                      }
                      if (lines.length) commitLines(lines);
                    }
                  } catch(e){}
                };
                try { tr.removeEventListener('cuechange', onCueChange); } catch(e){}
                try { tr.addEventListener('cuechange', onCueChange); } catch(e){}
                const interval = setInterval(onCueChange, pollMs);
                controllers.push({ track: tr, interval, onCueChange });
              } catch(e){}
            }
          } catch(e){}
        }
        return controllers;
      } catch(e) { return []; }
    }

    const userSelector = (window.__ccArm && window.__ccArm.userSelector) || (window.__ccUserSelector || null);
    const roots = findCaptionRoots(userSelector);

    if (!window.__ccArm) window.__ccArm = {};
    window.__ccArm.started = true;
    window.__ccArm.userSelector = userSelector || null;
    window.__ccArm.roots = roots;
    window.__ccArm.controllers = roots.map(r => createObserver(r));
    window.__ccArm.trackControllers = setupTextTrackCapture();
    window.__ccStable = window.__ccStable || [];

    // Expose a flush method to force a last commit when needed
    window.__ccFlushNow = function(){
      try {
        if (window.__ccArm && Array.isArray(window.__ccArm.roots)) {
          for (const r of window.__ccArm.roots) {
            try { commitLines(collectLinesWithPos(r)); } catch(e){}
          }
        }
      } catch(e){}
    };

    return { status: 'started', roots: roots.length, tracks: (window.__ccArm.trackControllers||[]).length };
  } catch (e) {
    return { status: 'error', message: String(e && e.stack || e) };
  }
})();
"""


def build_driver(headless: bool) -> webdriver.Chrome:
    """Create and return a Chrome WebDriver with reasonable defaults."""
    options = webdriver.ChromeOptions()
    if headless:
        # Chrome >= 109 supports new headless
        options.add_argument('--headless=new')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--window-size=1280,900')

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(60)
    return driver


def wait_for_caption_roots(driver: webdriver.Chrome, selector: Optional[str], timeout_sec: int = 30) -> int:
    """Try to find likely caption containers; return count found."""
    js_probe = r"""
      (function(sel){
        try {
          const s = sel && String(sel).trim() ? sel : null;
          window.__ccUserSelector = s;
          const selectors = [];
          if (s) selectors.push(s);
          selectors.push('[aria-live="assertive"]','[aria-live="polite"]','[role="alert"]','[role="log"]','[class*="caption"]','[class*="subtitl"]','[class*="cc"]','.ytp-caption-segment','.vjs-text-track-display','.shaka-text-container');
          const set = new Set();
          for (const q of selectors) { try { document.querySelectorAll(q).forEach(el => set.add(el)); } catch(e){} }
          const vis = Array.from(set).filter(el => {
            if (!el || !el.isConnected) return false;
            const cs = getComputedStyle(el);
            if (!cs) return false;
            if (cs.display==='none' || cs.visibility==='hidden' || Number(cs.opacity||'1')<0.05) return false;
            const r = el.getBoundingClientRect();
            if ((r.width===0 && r.height===0)) return false;
            return true;
          });
          return vis.length;
        } catch(e) { return 0; }
      })(arguments[0]);
    """

    end_time = time.time() + timeout_sec
    last_count = 0
    while time.time() < end_time:
        try:
            count = int(driver.execute_script(js_probe, selector))
            last_count = count
            if count > 0:
                return count
        except Exception:
            pass
        time.sleep(0.5)
    return last_count


def inject_stable_capture(driver: webdriver.Chrome, selector: Optional[str]) -> Dict[str, Any]:
    """Inject the stable capture JS and return status info from the page."""
    try:
        result = driver.execute_script(STABLE_CAPTURE_JS)
        # If roots == 0 and a user selector is provided, try reinjecting while setting the selector
        if isinstance(result, dict) and result.get('roots', 0) == 0 and selector:
            # Set a global for user selector then inject again
            driver.execute_script("window.__ccUserSelector = arguments[0];", selector)
            result = driver.execute_script(STABLE_CAPTURE_JS)
        return result if isinstance(result, dict) else {"status": "unknown", "detail": str(result)}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def check_browser_crash(driver: webdriver.Chrome) -> bool:
    """Check if browser has crashed (Aw snap! error code 5)."""
    try:
        # Try to get current URL - if browser crashed, this will fail
        driver.current_url
        return False
    except Exception as e:
        error_msg = str(e).lower()
        if any(phrase in error_msg for phrase in ['aw snap', 'error code 5', 'chrome crashed', 'browser crashed']):
            return True
        return False


def poll_captions(driver: webdriver.Chrome, max_seconds: int, poll_interval: float = 1.0, allow_manual_continue: bool = False) -> List[Dict[str, Any]]:
    """Poll the window.__ccStable buffer incrementally for up to max_seconds.

    Also detects end-of-video conservatively and stops early when playback is finished.
    Aborts early if no captions are captured within 30 seconds.
    """
    start = time.time()
    snapshot: List[Dict[str, Any]] = []
    seen_texts = set()

    # End-of-video detection state
    last_ct: Optional[float] = None
    ended_streak = 0
    near_end_streak = 0
    stagnation_accum = 0.0
    # Thresholds (conservative)
    ended_required = 3  # require ended flag for 3 consecutive polls
    near_end_required = 5  # require near-end for 5 polls
    stagnation_required_sec = 2.0  # require ~2s with no ct increase while near end

    # Early detection for no captions
    no_captions_threshold = 30.0  # abort if no captions after 30 seconds
    first_caption_time: Optional[float] = None

    print(f"🔍 Monitoring for captions (will abort after {no_captions_threshold}s if none found)...")

    while (time.time() - start) < max_seconds:
        
        # Check for browser crash before each poll
        if check_browser_crash(driver):
            print("💥 Browser crashed detected! Saving in-progress captions...")
            break
        
        try:
            data = driver.execute_script("return (window.__ccStable||[]);")
            if isinstance(data, list):
                for entry in data:
                    try:
                        text = (entry.get('t') if isinstance(entry, dict) else None) or ''
                        if not text:
                            continue
                        key = text.strip()
                        if key and key not in seen_texts:
                            # Track when we get our first caption
                            if first_caption_time is None:
                                first_caption_time = time.time()
                                elapsed = first_caption_time - start
                                print(f"✅ First caption captured after {elapsed:.1f}s")
                            
                            # normalize entry structure
                            ct = entry.get('ct') if isinstance(entry, dict) else None
                            ts = entry.get('ts') if isinstance(entry, dict) else None
                            snapshot.append({"t": key, "ct": ct, "ts": ts})
                            seen_texts.add(key)
                    except Exception:
                        continue
        except Exception as e:
            # Check if this is a browser crash
            error_msg = str(e).lower()
            if any(phrase in error_msg for phrase in ['aw snap', 'error code 5', 'chrome crashed', 'browser crashed']):
                print("💥 Browser crashed during polling! Saving in-progress captions...")
                break
            pass

        # Early abort if no captions after threshold
        current_time = time.time()
        elapsed = current_time - start
        if first_caption_time is None and elapsed >= no_captions_threshold:
            print(f"⚠️  No captions captured after {elapsed:.1f}s - aborting capture")
            print("   Possible issues:")
            print("   - Captions not enabled")
            print("   - Wrong selector")
            print("   - Video not playing")
            print("   - Caption container not found")
            
            # Additional debugging - check if JS injection is working
            try:
                js_status = driver.execute_script("return window.__ccArm ? 'active' : 'not_found';")
                js_roots = driver.execute_script("return window.__ccArm ? (window.__ccArm.roots || []).length : 0;")
                js_buffer = driver.execute_script("return window.__ccStable ? window.__ccStable.length : 0;")
                print(f"   Debug: JS status={js_status}, roots={js_roots}, buffer_size={js_buffer}")
            except Exception as e:
                print(f"   Debug check failed: {e}")
            
            break
        
        # Periodic status update every 10 seconds
        if int(elapsed) % 10 == 0 and elapsed > 0 and first_caption_time is None:
            print(f"⏱️  Still waiting for captions... ({elapsed:.1f}s elapsed)")

        # End-of-video detection (robust)
        try:
            status = driver.execute_script(
                """
                return (function(){
                  try{
                    const vids = Array.from(document.getElementsByTagName('video'));
                    let videoInfo = {has:false};
                    if (vids.length) {
                      let v = vids.find(x => !x.paused && !x.ended && x.readyState >= 2) || vids[0];
                      const d = Number(v.duration||0);
                      const ct = Number(v.currentTime||0);
                      const ended = !!v.ended;
                      const nearEnd = (isFinite(d) && d>0 && ct >= d - 0.5);
                      videoInfo = {has:true, d:d, ct:ct, ended:ended, nearEnd:nearEnd, ready:v.readyState, paused:v.paused, ns:v.networkState};
                    }
                    
                    // Check time display element for better end detection
                    const timeEl = document.querySelector('section[aria-label*="Current time"][data-testid="controls-current-time"]');
                    let timeDisplayInfo = null;
                    if (timeEl) {
                      const text = (timeEl.textContent || '').trim();
                      const ariaLabel = timeEl.getAttribute('aria-label') || '';
                      timeDisplayInfo = {text: text, ariaLabel: ariaLabel};
                      
                      // Parse "22:43 / 2:03:11" format
                      const match = text.match(/^(\d{1,2}:\d{2}(?::\d{2})?)\s*\/\s*(\d{1,2}:\d{2}(?::\d{2})?)$/);
                      if (match) {
                        const [, currentTimeStr, totalTimeStr] = match;
                        const parseTime = (timeStr) => {
                          const parts = timeStr.split(':').map(Number);
                          if (parts.length === 2) return parts[0] * 60 + parts[1];
                          if (parts.length === 3) return parts[0] * 3600 + parts[1] * 60 + parts[2];
                          return 0;
                        };
                        const currentSecs = parseTime(currentTimeStr);
                        const totalSecs = parseTime(totalTimeStr);
                        const within2Secs = totalSecs > 0 && currentSecs >= (totalSecs - 2);
                        timeDisplayInfo.parsed = {current: currentSecs, total: totalSecs, within2Secs: within2Secs};
                      }
                    }
                    
                    return {video: videoInfo, timeDisplay: timeDisplayInfo};
                  }catch(e){ return {has:false}; }
                })();
                """
            )
            if isinstance(status, dict):
                video_info = status.get('video', {})
                time_display_info = status.get('timeDisplay')
                
                # Extract video info
                ct = float(video_info.get('ct', 0.0)) if video_info.get('has') else 0.0
                d = float(video_info.get('d', 0.0)) if video_info.get('has') else 0.0
                ended = bool(video_info.get('ended', False))
                near_end = bool(video_info.get('nearEnd', False))
                
                # Check time display element for better end detection
                time_display_near_end = False
                if time_display_info and time_display_info.get('parsed'):
                    parsed = time_display_info['parsed']
                    time_display_near_end = parsed.get('within2Secs', False)
                    if time_display_near_end:
                        print(f"Time display shows near end: {time_display_info['text']} (within 2s)")

                # Track ended streak
                ended_streak = ended_streak + 1 if ended else 0
                # Track near-end streak (either video-based or time display-based)
                combined_near_end = near_end or time_display_near_end
                near_end_streak = near_end_streak + 1 if combined_near_end else 0

                # Track stagnation near end (ct not increasing)
                if last_ct is not None and combined_near_end:
                    delta = ct - last_ct
                    if delta <= 0.01:
                        stagnation_accum += max(0.1, poll_interval)
                    else:
                        stagnation_accum = 0.0

                # Decide to stop - more sensitive to time display detection
                if ended_streak >= ended_required:
                    print("Detected video ended (stable). Stopping capture.")
                    break
                if time_display_near_end and stagnation_accum >= 1.0:  # Reduced from 2.0s to 1.0s
                    print("Detected video end by time display + stagnation. Stopping capture.")
                    break
                if near_end_streak >= near_end_required and stagnation_accum >= stagnation_required_sec:
                    print("Detected video end by near-end + stagnation. Stopping capture.")
                    break

                last_ct = ct
        except Exception:
            pass

        time.sleep(max(0.05, poll_interval))

    # Final flush to gather any trailing words still being appended
    try:
        final_data = driver.execute_script(
            """
            try { if (typeof window.__ccFlushNow === 'function') window.__ccFlushNow(); } catch(e){}
            try { return (window.__ccStable||[]); } catch(e) { return []; }
            """
        )
        if isinstance(final_data, list):
            for entry in final_data:
                try:
                    text = (entry.get('t') if isinstance(entry, dict) else None) or ''
                    if not text:
                        continue
                    key = text.strip()
                    if key and key not in seen_texts:
                        ct = entry.get('ct') if isinstance(entry, dict) else None
                        ts = entry.get('ts') if isinstance(entry, dict) else None
                        snapshot.append({"t": key, "ct": ct, "ts": ts})
                        seen_texts.add(key)
                except Exception:
                    continue
    except Exception:
        pass

    # Report final status
    if len(snapshot) == 0:
        if first_caption_time is None:
            print("❌ No captions captured - early abort due to no captions found")
        else:
            print("❌ No captions captured - unknown issue")
    else:
        elapsed_total = time.time() - start
        print(f"📊 Capture complete: {len(snapshot)} captions in {elapsed_total:.1f}s")

    return snapshot


def write_captions(lines: List[Dict[str, Any]], out_path: str) -> bool:
    """Write captured caption lines to out_path, removing progressive duplicates.
    
    Returns:
        True if captions were written, False if no captions to write.
    """
    if not lines:
        return False
        
    os.makedirs(os.path.dirname(out_path) or '.', exist_ok=True)
    
    # Deduplicate progressive captions: keep only lines that aren't prefixes of later lines
    unique_lines = []
    seen_texts = set()
    
    for entry in lines:
        text = str(entry.get('t', '')).strip()
        if not text:
            continue
        
        # Skip if this exact text was already seen
        if text in seen_texts:
            continue
        
        # Check if this is a prefix of any already-added line (then skip it)
        is_prefix = False
        for existing in unique_lines:
            existing_text = existing.get('t', '').strip()
            if existing_text.startswith(text + ' ') or existing_text.startswith(text):
                is_prefix = True
                break
        
        if is_prefix:
            continue
        
        # Remove any existing lines that are prefixes of this new line
        unique_lines = [
            existing for existing in unique_lines
            if not text.startswith(existing.get('t', '').strip() + ' ') and 
               not text.startswith(existing.get('t', '').strip())
        ]
        
        unique_lines.append(entry)
        seen_texts.add(text)
    
    if not unique_lines:
        return False
    
    # Write with timestamps for debugging
    with open(out_path, 'w', encoding='utf-8') as f:
        for entry in unique_lines:
            text = str(entry.get('t', '')).strip()
            ct = entry.get('ct', None)
            if ct is not None:
                try:
                    f.write(f"[ct={float(ct):.3f}] {text}\n")
                except Exception:
                    f.write(f"[ct=?] {text}\n")
            else:
                f.write(f"[ct=None] {text}\n")
    
    return True


def switch_to_frame_with_selector(driver: webdriver.Chrome, selector: str, max_depth: int = 3) -> bool:
    """Switch into the iframe (possibly nested) that contains the selector. Returns True if switched.

    This scans the current document for the selector; if not present, it recursively
    descends into iframes up to max_depth. On success, the driver's context is left
    focused on the frame that contains the element. On failure, context is restored
    to the top-level document and False is returned.
    """
    from selenium.common.exceptions import NoSuchFrameException

    def _search(current_depth: int) -> bool:
        # Check current document for selector
        try:
            count = driver.execute_script("return document.querySelectorAll(arguments[0]).length;", selector)
            if isinstance(count, (int, float)) and int(count) > 0:
                return True
        except Exception:
            pass

        if current_depth >= max_depth:
            return False

        # Try each iframe
        try:
            frames = driver.find_elements(By.TAG_NAME, 'iframe')
        except Exception:
            frames = []

        for frame in frames:
            try:
                driver.switch_to.frame(frame)
                if _search(current_depth + 1):
                    return True  # stay inside the found frame
                # Not found here; go back up one level
                driver.switch_to.parent_frame()
            except NoSuchFrameException:
                # Frame vanished, skip
                try:
                    driver.switch_to.parent_frame()
                except Exception:
                    pass
            except Exception:
                # Any other error; attempt to return to parent and continue
                try:
                    driver.switch_to.parent_frame()
                except Exception:
                    pass
                continue

        return False

    # Begin from top-level
    try:
        driver.switch_to.default_content()
    except Exception:
        pass
    found = _search(0)
    if not found:
        try:
            driver.switch_to.default_content()
        except Exception:
            pass
    return found


def process_single_url(driver: webdriver.Chrome, url: str, output_path: str, args, skip_navigation: bool = False) -> bool:
    """Process a single URL and save captions to output_path.
    
    Returns:
        True if successful, False if failed.
    """
    try:
        print(f"\n{'='*80}")
        print(f"Processing: {url}")
        print(f"Output: {output_path}")
        print(f"{'='*80}")
        
        # Validate URL before navigation
        if not url or not url.startswith(('http://', 'https://')):
            print(f"❌ Invalid URL format: '{url}'")
            return False
        
        if not skip_navigation:
            print(f"🌐 Navigating to: {url}")
            driver.get(url)
            
            # Check what URL we actually ended up at
            current_url = driver.current_url
            print(f"📍 Current URL after navigation: {current_url}")
        else:
            print("🌐 Skipping navigation (already on page)")

        # Install an early quality blocker that runs on every new document
        # This ensures low-quality enforcement applies even if the page auto-loads video during auth
        if args.force_low_quality:
            try:
                driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                    'source': """
                        (function(){
                          try {
                            const originalFetch = window.fetch;
                            window.fetch = function(url, options){
                              if (typeof url === 'string') {
                                if (url.includes('1006400') || url.includes('1426400') ||
                                    url.includes('2266400') || url.includes('3526400') ||
                                    url.includes('5416400') || url.includes('8128000')) {
                                  try { console.log('Blocking high-quality stream (early):', url); } catch(e){}
                                  return Promise.reject(new Error('Blocked high quality stream'));
                                }
                              }
                              return originalFetch(url, options);
                            };

                            const originalXHROpen = XMLHttpRequest.prototype.open;
                            XMLHttpRequest.prototype.open = function(method, url, ...args){
                              if (typeof url === 'string') {
                                if (url.includes('1006400') || url.includes('1426400') ||
                                    url.includes('2266400') || url.includes('3526400') ||
                                    url.includes('5416400') || url.includes('8128000')) {
                                  try { console.log('Blocking high-quality XHR (early):', url); } catch(e){}
                                  throw new Error('Blocked high quality stream');
                                }
                              }
                              return originalXHROpen.call(this, method, url, ...args);
                            };

                            const originalXHRSend = XMLHttpRequest.prototype.send;
                            XMLHttpRequest.prototype.send = function(data){
                              try { /* record URL captured in open */ } catch(e){}
                              return originalXHRSend.call(this, data);
                            };
                          } catch(e) { /* swallow */ }
                        })();
                    """
                })

                # If we already loaded the first page during auth (skip_navigation=True),
                # refresh once so the early blocker applies before any network requests.
                if skip_navigation:
                    print("Refreshing page to apply early quality control...")
                    driver.refresh()
                    time.sleep(2)
            except Exception as e:
                print(f"Could not install early quality control: {e}")

        # Force lower quality streams by blocking high-quality requests
        # This MUST happen immediately after navigation, before any video requests
        if args.force_low_quality:
            try:
                print("Setting up quality control to force lower bandwidth streams...")
                driver.execute_script("""
                    (function() {
                        // Block high-quality video streams before they're requested
                        const originalFetch = window.fetch;
                        window.fetch = function(url, options) {
                            if (typeof url === 'string') {
                                // Block high-quality streams based on actual m3u8 bandwidth values
                                // Keep lowest quality (355400, 502400, 691400) and block higher ones
                                if (url.includes('1006400') || url.includes('1426400') || 
                                    url.includes('2266400') || url.includes('3526400') || 
                                    url.includes('5416400') || url.includes('8128000')) {
                                    console.log('Blocking high-quality stream:', url);
                                    return Promise.reject(new Error('Blocked high quality stream'));
                                }
                            }
                            return originalFetch(url, options);
                        };

                        // Intercept XMLHttpRequest
                        const originalXHROpen = XMLHttpRequest.prototype.open;
                        XMLHttpRequest.prototype.open = function(method, url, ...args) {
                            if (typeof url === 'string') {
                                // Block high-quality streams
                                if (url.includes('1006400') || url.includes('1426400') || 
                                    url.includes('2266400') || url.includes('3526400') || 
                                    url.includes('5416400') || url.includes('8128000')) {
                                    console.log('Blocking high-quality XHR:', url);
                                    throw new Error('Blocked high quality stream');
                                }
                            }
                            return originalXHROpen.call(this, method, url, ...args);
                        };
                        
                        // Also block at HLS level by intercepting m3u8 requests
                        const originalXHRSend = XMLHttpRequest.prototype.send;
                        XMLHttpRequest.prototype.send = function(data) {
                            const url = this._url || '';
                            if (typeof url === 'string' && url.includes('.m3u8')) {
                                console.log('Intercepted m3u8 request:', url);
                                // Let it proceed but we'll block individual segments later
                            }
                            return originalXHRSend.call(this, data);
                        };
                        
                        // Store original open to capture URL
                        XMLHttpRequest.prototype.open = function(method, url, ...args) {
                            this._url = url;
                            return originalXHROpen.call(this, method, url, ...args);
                        };
                        
                        console.log('Quality control enabled - blocking high-quality streams and monitoring HLS');
                    })();
                """)
            except Exception as e:
                print(f"Could not set up quality control: {e}")
        else:
            print("Using default video quality (use --force-low-quality to reduce bandwidth)")

        # Immediately pause video to avoid uncaptured playback during setup/login
        try:
            driver.execute_script("""
                (function(){
                    try {
                        const v = document.querySelector('video');
                        if (v) { v.pause(); }
                    } catch (e) {}
                })();
            """)
        except Exception:
            pass

        # Wait for either a <video> element or likely caption root to appear
        try:
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.TAG_NAME, 'video'))
            )
            print("<video> element detected.")
            
            # Enable captions before scanning for containers
            try:
                print("Priming video and enabling captions...")
                # Briefly play to initialize tracks, then pause again
                driver.execute_script("""
                    (function(){
                        try {
                            const v = document.querySelector('video');
                            if (v && v.paused) { v.play().catch(()=>{}); }
                        } catch(e){}
                    })();
                """)
                time.sleep(0.6)
                driver.execute_script("""
                    (function(){
                        try { const v = document.querySelector('video'); if (v) v.pause(); } catch(e){}
                    })();
                """)

                # Try to enable via UI (robust search of possible caption buttons)
                caption_enabled = driver.execute_script("""
                    (function(){
                        try {
                            const candidates = Array.from(document.querySelectorAll('button, [role="button"]'));
                            const matches = candidates.filter(el => {
                                const label = (el.getAttribute('aria-label') || el.textContent || '').toLowerCase();
                                return /caption|subtitl|\bcc\b/.test(label);
                            });
                            for (const btn of matches) {
                                const label = (btn.getAttribute('aria-label') || '').toLowerCase();
                                const pressed = (btn.getAttribute('aria-pressed') || '').toLowerCase();
                                if (pressed === 'true' || /\bon\b/.test(label)) return 'already_on';
                                try { btn.click(); return 'enabled'; } catch(e){}
                            }
                            return 'not_found';
                        } catch(e) { return 'error:' + String(e); }
                    })();
                """)
                if caption_enabled == 'enabled':
                    print("✓ Captions enabled via UI")
                    time.sleep(0.8)
                elif caption_enabled == 'already_on':
                    print("✓ Captions already on (UI)")
                else:
                    print("⚠️  Caption button not found or not clickable; attempting textTracks fallback")
                    # Fallback: programmatically enable text tracks (hidden = active cues without on-screen rendering)
                    tt_result = driver.execute_script("""
                        (function(){
                            try {
                                const v = document.querySelector('video');
                                let changed = 0, any = false;
                                if (v && v.textTracks) {
                                    const tracks = Array.from(v.textTracks);
                                    for (const tr of tracks) {
                                        any = true;
                                        const kind = String(tr.kind||'').toLowerCase();
                                        if (kind === 'subtitles' || kind === 'captions') {
                                            if (tr.mode === 'disabled') { tr.mode = 'hidden'; changed++; }
                                        }
                                    }
                                }
                                return { any, changed };
                            } catch(e) { return { any:false, changed:0, error:String(e) }; }
                        })();
                    """)
                    if isinstance(tt_result, dict) and tt_result.get('changed', 0) > 0:
                        print("✓ Captions activated via textTracks")
                    else:
                        print("⚠️  Could not activate captions via UI or textTracks")
            except Exception as e:
                print(f"Could not enable captions: {e}")
            
            # Prepare playback speed while keeping video paused until capture starts
            try:
                const_speed = float(args.playback_speed)
                print(f"Preparing video playback speed {const_speed}x (paused until capture starts)...")
                driver.execute_script("""
                    (function(sp){
                        try { if (window.__ccSpeedInterval) { clearInterval(window.__ccSpeedInterval); window.__ccSpeedInterval = null; } } catch(e){}
                        try {
                            const v = document.querySelector('video');
                            if (v) {
                                if (v.playbackRate !== sp) v.playbackRate = sp;
                                // keep paused for now
                                try { v.pause(); } catch(e){}
                            }
                        } catch(e){}
                        try { window.__ccPlaybackSpeed = sp; window.__ccArm = window.__ccArm || {}; window.__ccArm.playbackSpeed = sp; } catch(e){}
                    })(arguments[0]);
                """, const_speed)
                time.sleep(1)
            except Exception as e:
                print(f"Could not set playback speed: {e}")
        except Exception:
            print("No <video> detected within 30s; continuing anyway...")

        print("Searching for caption containers...")
        selected_selector: Optional[str] = None
        selectors_to_try: List[Optional[str]] = []
        if args.selector:
            selectors_to_try.append(args.selector)
        selectors_to_try.extend(DEFAULT_CAPTION_SELECTORS)

        for s in selectors_to_try:
            if not s:
                continue
            try:
                try:
                    driver.switch_to.default_content()
                except Exception:
                    pass
                print(f"Trying selector: {s}")
                count = wait_for_caption_roots(driver, selector=s, timeout_sec=3)
                if count <= 0:
                    # Quick iframe scan and re-check
                    print(f"Quick iframe scan for selector '{s}'...")
                    try:
                        if switch_to_frame_with_selector(driver, s, max_depth=4):
                            print("✓ Found frame containing selector.")
                            count = wait_for_caption_roots(driver, selector=s, timeout_sec=2)
                        else:
                            print("⚠️  Selector not found during quick iframe scan; proceeding at top-level.")
                    except Exception as e:
                        print(f"Iframe scan error: {e}")
                if count and count > 0:
                    selected_selector = s
                    print(f"✓ Using selector: {s} (found {count} elements)")
                    break
                else:
                    print(f"✗ Selector not found: {s}")
            except Exception as e:
                print(f"Selector probe error for {s}: {e}")

        print("Injecting stable capture observer...")
        # Ensure playback speed is available to the page before injection
        try:
            const_speed = float(args.playback_speed)
            driver.execute_script("(function(sp){ try { window.__ccPlaybackSpeed = sp; window.__ccArm = window.__ccArm || {}; window.__ccArm.playbackSpeed = sp; } catch(e){} })(arguments[0]);", const_speed)
        except Exception:
            pass
        # If we found a preferred selector, set it on the page before injection
        try:
            if selected_selector:
                driver.execute_script("window.__ccUserSelector = arguments[0];", selected_selector)
                print(f"Using caption selector: {selected_selector}")
        except Exception:
            pass
        inject_result = inject_stable_capture(driver, selector=selected_selector)
        status = inject_result.get('status', 'unknown') if isinstance(inject_result, dict) else str(inject_result)
        roots = inject_result.get('roots', 0) if isinstance(inject_result, dict) else 0
        tracks = inject_result.get('tracks', 0) if isinstance(inject_result, dict) else 0
        print(f"Injection status: {status}; roots={roots}; text tracks={tracks}")
        
        # Additional debugging information
        if roots == 0:
            print("⚠️  No caption roots found - checking for common issues...")
            try:
                debug_info = driver.execute_script("""
                    return {
                        hasVideo: !!document.querySelector('video'),
                        videoPaused: document.querySelector('video')?.paused ?? 'no-video',
                        videoReadyState: document.querySelector('video')?.readyState ?? 'no-video',
                        captionButtons: document.querySelectorAll('button[aria-label*="Caption"]').length,
                        ariaLiveElements: document.querySelectorAll('[aria-live]').length,
                        iframes: document.querySelectorAll('iframe').length
                    };
                """)
                print(f"   Debug info: {debug_info}")
            except Exception as e:
                print(f"   Debug check failed: {e}")

        # Quick reinjection after frame scan if needed (no long waits)
        if roots == 0:
            fallback_selector: Optional[str] = None
            # If a custom selector was provided and failed, try defaults
            if selected_selector is None and DEFAULT_CAPTION_SELECTORS:
                fallback_selector = DEFAULT_CAPTION_SELECTORS[0]
            # If first default failed, try the second
            elif selected_selector in DEFAULT_CAPTION_SELECTORS:
                try:
                    idx = DEFAULT_CAPTION_SELECTORS.index(selected_selector)
                    if idx == 0 and len(DEFAULT_CAPTION_SELECTORS) > 1:
                        fallback_selector = DEFAULT_CAPTION_SELECTORS[1]
                except Exception:
                    fallback_selector = None

            if fallback_selector:
                print(f"Re-checking frames and reinjecting with fallback selector: {fallback_selector}")
                try:
                    driver.switch_to.default_content()
                except Exception:
                    pass
                try:
                    if switch_to_frame_with_selector(driver, fallback_selector, max_depth=4):
                        try:
                            driver.execute_script("window.__ccUserSelector = arguments[0];", fallback_selector)
                        except Exception:
                            pass
                        inject_result = inject_stable_capture(driver, selector=fallback_selector)
                        roots = inject_result.get('roots', 0) if isinstance(inject_result, dict) else 0
                        print(f"Reinjection roots={roots}")
                        if roots > 0:
                            selected_selector = fallback_selector
                except Exception as e:
                    print(f"Reinjection error: {e}")

        if isinstance(inject_result, dict) and inject_result.get('status') == 'error':
            print(f"Injection error: {inject_result.get('message')}")
            return False

        # Start playback right before capture and enforce speed periodically
        try:
            const_speed = float(args.playback_speed)
            print("Starting playback and beginning capture...")
            driver.execute_script("""
                (function(sp){
                    try { if (window.__ccSpeedInterval) { clearInterval(window.__ccSpeedInterval); window.__ccSpeedInterval = null; } } catch(e){}
                    function setSpeed(){
                        try {
                            const v = document.querySelector('video');
                            if (v) {
                                if (v.playbackRate !== sp) v.playbackRate = sp;
                                if (v.paused) v.play().catch(()=>{});
                            }
                        } catch(e){}
                    }
                    setSpeed();
                    window.__ccSpeedInterval = setInterval(setSpeed, 2000);
                    try { window.__ccPlaybackSpeed = sp; window.__ccArm = window.__ccArm || {}; window.__ccArm.playbackSpeed = sp; } catch(e){}
                })(arguments[0]);
            """, const_speed)
        except Exception as e:
            print(f"Could not start playback: {e}")

        effective_poll_interval = max(0.05, float(args.poll_interval) / max(1.0, float(args.playback_speed) if args.playback_speed else 1.0))
        print(f"Capturing for up to {args.duration}s... (poll every {effective_poll_interval:.3f}s)")
        
        try:
            lines = poll_captions(driver, max_seconds=int(args.duration), poll_interval=effective_poll_interval)
            print(f"Captured {len(lines)} unique stabilized lines.")
        except Exception as e:
            print(f"⚠️  Error during caption polling: {e}")
            # Try to save any in-progress captions before the crash
            try:
                lines = driver.execute_script("return (window.__ccStable||[]);")
                if isinstance(lines, list) and lines:
                    print(f"💾 Saving {len(lines)} in-progress captions from before crash...")
                    # Convert to expected format
                    processed_lines = []
                    for entry in lines:
                        if isinstance(entry, dict) and entry.get('t'):
                            processed_lines.append({
                                "t": entry.get('t', '').strip(),
                                "ct": entry.get('ct'),
                                "ts": entry.get('ts')
                            })
                    lines = processed_lines
                else:
                    lines = []
            except Exception:
                print("❌ Could not recover in-progress captions")
                lines = []

        if lines:
            print(f"Writing output → {output_path}")
            write_captions(lines, output_path)
            print("✓ Successfully completed")
            return True
        else:
            print("❌ No captions captured - skipping file creation")
            return False
        
    except Exception as e:
        print(f"❌ Error processing {url}: {e}")
        return False


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Capture DOM-rendered captions with a stable MutationObserver.")
    parser.add_argument('input', help='Single URL or path to CSV file with date,url columns')
    parser.add_argument('--out', dest='out', default='captions.txt', help='Output file path (single URL) or directory (CSV batch)')
    parser.add_argument('--duration', dest='duration', type=int, default=600, help='Capture duration in seconds (default: 600)')
    parser.add_argument('--selector', dest='selector', default=None, help='CSS selector for the caption root (optional)')
    parser.add_argument('--headless', dest='headless', action='store_true', help='Run Chrome in headless mode')
    parser.add_argument('--login-wait', dest='login_wait', type=int, default=0, help='Seconds to wait after page load for manual login (deprecated, use --wait-for-enter)')
    parser.add_argument('--wait-for-enter', dest='wait_for_enter', action='store_true', help='Wait for user to press Enter after login/setup')
    parser.add_argument('--poll-interval', dest='poll_interval', type=float, default=1.0, help='Polling interval seconds (default: 1.0)')
    parser.add_argument('--playback-speed', dest='playback_speed', type=float, default=1.0, help='Video playback speed (e.g., 1.0, 5.0, 10.0)')
    parser.add_argument('--force-low-quality', dest='force_low_quality', action='store_true', help='Force lower quality streams to reduce bandwidth usage')

    args = parser.parse_args(argv)

    # Determine if input is a CSV file or single URL
    is_csv = args.input.endswith('.csv') and os.path.isfile(args.input)
    
    if is_csv:
        # Batch processing mode
        urls = load_urls_from_csv(args.input)
        
        # Ensure output is a directory
        output_dir = args.out
        if not output_dir.endswith('/'):
            output_dir += '/'
        os.makedirs(output_dir, exist_ok=True)
        print(f"Output directory: {output_dir}")
        
        driver: Optional[webdriver.Chrome] = None
        try:
            print(f"Opening Chrome (headless={bool(args.headless)})...")
            driver = build_driver(headless=bool(args.headless))
            
            # Navigate to first URL to trigger login
            first_url = urls[0]['url']
            print(f"Navigating to first URL for authentication: {first_url}")
            driver.get(first_url)
            
            # Handle authentication once at the beginning
            if args.wait_for_enter:
                print("\n" + "="*60)
                print("Please complete login and authentication.")
                print("Press ENTER when ready to start batch processing...")
                print("="*60)
                input()
            elif args.login_wait and args.login_wait > 0:
                print(f"Waiting {args.login_wait}s for login/consents...")
                time.sleep(args.login_wait)
            
            # Process each URL
            successful = 0
            failed = 0
            for i, url_data in enumerate(urls, 1):
                url = url_data['url']
                date = url_data['date']
                
                print(f"\n🔍 Debug: Processing URL #{i}")
                print(f"  Date: '{date}'")
                print(f"  URL: '{url}'")
                
                # Validate URL
                if not url or url.strip() == '' or url.strip() == 'data:':
                    print(f"  ❌ Invalid URL, skipping: '{url}'")
                    failed += 1
                    continue
                
                # Parse team names from URL
                team1, team2 = parse_team_names_from_url(url)
                print(f"  Teams: {team1} at {team2}")
                output_path = generate_output_filename(date, team1, team2, output_dir)
                print(f"  Output: {output_path}")
                
                # Skip if output already exists
                if os.path.exists(output_path):
                    print(f"\n⏭️  Skipping {i}/{len(urls)}: {output_path} already exists")
                    continue
                
                print(f"\n🎯 Processing {i}/{len(urls)}: {team1} at {team2} ({date})")
                
                # For the first URL, we already navigated to it for authentication, so skip navigation
                if i == 1:
                    print("  (Already navigated for authentication)")
                    success = process_single_url(driver, url, output_path, args, skip_navigation=True)
                else:
                    success = process_single_url(driver, url, output_path, args)
                
                if success:
                    successful += 1
                else:
                    failed += 1
                    # Check if browser crashed and needs restart
                    try:
                        if check_browser_crash(driver):
                            print("💥 Browser crashed! Restarting for next URL...")
                            try:
                                driver.quit()
                            except Exception:
                                pass
                            driver = build_driver(headless=bool(args.headless))
                    except Exception:
                        pass
                    # Continue with next URL even if one fails
                    print(f"Continuing with next URL...")
            
            print(f"\n{'='*80}")
            print(f"Batch processing complete!")
            print(f"✅ Successful: {successful}")
            print(f"❌ Failed: {failed}")
            print(f"📁 Output directory: {output_dir}")
            print(f"{'='*80}")
            
            return 0 if failed == 0 else 1
            
        except Exception as e:
            print(f"Batch processing error: {e}")
            return 1
        finally:
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass
                    
    else:
        # Single URL mode (original behavior)
        url = args.input
        
        driver: Optional[webdriver.Chrome] = None
        try:
            print(f"Opening Chrome (headless={bool(args.headless)})...")
            driver = build_driver(headless=bool(args.headless))
            
            # Handle authentication
            if args.wait_for_enter:
                print("\n" + "="*60)
                print("Please complete login and start video playback.")
                print("Press ENTER when ready to start caption capture...")
                print("="*60)
                input()
            elif args.login_wait and args.login_wait > 0:
                print(f"Waiting {args.login_wait}s for login/consents...")
                time.sleep(args.login_wait)
            
            success = process_single_url(driver, url, args.out, args)
            return 0 if success else 1
            
        except Exception as e:
            print(f"Error: {e}")
            return 1
        finally:
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass


if __name__ == '__main__':
    sys.exit(main())


