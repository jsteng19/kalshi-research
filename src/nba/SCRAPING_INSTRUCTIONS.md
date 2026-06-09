# NBA Game Audio Scraping Instructions

Comprehensive guide for an AI agent to scrape NBA game audio from online replay sites.

---

## Table of Contents
1. [Overview](#overview)
2. [Data Sources](#data-sources)
3. [Replay Websites](#replay-websites)
4. [Video Hosting Platforms](#video-hosting-platforms)
5. [Automation Scripts](#automation-scripts)
6. [Filename Convention](#filename-convention)
7. [Step-by-Step Process](#step-by-step-process)
8. [Browser-Based Extraction](#browser-based-extraction)
9. [Parallel Download Strategy](#parallel-download-strategy)
10. [Validation](#validation)
11. [Common Issues & Solutions](#common-issues--solutions)
12. [Quick Reference Commands](#quick-reference-commands)

---

## Overview

**Goal:** Download audio from NBA full game replays for announcer speech analysis.

**Pipeline:**
1. Load game lists from ICDB CSV files (`data-nba/icdb/`)
2. Search replay websites for each game (basketball-video.com, basketballreplays.net)
3. Extract video embed URLs (Filemoon preferred for speed, OK.ru as reliable fallback)
4. Download and convert to audio (MP3, 16kHz mono)
5. Validate completeness (≥85 minutes for full games)

**Success Rate Expectations:**
- Recent games (2024-2026): ~70-80% success rate
- Older games (2020-2023): ~30-50% success rate (many expired streams)
- Games older than 2020: Very low success rate

---

## Data Sources

### ICDB Game Lists

Location: `data-nba/icdb/`

**Available announcers:**
```
dave_pasch_icdb.csv
michael_grady_icdb.csv  
eric_collins_icdb.csv
ian_eagle_icdb.csv
kate_scott_icdb.csv
kevin_harlan_icdb.csv
marc_kestecher_icdb.csv
mark_followill_icdb.csv
mark_jones_icdb.csv
mike_breen_icdb.csv
mike_tirico_icdb.csv
noah_eagle_icdb.csv
ryan_ruocco_icdb.csv
terry_gannon_icdb.csv
combined_announcers.csv
```

**CSV Format:**
```csv
date,timestamp,teams,home_team,away_team,competition,channel,match_id,match_url,main_commentator,...
2026-01-16 18:30,1768617000,Minnesota Timberwolves @ Houston Rockets,Houston Rockets,Minnesota Timberwolves,NBA Regular Season,"ESPN (US), ESPN Unlimited",...
```

**Key fields:**
| Field | Description | Example |
|-------|-------------|---------|
| `date` | Game datetime | `2026-01-16 18:30` |
| `home_team` | Full home team name | `Houston Rockets` |
| `away_team` | Full away team name | `Minnesota Timberwolves` |
| `competition` | Game type | `NBA Regular Season`, `NBA Playoffs`, `NBA Cup` |
| `channel` | Broadcast network | `ESPN (US)`, `ABC (usa)` |
| `main_commentator` | Play-by-play announcer | `Dave Pasch` |

---

## Replay Websites

### 1. basketball-video.com (Primary)

**URL Formats:**

Format 1 (newer games, 2024+):
```
https://basketball-video.com/{away-slug}-vs-{home-slug}-full-game-replay-{month}-{day}-{year}-nba
```

Format 2 (older games):
```
https://basketball-video.com/{away-slug}-vs-{home-slug}-{mon}-{day}-{year}-nba-full-game-replay
```

**Examples:**
```
# Format 1
https://basketball-video.com/minnesota-timberwolves-vs-houston-rockets-full-game-replay-january-16-2026-nba

# Format 2
https://basketball-video.com/minnesota-timberwolves-vs-houston-rockets-jan-16-2026-nba-full-game-replay
```

**Website Quirks:**
- Has popup ads - ignore them
- Video sources listed as "Server #1 (OK)", "Server #2", etc.
- OK.ru embeds show video duration in preview (e.g., "2:04:16")
- Some games have multiple video sources to try
- Uses `guidedesgemmes.com` redirects for some OK.ru embeds

### 2. basketballreplays.net (Secondary)

**URL Format:**
```
https://basketballreplays.net/{away-slug}-vs-{home-slug}-{day}-{month}-{year}-nba-full-game-replay
```

**Example:**
```
https://basketballreplays.net/san-antonio-spurs-vs-oklahoma-city-thunder-30-october-2024-nba-full-game-replay
```

**Website Quirks:**
- Different URL structure (day before month)
- Sometimes has different OK.ru video IDs than basketball-video.com
- Good fallback when primary fails

### Team Name Slugs

Convert team names to URL slugs:
```python
TEAM_SLUGS = {
    'Atlanta Hawks': 'atlanta-hawks',
    'Boston Celtics': 'boston-celtics',
    'Brooklyn Nets': 'brooklyn-nets',
    'Charlotte Hornets': 'charlotte-hornets',
    'Chicago Bulls': 'chicago-bulls',
    'Cleveland Cavaliers': 'cleveland-cavaliers',
    'Dallas Mavericks': 'dallas-mavericks',
    'Denver Nuggets': 'denver-nuggets',
    'Detroit Pistons': 'detroit-pistons',
    'Golden State Warriors': 'golden-state-warriors',
    'Houston Rockets': 'houston-rockets',
    'Indiana Pacers': 'indiana-pacers',
    'Los Angeles Clippers': 'la-clippers',  # NOTE: "la" not "los-angeles"
    'Los Angeles Lakers': 'los-angeles-lakers',
    'Memphis Grizzlies': 'memphis-grizzlies',
    'Miami Heat': 'miami-heat',
    'Milwaukee Bucks': 'milwaukee-bucks',
    'Minnesota Timberwolves': 'minnesota-timberwolves',
    'New Orleans Pelicans': 'new-orleans-pelicans',
    'New York Knicks': 'new-york-knicks',
    'Oklahoma City Thunder': 'oklahoma-city-thunder',
    'Orlando Magic': 'orlando-magic',
    'Philadelphia 76ers': 'philadelphia-76ers',
    'Phoenix Suns': 'phoenix-suns',
    'Portland Trail Blazers': 'portland-trail-blazers',
    'Sacramento Kings': 'sacramento-kings',
    'San Antonio Spurs': 'san-antonio-spurs',
    'Toronto Raptors': 'toronto-raptors',
    'Utah Jazz': 'utah-jazz',
    'Washington Wizards': 'washington-wizards',
}
```

---

## Video Hosting Platforms

### Filemoon (Preferred for Speed)

**Why preferred when available:**
- **Fastest download** (~2-3 min per game with parallel HLS)
- Parallel segment downloading (16 workers)
- Good for recent games

**Embed URL formats:**
```
https://filemoon.to/e/{VIDEO_ID}
https://filemoon.sx/e/{VIDEO_ID}
https://luluvdo.com/e/{VIDEO_ID}
```

**Extraction method:**
1. Navigate to Filemoon embed page in browser
2. Capture HLS URL from network requests (look for `master.m3u8`)
3. Use parallel segment downloader

```python
from src.nba.filemoon_extractor import extract_audio_from_hls_parallel

success = extract_audio_from_hls_parallel(
    'https://fin-3dg-b1.example.com/hls2/.../master.m3u8?...',
    'output.mp3',
    max_workers=16
)
```

**Limitations:**
- Requires JavaScript execution to get HLS URL
- Videos expire quickly (24-48 hours)
- More complex extraction process

---

### OK.ru (Reliable Fallback)

**Why use as fallback:**
- More reliable/stable long-term
- Direct extraction via yt-dlp (simpler)
- Works when Filemoon unavailable
- **Slower** (~5-8 min per game, often throttled)

**Embed URL format:**
```
https://ok.ru/videoembed/{VIDEO_ID}
```

**How to find OK.ru IDs:**
1. Navigate to replay page
2. Look in page source for `ok.ru/videoembed/` pattern
3. The VIDEO_ID is a numeric string (e.g., `11515307821654`)

**Extraction method:**
```python
from src.nba.fast_audio_extractor import extract_audio_fast

success = extract_audio_fast(
    'https://ok.ru/videoembed/11515307821654',
    'output.mp3'
)
```

**Common OK.ru errors:**
| Error | Cause | Solution |
|-------|-------|----------|
| "Invalid data found when processing input" | Corrupted source video | Try different source |
| "Video is not available" | Removed/expired | Try different website |
| Timeout | Slow server | Retry or try later |

### Dailymotion (Last Resort)

**Issues:**
- Often split into Part 1 and Part 2
- Requires merging parts
- Lower priority than single-file sources

---

## Automation Scripts

### Script Overview

| Script | Purpose | Use When |
|--------|---------|----------|
| `parallel_batch_downloader.py` | Scan + download many games | Large batch processing |
| `batch_game_downloader.py` | Sequential batch download | Moderate batches, debugging |
| `game_audio_pipeline.py` | Full pipeline with browser support | Complex sources |
| `fast_audio_extractor.py` | Download from OK.ru | Single OK.ru downloads |
| `filemoon_extractor.py` | Download from HLS/Filemoon | When HLS URL is known |

### parallel_batch_downloader.py (Recommended)

**Location:** `src/nba/parallel_batch_downloader.py`

**Features:**
- Scans multiple games simultaneously for sources
- Downloads multiple games in parallel
- Validates duration after download
- Removes incomplete files automatically
- Skips existing valid files

**Usage:**
```bash
python src/nba/parallel_batch_downloader.py data-nba/icdb/dave_pasch_icdb.csv \
  -o data-nba/audio/dave_pasch \
  -n 50 \
  --scan-workers 15 \
  --download-workers 6
```

**Arguments:**
| Argument | Description | Default |
|----------|-------------|---------|
| `input` | CSV file with games | Required |
| `-o, --output-dir` | Output directory | Required |
| `-n, --max-games` | Max games to process | 50 |
| `--scan-workers` | Parallel scanners | 15 |
| `--download-workers` | Parallel downloaders | 6 |

**How it works:**
1. Loads games from CSV
2. Checks which already exist and are valid (≥85 min)
3. Scans all URLs in parallel for OK.ru IDs
4. Downloads found games in parallel
5. Validates each download
6. Prints summary

### fast_audio_extractor.py

**Location:** `src/nba/fast_audio_extractor.py`

**Features:**
- Streams audio directly via yt-dlp → FFmpeg pipe
- ~3x faster than full video download
- Optimized for speech (16kHz, mono, 48kbps)

**Usage:**
```python
from src.nba.fast_audio_extractor import extract_audio_fast

success = extract_audio_fast(
    'https://ok.ru/videoembed/11515307821654',
    'data-nba/audio/dave_pasch/2026-01-16_min-at-hou.mp3',
    sample_rate=16000,  # Default: 16000 Hz
    channels=1,          # Default: mono
    bitrate='48k'        # Default: 48kbps
)
```

**Technical details:**
- Uses `yt-dlp -f hls-333/worst` to get lowest quality (only need audio)
- Pipes stdout to FFmpeg for on-the-fly conversion
- 30 minute timeout for safety
- Progress monitor shows size and elapsed time

### filemoon_extractor.py

**Location:** `src/nba/filemoon_extractor.py`

**Key functions:**

```python
# Extract HLS URL from Filemoon page (requires browser)
from src.nba.filemoon_extractor import extract_hls_url_with_browser
hls_url = extract_hls_url_with_browser('https://filemoon.to/e/...')

# Download from HLS URL with parallel segments
from src.nba.filemoon_extractor import extract_audio_from_hls_parallel
success = extract_audio_from_hls_parallel(hls_url, 'output.mp3', max_workers=8)
```

**How parallel HLS download works:**
1. Parse M3U8 playlist to get segment URLs
2. Download all `.ts` segments in parallel (16 workers default)
3. Concatenate segments with FFmpeg
4. Extract audio in one pass

---

## Filename Convention

**Standard format:**
```
{date}_{away-team-slug}-at-{home-team-slug}.mp3
```

**Example:**
```
2026-01-16_minnesota-timberwolves-at-houston-rockets.mp3
2025-12-25_dallas-mavericks-at-golden-state-warriors.mp3
2023-12-25_milwaukee-bucks-at-new-york-knicks.mp3
```

**Generating filenames:**
```python
def generate_filename(date: str, away_team: str, home_team: str) -> str:
    """Generate filename from team names."""
    away_slug = away_team.lower().replace(' ', '-').replace("'", '')
    home_slug = home_team.lower().replace(' ', '-').replace("'", '')
    return f"{date}_{away_slug}-at-{home_slug}.mp3"

# Example
generate_filename('2026-01-16', 'Minnesota Timberwolves', 'Houston Rockets')
# Returns: '2026-01-16_minnesota-timberwolves-at-houston-rockets.mp3'
```

**IMPORTANT:** Always use full team name slugs (lowercase, hyphen-separated) to match existing files in the pipeline.

---

## Step-by-Step Process

### 1. Prepare Working Environment

```bash
cd /Users/jstenger/Documents/repos/kalshi-research
source venv/bin/activate
```

### 2. Select Announcer and Check Existing Downloads

```python
import pandas as pd
import os

# Load games for announcer
announcer = 'dave_pasch'
df = pd.read_csv(f'data-nba/icdb/{announcer}_icdb.csv')

# Parse date column
df['date_parsed'] = pd.to_datetime(df['date'])
df['date_str'] = df['date_parsed'].dt.strftime('%Y-%m-%d')

# Check existing downloads
audio_dir = f'data-nba/audio/{announcer}'
existing = set()
if os.path.exists(audio_dir):
    for f in os.listdir(audio_dir):
        if f.endswith('.mp3'):
            date = f.split('_')[0]
            existing.add(date)

# Filter to remaining games
remaining = df[~df['date_str'].isin(existing)]
print(f"Total games: {len(df)}")
print(f"Already downloaded: {len(existing)}")
print(f"Remaining: {len(remaining)}")
```

### 3. Run Batch Download

**Option A: Use parallel batch downloader (recommended)**
```bash
python src/nba/parallel_batch_downloader.py data-nba/icdb/dave_pasch_icdb.csv \
  -o data-nba/audio/dave_pasch \
  -n 100 \
  --scan-workers 15 \
  --download-workers 6
```

**Option B: Manual browser-assisted download**

For games where automated extraction fails, use MCP browser tools:

1. Navigate to replay page:
```
browser_navigate: https://basketball-video.com/minnesota-timberwolves-vs-houston-rockets-full-game-replay-january-16-2026-nba
```

2. Take snapshot to find video sources:
```
browser_snapshot
```

3. Look for OK.ru embed IDs in the page
4. Download using fast_audio_extractor

---

## Browser-Based Extraction

When automated scripts fail, use MCP browser tools for manual extraction.

### Finding OK.ru IDs via Browser

1. Navigate to game page
2. Snapshot to see page structure
3. Look for elements containing `ok.ru/videoembed/`
4. Extract the numeric ID

### Capturing Filemoon HLS URLs

1. Navigate to Filemoon embed page
2. Check network requests for `master.m3u8` or `.m3u8` URLs
3. The HLS URL will contain parameters like `?t=...&s=...&e=...`
4. These URLs expire quickly - download immediately after capturing

### Example Browser Workflow

```python
# After finding OK.ru ID via browser snapshot
okru_id = '11515307821654'

from src.nba.fast_audio_extractor import extract_audio_fast
success = extract_audio_fast(
    f'https://ok.ru/videoembed/{okru_id}',
    'data-nba/audio/dave_pasch/2026-01-16_min-at-hou.mp3'
)
```

---

## Parallel Download Strategy

For maximum efficiency, run multiple downloads simultaneously:

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.nba.fast_audio_extractor import extract_audio_fast

# List of games with found OK.ru IDs
games = [
    {'date': '2025-12-29', 'away': 'cleveland-cavaliers', 'home': 'san-antonio-spurs', 'okru_id': '11190527593132'},
    {'date': '2025-12-26', 'away': 'los-angeles-clippers', 'home': 'portland-trail-blazers', 'okru_id': '11179200547414'},
    # ... more games
]

def download_game(game):
    output = f"data-nba/audio/announcer/{game['date']}_{game['away']}-at-{game['home']}.mp3"
    url = f"https://ok.ru/videoembed/{game['okru_id']}"
    return extract_audio_fast(url, output)

# Run 6 downloads in parallel
with ThreadPoolExecutor(max_workers=6) as executor:
    futures = {executor.submit(download_game, g): g for g in games}
    for future in as_completed(futures):
        game = futures[future]
        try:
            result = future.result()
            print(f"✅ {game['date']}: {'Success' if result else 'Failed'}")
        except Exception as e:
            print(f"❌ {game['date']}: Error - {e}")
```

**Recommended worker counts:**
- Scanning: 15 workers (lightweight HTTP requests)
- Downloading: 6 workers (bandwidth-intensive)

---

## Validation

### Duration Check

Full NBA games should be ≥85 minutes. Check with:

```bash
# Single file
ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 file.mp3

# All files in directory
for f in data-nba/audio/dave_pasch/*.mp3; do
  dur=$(ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "$f")
  min=$(echo "scale=1; $dur/60" | bc)
  echo "$(basename $f): ${min} min"
done
```

### Duration Thresholds

| Duration | Status | Action |
|----------|--------|--------|
| ≥85 min | Complete ✅ | Keep |
| 60-85 min | Partial ⚠️ | May be usable, investigate |
| <60 min | Incomplete ❌ | Delete and retry |

### Validation Script

```python
import subprocess
import os

def validate_audio_duration(filepath: str, min_minutes: float = 85) -> tuple:
    """Check if audio file is long enough."""
    try:
        result = subprocess.run(
            ['ffprobe', '-v', 'error', '-show_entries', 'format=duration',
             '-of', 'default=noprint_wrappers=1:nokey=1', filepath],
            capture_output=True, text=True, timeout=10
        )
        duration_sec = float(result.stdout.strip())
        duration_min = duration_sec / 60
        return duration_min >= min_minutes, duration_min
    except:
        return False, 0

# Check all files
audio_dir = 'data-nba/audio/dave_pasch'
for f in os.listdir(audio_dir):
    if f.endswith('.mp3'):
        filepath = os.path.join(audio_dir, f)
        valid, duration = validate_audio_duration(filepath)
        status = "✅" if valid else "⚠️"
        print(f"{status} {f}: {duration:.1f} min")
```

---

## Common Issues & Solutions

### Issue: "Invalid data found when processing input"
**Cause:** Corrupted OK.ru stream (common for older videos)
**Solutions:**
1. Try alternative sources on the same page
2. Try basketballreplays.net instead of basketball-video.com
3. Skip the game if all sources fail

### Issue: "Video source is unavailable"
**Cause:** Video removed/expired from hosting platform
**Solutions:**
1. Try different replay website
2. Try different server on same page
3. Game may be permanently unavailable

### Issue: "404 - Page not Found"
**Cause:** Wrong URL format or game not uploaded
**Solutions:**
1. Try alternative URL format (see [Replay Websites](#replay-websites))
2. Check if game actually occurred on that date
3. Try different replay website

### Issue: Partial downloads (< 85 min)
**Cause:** Source video was uploaded incomplete
**Solutions:**
1. Check all available sources on the page
2. Try different replay website
3. If all partial, this is a source limitation - keep the file

### Issue: Duplicate filenames
**Cause:** Different naming conventions (full names vs abbreviations)
**Prevention:** Always use 3-letter abbreviations from `TEAM_ABBREVS`
**Fix:** Delete duplicates, keeping larger file

### Issue: Download hangs
**Cause:** Network issues or server throttling
**Solutions:**
1. Increase timeout
2. Retry later
3. Use fewer parallel workers

---

## Quick Reference Commands

### Activate environment
```bash
cd /Users/jstenger/Documents/repos/kalshi-research && source venv/bin/activate
```

### Scan and download batch
```bash
python src/nba/parallel_batch_downloader.py data-nba/icdb/{announcer}_icdb.csv \
  -o data-nba/audio/{announcer} -n 50 --scan-workers 15 --download-workers 6
```

### Download single game (OK.ru)
```python
from src.nba.fast_audio_extractor import extract_audio_fast
extract_audio_fast(
    'https://ok.ru/videoembed/11515307821654', 
    'data-nba/audio/dave_pasch/2026-01-16_minnesota-timberwolves-at-houston-rockets.mp3'
)
```

### Download from HLS URL
```python
from src.nba.filemoon_extractor import extract_audio_from_hls_parallel
extract_audio_from_hls_parallel(hls_url, 'output.mp3', max_workers=8)
```

### Check file durations
```bash
for f in data-nba/audio/dave_pasch/*.mp3; do
  dur=$(ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "$f")
  min=$(echo "scale=1; $dur/60" | bc)
  echo "$(basename $f): ${min} min"
done
```

### Count complete vs partial
```bash
complete=$(for f in data-nba/audio/dave_pasch/*.mp3; do 
  ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "$f" | awk '{print $1/60}'
done | awk '$1>=85' | wc -l)
echo "Complete games: $complete"
```

### List downloaded games
```bash
ls data-nba/audio/dave_pasch/*.mp3 | wc -l
```

---

## Output Structure

```
data-nba/
├── icdb/                          # Source game lists from ICDB
│   ├── dave_pasch_icdb.csv
│   ├── michael_grady_icdb.csv
│   └── ...
├── audio/                         # Downloaded audio files
│   ├── dave_pasch/
│   │   ├── 2026-01-16_minnesota-timberwolves-at-houston-rockets.mp3
│   │   ├── 2025-12-25_dallas-mavericks-at-golden-state-warriors.mp3
│   │   ├── 2023-12-25_milwaukee-bucks-at-new-york-knicks.mp3
│   │   └── ...
│   └── michael_grady/
│       └── ...
└── transcripts/                   # Transcription output (separate pipeline)
    ├── dave-pasch/
    │   ├── transcripts/           # Raw transcripts
    │   └── diarized/              # Speaker-diarized transcripts
    └── michael-grady/
```

---

## Tips for Success

1. **Prioritize recent games** (2024-2026): Better stream availability
2. **Try Filemoon first**: Fastest with parallel HLS (~2-3 min/game)
3. **Fall back to OK.ru**: More reliable but slower (~5-8 min/game)
4. **Scan before downloading**: Use parallel_batch_downloader to find sources first
4. **Validate after download**: Always check duration
5. **Be patient with older games**: May need to try multiple sources
6. **Use browser tools**: MCP browser can capture network requests for OK.ru IDs
7. **Monitor progress**: Check terminal output for errors
8. **Clean up duplicates**: Maintain consistent filename format
9. **Automate where possible**: Use batch scripts for efficiency
10. **Be flexible**: Different games may require different approaches
