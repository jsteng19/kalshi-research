# Kalshi Research

Tools for sourcing transcripts and modeling fair value on Kalshi [mention markets](https://kalshi.com/category/mentions) — contracts that resolve on
what someone says in a speech, broadcast, interview, etc.

Most of the work is data sourcing and base-rate calibration: get a clean transcript corpus for a
speaker, find a representative subset for a given event, and compare those transcripts to historical
resolutions if available to measure their reliability. For some recurring events, Kalshi resolution history alone has enough sample size to inform fair value.  

## Data

**Sourcing.** Transcripts come from various sources: Factbase for political speeches, Truth Social for Trump posts, YouTube for many recorded appearances, and some manual extraction and transcription from audio streams. Currently Deepgram is used for STT. 

Sports game lists and announcer mappings were sourced from [506 Sports](https://506sports.com) and [ICDB](https://icdb.com). 
[youtube-transcript.io](https://youtube-transcript.io) was used for collecting Youtube captions.

**Cleaning.** If raw transcripts are diarized, they're reduced to just the target
speaker's lines, then normalized (unicode/quotes to ASCII, line-break and timestamp removal) into
one clean text file per appearance. For sports, broadcasts are organized by announcer, since the
market is specific to who's calling the game.

**Data Storage.** All transcripts under `data/`, one folder per speaker or topic, split into raw and
processed transcripts. Sports additionally carry game/announcer mappings scraped from ICDB. Some data is included here publicly. 

## Automated Data Pipeline

Data collection is automated end-to-end (`src/auto_collect/`):

1. **Collect & extract.** Youtube transcript collection is fully automated; the pipeline finds candidate videos, downloads the transcripts, and uses an LLM to pull out the target speaker's dialogue.
2. **Pull the markets.** Open mention markets/events are pulled straight from the Kalshi API, and the
   target phrases are parsed out of the market titles/subtitles — so dashboards always target the
   phrases that are traded.
3. **Generate match patterns.** Those phrases are turned into regex programmatically
   (`src/utils/regex_pattern_generator.py`), handling plurals, contractions, punctuation, and spacing,
   so a phrase can be counted reliably across transcripts. These are intended to match Kalshi's exact contract specifications regarding phrase occurrence.

```bash
python -m src.auto_collect discover "What will the host say during the monologue?"
python -m src.auto_collect collect  data/mentions/<speaker>_videos.csv
python -m src.auto_collect process  data/mentions/<speaker>/raw/ --speaker "<Name>"
```

## Analysis

The core of the analysis is identifying a base rate on a representative sample: what percent of transcripts had n or more appearances of the phrase.

For some events, the length of a particular event can be estimated, and a statistical word-model can be used:

- **Poisson** treats each word as an IID random variable, frequencies estimated from the dataset
- **Negative Binomial** Introduces a dispersion parameter to better model words that cluster (a phrase clusters in some appearances and is
  absent in others — common for topical language).

These give the probability a phrase is said in an event with a specific wordcount, given the historical hit-rate.

Exploratory analysis started in Jupyter notebooks (`notebooks/`), one per speaker or event. For
recurring markets — NBA/NCAAB announcer mentions especially — that work was packaged into scripts
that generate self-contained HTML dashboards (`src/nba/generate_report.py`), which are far cleaner
and more reproducible than the notebooks. A few examples are in [`examples/reports/`](examples/reports).

## Usage

```bash
pip install -r requirements.txt
cp .env.example .env   # Kalshi, Deepgram/AssemblyAI, YouTube, OpenAI/Anthropic keys
```

## Notes

Transcripts under `data/` are third-party source material collected for personal research.

