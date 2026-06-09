#!/usr/bin/env python3
"""
Mention Market Report Generator CLI

Generate analysis reports for mention market transcripts.

Usage:
    python -m src.auto_collect.report data/bernie/processed-transcripts/ -e KXBERNIEMENTION-26JAN20
    python -m src.auto_collect.report data/bernie -e KXMENTION-BERN26MAR10 -s KXBERNIEMENTION-26MAR05
    python -m src.auto_collect.report data/mentions/will_smith/ --phrases "Oscar,slap,Chris Rock"
"""

import argparse
import base64
import csv
import html
import io
import os
import re
import sys
import webbrowser
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# Add project root
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.models.phrase_analysis import (
    process_directory, find_phrase_context, get_per_appearance_frequency,
    calculate_confidence_interval
)
from src.utils.regex_pattern_generator import generate_regex_patterns


def load_phrases_from_event(event_ticker: str) -> List[str]:
    """Load phrases from a Kalshi event ticker."""
    from src.models.phrase_analysis import get_phrases
    phrases = get_phrases(event_ticker)
    phrases = [re.sub(r"\s*\([^)]*\)", "", p).strip() for p in phrases]
    return phrases


def _normalize_phrase_key(phrase: str) -> str:
    if not phrase:
        return ""
    cleaned = re.sub(r"\s*\([^)]*\)", "", phrase)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip().lower()


def load_market_summary(event_ticker: str, phrases: Optional[List[str]] = None, series_ticker_override: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Load live market snapshot + historical same-series rates for an event."""
    try:
        from src.auto_collect.market_discovery import get_event_details
        from src.nba.kalshi_api import get_event_markets, get_event_vwaps, fetch_series_results
    except Exception as e:
        print(f"⚠️  Market summary unavailable: {e}")
        return None

    try:
        event = get_event_details(event_ticker, verbose=False)
    except Exception as e:
        print(f"⚠️  Failed to load event details for market summary: {e}")
        event = None

    if series_ticker_override:
        series_ticker = series_ticker_override
    elif event and getattr(event, 'series_ticker', None):
        series_ticker = event.series_ticker
    elif '-' in event_ticker:
        series_ticker = event_ticker.split('-', 1)[0]
    else:
        series_ticker = None

    if not series_ticker:
        return None

    try:
        market_infos = get_event_markets(event_ticker)
    except Exception as e:
        print(f"⚠️  Failed to load market prices: {e}")
        market_infos = []

    vwap_map: Dict[str, float] = {}
    if market_infos:
        try:
            vwap_map = get_event_vwaps([m.ticker for m in market_infos], event_ticker=event_ticker)
        except Exception as e:
            print(f"⚠️  Failed to load VWAPs: {e}")

    try:
        series_results = fetch_series_results(series_ticker, delay=0.08, verbose=True)
    except Exception as e:
        print(f"⚠️  Failed to load series results: {e}")
        series_results = {}

    phrase_stats: Dict[str, Dict[str, int]] = defaultdict(lambda: {'hits': 0, 'total': 0})
    overall_hits = 0
    overall_total = 0
    resolved_events = 0

    for _, event_results in series_results.items():
        event_has_resolution = False
        for phrase, result in event_results.items():
            result_text = str(result).strip().lower() if result is not None else ""
            if result_text not in ("yes", "no"):
                continue
            event_has_resolution = True
            norm = _normalize_phrase_key(phrase)
            phrase_stats[norm]['total'] += 1
            overall_total += 1
            if result_text == "yes":
                phrase_stats[norm]['hits'] += 1
                overall_hits += 1
        if event_has_resolution:
            resolved_events += 1

    phrase_order = [_normalize_phrase_key(p) for p in (phrases or [])]
    phrase_rank = {p: i for i, p in enumerate(phrase_order)}

    market_rows = []
    for market in market_infos:
        market.vwap = vwap_map.get(market.ticker)
        phrase = market.phrase
        norm = _normalize_phrase_key(phrase)
        stat = phrase_stats.get(norm, {'hits': 0, 'total': 0})
        total = stat['total']
        hits = stat['hits']
        yes_rate = (hits / total) if total > 0 else None
        market_rows.append({
            'ticker': market.ticker,
            'phrase': phrase,
            'yes_bid': market.yes_bid,
            'yes_ask': market.yes_ask,
            'last_price': market.last_price,
            'volume': market.volume,
            'vwap': market.vwap,
            'result': market.result,
            'series_hits': hits,
            'series_total': total,
            'series_yes_rate': yes_rate,
            'phrase_rank': phrase_rank.get(norm, 10_000),
        })

    market_rows.sort(key=lambda row: (row['phrase_rank'], row['phrase'].lower()))

    overall_yes_rate = (overall_hits / overall_total) if overall_total > 0 else None
    return {
        'event_ticker': event_ticker,
        'series_ticker': series_ticker,
        'series_event_count': len(series_results),
        'resolved_event_count': resolved_events,
        'overall_yes_rate': overall_yes_rate,
        'overall_hits': overall_hits,
        'overall_total': overall_total,
        'markets': market_rows,
    }


def _render_market_summary_html(summary: Optional[Dict[str, Any]]) -> str:
    if not summary or not summary.get('markets'):
        return ""

    rows = []
    for row in summary['markets']:
        yes_bid = row.get('yes_bid')
        yes_ask = row.get('yes_ask')
        bid_ask = f"{yes_bid}c / {yes_ask}c" if yes_bid is not None and yes_ask is not None else "-"
        last_price = f"{row['last_price']}c" if row.get('last_price') is not None else "-"
        vwap = f"{row['vwap']:.0f}c" if row.get('vwap') is not None else "-"
        volume = f"{int(row['volume']):,}" if row.get('volume') is not None else "-"
        result = str(row.get('result') or "-").upper()
        hits = row.get('series_hits', 0)
        total = row.get('series_total', 0)
        rate = row.get('series_yes_rate')
        rate_text = f"{rate:.0%} ({hits}/{total})" if rate is not None else "-"

        rows.append(
            "<tr>"
            f"<td title=\"{html.escape(row.get('ticker', ''))}\">{html.escape(row.get('phrase', ''))}</td>"
            f"<td>{rate_text}</td>"
            f"<td>{bid_ask}</td>"
            f"<td>{last_price}</td>"
            f"<td>{vwap}</td>"
            f"<td>{volume}</td>"
            f"<td>{result}</td>"
            "</tr>"
        )

    series_label = html.escape(summary.get('series_ticker', '-'))
    return f"""
    <h2>📉 Kalshi Market Snapshot ({series_label})</h2>
    <div class="card freq-table">
        <table>
            <tr>
                <th>Phrase</th>
                <th>Series YES Rate</th>
                <th>YES Bid/Ask</th>
                <th>Last</th>
                <th>VWAP</th>
                <th>Volume</th>
                <th>Result</th>
            </tr>
            {''.join(rows)}
        </table>
    </div>
"""


def _filter_df_for_venue(df: pd.DataFrame, venue: str) -> pd.DataFrame:
    """Return subset rows whose filename/text indicate the venue."""
    if df is None or df.empty or not venue:
        return pd.DataFrame()

    venue_l = venue.lower().strip()
    if not venue_l:
        return pd.DataFrame()

    ignore = {'the', 'with', 'show', 'interview', 'news', 'during', 'live', 'on', 'at'}
    tokens = [t for t in re.findall(r'[a-z0-9]+', venue_l) if t not in ignore]
    has_short_token = any(len(t) <= 2 for t in tokens)
    min_token_hits = min(2, len(tokens)) if tokens else 0

    # Strong filename matcher: venue tokens must appear in sequence with separators.
    venue_seq_regex = None
    if tokens:
        sep = r'\s+'
        venue_seq_regex = re.compile(r'\b' + sep.join(re.escape(t) for t in tokens) + r'\b')

    def row_matches(row: pd.Series) -> bool:
        file_text = str(row.get('file', '')).lower()
        body_text = str(row.get('text', '')).lower()
        normalized_file = re.sub(r'[_\-/|:]+', ' ', file_text)

        # Prefer filename/source-style signals (most stable for venue).
        if venue_l in file_text:
            return True
        if venue_seq_regex and venue_seq_regex.search(normalized_file):
            return True

        # If the venue includes very short tokens (e.g., "MS NOW"), don't use
        # loose transcript-text token matching; it creates many false positives.
        if has_short_token:
            return False

        haystack = f"{file_text} {body_text}"
        if venue_l in haystack:
            return True
        if min_token_hits == 0:
            return False
        hits = sum(1 for token in tokens if token in haystack)
        return hits >= min_token_hits

    mask = df.apply(row_matches, axis=1)
    return df[mask].copy()


def infer_venue_from_event(event_ticker: Optional[str]) -> Optional[str]:
    """Infer venue from event title when possible."""
    if not event_ticker:
        return None
    try:
        from src.auto_collect.market_discovery import get_event_details
        from src.auto_collect.collector import parse_event_context
    except Exception:
        return None

    try:
        event = get_event_details(event_ticker, verbose=False)
    except Exception:
        return None
    if not event or not getattr(event, 'title', None):
        return None
    try:
        context = parse_event_context(event.title, event_ticker)
    except Exception:
        return None
    return (context.venue or "").strip() or None


def load_transcripts_as_df(
    directory: str,
    patterns: Dict[str, str],
    file_filter: Optional[str] = None,
) -> pd.DataFrame:
    """Load transcripts using phrase_analysis.process_directory."""
    if not os.path.isdir(directory):
        return pd.DataFrame()
    
    df = process_directory(directory, patterns)
    
    if df.empty:
        return df
    
    # Apply filter if specified
    if file_filter:
        df = df[df['file'].str.contains(file_filter, case=False, na=False, regex=True)]
    
    # Sort by date
    df = df.sort_values('date').reset_index(drop=True)
    
    return df


def create_phrase_chart(df: pd.DataFrame, phrase: str) -> str:
    """Create bar chart for a phrase over time, return as base64 PNG."""
    if phrase not in df.columns:
        return ""
    
    # Sort by date
    df_sorted = df.sort_values('date').reset_index(drop=True)
    
    counts = df_sorted[phrase].values
    dates = df_sorted['date'].values
    filenames = df_sorted['file'].values
    word_counts = df_sorted['text_length'].values
    
    avg = counts.mean() if len(counts) > 0 else 0
    appearances_with = (counts > 0).sum()
    total = len(counts)
    pct = (appearances_with / total * 100) if total > 0 else 0
    
    fig, ax1 = plt.subplots(figsize=(14, 6))
    
    xvals = np.arange(len(df_sorted))
    bars = ax1.bar(xvals, counts, alpha=0.7, color='#3498db', width=0.8)
    
    # Average line
    ax1.axhline(y=avg, color='#e74c3c', linestyle='--', alpha=0.7,
                label=f'Average ({avg:.1f})')
    
    ax1.set_ylabel('Count', color='#3498db')
    ax1.tick_params(axis='y', labelcolor='#3498db')
    ax1.set_title(f'"{phrase}" - {pct:.0f}% of files ({appearances_with}/{total})')
    ax1.set_xlabel('File (chronological order)')
    ax1.grid(True, alpha=0.3)
    
    # Word count on secondary axis
    ax2 = ax1.twinx()
    ax2.plot(xvals, word_counts, 'o-', color='#95a5a6', alpha=0.4, markersize=3, label='Words')
    ax2.set_ylabel('Word Count', color='#95a5a6')
    ax2.tick_params(axis='y', labelcolor='#95a5a6')
    
    # X-axis labels - show dates and filenames
    # Show more labels if fewer files, fewer labels if many files
    if len(xvals) <= 20:
        step = max(1, len(xvals) // 12)
    else:
        step = max(1, len(xvals) // 15)
    
    ax1.set_xticks(xvals[::step])
    
    def format_label(date, filename):
        """Format label with date and shortened filename."""
        try:
            date_str = pd.Timestamp(date).strftime('%Y-%m-%d')
        except:
            date_str = 'N/A'
        
        # Shorten filename: take first 25 chars, remove extension
        if filename:
            short_name = Path(filename).stem[:25]
            if len(Path(filename).stem) > 25:
                short_name += '...'
        else:
            short_name = 'N/A'
        
        return f"{date_str}\n{short_name}"
    
    labels = [format_label(dates[i], filenames[i]) for i in range(0, len(dates), step)]
    ax1.set_xticklabels(labels, rotation=45, ha='right', fontsize=7)
    
    ax1.legend(loc='upper left')
    plt.tight_layout()
    
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    buf.seek(0)
    img_data = base64.b64encode(buf.read()).decode()
    plt.close()
    
    return img_data


def create_summary_chart(df: pd.DataFrame, phrases: List[str]) -> str:
    """Create horizontal bar chart of phrase totals."""
    totals = []
    for p in phrases:
        if p in df.columns:
            totals.append((p, df[p].sum()))
        else:
            totals.append((p, 0))
    
    # Sort by count
    totals.sort(key=lambda x: x[1])
    names = [t[0] for t in totals]
    counts = [t[1] for t in totals]
    
    fig, ax = plt.subplots(figsize=(10, max(4, len(names) * 0.35)))
    
    y_pos = np.arange(len(names))
    bars = ax.barh(y_pos, counts, color='#3498db', alpha=0.8)
    
    ax.set_yticks(y_pos)
    ax.set_yticklabels(names, fontsize=9)
    ax.set_xlabel('Total Count')
    ax.set_title('Total Phrase Counts')
    ax.grid(True, alpha=0.3, axis='x')
    
    # Add count labels
    max_count = max(counts) if counts else 1
    for bar, count in zip(bars, counts):
        if count > 0:
            ax.text(bar.get_width() + max_count * 0.01, bar.get_y() + bar.get_height()/2,
                    str(int(count)), va='center', fontsize=8)
    
    plt.tight_layout()
    
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    buf.seek(0)
    img_data = base64.b64encode(buf.read()).decode()
    plt.close()
    
    return img_data


def get_recent_contexts(df: pd.DataFrame, phrase: str, pattern: str, n: int = 8) -> List[Dict]:
    """Get recent usage contexts for a phrase."""
    contexts = []
    
    if phrase not in df.columns:
        return contexts
    
    # Sort by date descending, only look at files with matches
    df_with = df[df[phrase] > 0].sort_values('date', ascending=False)
    
    for _, row in df_with.head(20).iterrows():
        matches = find_phrase_context(row['text'], pattern, window=150)
        for match in matches[:2]:  # Max 2 per file
            contexts.append({
                'file': row['file'],
                'date': row['date'],
                'context': match,
            })
        if len(contexts) >= n:
            break
    
    return contexts[:n]


def generate_html_report(
    df: pd.DataFrame,
    patterns: Dict[str, str],
    phrases: List[str],
    output_path: str,
    title: str = "Transcript Analysis",
    comparison_dfs: Optional[Dict[str, pd.DataFrame]] = None,
    market_summary: Optional[Dict[str, Any]] = None,
):
    """Generate comprehensive HTML report."""
    
    comparison_dfs = comparison_dfs or {}
    
    # Stats
    total_files = len(df)
    total_words = df['text_length'].sum() if 'text_length' in df.columns else 0
    avg_words = df['text_length'].mean() if 'text_length' in df.columns and len(df) > 0 else 0
    
    date_range = ""
    dates = df['date'].dropna()
    if len(dates) > 0:
        d1, d2 = dates.min(), dates.max()
        try:
            date_range = f"{pd.Timestamp(d1).strftime('%Y-%m-%d')} to {pd.Timestamp(d2).strftime('%Y-%m-%d')}"
        except:
            pass
    
    # Generate charts
    summary_chart = create_summary_chart(df, phrases)

    extra_stats_html = ""
    if market_summary:
        overall_yes_rate = market_summary.get('overall_yes_rate')
        overall_hits = market_summary.get('overall_hits', 0)
        overall_total = market_summary.get('overall_total', 0)
        resolved_events = market_summary.get('resolved_event_count', 0)
        total_events = market_summary.get('series_event_count', 0)
        rate_text = f"{overall_yes_rate:.1%}" if overall_yes_rate is not None else "-"
        extra_stats_html = f"""
        <div class="stat-box">
            <div class="value">{rate_text}</div>
            <div class="label">Series YES Rate ({overall_hits}/{overall_total})</div>
        </div>
        <div class="stat-box">
            <div class="value">{resolved_events}/{total_events}</div>
            <div class="label">Resolved Events (Series)</div>
        </div>
"""
    
    # Build frequency table across datasets
    dfs_for_freq = {'All': df}
    dfs_for_freq.update(comparison_dfs)
    
    freq_df = get_per_appearance_frequency(
        dfs_for_freq,
        {p: patterns[p] for p in phrases if p in patterns},
        show_confidence_interval=True,
        ci_method='wilson',
        return_df=True
    )
    
    # Start HTML
    html_parts = []
    
    html_parts.append(f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>
        :root {{
            --primary: #2563eb;
            --secondary: #475569;
            --accent: #f59e0b;
            --success: #10b981;
            --danger: #ef4444;
            --bg: #f8fafc;
            --card: #ffffff;
            --text: #1e293b;
            --muted: #64748b;
            --border: #e2e8f0;
        }}
        * {{ box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: var(--bg);
            color: var(--text);
            line-height: 1.6;
            margin: 0;
            padding: 20px 30px;
        }}
        .container {{ max-width: 1400px; margin: 0 auto; }}
        h1 {{
            color: var(--text);
            border-bottom: 3px solid var(--primary);
            padding-bottom: 12px;
            margin-bottom: 25px;
        }}
        h2 {{
            color: var(--secondary);
            margin-top: 35px;
            padding-bottom: 8px;
            border-bottom: 1px solid var(--border);
        }}
        .card {{
            background: var(--card);
            border-radius: 10px;
            padding: 20px;
            margin: 15px 0;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }}
        .stat-box {{
            background: linear-gradient(135deg, var(--primary), #1d4ed8);
            color: white;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
        }}
        .stat-box .value {{
            font-size: 2em;
            font-weight: 700;
        }}
        .stat-box .label {{
            opacity: 0.9;
            font-size: 0.85em;
        }}
        details {{
            background: var(--card);
            border: 1px solid var(--border);
            border-radius: 8px;
            margin: 10px 0;
        }}
        summary {{
            padding: 14px 18px;
            cursor: pointer;
            font-weight: 600;
            background: #f8fafc;
            border-radius: 8px 8px 0 0;
            list-style: none;
        }}
        summary::-webkit-details-marker {{ display: none; }}
        summary::before {{
            content: '▶ ';
            color: var(--accent);
        }}
        details[open] summary::before {{ content: '▼ '; }}
        details[open] summary {{
            border-bottom: 1px solid var(--border);
        }}
        .details-content {{ padding: 18px; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
            font-size: 0.9em;
        }}
        th, td {{
            padding: 10px 12px;
            text-align: left;
            border-bottom: 1px solid var(--border);
        }}
        th {{
            background: var(--primary);
            color: white;
            font-weight: 600;
            position: sticky;
            top: 0;
        }}
        tr:hover {{ background: #f8fafc; }}
        .context-box {{
            background: #f8fafc;
            border-left: 4px solid var(--accent);
            padding: 12px 15px;
            margin: 10px 0;
            font-size: 0.9em;
            border-radius: 0 6px 6px 0;
        }}
        .context-box .source {{
            color: var(--muted);
            font-size: 0.8em;
            margin-bottom: 6px;
        }}
        .highlight {{
            background: #fef3c7;
            padding: 1px 3px;
            border-radius: 2px;
            font-weight: 600;
        }}
        .chart-img {{
            max-width: 100%;
            height: auto;
            border-radius: 6px;
        }}
        .generated {{
            color: var(--muted);
            font-size: 0.8em;
            text-align: right;
            margin-top: 30px;
            padding-top: 15px;
            border-top: 1px solid var(--border);
        }}
        .freq-table {{ overflow-x: auto; }}
        .freq-table table th {{ white-space: nowrap; }}
    </style>
</head>
<body>
<div class="container">
    <h1>📊 {title}</h1>
    
    <div class="stats-grid">
        <div class="stat-box">
            <div class="value">{total_files}</div>
            <div class="label">Total Files</div>
        </div>
        <div class="stat-box">
            <div class="value">{total_words:,}</div>
            <div class="label">Total Words</div>
        </div>
        <div class="stat-box">
            <div class="value">{avg_words:,.0f}</div>
            <div class="label">Avg Words/File</div>
        </div>
        <div class="stat-box">
            <div class="value" style="font-size:1.2em">{date_range or '-'}</div>
            <div class="label">Date Range</div>
        </div>
        {extra_stats_html}
    </div>
""")

    market_summary_html = _render_market_summary_html(market_summary)
    if market_summary_html:
        html_parts.append(market_summary_html)
    
    # Frequency table
    if freq_df is not None and not freq_df.empty:
        html_parts.append("""
    <h2>📋 Phrase Frequency (% with 1+ appearances)</h2>
    <div class="card freq-table">
""")
        html_parts.append(freq_df.to_html(classes='', escape=False))
        html_parts.append("</div>")
    
    # Summary chart
    html_parts.append(f"""
    <h2>📊 Total Counts</h2>
    <div class="card">
        <img src="data:image/png;base64,{summary_chart}" class="chart-img" alt="Summary Chart">
    </div>
""")
    
    # Individual phrase charts
    html_parts.append("""
    <h2>📈 Phrase Trends Over Time</h2>
    <p style="color: var(--muted); font-size: 0.9em;">Click each phrase to see count per file (chronological order)</p>
""")
    
    for phrase in phrases:
        if phrase not in df.columns:
            continue
        
        count = (df[phrase] > 0).sum()
        total = len(df)
        pct = count / total * 100 if total > 0 else 0
        total_mentions = df[phrase].sum()
        
        try:
            chart_img = create_phrase_chart(df, phrase)
        except Exception as e:
            print(f"Warning: Could not create chart for {phrase}: {e}")
            chart_img = ""
        
        html_parts.append(f"""
    <details>
        <summary>📊 {phrase} - {pct:.0f}% ({count}/{total} files, {int(total_mentions)} total)</summary>
        <div class="details-content">
""")
        if chart_img:
            html_parts.append(f'<img src="data:image/png;base64,{chart_img}" class="chart-img" alt="{phrase} chart">')
        else:
            html_parts.append("<p>Could not generate chart</p>")
        html_parts.append("</div></details>")
    
    # Recent contexts
    html_parts.append("""
    <h2>📝 Recent Contexts</h2>
    <p style="color: var(--muted); font-size: 0.9em;">Click each phrase to see recent usage examples</p>
""")
    
    for phrase in phrases:
        if phrase not in patterns:
            continue
        
        pattern = patterns[phrase]
        contexts = get_recent_contexts(df, phrase, pattern, n=8)
        
        count = (df[phrase] > 0).sum() if phrase in df.columns else 0
        total = len(df)
        pct = count / total * 100 if total > 0 else 0
        
        if contexts:
            html_parts.append(f"""
    <details>
        <summary>💬 {phrase} - {len(contexts)} recent context(s)</summary>
        <div class="details-content">
""")
            for ctx in contexts:
                try:
                    date_str = pd.Timestamp(ctx['date']).strftime('%Y-%m-%d')
                except:
                    date_str = 'Unknown'
                
                # Highlight the match in context
                context_html = ctx['context']
                
                html_parts.append(f"""
            <div class="context-box">
                <div class="source">{ctx['file']} ({date_str})</div>
                <div>{context_html}</div>
            </div>
""")
            html_parts.append("</div></details>")
        else:
            html_parts.append(f"""
    <details>
        <summary style="color: var(--muted);">💬 {phrase} - No recent contexts</summary>
        <div class="details-content">
            <p>No occurrences found in data.</p>
        </div>
    </details>
""")
    
    # Files table
    html_parts.append("""
    <h2>📁 Files</h2>
    <div class="card" style="max-height: 500px; overflow-y: auto;">
        <table>
            <tr><th>Date</th><th>File</th><th>Words</th><th>Total Matches</th></tr>
""")
    
    for _, row in df.sort_values('date', ascending=False).head(50).iterrows():
        try:
            date_str = pd.Timestamp(row['date']).strftime('%Y-%m-%d')
        except:
            date_str = '-'
        
        total_matches = sum(row.get(p, 0) for p in phrases if p in row.index)
        
        html_parts.append(f"""
            <tr>
                <td>{date_str}</td>
                <td title="{row['file']}">{row['file'][:60]}{'...' if len(row['file']) > 60 else ''}</td>
                <td>{row.get('text_length', 0):,}</td>
                <td>{int(total_matches)}</td>
            </tr>
""")
    
    html_parts.append("""
        </table>
    </div>
""")
    
    # Footer
    html_parts.append(f"""
    <div class="generated">
        Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    </div>
</div>
</body>
</html>
""")
    
    # Write file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(''.join(html_parts))
    
    abs_path = os.path.abspath(output_path)
    file_url = Path(abs_path).as_uri()
    print(f"✅ Report saved: {output_path}")
    print(f"   Open in browser: {file_url}")


def generate_report(
    directory: str,
    event_ticker: Optional[str] = None,
    phrases: Optional[List[str]] = None,
    venue: Optional[str] = None,
    file_filter: Optional[str] = None,
    output_path: Optional[str] = None,
    title: Optional[str] = None,
    compare_dirs: Optional[List[str]] = None,
    open_browser: bool = False,
    series_ticker: Optional[str] = None,
) -> Tuple[int, Optional[str]]:
    dir_exists = os.path.isdir(directory)
    if not dir_exists:
        if not event_ticker:
            print(f"❌ Directory not found: {directory}")
            return 1, None
        print(f"⚠️  Directory not found: {directory} — will use Kalshi series data only")

    if event_ticker:
        print(f"📋 Loading phrases from: {event_ticker}")
        phrases = load_phrases_from_event(event_ticker)
    elif phrases:
        phrases = [p.strip() for p in phrases if p and p.strip()]
    else:
        print("❌ Specify event_ticker or phrases")
        return 1, None

    if not phrases:
        print("❌ No phrases found")
        return 1, None

    print(f"   {len(phrases)} phrases")
    patterns = generate_regex_patterns(phrases)

    # Check if directory has raw/processed subfolders (if it's a base directory)
    # But respect if user explicitly passes processed/ or raw/
    is_base_dir = False
    processed_dir = os.path.join(directory, 'processed')
    raw_dir = os.path.join(directory, 'raw')

    has_processed = os.path.isdir(processed_dir)
    has_raw = os.path.isdir(raw_dir)
    dir_name = os.path.basename(directory.rstrip('/'))
    is_explicit_subfolder = dir_name in ('processed', 'raw')

    if is_explicit_subfolder:
        main_dir = directory
        print(f"📂 Loading from: {directory}")
        is_base_dir = False
    elif has_processed:
        main_dir = processed_dir
        print(f"📂 Loading from: {processed_dir} (processed)")
        is_base_dir = True
    elif has_raw:
        main_dir = raw_dir
        print(f"📂 Loading from: {raw_dir} (raw)")
        is_base_dir = False
    else:
        main_dir = directory
        print(f"📂 Loading from: {directory}")
        is_base_dir = False

    df = load_transcripts_as_df(main_dir, patterns, file_filter)
    if df.empty:
        print("⚠️  No transcript files found — generating market-only report")
        # Build an empty DataFrame with the expected columns so the report renders
        cols = ['file', 'date', 'text', 'text_length'] + list(patterns.keys())
        df = pd.DataFrame(columns=cols)
        for col in ['text_length'] + list(patterns.keys()):
            df[col] = df[col].astype(float)
    else:
        print(f"   {len(df)} files")
    if file_filter:
        print(f"   (filter: {file_filter})")

    comparison_dfs: Dict[str, pd.DataFrame] = {}

    if is_base_dir and main_dir == processed_dir and has_raw:
        raw_df = load_transcripts_as_df(raw_dir, patterns)
        if not raw_df.empty:
            comparison_dfs['Raw'] = raw_df
            print(f"   + Raw: {len(raw_df)} files")

    for comp_dir in compare_dirs or []:
        if os.path.isdir(comp_dir):
            comp_name = os.path.basename(comp_dir.rstrip('/'))
            comp_df = load_transcripts_as_df(comp_dir, patterns)
            if not comp_df.empty:
                comparison_dfs[comp_name] = comp_df
                print(f"   + {comp_name}: {len(comp_df)} files")

    effective_venue = (venue or "").strip() or infer_venue_from_event(event_ticker)
    if effective_venue:
        venue_df = _filter_df_for_venue(df, effective_venue)
        if not venue_df.empty:
            comparison_dfs[f"Venue ({effective_venue})"] = venue_df
            print(f"   + Venue ({effective_venue}): {len(venue_df)} files")
        else:
            print(f"   + Venue ({effective_venue}): 0 files")

    if output_path is None:
        out_dir_name = os.path.basename(directory.rstrip('/'))
        date_prefix = datetime.now().strftime('%Y%m%d')
        output_path = f"data/mentions/{date_prefix}_{out_dir_name}_report.html"

    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
    report_title = title or f"Analysis: {os.path.basename(directory.rstrip('/'))}"
    market_summary = load_market_summary(event_ticker, phrases, series_ticker_override=series_ticker) if event_ticker else None

    print("🔍 Generating report...")
    generate_html_report(
        df,
        patterns,
        phrases,
        output_path,
        report_title,
        comparison_dfs,
        market_summary=market_summary,
    )

    if open_browser:
        abs_path = os.path.abspath(output_path)
        webbrowser.open(Path(abs_path).as_uri())

    return 0, output_path


def main():
    parser = argparse.ArgumentParser(
        prog='python -m src.auto_collect.report',
        description='Generate transcript analysis HTML reports',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # With Kalshi event phrases
  python -m src.auto_collect.report data/bernie/processed-transcripts/ -e KXBERNIEMENTION-26JAN20

  # Filter files by regex
  python -m src.auto_collect.report data/bernie/processed-transcripts/ -e KXBERNIE... --filter "town.?hall"

  # Custom phrases
  python -m src.auto_collect.report data/mentions/will_smith/ --phrases "Oscar,slap,Chris Rock"

  # Compare with raw data
  python -m src.auto_collect.report data/bernie/processed-transcripts/ -e KXBERNIE... --compare data/bernie/raw/

  # Custom output
  python -m src.auto_collect.report data/ -e KXBERNIE... -o reports/my_report.html --open

  # Override historical series (phrases from event, history from series)
  python -m src.auto_collect.report data/bernie -e KXMENTION-BERN26MAR10 -s KXBERNIEMENTION-26MAR05
        """
    )

    parser.add_argument('directory', help='Directory with transcript files')

    phrase_group = parser.add_mutually_exclusive_group()
    phrase_group.add_argument('-e', '--event', help='Kalshi event ticker')
    phrase_group.add_argument('-p', '--phrases', help='Comma-separated phrases')

    parser.add_argument('-f', '--filter', help='Regex filter for filenames')
    parser.add_argument('-o', '--output', help='Output file path')
    parser.add_argument('-t', '--title', help='Report title')
    parser.add_argument('-s', '--series', help='Override historical Kalshi series ticker (e.g., KXBERNIEMENTION-26MAR05)')
    parser.add_argument('--venue', help='Venue filter hint for an extra frequency column (e.g., "MS NOW")')
    parser.add_argument('--compare', action='append', help='Additional directories to compare (can use multiple times)')
    parser.add_argument('--open', action='store_true', help='Open report in browser')

    args = parser.parse_args()

    parsed_phrases = [p.strip() for p in args.phrases.split(',')] if args.phrases else None
    status, _ = generate_report(
        directory=args.directory,
        event_ticker=args.event,
        phrases=parsed_phrases,
        venue=args.venue,
        file_filter=args.filter,
        output_path=args.output,
        title=args.title,
        compare_dirs=args.compare,
        open_browser=args.open,
        series_ticker=args.series,
    )
    return status


if __name__ == '__main__':
    sys.exit(main() or 0)
