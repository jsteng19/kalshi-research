#!/usr/bin/env python3
"""
NFL Phrase Analysis Report Generator

Generates interactive HTML reports for NFL broadcast phrase analysis.

Usage:
    # Basic usage with market ticker and announcer crew
    python src/nfl/nfl_report_generator.py KXNFLMENTION-25DEC28PHIBUF --crew kevin-kugler

    # With color commentator filter
    python src/nfl/nfl_report_generator.py KXNFLMENTION-25DEC28PHIBUF --crew kevin-kugler --color-commentator "Daryl Johnston"
    
    # Output to specific file
    python src/nfl/nfl_report_generator.py KXNFLMENTION-25DEC28PHIBUF --crew snf -o report.html
    
    # List available crews
    python src/nfl/nfl_report_generator.py --list-crews
"""

import os
import sys
import argparse
import re
import json
import base64
import io
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.models.phrase_analysis import (
    process_directory, count_phrases, get_date_from_filename,
    get_per_appearance_frequency, analyze_files_phrase_occurrences,
    find_phrase_context, calculate_confidence_interval
)
from src.utils.regex_pattern_generator import generate_regex_patterns
from src.nfl.nfl_config import (
    ANNOUNCER_CREWS, AnnouncerCrew, get_announcer_crew, get_all_announcer_names,
    TEAM_NAMES, get_team_name, STADIUMS, get_stadium, StadiumInfo,
    GRASS_TEAMS_PATTERN, TURF_TEAMS_PATTERN, INDOOR_TEAMS_PATTERN, 
    OUTDOOR_TEAMS_PATTERN, RETRACTABLE_TEAMS_PATTERN,
    parse_game_url, parse_filename, extract_teams_from_ticker, get_market_phrases
)


@dataclass
class ReportConfig:
    """Configuration for report generation."""
    event_ticker: str
    crew_name: str
    color_commentator_filter: Optional[str] = None
    output_path: Optional[str] = None
    include_comparison_crews: List[str] = None
    ci_method: str = 'wilson'
    multiple_testing_correction: str = 'bonferroni'
    family_confidence: float = 0.95
    min_announcer_mentions: float = 5.0
    away_team: Optional[str] = None
    home_team: Optional[str] = None
    

def get_phrases_for_report(event_ticker: str, announcer_names: List[str]) -> Tuple[List[str], Dict[str, str]]:
    """
    Get phrases for report including market phrases and announcer validation names.
    
    Returns:
        Tuple of (phrase_list, search_patterns_dict)
    """
    # Get market phrases
    try:
        market_phrases = get_market_phrases(event_ticker)
    except Exception as e:
        print(f"Warning: Could not fetch market phrases: {e}")
        market_phrases = []
    
    # Add announcer names for validation
    all_phrases = market_phrases + announcer_names + ['Grass', 'Turf']
    
    # Generate regex patterns
    search_patterns = generate_regex_patterns(all_phrases)
    
    return all_phrases, search_patterns


def load_transcript_data(crew: AnnouncerCrew, search_patterns: Dict[str, str],
                         data_base_path: str = 'data/football') -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load transcript data for an announcer crew.
    
    Returns:
        Tuple of (filtered_transcripts_df, diarized_df)
    """
    transcripts_path = os.path.join(data_base_path, crew.folder, 'transcripts')
    diarized_path = os.path.join(data_base_path, crew.folder, 'diarized')
    
    df = pd.DataFrame()
    df_raw = pd.DataFrame()
    
    if os.path.exists(transcripts_path):
        df = process_directory(transcripts_path, search_patterns)
    else:
        print(f"Warning: Transcripts path not found: {transcripts_path}")
    
    if os.path.exists(diarized_path):
        df_raw = process_directory(diarized_path, search_patterns)
    
    return df, df_raw


def filter_by_color_commentator(df: pd.DataFrame, crew: AnnouncerCrew, 
                                 color_commentator: str, 
                                 data_base_path: str = 'data/football') -> pd.DataFrame:
    """Filter transcript DataFrame by color commentator from CSV."""
    if not crew.csv_path or not os.path.exists(crew.csv_path):
        print(f"Warning: CSV path not found for filtering: {crew.csv_path}")
        return df
    
    try:
        csv_df = pd.read_csv(crew.csv_path)
        
        if 'color-commentator' not in csv_df.columns:
            print("Warning: 'color-commentator' column not found in CSV")
            return df
        
        # Filter CSV by color commentator
        mask = csv_df['color-commentator'].str.lower().str.contains(
            color_commentator.lower(), na=False
        )
        filtered_urls = csv_df[mask]['url'].tolist()
        
        # Match URLs to transcript filenames
        url_matchups = set()
        for url in filtered_urls:
            away, home, year = parse_game_url(str(url))
            if away and home and year:
                url_matchups.add((year, f"{away}-at-{home}"))
                url_matchups.add((year, f"{home}-at-{away}"))  # Handle reversed order
        
        def matches_url(filename):
            date_str, away, home, year = parse_filename(filename)
            if year and away and home:
                return (year, f"{away}-at-{home}") in url_matchups
            return False
        
        filtered_df = df[df['file'].apply(matches_url)].reset_index(drop=True)
        print(f"Filtered to {len(filtered_df)} games with {color_commentator}")
        return filtered_df
        
    except Exception as e:
        print(f"Warning: Error filtering by color commentator: {e}")
        return df


def validate_announcer_mentions(df: pd.DataFrame, crew: AnnouncerCrew,
                                 min_threshold: float = 5.0) -> Dict[str, Any]:
    """
    Validate that transcripts contain expected announcer mentions.
    
    Returns:
        Dictionary with validation results
    """
    announcer_names = get_all_announcer_names(crew)
    
    # Calculate total announcer mentions per game
    if not announcer_names:
        return {'valid': True, 'message': 'No announcer names configured', 'details': {}}
    
    # Sum all announcer name columns that exist
    existing_cols = [name for name in announcer_names if name in df.columns]
    if not existing_cols:
        return {'valid': False, 'message': 'No announcer columns found in data', 'details': {}}
    
    df_check = df.copy()
    df_check['_total_announcer_mentions'] = df_check[existing_cols].sum(axis=1)
    
    avg_mentions = df_check['_total_announcer_mentions'].mean()
    
    # Find low-mention games
    low_mention_games = df_check[df_check['_total_announcer_mentions'] < min_threshold]
    
    # Include individual announcer columns in low_mention_games records
    low_mention_cols = ['file', 'date'] + existing_cols + ['_total_announcer_mentions', 'text_length']
    low_mention_cols = [c for c in low_mention_cols if c in df_check.columns]
    
    validation_result = {
        'valid': avg_mentions >= min_threshold,
        'average_mentions': avg_mentions,
        'min_threshold': min_threshold,
        'low_mention_games': low_mention_games[low_mention_cols].to_dict('records') if len(low_mention_games) > 0 else [],
        'announcer_breakdown': {
            name: df[name].mean() for name in existing_cols
        }
    }
    
    return validation_result


def get_team_games(df: pd.DataFrame, team: str, home_only: bool = False, 
                   away_only: bool = False) -> pd.DataFrame:
    """Get games involving a specific team.
    
    Args:
        df: DataFrame with 'file' column containing filenames
        team: Team ticker (e.g., 'buf') or name (e.g., 'bills')
        home_only: Only get games where team is home
        away_only: Only get games where team is away
    """
    # Convert ticker to team name for matching (files use team names like 'bills')
    team_name = get_team_name(team).lower()
    
    if home_only:
        pattern = f'at-{team_name}'
    elif away_only:
        pattern = f'{team_name}-at'
    else:
        pattern = f'{team_name}'
    
    return df[df['file'].str.contains(pattern, case=False, na=False)].reset_index(drop=True)


def get_surface_games(df: pd.DataFrame, surface: str) -> pd.DataFrame:
    """Get games played on specific surface type."""
    if surface.lower() == 'grass':
        pattern = GRASS_TEAMS_PATTERN
    elif surface.lower() == 'turf':
        pattern = TURF_TEAMS_PATTERN
    else:
        return df
    
    return df[df['file'].str.contains(pattern, case=False, na=False)].reset_index(drop=True)


def get_venue_games(df: pd.DataFrame, venue_type: str) -> pd.DataFrame:
    """Get games played at specific venue type."""
    if venue_type.lower() == 'indoor':
        pattern = INDOOR_TEAMS_PATTERN
    elif venue_type.lower() == 'outdoor':
        pattern = OUTDOOR_TEAMS_PATTERN
    elif venue_type.lower() == 'retractable':
        pattern = RETRACTABLE_TEAMS_PATTERN
    else:
        return df
    
    return df[df['file'].str.contains(pattern, case=False, na=False)].reset_index(drop=True)


def create_length_histogram(df: pd.DataFrame, title: str = 'Transcript Length Distribution') -> str:
    """Create a transcript length histogram and return as base64 PNG."""
    fig, ax = plt.subplots(figsize=(10, 5))
    
    lengths = df['text_length'].values
    avg = lengths.mean() if len(lengths) > 0 else 0
    
    ax.hist(lengths, bins=20, alpha=0.7, color='#3498db', edgecolor='white')
    ax.axvline(x=avg, color='#e74c3c', linestyle='--', linewidth=2, 
               label=f'Mean: {avg:,.0f} words')
    
    ax.set_title(title)
    ax.set_xlabel('Word Count')
    ax.set_ylabel('Number of Games')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    buf.seek(0)
    img_data = base64.b64encode(buf.read()).decode()
    plt.close()
    
    return img_data


def create_phrase_chart(df: pd.DataFrame, phrase: str, title_suffix: str = '') -> str:
    """Create a phrase frequency chart and return as base64 PNG."""
    counts = df[phrase].values
    dates = df['date'].values
    filenames = df['file'].values
    
    avg = counts.mean() if len(counts) > 0 else 0
    appearances_with = (counts > 0).sum()
    total = len(counts)
    pct = (appearances_with / total * 100) if total > 0 else 0
    
    fig, ax = plt.subplots(figsize=(12, 5))
    
    # Sort by date
    sorted_indices = np.argsort(dates)
    counts_sorted = counts[sorted_indices]
    
    xvals = np.arange(len(counts_sorted))
    ax.bar(xvals, counts_sorted, alpha=0.7, color='#3498db', width=0.8)
    ax.axhline(y=avg, color='#e74c3c', linestyle='--', alpha=0.7, 
               label=f'Average ({avg:.1f})')
    
    ax.set_title(f'{phrase}{title_suffix} - {pct:.1f}% of games ({appearances_with}/{total})')
    ax.set_ylabel('Count')
    ax.set_xlabel('Game (chronological)')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # Reduce x-axis labels
    if len(xvals) > 15:
        step = len(xvals) // 10
        ax.set_xticks(xvals[::step])
    
    plt.tight_layout()
    
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    buf.seek(0)
    img_data = base64.b64encode(buf.read()).decode()
    plt.close()
    
    return img_data


def generate_html_report(config: ReportConfig) -> str:
    """Generate the complete HTML report."""
    
    # Get announcer crew
    crew = get_announcer_crew(config.crew_name)
    if not crew:
        raise ValueError(f"Unknown crew: {config.crew_name}. Use --list-crews to see available crews.")
    
    # Get phrases
    announcer_names = get_all_announcer_names(crew)
    phrases, search_patterns = get_phrases_for_report(config.event_ticker, announcer_names)
    
    # Extract teams from ticker or use manually specified teams
    if config.away_team and config.home_team:
        # Use manually specified teams from CSV
        away_team = config.away_team.lower()
        home_team = config.home_team.lower()
    else:
        # Fall back to extracting from ticker
        away_team, home_team = extract_teams_from_ticker(config.event_ticker)
    
    away_name = get_team_name(away_team) if away_team else 'Away'
    home_name = get_team_name(home_team) if home_team else 'Home'
    
    # Get stadium info for home team
    home_stadium = get_stadium(home_team) if home_team else None
    
    # Load data
    df, df_raw = load_transcript_data(crew, search_patterns)
    
    if df.empty:
        raise ValueError(f"No transcript data found for crew: {config.crew_name}")
    
    # Store full dataset before filtering
    df_full = df.copy()
    
    # Apply color commentator filter if specified
    if config.color_commentator_filter:
        df = filter_by_color_commentator(df, crew, config.color_commentator_filter)
    
    # Validate announcer mentions
    validation = validate_announcer_mentions(df, crew, config.min_announcer_mentions)
    
    # Load comparison datasets (only if explicitly requested)
    comparison_dfs = {}
    if config.include_comparison_crews:
        for crew_name in config.include_comparison_crews:
            if crew_name != config.crew_name:
                comp_crew = get_announcer_crew(crew_name)
                if comp_crew:
                    comp_df, _ = load_transcript_data(comp_crew, search_patterns)
                    if not comp_df.empty:
                        comparison_dfs[crew_name.upper()] = comp_df
    
    # Create combined DataFrame from all comparison datasets
    all_dfs_for_combined = [df_full] + list(comparison_dfs.values())
    df_combined = pd.concat(all_dfs_for_combined, ignore_index=True) if all_dfs_for_combined else pd.DataFrame()
    
    # Get team-specific data with position filtering
    # For filtered dataset (df)
    df_away_any = get_team_games(df, away_team) if away_team else pd.DataFrame()
    df_away_away = get_team_games(df, away_team, away_only=True) if away_team else pd.DataFrame()
    df_home_any = get_team_games(df, home_team) if home_team else pd.DataFrame()
    df_home_home = get_team_games(df, home_team, home_only=True) if home_team else pd.DataFrame()
    
    # For full dataset (df_full)
    df_full_away_any = get_team_games(df_full, away_team) if away_team else pd.DataFrame()
    df_full_away_away = get_team_games(df_full, away_team, away_only=True) if away_team else pd.DataFrame()
    df_full_home_any = get_team_games(df_full, home_team) if home_team else pd.DataFrame()
    df_full_home_home = get_team_games(df_full, home_team, home_only=True) if home_team else pd.DataFrame()
    
    # For combined dataset
    df_combined_away_any = get_team_games(df_combined, away_team) if away_team else pd.DataFrame()
    df_combined_away_away = get_team_games(df_combined, away_team, away_only=True) if away_team else pd.DataFrame()
    df_combined_home_any = get_team_games(df_combined, home_team) if home_team else pd.DataFrame()
    df_combined_home_home = get_team_games(df_combined, home_team, home_only=True) if home_team else pd.DataFrame()
    
    # Legacy for backward compatibility
    df_teams = pd.DataFrame()
    if away_team or home_team:
        team_pattern = f'{away_team}|{home_team}' if away_team and home_team else (away_team or home_team)
        df_teams = df[df['file'].str.contains(team_pattern, case=False, na=False)]
    
    # Surface and venue analysis
    df_grass = get_surface_games(df, 'grass')
    df_turf = get_surface_games(df, 'turf')
    df_indoor = get_venue_games(df, 'indoor')
    df_outdoor = get_venue_games(df, 'outdoor')
    df_retractable = get_venue_games(df, 'retractable')
    
    # Build HTML
    html_parts = []
    
    # Header
    html_parts.append(f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NFL Phrase Analysis: {config.event_ticker}</title>
    <style>
        :root {{
            --primary: #1a365d;
            --secondary: #2c5282;
            --accent: #ed8936;
            --success: #38a169;
            --warning: #d69e2e;
            --danger: #e53e3e;
            --bg: #f7fafc;
            --card-bg: #ffffff;
            --text: #2d3748;
            --text-muted: #718096;
            --border: #e2e8f0;
        }}
        
        * {{ box-sizing: border-box; }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: var(--bg);
            color: var(--text);
            line-height: 1.6;
            margin: 0;
            padding: 20px;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        
        h1 {{
            color: var(--primary);
            border-bottom: 3px solid var(--accent);
            padding-bottom: 10px;
        }}
        
        h2 {{
            color: var(--secondary);
            margin-top: 30px;
        }}
        
        .card {{
            background: var(--card-bg);
            border-radius: 8px;
            padding: 20px;
            margin: 15px 0;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }}
        
        .stat-box {{
            background: linear-gradient(135deg, var(--primary), var(--secondary));
            color: white;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
        }}
        
        .stat-box .value {{
            font-size: 2em;
            font-weight: bold;
        }}
        
        .stat-box .label {{
            opacity: 0.9;
            font-size: 0.9em;
        }}
        
        details {{
            background: var(--card-bg);
            border: 1px solid var(--border);
            border-radius: 8px;
            margin: 10px 0;
        }}
        
        summary {{
            padding: 15px 20px;
            cursor: pointer;
            font-weight: 600;
            background: #f8f9fa;
            border-radius: 8px 8px 0 0;
            list-style: none;
        }}
        
        summary::-webkit-details-marker {{
            display: none;
        }}
        
        summary::before {{
            content: '▶ ';
            color: var(--accent);
        }}
        
        details[open] summary::before {{
            content: '▼ ';
        }}
        
        details[open] summary {{
            border-bottom: 1px solid var(--border);
        }}
        
        .details-content {{
            padding: 20px;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
            font-size: 0.9em;
        }}
        
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid var(--border);
        }}
        
        th {{
            background: var(--primary);
            color: white;
            position: sticky;
            top: 0;
        }}
        
        tr:hover {{
            background: #f7fafc;
        }}
        
        .badge {{
            display: inline-block;
            padding: 4px 10px;
            border-radius: 20px;
            font-size: 0.8em;
            font-weight: 600;
        }}
        
        .badge-success {{ background: #c6f6d5; color: #22543d; }}
        .badge-warning {{ background: #fefcbf; color: #744210; }}
        .badge-danger {{ background: #fed7d7; color: #742a2a; }}
        .badge-info {{ background: #bee3f8; color: #2a4365; }}
        
        .validation-ok {{ color: var(--success); }}
        .validation-warn {{ color: var(--warning); }}
        .validation-fail {{ color: var(--danger); }}
        
        .chart-container {{
            text-align: center;
            margin: 15px 0;
        }}
        
        .chart-container img {{
            max-width: 100%;
            height: auto;
            border-radius: 4px;
        }}
        
        .context-box {{
            background: #f8f9fa;
            border-left: 4px solid var(--accent);
            padding: 15px;
            margin: 10px 0;
            font-size: 0.9em;
        }}
        
        .context-box .source {{
            color: var(--text-muted);
            font-size: 0.85em;
            margin-bottom: 8px;
        }}
        
        .highlight {{
            background: #fef3c7;
            padding: 2px 4px;
            border-radius: 2px;
        }}
        
        .two-col {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
        }}
        
        @media (max-width: 768px) {{
            .two-col {{ grid-template-columns: 1fr; }}
            .stats-grid {{ grid-template-columns: 1fr 1fr; }}
        }}
        
        .generated-time {{
            color: var(--text-muted);
            font-size: 0.85em;
            text-align: right;
            margin-top: 30px;
        }}
        
        .notes-input {{
            width: 120px;
            padding: 4px 6px;
            border: 1px solid var(--border);
            border-radius: 4px;
            font-size: 0.85em;
            background: #fafafa;
        }}
        
        .notes-input:focus {{
            outline: none;
            border-color: var(--accent);
            background: white;
        }}
        
        .notes-buttons {{
            margin: 10px 0;
            display: flex;
            gap: 10px;
            align-items: center;
        }}
        
        .notes-btn {{
            padding: 6px 12px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.85em;
            font-weight: 500;
        }}
        
        .notes-btn-save {{
            background: var(--success);
            color: white;
        }}
        
        .notes-btn-export {{
            background: var(--secondary);
            color: white;
        }}
        
        .notes-btn-import {{
            background: var(--text-muted);
            color: white;
        }}
        
        .notes-status {{
            font-size: 0.85em;
            color: var(--text-muted);
            margin-left: 10px;
        }}
    </style>
</head>
<body>
<div class="container">
    <h1>🏈 NFL Phrase Analysis Report</h1>
    
    <div class="card">
        <h3>Report Configuration</h3>
        <p><strong>Event Ticker:</strong> {config.event_ticker}</p>
        <p><strong>Announcer Crew:</strong> {crew.name}</p>
        <p><strong>Matchup:</strong> {away_name} @ {home_name}</p>
""")
    
    if home_stadium:
        html_parts.append(f"""
        <p><strong>Stadium:</strong> {home_stadium.stadium_name} ({home_stadium.city}, {home_stadium.state})</p>
        <p><strong>Surface:</strong> <span class="badge badge-info">{home_stadium.surface.title()}</span> 
           <strong>Roof:</strong> <span class="badge badge-info">{home_stadium.roof_type.title()}</span></p>
""")
    
    if config.color_commentator_filter:
        html_parts.append(f"""
        <p><strong>Color Commentator Filter:</strong> {config.color_commentator_filter}</p>
""")
    
    html_parts.append("""
    </div>
""")
    
    # Data validation section
    validation_class = 'validation-ok' if validation['valid'] else 'validation-warn'
    validation_icon = '✅' if validation['valid'] else '⚠️'
    
    html_parts.append(f"""
    <h2>📊 Data Validation</h2>
    <div class="card">
        <p class="{validation_class}"><strong>{validation_icon} Announcer Validation:</strong> 
           Average {validation['average_mentions']:.1f} mentions per game (threshold: {validation['min_threshold']})</p>
""")
    
    # Detailed announcer name table
    if validation.get('announcer_breakdown'):
        html_parts.append("""
        <h4>Announcer Name Mentions (Average per Game)</h4>
        <table>
            <tr><th>Name</th><th>Avg Mentions</th><th>Role</th></tr>
""")
        for name, avg in validation['announcer_breakdown'].items():
            role = 'Play-by-Play' if name in crew.play_by_play else ('Color' if name in crew.color else 'Other')
            html_parts.append(f"""
            <tr>
                <td>{name}</td>
                <td>{avg:.1f}</td>
                <td><span class="badge badge-info">{role}</span></td>
            </tr>
""")
        html_parts.append("</table>")
    
    if validation.get('low_mention_games'):
        # Build header row with individual announcer name columns
        announcer_cols = list(validation.get('announcer_breakdown', {}).keys())
        header_row = "<tr><th>File</th><th>Date</th>"
        for name in announcer_cols:
            header_row += f"<th>{name}</th>"
        header_row += "<th>Total</th><th>Word Count</th></tr>"
        
        html_parts.append(f"""
        <details>
            <summary>⚠️ Low-mention games ({len(validation['low_mention_games'])} games)</summary>
            <div class="details-content">
                <div style="overflow-x: auto;">
                <table>
                    {header_row}
""")
        for game in validation['low_mention_games'][:15]:
            html_parts.append(f"""
                    <tr>
                        <td>{game.get('file', 'N/A')}</td>
                        <td>{game.get('date', 'N/A')}</td>
""")
            for name in announcer_cols:
                val = game.get(name, 0)
                html_parts.append(f"                        <td>{val}</td>\n")
            html_parts.append(f"""
                        <td><strong>{game.get('_total_announcer_mentions', 0):.0f}</strong></td>
                        <td>{game.get('text_length', 0):,}</td>
                    </tr>
""")
        html_parts.append("</table></div></div></details>")
    
    html_parts.append("</div>")
    
    # Summary statistics
    html_parts.append(f"""
    <h2>📈 Summary Statistics</h2>
    <div class="stats-grid">
        <div class="stat-box">
            <div class="value">{len(df)}</div>
            <div class="label">Total Games{' (filtered)' if config.color_commentator_filter else ''}</div>
        </div>
        <div class="stat-box">
            <div class="value">{len(df_full)}</div>
            <div class="label">Total Games (Full)</div>
        </div>
        <div class="stat-box">
            <div class="value">{df['text_length'].mean():,.0f}</div>
            <div class="label">Avg Word Count</div>
        </div>
        <div class="stat-box">
            <div class="value">{len(phrases)}</div>
            <div class="label">Phrases Tracked</div>
        </div>
    </div>
    
    <div class="stats-grid">
        <div class="stat-box">
            <div class="value">{len(df_away_any)}</div>
            <div class="label">{away_name} (Any)</div>
        </div>
        <div class="stat-box">
            <div class="value">{len(df_away_away)}</div>
            <div class="label">{away_name} (Away)</div>
        </div>
        <div class="stat-box">
            <div class="value">{len(df_home_any)}</div>
            <div class="label">{home_name} (Any)</div>
        </div>
        <div class="stat-box">
            <div class="value">{len(df_home_home)}</div>
            <div class="label">{home_name} (Home)</div>
        </div>
    </div>
""")
    
    # Transcript length histogram - always show full dataset
    if len(df_full) > 0:
        hist_img = create_length_histogram(df_full, f'Transcript Length Distribution ({crew.name} - All)')
        html_parts.append(f"""
    <div class="card">
        <h3>Transcript Length Distribution ({crew.name} - All)</h3>
        <img src="data:image/png;base64,{hist_img}" alt="Transcript Length Histogram" style="max-width: 100%;">
        <p class="text-muted">Min: {df_full['text_length'].min():,} | Max: {df_full['text_length'].max():,} | Std: {df_full['text_length'].std():,.0f}</p>
    </div>
""")
    
    # Phrase frequency tables - include all phrases except announcer names
    market_phrases = [p for p in phrases if p not in announcer_names]
    
    html_parts.append("""
    <h2>📋 Phrase Frequency Analysis</h2>
""")
    
    # Build frequency table with appropriate columns
    dfs_for_freq = {}
    
    # If filtering by commentator, show both filtered and full dataset
    if config.color_commentator_filter:
        filter_label = config.color_commentator_filter.split()[0]  # First name
        dfs_for_freq[f'{crew.name} ({filter_label})'] = df
        dfs_for_freq[f'{crew.name} (All)'] = df_full
    else:
        dfs_for_freq[crew.name] = df
    
    # Add raw/diarized data (all speakers, not filtered by color commentator)
    if not df_raw.empty:
        dfs_for_freq[f'{crew.name} (Raw)'] = df_raw
    
    # Add comparison datasets
    dfs_for_freq.update(comparison_dfs)
    
    # Add combined column
    if not df_combined.empty:
        dfs_for_freq['Combined'] = df_combined
    
    # Add postseason column (games with postseason indicators in filename)
    # Check for various postseason patterns: post, playoff, wildcard, divisional, conference, superbowl, super-bowl
    postseason_patterns = ['post', 'playoff', 'wildcard', 'divisional', 'conference', 'superbowl', 'super-bowl']
    postseason_mask = pd.Series([False] * len(df), index=df.index)
    for pattern in postseason_patterns:
        postseason_mask |= df['file'].str.contains(pattern, case=False, na=False)
    df_postseason = df[postseason_mask]
    if not df_postseason.empty:
        dfs_for_freq['Postseason'] = df_postseason
    
    # Add team subsets (to the right)
    away_abbrev = away_team.upper() if away_team else 'Away'
    home_abbrev = home_team.upper() if home_team else 'Home'
    
    if not df_away_any.empty:
        dfs_for_freq[f'{away_abbrev} (Any)'] = df_away_any
    if not df_away_away.empty:
        dfs_for_freq[f'{away_abbrev} (Away)'] = df_away_away
    if not df_home_any.empty:
        dfs_for_freq[f'{home_abbrev} (Any)'] = df_home_any
    if not df_home_home.empty:
        dfs_for_freq[f'{home_abbrev} (Home)'] = df_home_home
    
    freq_df = get_per_appearance_frequency(
        dfs_for_freq, 
        {p: search_patterns[p] for p in market_phrases if p in search_patterns},
        show_confidence_interval=True,
        ci_method=config.ci_method,
        multiple_testing_correction=config.multiple_testing_correction,
        family_confidence=config.family_confidence,
        return_df=True
    )
    
    if freq_df is not None:
        # Add notes column to the frequency table
        notes_key = f"nfl_notes_{config.event_ticker}_{config.crew_name}"
        
        html_parts.append("""
    <div class="card">
        <h3>Per-Game Frequency (with {:.0f}% CI, {} corrected)</h3>
        <div class="notes-buttons">
            <button class="notes-btn notes-btn-save" onclick="saveNotes()">💾 Save Notes</button>
            <button class="notes-btn notes-btn-export" onclick="exportNotes()">📤 Export to File</button>
            <button class="notes-btn notes-btn-import" onclick="document.getElementById('importFile').click()">📥 Import from File</button>
            <input type="file" id="importFile" accept=".json" style="display:none" onchange="importNotes(event)">
            <span id="notesStatus" class="notes-status"></span>
        </div>
        <div style="overflow-x: auto;">
""".format(config.family_confidence * 100, config.multiple_testing_correction))
        
        # Create custom HTML table with notes column
        html_parts.append("<table>")
        html_parts.append("<thead><tr><th>Notes</th><th>Phrase</th>")
        for col in freq_df.columns:
            html_parts.append(f"<th>{col}</th>")
        html_parts.append("</tr></thead>")
        html_parts.append("<tbody>")
        
        for idx, row in freq_df.iterrows():
            phrase_id = idx.replace(' ', '_').replace('/', '_').replace("'", "")
            html_parts.append(f'<tr><td><input type="text" class="notes-input" id="note_{phrase_id}" data-phrase="{idx}" placeholder="Add note..."></td>')
            html_parts.append(f"<td><strong>{idx}</strong></td>")
            for col in freq_df.columns:
                html_parts.append(f"<td>{row[col]}</td>")
            html_parts.append("</tr>")
        
        html_parts.append("</tbody></table>")
        html_parts.append("</div>")
        
        # Add JavaScript for notes functionality
        html_parts.append(f"""
        <script>
        const NOTES_KEY = '{notes_key}';
        
        // Load notes from localStorage on page load
        function loadNotes() {{
            const saved = localStorage.getItem(NOTES_KEY);
            if (saved) {{
                const notes = JSON.parse(saved);
                document.querySelectorAll('.notes-input').forEach(input => {{
                    const phrase = input.dataset.phrase;
                    if (notes[phrase]) {{
                        input.value = notes[phrase];
                    }}
                }});
                showStatus('Notes loaded from browser storage');
            }}
        }}
        
        // Save notes to localStorage
        function saveNotes() {{
            const notes = {{}};
            document.querySelectorAll('.notes-input').forEach(input => {{
                const phrase = input.dataset.phrase;
                if (input.value.trim()) {{
                    notes[phrase] = input.value.trim();
                }}
            }});
            localStorage.setItem(NOTES_KEY, JSON.stringify(notes));
            showStatus('Notes saved to browser storage');
        }}
        
        // Export notes to JSON file
        function exportNotes() {{
            const notes = {{}};
            document.querySelectorAll('.notes-input').forEach(input => {{
                const phrase = input.dataset.phrase;
                if (input.value.trim()) {{
                    notes[phrase] = input.value.trim();
                }}
            }});
            
            const blob = new Blob([JSON.stringify(notes, null, 2)], {{type: 'application/json'}});
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = NOTES_KEY + '.json';
            a.click();
            URL.revokeObjectURL(url);
            showStatus('Notes exported to file');
        }}
        
        // Import notes from JSON file
        function importNotes(event) {{
            const file = event.target.files[0];
            if (!file) return;
            
            const reader = new FileReader();
            reader.onload = function(e) {{
                try {{
                    const notes = JSON.parse(e.target.result);
                    document.querySelectorAll('.notes-input').forEach(input => {{
                        const phrase = input.dataset.phrase;
                        input.value = notes[phrase] || '';
                    }});
                    localStorage.setItem(NOTES_KEY, JSON.stringify(notes));
                    showStatus('Notes imported from file');
                }} catch (err) {{
                    showStatus('Error: Invalid JSON file');
                }}
            }};
            reader.readAsText(file);
        }}
        
        // Show status message
        function showStatus(msg) {{
            const status = document.getElementById('notesStatus');
            status.textContent = msg;
            setTimeout(() => {{ status.textContent = ''; }}, 3000);
        }}
        
        // Auto-save on input change
        document.querySelectorAll('.notes-input').forEach(input => {{
            input.addEventListener('change', saveNotes);
        }});
        
        // Load notes on page load
        document.addEventListener('DOMContentLoaded', loadNotes);
        </script>
""")
        html_parts.append("</div>")
    
    # Phrase charts (collapsible) - ALL phrases
    html_parts.append("""
    <h2>📊 Phrase Trends Over Time</h2>
    <p style="color: var(--text-muted); font-size: 0.9em;">Click on each phrase to see its frequency chart over time.</p>
""")
    
    for phrase in market_phrases:  # ALL market phrases
        if phrase in df.columns:
            try:
                img_data = create_phrase_chart(df, phrase)
                count = (df[phrase] > 0).sum()
                total = len(df)
                pct = count / total * 100 if total > 0 else 0
                
                html_parts.append(f"""
    <details>
        <summary>📊 {phrase} - {pct:.1f}% ({count}/{total} games)</summary>
        <div class="details-content">
            <div class="chart-container">
                <img src="data:image/png;base64,{img_data}" alt="{phrase} frequency chart">
            </div>
        </div>
    </details>
""")
            except Exception as e:
                print(f"Warning: Could not create chart for {phrase}: {e}")
    
    # Team-specific analysis
    html_parts.append(f"""
    <h2>🏟️ Team Analysis: {away_name} @ {home_name}</h2>
""")
    
    filter_label = config.color_commentator_filter.split()[0] if config.color_commentator_filter else None
    
    # Helper to create transposed team table (phrases as rows, games as columns)
    def create_team_table(team_df, market_phrases):
        if team_df.empty:
            return "<p>No games found</p>"
        
        # Sort by date chronologically (ascending) and take up to 30 games
        team_df = team_df.copy().sort_values('date', ascending=True).head(30)
        
        # Get all phrase columns that exist
        phrase_cols = [p for p in market_phrases if p in team_df.columns]
        
        if not phrase_cols:
            return "<p>No phrase data available</p>"
        
        # Create transposed table: phrases as rows, games as columns
        # Create short game labels from filenames (include year)
        game_labels = []
        for _, row in team_df.iterrows():
            date_str = row['date'].strftime('%y-%m-%d') if row['date'] else ''
            # Extract team info from filename (e.g., "bills-at-chiefs" from filename)
            fname = row['file']
            if '-at-' in fname.lower():
                teams_part = fname.split('_')[-1].replace('.txt', '').replace('.json', '')
                game_labels.append(f"{date_str} {teams_part}")
            else:
                game_labels.append(date_str)
        
        # Build transposed data
        transposed_data = {}
        for i, (_, row) in enumerate(team_df.iterrows()):
            col_name = game_labels[i] if i < len(game_labels) else f"Game {i+1}"
            transposed_data[col_name] = [row[p] if p in row.index else 0 for p in phrase_cols]
        
        transposed_df = pd.DataFrame(transposed_data, index=phrase_cols)
        transposed_df.index.name = 'Phrase'
        
        return f"<div style='overflow-x: auto; max-height: 600px;'>{transposed_df.to_html(classes='', escape=False)}</div>"
    
    # Away Team Section
    html_parts.append(f"""
    <h3>{away_name} Games</h3>
""")
    
    if away_team:
        # Crew (filtered) dataset
        html_parts.append(f"""
    <details open>
        <summary><strong>{crew.name}{' (' + filter_label + ')' if filter_label else ''}</strong> - {away_name}</summary>
        <div class="details-content">
            <div class="two-col">
                <div class="card">
                    <h4>{away_name} as Away Team ({len(df_away_away)} games)</h4>
                    {create_team_table(df_away_away, market_phrases)}
                </div>
                <div class="card">
                    <h4>{away_name} Either Position ({len(df_away_any)} games)</h4>
                    {create_team_table(df_away_any, market_phrases)}
                </div>
            </div>
        </div>
    </details>
""")
        
        # Full crew dataset (if filtering)
        if config.color_commentator_filter:
            html_parts.append(f"""
    <details>
        <summary><strong>{crew.name} (All)</strong> - {away_name}</summary>
        <div class="details-content">
            <div class="two-col">
                <div class="card">
                    <h4>{away_name} as Away Team ({len(df_full_away_away)} games)</h4>
                    {create_team_table(df_full_away_away, market_phrases)}
                </div>
                <div class="card">
                    <h4>{away_name} Either Position ({len(df_full_away_any)} games)</h4>
                    {create_team_table(df_full_away_any, market_phrases)}
                </div>
            </div>
        </div>
    </details>
""")
        
        # Combined dataset
        if not df_combined.empty:
            html_parts.append(f"""
    <details>
        <summary><strong>Combined (All Crews)</strong> - {away_name}</summary>
        <div class="details-content">
            <div class="two-col">
                <div class="card">
                    <h4>{away_name} as Away Team ({len(df_combined_away_away)} games)</h4>
                    {create_team_table(df_combined_away_away, market_phrases)}
                </div>
                <div class="card">
                    <h4>{away_name} Either Position ({len(df_combined_away_any)} games)</h4>
                    {create_team_table(df_combined_away_any, market_phrases)}
                </div>
            </div>
        </div>
    </details>
""")
    else:
        html_parts.append("<div class='card'><p>No away team data available</p></div>")
    
    # Home Team Section
    html_parts.append(f"""
    <h3>{home_name} Games</h3>
""")
    
    if home_team:
        # Crew (filtered) dataset
        html_parts.append(f"""
    <details open>
        <summary><strong>{crew.name}{' (' + filter_label + ')' if filter_label else ''}</strong> - {home_name}</summary>
        <div class="details-content">
            <div class="two-col">
                <div class="card">
                    <h4>{home_name} as Home Team ({len(df_home_home)} games)</h4>
                    {create_team_table(df_home_home, market_phrases)}
                </div>
                <div class="card">
                    <h4>{home_name} Either Position ({len(df_home_any)} games)</h4>
                    {create_team_table(df_home_any, market_phrases)}
                </div>
            </div>
        </div>
    </details>
""")
        
        # Full crew dataset (if filtering)
        if config.color_commentator_filter:
            html_parts.append(f"""
    <details>
        <summary><strong>{crew.name} (All)</strong> - {home_name}</summary>
        <div class="details-content">
            <div class="two-col">
                <div class="card">
                    <h4>{home_name} as Home Team ({len(df_full_home_home)} games)</h4>
                    {create_team_table(df_full_home_home, market_phrases)}
                </div>
                <div class="card">
                    <h4>{home_name} Either Position ({len(df_full_home_any)} games)</h4>
                    {create_team_table(df_full_home_any, market_phrases)}
                </div>
            </div>
        </div>
    </details>
""")
        
        # Combined dataset
        if not df_combined.empty:
            html_parts.append(f"""
    <details>
        <summary><strong>Combined (All Crews)</strong> - {home_name}</summary>
        <div class="details-content">
            <div class="two-col">
                <div class="card">
                    <h4>{home_name} as Home Team ({len(df_combined_home_home)} games)</h4>
                    {create_team_table(df_combined_home_home, market_phrases)}
                </div>
                <div class="card">
                    <h4>{home_name} Either Position ({len(df_combined_home_any)} games)</h4>
                    {create_team_table(df_combined_home_any, market_phrases)}
                </div>
            </div>
        </div>
    </details>
""")
    else:
        html_parts.append("<div class='card'><p>No home team data available</p></div>")
    
    # Surface and venue analysis
    # Get surface/venue data for full dataset too
    df_full_grass = get_surface_games(df_full, 'grass')
    df_full_turf = get_surface_games(df_full, 'turf')
    df_full_indoor = get_venue_games(df_full, 'indoor')
    df_full_outdoor = get_venue_games(df_full, 'outdoor')
    df_full_retractable = get_venue_games(df_full, 'retractable')
    
    html_parts.append("""
    <h2>🏟️ Surface Analysis</h2>
""")
    
    # Surface Analysis - Filtered Dataset
    crew_label = f"{crew.name}{' (' + filter_label + ')' if filter_label else ''}"
    html_parts.append(f"""
    <details open>
        <summary><strong>{crew_label}</strong> - Surface Type</summary>
        <div class="details-content">
            <div class="card">
                <h4>Surface Type Frequency Comparison</h4>
                <p>Grass: {len(df_grass)} games | Turf: {len(df_turf)} games</p>
""")
    
    surface_dfs = {'Grass': df_grass, 'Turf': df_turf}
    surface_dfs_filtered = {k: v for k, v in surface_dfs.items() if not v.empty}
    
    if not surface_dfs_filtered:
        html_parts.append("<p>No surface data available</p>")
    else:
        # Add frequency table for market phrases by surface
        surface_freq = get_per_appearance_frequency(
            surface_dfs_filtered,
            {p: search_patterns[p] for p in market_phrases if p in search_patterns},
            show_confidence_interval=True,
            return_df=True
        )
        if surface_freq is not None:
            html_parts.append("<div style='overflow-x: auto; max-height: 500px;'>")
            html_parts.append(surface_freq.to_html(classes='', escape=False))
            html_parts.append("</div>")
    
    html_parts.append("</div></div></details>")
    
    # Surface Analysis - Full Dataset (if filtering)
    if config.color_commentator_filter:
        html_parts.append(f"""
    <details>
        <summary><strong>{crew.name} (All)</strong> - Surface Type</summary>
        <div class="details-content">
            <div class="card">
                <h4>Surface Type Frequency Comparison</h4>
                <p>Grass: {len(df_full_grass)} games | Turf: {len(df_full_turf)} games</p>
""")
        
        full_surface_dfs = {'Grass': df_full_grass, 'Turf': df_full_turf}
        full_surface_dfs_filtered = {k: v for k, v in full_surface_dfs.items() if not v.empty}
        
        if not full_surface_dfs_filtered:
            html_parts.append("<p>No surface data available</p>")
        else:
            # Add frequency table for market phrases by surface
            full_surface_freq = get_per_appearance_frequency(
                full_surface_dfs_filtered,
                {p: search_patterns[p] for p in market_phrases if p in search_patterns},
                show_confidence_interval=True,
                return_df=True
            )
            if full_surface_freq is not None:
                html_parts.append("<div style='overflow-x: auto; max-height: 500px;'>")
                html_parts.append(full_surface_freq.to_html(classes='', escape=False))
                html_parts.append("</div>")
        
        html_parts.append("</div></div></details>")
    
    # Weather-related phrases analysis
    weather_phrases = [p for p in market_phrases if any(w in p.lower() for w in ['wind', 'windy', 'snow', 'rain', 'cold', 'weather', 'temperature', 'ice', 'freeze', 'fog'])]
    
    if weather_phrases:
        html_parts.append("""
    <h2>🌤️ Weather-Related Phrases</h2>
""")
        
        # Filtered dataset weather analysis
        html_parts.append(f"""
    <details open>
        <summary><strong>{crew_label}</strong> - Weather Phrases</summary>
        <div class="details-content card">
""")
        weather_venue_dfs = {}
        if not df_indoor.empty:
            weather_venue_dfs['Indoor'] = df_indoor
        if not df_outdoor.empty:
            weather_venue_dfs['Outdoor'] = df_outdoor
        if not df_retractable.empty:
            weather_venue_dfs['Retractable'] = df_retractable
        weather_venue_dfs['All Games'] = df
        
        if weather_venue_dfs:
            weather_freq = get_per_appearance_frequency(
                weather_venue_dfs,
                {p: search_patterns[p] for p in weather_phrases if p in search_patterns},
                show_confidence_interval=True,
                return_df=True
            )
            if weather_freq is not None:
                html_parts.append("<div style='overflow-x: auto;'>")
                html_parts.append(weather_freq.to_html(classes='', escape=False))
                html_parts.append("</div>")
        html_parts.append("</div></details>")
        
        # Full dataset weather analysis (if filtering)
        if config.color_commentator_filter:
            html_parts.append(f"""
    <details>
        <summary><strong>{crew.name} (All)</strong> - Weather Phrases</summary>
        <div class="details-content card">
""")
            full_weather_venue_dfs = {}
            if not df_full_indoor.empty:
                full_weather_venue_dfs['Indoor'] = df_full_indoor
            if not df_full_outdoor.empty:
                full_weather_venue_dfs['Outdoor'] = df_full_outdoor
            if not df_full_retractable.empty:
                full_weather_venue_dfs['Retractable'] = df_full_retractable
            full_weather_venue_dfs['All Games'] = df_full
            
            if full_weather_venue_dfs:
                full_weather_freq = get_per_appearance_frequency(
                    full_weather_venue_dfs,
                    {p: search_patterns[p] for p in weather_phrases if p in search_patterns},
                    show_confidence_interval=True,
                    return_df=True
                )
                if full_weather_freq is not None:
                    html_parts.append("<div style='overflow-x: auto;'>")
                    html_parts.append(full_weather_freq.to_html(classes='', escape=False))
                    html_parts.append("</div>")
            html_parts.append("</div></details>")
    
    # Recent contexts - ALL phrases
    html_parts.append("""
    <h2>📝 Recent Phrase Contexts</h2>
    <p style="color: var(--text-muted); font-size: 0.9em;">Click on each phrase to see recent usage examples from the transcripts.</p>
""")
    
    for phrase in market_phrases:  # ALL market phrases
        if phrase not in search_patterns:
            continue
            
        pattern = search_patterns[phrase]
        contexts = []
        
        # Look through more games to find contexts
        for _, row in df.sort_values('date', ascending=False).head(50).iterrows():
            matches = find_phrase_context(row['text'], pattern, window=150)
            for match in matches[:2]:  # Max 2 per file
                contexts.append({
                    'file': row['file'],
                    'date': row['date'],
                    'context': match
                })
            if len(contexts) >= 10:  # Get up to 10 contexts
                break
        
        # Calculate phrase stats for summary
        count = (df[phrase] > 0).sum() if phrase in df.columns else 0
        total = len(df)
        pct = count / total * 100 if total > 0 else 0
        
        if contexts:
            html_parts.append(f"""
    <details>
        <summary>💬 {phrase} - {pct:.1f}% ({count}/{total} games) - {len(contexts)} context(s)</summary>
        <div class="details-content">
""")
            for ctx in contexts[:10]:
                date_str = ctx['date'].strftime('%Y-%m-%d') if ctx['date'] else 'Unknown'
                html_parts.append(f"""
            <div class="context-box">
                <div class="source">{ctx['file']} ({date_str})</div>
                <div>{ctx['context']}</div>
            </div>
""")
            html_parts.append("</div></details>")
        else:
            # Show phrases with no recent contexts
            html_parts.append(f"""
    <details>
        <summary style="color: var(--text-muted);">💬 {phrase} - {pct:.1f}% ({count}/{total} games) - No recent contexts</summary>
        <div class="details-content">
            <p>No occurrences found in recent games.</p>
        </div>
    </details>
""")
    
    # Footer
    html_parts.append(f"""
    <div class="generated-time">
        Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    </div>
</div>
</body>
</html>
""")
    
    return ''.join(html_parts)


def main():
    parser = argparse.ArgumentParser(
        description='Generate NFL phrase analysis HTML reports',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic report
  python src/nfl/nfl_report_generator.py KXNFLMENTION-25DEC28PHIBUF --crew kevin-kugler

  # With color commentator filter
  python src/nfl/nfl_report_generator.py KXNFLMENTION-25DEC28PHIBUF --crew kevin-kugler --color-commentator "Daryl Johnston"
  
  # Custom output path
  python src/nfl/nfl_report_generator.py KXNFLMENTION-25DEC28PHIBUF --crew snf -o reports/snf_report.html
  
  # List available crews
  python src/nfl/nfl_report_generator.py --list-crews
        """
    )
    
    parser.add_argument('event_ticker', nargs='?', help='Kalshi event ticker (e.g., KXNFLMENTION-25DEC28PHIBUF)')
    parser.add_argument('--crew', '-c', help='Announcer crew name (e.g., snf, mnf, kevin-kugler)')
    parser.add_argument('--color-commentator', help='Filter by color commentator name')
    parser.add_argument('-o', '--output', help='Output HTML file path')
    parser.add_argument('--list-crews', action='store_true', help='List available announcer crews')
    
    # Analysis options
    parser.add_argument('--ci-method', choices=['wilson', 'agresti-coull', 'clopper-pearson'],
                       default='wilson', help='Confidence interval method (default: wilson)')
    parser.add_argument('--correction', choices=['bonferroni', 'holm', 'fdr_bh', 'none'],
                       default='bonferroni', help='Multiple testing correction (default: bonferroni)')
    parser.add_argument('--confidence', type=float, default=0.95,
                       help='Family-wise confidence level (default: 0.95)')
    parser.add_argument('--min-mentions', type=float, default=5.0,
                       help='Minimum average announcer mentions for validation (default: 5.0)')
    
    args = parser.parse_args()
    
    # List crews mode
    if args.list_crews:
        print("\n📋 Available Announcer Crews:\n")
        print(f"{'Name':<20} {'Folder':<20} {'Play-by-Play':<25} {'Color'}")
        print("-" * 85)
        for name, crew in ANNOUNCER_CREWS.items():
            pbp = ', '.join(crew.play_by_play[:2])
            color = ', '.join(crew.color[:2]) if crew.color else 'Various'
            print(f"{name:<20} {crew.folder:<20} {pbp:<25} {color}")
        print()
        return
    
    # Validate required arguments
    if not args.event_ticker:
        print("Error: event_ticker is required. Use --help for usage.")
        sys.exit(1)
    
    if not args.crew:
        print("Error: --crew is required. Use --list-crews to see available crews.")
        sys.exit(1)
    
    # Build config
    config = ReportConfig(
        event_ticker=args.event_ticker,
        crew_name=args.crew,
        color_commentator_filter=args.color_commentator,
        output_path=args.output,
        ci_method=args.ci_method,
        multiple_testing_correction=args.correction if args.correction != 'none' else None,
        family_confidence=args.confidence,
        min_announcer_mentions=args.min_mentions,
    )
    
    print(f"🏈 Generating report for {config.event_ticker}")
    print(f"   Crew: {config.crew_name}")
    if config.color_commentator_filter:
        print(f"   Color Commentator: {config.color_commentator_filter}")
    
    try:
        html = generate_html_report(config)
        
        # Determine output path
        if config.output_path:
            output_path = config.output_path
        else:
            # Default: reports/<ticker>_<crew>_<timestamp>.html
            os.makedirs('reports', exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = f"reports/{config.event_ticker}_{config.crew_name}_{timestamp}.html"
        
        # Write file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html)
        
        # Get absolute path for file:// URL
        abs_path = os.path.abspath(output_path)
        # Use forward slashes for URL (standard format: file:// + path)
        file_url_path = abs_path.replace(os.sep, '/')
        # On Unix, abs path starts with /, so file:// + /path = file:///path
        file_url = f"file://{file_url_path}"
        
        print(f"\n✅ Report generated: {output_path}")
        print(f"\n📄 Open in browser:")
        print(f"   {file_url}")
        print(f"\n   (Copy and paste the link above into your browser address bar)")
        
    except Exception as e:
        print(f"\n❌ Error generating report: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()

