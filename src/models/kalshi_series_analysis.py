"""
Kalshi Series Phrase Analysis Module

Provides tools for analyzing phrase hit rates across Kalshi market series.
Useful for NBA mentions, speech word mentions, and other phrase-based markets.
"""

import time
from typing import Optional, List, Dict, Any
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from IPython.display import display, HTML
import io
import base64

# Set default styling
plt.style.use('default')
sns.set_theme(style='whitegrid')
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


def fetch_series_title(series_ticker: str) -> str:
    """
    Fetch the title of a series from the Kalshi API.
    
    Args:
        series_ticker: The series ticker (e.g., "KXNBAMENTION")
        
    Returns:
        The series title string
    """
    from kalshi import constants
    constants.use_prod()
    from kalshi.rest import market
    
    series = market.GetSeries(series_ticker=series_ticker)
    return series.get('series', {}).get('title', series_ticker)


def get_open_events(
    series_ticker: str, 
    delay_seconds: float = 1.5,
    verbose: bool = True
) -> List[Dict[str, Any]]:
    """
    Get the list of all open events on an input series.
    
    An event is considered open if its first market has status='active'.
    (All markets in an event share the same status.)

    Args:
        series_ticker: The series ticker (e.g., "KXNBAMENTION")
        delay_seconds: Delay between API requests to avoid rate limiting
        verbose: Whether to print progress/errors
        
    Returns:
        List of event dictionaries that are currently open
    """
    from kalshi import constants
    constants.use_prod()
    from kalshi.rest import market

    # Get all events in the series
    series = market.GetEvents(series_ticker=series_ticker)
    events = series.get('events', [])
    
    if verbose:
        print(f"Found {len(events)} total events in series {series_ticker}")

    # Check each event's first market for active status
    open_events = []
    for idx, event in enumerate(events):
        event_ticker = event.get('event_ticker')
        try:
            markets_resp = market.GetMarkets(event_ticker=event_ticker)
        except Exception as e:
            if verbose:
                print(f"Error on {event_ticker}: {e}. Retrying after delay...")
            time.sleep(delay_seconds * 4)
            try:
                markets_resp = market.GetMarkets(event_ticker=event_ticker)
            except Exception as e2:
                if verbose:
                    print(f"Failed again on {event_ticker}: {e2}. Skipping.")
                continue
        
        mkts = markets_resp.get('markets', [])
        
        # Check first market only - all markets in an event share the same status
        if mkts and mkts[0].get('status', '').lower() == 'active':
            open_events.append(event)
        
        # Rate limit (skip delay on last iteration)
        if idx < len(events) - 1:
            time.sleep(delay_seconds)
    
    if verbose:
        print(f"Found {len(open_events)} open events")

    return [open_events['event_ticker'] for open_events in open_events]


def fetch_event_titles(
    series_ticker: str,
    verbose: bool = True
) -> Dict[str, str]:
    """
    Fetch titles for all events in a series.
    
    Args:
        series_ticker: The series ticker (e.g., "KXNFLMENTION")
        verbose: Whether to print progress/errors
        
    Returns:
        Dict mapping event_ticker -> event_title
        e.g., {"KXNFLMENTION-26JAN10GBCHI": "What will the announcers say during the Green Bay at Chicago pro football game?"}
    """
    from kalshi import constants
    constants.use_prod()
    from kalshi.rest import market
    
    # Get all events in the series
    series = market.GetEvents(series_ticker=series_ticker)
    events = series.get('events', [])
    
    titles = {}
    for event in events:
        ticker = event.get('event_ticker')
        title = event.get('title', '')
        if ticker:
            titles[ticker] = title
    
    if verbose:
        print(f"Fetched titles for {len(titles)} events in series {series_ticker}")
    
    return titles


def fetch_series_results(
    series_ticker: str,
    delay_seconds: float = 1.5,
    verbose: bool = True,
    include_titles: bool = False
) -> Dict[str, Dict[str, str]]:
    """
    Fetch results for all events in a series.
    
    Args:
        series_ticker: The series ticker (e.g., "KXNBAMENTION")
        delay_seconds: Delay between API requests to avoid rate limiting
        verbose: Whether to print progress/errors
        include_titles: If True, also fetch event titles (no extra API calls needed)
        
    Returns:
        Dict mapping event_ticker -> {phrase: result} where result is 'yes'/'no'/None
        If include_titles=True, also includes '_title' key with event title
    """
    from kalshi import constants
    constants.use_prod()
    from kalshi.rest import market
    
    # Get all events in the series
    series = market.GetEvents(series_ticker=series_ticker)
    events = series.get('events', [])
    event_tickers = [event['event_ticker'] for event in events]
    
    # Build title lookup if requested
    title_lookup = {}
    if include_titles:
        for event in events:
            ticker = event.get('event_ticker')
            title = event.get('title', '')
            if ticker:
                title_lookup[ticker] = title
    
    if verbose:
        print(f"Found {len(event_tickers)} events in series {series_ticker}")
    
    results = {}
    
    for idx, event_ticker in enumerate(event_tickers):
        event_results = {}
        try:
            markets = market.GetMarkets(event_ticker=event_ticker)
        except Exception as e:
            if verbose:
                print(f"Error on {event_ticker}: {e}. Retrying after delay...")
            time.sleep(delay_seconds * 4)
            try:
                markets = market.GetMarkets(event_ticker=event_ticker)
            except Exception as e2:
                if verbose:
                    print(f"Failed again on {event_ticker}: {e2}. Skipping.")
                continue
        
        for mkt in markets.get('markets', []):
            phrase = None
            custom_strike = mkt.get('custom_strike')
            if custom_strike and isinstance(custom_strike, dict):
                phrase = custom_strike.get('Word')
            
            if phrase is not None:
                result = mkt.get('result')
                event_results[phrase] = result
        
        # Add title if requested
        if include_titles and event_ticker in title_lookup:
            event_results['_title'] = title_lookup[event_ticker]
        
        results[event_ticker] = event_results
        
        # Rate limit between requests
        if idx < len(event_tickers) - 1:
            time.sleep(delay_seconds)
    
    if verbose:
        events_with_results = sum(1 for e in results.values() if any(v is not None for v in e.values() if v != '_title'))
        print(f"Fetched results for {events_with_results} events with resolved markets")
    
    return results


def results_to_dataframe(results: Dict[str, Dict[str, str]]) -> pd.DataFrame:
    """
    Convert results dict to a DataFrame.
    
    Args:
        results: Dict from fetch_series_results
        
    Returns:
        DataFrame with event_tickers as index, phrases as columns, values are 'yes'/'no'/''
    """
    df = pd.DataFrame(results).T
    df = df.fillna('')
    df.index.name = 'event_ticker'
    return df


def _to_numeric(val: Any) -> Any:
    """Convert yes/no to 1/0, keeping NA for empty/missing."""
    if pd.isna(val):
        return pd.NA
    if isinstance(val, str):
        val_clean = val.strip().lower()
        if val_clean == 'yes':
            return 1
        elif val_clean == 'no':
            return 0
        elif val_clean == '':
            return pd.NA
    return pd.NA


def calculate_hit_rates(df: pd.DataFrame, phrases: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Calculate hit rates and counts for each phrase.
    
    Args:
        df: DataFrame from results_to_dataframe
        phrases: Optional list of phrases to filter to
        
    Returns:
        DataFrame with columns: Phrase, Hit Rate, Hits, Misses, Total, Events Without Phrase
    """
    if phrases is not None:
        cols = [p for p in phrases if p in df.columns]
    else:
        cols = list(df.columns)
    
    stats = []
    for phrase in cols:
        numeric_col = df[phrase].apply(_to_numeric)
        hits = (numeric_col == 1).sum()
        misses = (numeric_col == 0).sum()
        total = hits + misses
        hit_rate = hits / total if total > 0 else np.nan
        events_without = len(df) - total
        
        stats.append({
            'Phrase': phrase,
            'Hit Rate': hit_rate,
            'Hits': int(hits),
            'Misses': int(misses),
            'Total': int(total),
            'Events Without Phrase': int(events_without)
        })
    
    stats_df = pd.DataFrame(stats)
    stats_df = stats_df.sort_values('Total', ascending=False)
    return stats_df


def display_hit_rates(
    df: pd.DataFrame,
    phrases: Optional[List[str]] = None,
    sort_by: str = 'Total',
    ascending: bool = False,
    min_events: int = 1,
    title: Optional[str] = None
) -> pd.DataFrame:
    """
    Display a formatted hit rate table with styling.
    
    Args:
        df: DataFrame from results_to_dataframe
        phrases: Optional list of phrases to filter to
        sort_by: Column to sort by ('Total', 'Hit Rate', 'Hits', 'Phrase')
        ascending: Sort direction
        min_events: Minimum number of events for a phrase to be included
        title: Optional title for the display
        
    Returns:
        The styled DataFrame (also displays it)
    """
    stats_df = calculate_hit_rates(df, phrases)
    
    # Filter by minimum events
    stats_df = stats_df[stats_df['Total'] >= min_events]
    
    # Sort
    if sort_by in stats_df.columns:
        stats_df = stats_df.sort_values(sort_by, ascending=ascending)
    
    # Format for display
    display_df = stats_df.copy()
    display_df['Hit Rate'] = display_df['Hit Rate'].apply(
        lambda x: f"{x:.1%}" if pd.notna(x) else "N/A"
    )
    
    if title:
        display(HTML(f"<h3>{title}</h3>"))
    
    # Style the table
    styled = display_df.style.set_table_styles([
        {'selector': 'th', 'props': [('text-align', 'center')]},
        {'selector': 'td', 'props': [('text-align', 'center')]}
    ]).hide(axis='index')
    
    display(styled)
    return stats_df


def display_full_results_table(
    df: pd.DataFrame,
    phrases: Optional[List[str]] = None,
    show_summary: bool = True,
    max_events: int = 50,
    title: Optional[str] = None
) -> None:
    """
    Display the full results matrix with events as rows and phrases as columns.
    Shows yes/no values and includes summary rows for averages and counts.
    
    Args:
        df: DataFrame from results_to_dataframe
        phrases: Optional list of phrases to filter columns
        show_summary: Whether to add average and count summary rows
        max_events: Maximum number of events to display (most recent first)
        title: Optional title for the display
    """
    if phrases is not None:
        cols = [p for p in phrases if p in df.columns]
        display_df = df[cols].copy()
    else:
        display_df = df.copy()
    
    # Limit rows
    if len(display_df) > max_events:
        display_df = display_df.head(max_events)
    
    if show_summary:
        # Calculate summary stats
        numeric_df = display_df.apply(lambda col: col.apply(_to_numeric))
        avg_row = numeric_df.mean(skipna=True)
        count_row = numeric_df.notna().sum()
        
        # Build summary rows
        avg_values = []
        count_values = []
        for col in display_df.columns:
            val = avg_row[col]
            avg_values.append(f"{val:.2f}" if pd.notna(val) else '')
            count_val = count_row[col]
            count_values.append(str(int(count_val)) if count_val > 0 else '')
        
        summary_df = pd.DataFrame(
            [avg_values, count_values],
            columns=display_df.columns,
            index=['Hit Rate', 'Count']
        )
        display_df = pd.concat([display_df, summary_df])
    
    if title:
        display(HTML(f"<h3>{title}</h3>"))
    
    # Display with horizontal scrolling for many columns
    html = display_df.to_html(
        notebook=True,
        max_rows=None,
        max_cols=None,
        escape=False,
        index=True,
        show_dimensions=True
    )
    
    # Wrap in scrollable div
    scrollable_html = f"""
    <div style="overflow-x: auto; max-width: 100%;">
        {html}
    </div>
    """
    display(HTML(scrollable_html))


def plot_phrase_over_time(
    df: pd.DataFrame,
    phrase: str,
    event_dates: Optional[Dict[str, str]] = None,
    title: Optional[str] = None,
    figsize: tuple = (14, 5)
) -> None:
    """
    Plot a single phrase's hit/miss pattern over time (event order).
    
    Args:
        df: DataFrame from results_to_dataframe
        phrase: The phrase to plot
        event_dates: Optional dict mapping event_ticker -> date string
        title: Optional title override
        figsize: Figure size tuple
    """
    if phrase not in df.columns:
        print(f"Phrase '{phrase}' not found in data")
        return
    
    # Get numeric values
    series = df[phrase].apply(_to_numeric)
    
    # Filter to events that have this phrase
    mask = series.notna()
    series = series[mask]
    event_tickers = df.index[mask]
    
    if len(series) == 0:
        print(f"No resolved markets found for phrase '{phrase}'")
        return
    
    # Create figure
    fig, ax = plt.subplots(figsize=figsize)
    
    # Plot as bars
    colors = ['#2ecc71' if v == 1 else '#e74c3c' for v in series]
    x_pos = range(len(series))
    ax.bar(x_pos, series.values, color=colors, edgecolor='white', linewidth=0.5)
    
    # Calculate hit rate
    hit_rate = series.mean()
    ax.axhline(y=hit_rate, color='blue', linestyle='--', alpha=0.7, 
               label=f'Hit Rate: {hit_rate:.1%}')
    
    # X-axis labels
    if event_dates:
        labels = [event_dates.get(t, t[-10:]) for t in event_tickers]
    else:
        # Extract date portion from ticker (last part usually contains date info)
        labels = [t.split('-')[-1] if '-' in t else t[-8:] for t in event_tickers]
    
    ax.set_xticks(x_pos)
    ax.set_xticklabels(labels, rotation=45, ha='right', fontsize=8)
    
    ax.set_ylim(-0.1, 1.1)
    ax.set_yticks([0, 1])
    ax.set_yticklabels(['No', 'Yes'])
    ax.set_xlabel('Event')
    ax.set_ylabel('Result')
    
    title_text = title or f'"{phrase}" Results Over Time'
    hits = int(series.sum())
    total = len(series)
    ax.set_title(f'{title_text}\n({hits}/{total} = {hit_rate:.1%})')
    
    ax.legend(loc='upper right')
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.show()


def plot_phrase_over_time_collapsible(
    df: pd.DataFrame,
    phrase: str,
    event_dates: Optional[Dict[str, str]] = None,
    title: Optional[str] = None,
    figsize: tuple = (14, 5)
) -> None:
    """
    Plot a single phrase's hit/miss pattern in a collapsible HTML widget.
    """
    if phrase not in df.columns:
        print(f"Phrase '{phrase}' not found in data")
        return
    
    series = df[phrase].apply(_to_numeric)
    mask = series.notna()
    series = series[mask]
    event_tickers = df.index[mask]
    
    if len(series) == 0:
        print(f"No resolved markets found for phrase '{phrase}'")
        return
    
    fig, ax = plt.subplots(figsize=figsize)
    
    colors = ['#2ecc71' if v == 1 else '#e74c3c' for v in series]
    x_pos = range(len(series))
    ax.bar(x_pos, series.values, color=colors, edgecolor='white', linewidth=0.5)
    
    hit_rate = series.mean()
    ax.axhline(y=hit_rate, color='blue', linestyle='--', alpha=0.7, 
               label=f'Hit Rate: {hit_rate:.1%}')
    
    if event_dates:
        labels = [event_dates.get(t, t[-10:]) for t in event_tickers]
    else:
        labels = [t.split('-')[-1] if '-' in t else t[-8:] for t in event_tickers]
    
    ax.set_xticks(x_pos)
    ax.set_xticklabels(labels, rotation=45, ha='right', fontsize=8)
    
    ax.set_ylim(-0.1, 1.1)
    ax.set_yticks([0, 1])
    ax.set_yticklabels(['No', 'Yes'])
    ax.set_xlabel('Event')
    ax.set_ylabel('Result')
    
    title_text = title or f'"{phrase}" Results Over Time'
    hits = int(series.sum())
    total = len(series)
    ax.set_title(f'{title_text}\n({hits}/{total} = {hit_rate:.1%})')
    
    ax.legend(loc='upper right')
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    
    # Save to base64
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    buf.seek(0)
    plot_data = base64.b64encode(buf.read()).decode()
    plt.close()
    
    html = f"""
    <details>
        <summary style="cursor: pointer; font-weight: bold; font-size: 14px; padding: 5px;">
            📊 {phrase} - {hit_rate:.1%} ({hits}/{total})
        </summary>
        <div style="margin-top: 10px;">
            <img src="data:image/png;base64,{plot_data}" style="max-width: 100%; height: auto;">
        </div>
    </details>
    """
    display(HTML(html))


def plot_all_phrases_over_time(
    df: pd.DataFrame,
    phrases: Optional[List[str]] = None,
    min_events: int = 1,
    event_dates: Optional[Dict[str, str]] = None
) -> None:
    """
    Plot all phrases (or selected phrases) over time as collapsible widgets.
    
    Args:
        df: DataFrame from results_to_dataframe
        phrases: Optional list of phrases to plot (defaults to all)
        min_events: Minimum events for a phrase to be included
        event_dates: Optional dict mapping event_ticker -> date string
    """
    if phrases is None:
        phrases = list(df.columns)
    
    # Sort by hit rate descending
    stats = calculate_hit_rates(df, phrases)
    stats = stats[stats['Total'] >= min_events]
    sorted_phrases = stats.sort_values('Hit Rate', ascending=False)['Phrase'].tolist()
    
    for phrase in sorted_phrases:
        plot_phrase_over_time_collapsible(df, phrase, event_dates)


def display_phrases_summary_heatmap(
    df: pd.DataFrame,
    phrases: Optional[List[str]] = None,
    min_events: int = 3,
    figsize: tuple = (12, 10),
    title: Optional[str] = None
) -> None:
    """
    Display a heatmap summary of hit rates across phrases.
    Good for visualizing many phrases at once.
    
    Args:
        df: DataFrame from results_to_dataframe
        phrases: Optional list of phrases to include
        min_events: Minimum events for inclusion
        figsize: Figure size
        title: Optional title
    """
    stats = calculate_hit_rates(df, phrases)
    stats = stats[stats['Total'] >= min_events]
    
    if len(stats) == 0:
        print("No phrases meet the minimum events threshold")
        return
    
    # Sort by hit rate
    stats = stats.sort_values('Hit Rate', ascending=True)
    
    fig, ax = plt.subplots(figsize=figsize)
    
    # Create horizontal bar chart with hit rate coloring
    y_pos = range(len(stats))
    colors = plt.cm.RdYlGn(stats['Hit Rate'].fillna(0))
    
    bars = ax.barh(y_pos, stats['Hit Rate'].fillna(0), color=colors, edgecolor='white')
    
    # Add hit/total labels
    for i, (_, row) in enumerate(stats.iterrows()):
        rate = row['Hit Rate']
        if pd.notna(rate):
            ax.text(rate + 0.02, i, f"{int(row['Hits'])}/{int(row['Total'])}", 
                   va='center', fontsize=9)
    
    ax.set_yticks(y_pos)
    ax.set_yticklabels(stats['Phrase'], fontsize=9)
    ax.set_xlabel('Hit Rate')
    ax.set_xlim(0, 1.15)
    ax.set_title(title or 'Phrase Hit Rates')
    ax.axvline(x=0.5, color='gray', linestyle='--', alpha=0.5)
    
    plt.tight_layout()
    plt.show()


def filter_by_metadata(
    df: pd.DataFrame,
    metadata_df: pd.DataFrame,
    ticker_col: str = 'ticker',
    **filters
) -> pd.DataFrame:
    """
    Filter results DataFrame by metadata (e.g., play_by_play announcer).
    
    Args:
        df: Results DataFrame with event_ticker as index
        metadata_df: DataFrame with metadata (must have ticker_col matching df index)
        ticker_col: Column name in metadata_df that matches df index
        **filters: Column=value pairs to filter by
        
    Returns:
        Filtered DataFrame
    """
    # Apply filters to metadata
    mask = pd.Series(True, index=metadata_df.index)
    for col, val in filters.items():
        if col in metadata_df.columns:
            mask &= metadata_df[col] == val
    
    filtered_tickers = metadata_df.loc[mask, ticker_col].tolist()
    return df.loc[df.index.isin(filtered_tickers)]


class SeriesAnalyzer:
    """
    Convenience class for analyzing a Kalshi series.
    Stores fetched data and provides easy access to analysis functions.
    """
    
    def __init__(self, series_ticker: str, metadata_path: Optional[str] = None):
        """
        Initialize analyzer for a series.
        
        Args:
            series_ticker: The series ticker (e.g., "KXNBAMENTION")
            metadata_path: Optional path to a CSV with event metadata
        """
        self.series_ticker = series_ticker
        self.metadata_path = metadata_path
        self._results: Optional[Dict] = None
        self._df: Optional[pd.DataFrame] = None
        self._metadata: Optional[pd.DataFrame] = None
        self._title: Optional[str] = None
        self._event_titles: Optional[Dict[str, str]] = None
    
    def fetch(self, delay_seconds: float = 1.5, verbose: bool = True) -> 'SeriesAnalyzer':
        """Fetch data from Kalshi API."""
        # Fetch series title first
        self._title = fetch_series_title(self.series_ticker)
        
        # Fetch event titles (no extra API calls needed - uses GetEvents)
        self._event_titles = fetch_event_titles(self.series_ticker, verbose=False)
        
        self._results = fetch_series_results(
            self.series_ticker, 
            delay_seconds=delay_seconds, 
            verbose=verbose
        )
        self._df = results_to_dataframe(self._results)
        
        if self.metadata_path:
            try:
                self._metadata = pd.read_csv(self.metadata_path)
            except Exception as e:
                print(f"Warning: Could not load metadata from {self.metadata_path}: {e}")
        
        return self
    
    @property
    def df(self) -> pd.DataFrame:
        """Get the results DataFrame."""
        if self._df is None:
            raise ValueError("Data not fetched yet. Call fetch() first.")
        return self._df
    
    @property
    def metadata(self) -> Optional[pd.DataFrame]:
        """Get the metadata DataFrame if available."""
        return self._metadata
    
    @property
    def title(self) -> str:
        """Get the series title."""
        if self._title is None:
            raise ValueError("Data not fetched yet. Call fetch() first.")
        return self._title
    
    @property
    def phrases(self) -> List[str]:
        """Get list of all phrases."""
        return list(self.df.columns)
    
    @property
    def events(self) -> List[str]:
        """Get list of all event tickers."""
        return list(self.df.index)
    
    @property
    def event_titles(self) -> Dict[str, str]:
        """Get dict mapping event_ticker -> event_title."""
        if self._event_titles is None:
            raise ValueError("Data not fetched yet. Call fetch() first.")
        return self._event_titles
    
    def get_event_title(self, event_ticker: str) -> Optional[str]:
        """
        Get the title for a specific event.
        
        Args:
            event_ticker: The event ticker (e.g., "KXNFLMENTION-26JAN10GBCHI")
            
        Returns:
            The event title (e.g., "What will the announcers say during the Green Bay at Chicago pro football game?")
            or None if not found
        """
        if self._event_titles is None:
            raise ValueError("Data not fetched yet. Call fetch() first.")
        return self._event_titles.get(event_ticker)
    
    def hit_rates(self, phrases: Optional[List[str]] = None) -> pd.DataFrame:
        """Calculate hit rates for phrases."""
        return calculate_hit_rates(self.df, phrases)
    
    def display_hit_rates(
        self, 
        phrases: Optional[List[str]] = None,
        sort_by: str = 'Total',
        ascending: bool = False,
        min_events: int = 1,
        title: Optional[str] = None
    ) -> pd.DataFrame:
        """Display formatted hit rate table."""
        return display_hit_rates(
            self.df, phrases, sort_by, ascending, min_events, title
        )
    
    def display_full_table(
        self,
        phrases: Optional[List[str]] = None,
        show_summary: bool = True,
        max_events: int = 50,
        title: Optional[str] = None
    ) -> None:
        """Display full results matrix."""
        display_full_results_table(self.df, phrases, show_summary, max_events, title)
    
    def plot_phrase(
        self,
        phrase: str,
        collapsible: bool = True,
        title: Optional[str] = None
    ) -> None:
        """Plot a single phrase over time."""
        event_dates = None
        if self._metadata is not None and 'ticker' in self._metadata.columns and 'date' in self._metadata.columns:
            event_dates = dict(zip(self._metadata['ticker'], self._metadata['date'].astype(str)))
        
        if collapsible:
            plot_phrase_over_time_collapsible(self.df, phrase, event_dates, title)
        else:
            plot_phrase_over_time(self.df, phrase, event_dates, title)
    
    def plot_all_phrases(
        self,
        phrases: Optional[List[str]] = None,
        min_events: int = 1
    ) -> None:
        """Plot all phrases as collapsible widgets."""
        event_dates = None
        if self._metadata is not None and 'ticker' in self._metadata.columns and 'date' in self._metadata.columns:
            event_dates = dict(zip(self._metadata['ticker'], self._metadata['date'].astype(str)))
        
        plot_all_phrases_over_time(self.df, phrases, min_events, event_dates)
    
    def heatmap(
        self,
        phrases: Optional[List[str]] = None,
        min_events: int = 3,
        figsize: tuple = (12, 10),
        title: Optional[str] = None
    ) -> None:
        """Display hit rate heatmap."""
        display_phrases_summary_heatmap(self.df, phrases, min_events, figsize, title)
    
    def filter_by(self, **filters) -> pd.DataFrame:
        """
        Filter results by metadata columns.
        
        Example:
            analyzer.filter_by(play_by_play="Ian Eagle")
        """
        if self._metadata is None:
            raise ValueError("No metadata loaded. Provide metadata_path when creating analyzer.")
        
        return filter_by_metadata(self.df, self._metadata, 'ticker', **filters)
    
    def analyze_subset(
        self,
        phrases: Optional[List[str]] = None,
        title: Optional[str] = None,
        **filters
    ) -> None:
        """
        Analyze a filtered subset of the data.
        
        Args:
            phrases: Optional phrases to focus on
            title: Title for the analysis
            **filters: Metadata filters to apply
        """
        if filters:
            filtered_df = self.filter_by(**filters)
            filter_desc = ', '.join(f"{k}={v}" for k, v in filters.items())
            title = title or f"Analysis for {filter_desc}"
        else:
            filtered_df = self.df
            title = title or "Full Series Analysis"
        
        display(HTML(f"<h2>{title}</h2>"))
        display(HTML(f"<p>{len(filtered_df)} events</p>"))
        
        display_hit_rates(filtered_df, phrases, title="Hit Rates")

