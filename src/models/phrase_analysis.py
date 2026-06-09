import os
import re
from datetime import datetime
import pandas as pd
import numpy as np
from scipy import stats
# Defer kalshi import to avoid network calls during module import
# from kalshi.rest import market
import matplotlib.pyplot as plt
import seaborn as sns
plt.style.use('default')
sns.set_theme(style='whitegrid')
plt.rcParams['figure.figsize'] = [12, 6]

# Set pandas display options
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)

def get_phrases(event_ticker):
    """Get phrases for a given event ticker"""
    # Import only when needed to avoid network calls during module import
    from kalshi import constants
    from kalshi.auth import auth
    from kalshi.rest.rest import get

    constants.use_prod()

    # Authenticate with API key + RSA private key
    auth.set_key(
        access_key=os.environ['KALSHI_READ_KEYID'],
        private_key_path='/Users/jstenger/.kalshi/private_key.pem',
    )

    url = f"{constants.BASE_URL}{constants.BASE_PATH}/markets"
    headers = auth.request_headers("GET", url)
    markets = get(url, headers=headers, event_ticker=event_ticker)
    return [m['yes_sub_title'] for m in markets['markets']]


def count_phrases(text, phrases, compiled_patterns=None):
    """Count occurrences of phrases in text
    
    Args:
        text: Text to search
        phrases: Dictionary of phrase name -> pattern string
        compiled_patterns: Optional dict of pre-compiled patterns for performance
    """
    counts = {}
    text_lower = text.lower()  # Only lowercase once
    for name, pattern in phrases.items():
        if compiled_patterns and name in compiled_patterns:
            counts[name] = len(compiled_patterns[name].findall(text_lower))
        else:
            counts[name] = len(re.findall(pattern, text_lower))
    return counts


def compile_patterns(phrases):
    """Pre-compile regex patterns for better performance"""
    return {name: re.compile(pattern) for name, pattern in phrases.items()}

def get_date_from_filename(filename):
    """
    Extract date from common filename formats:
    - YYYY-MM-DD_...
    - YYYY_...
    - YYYY-title
    - title.yyyymmdd.txt
    - yyyymmdd.title.txt
    - Just a date (e.g. 2023-04-18 or 20230418)
    """
    try:
        name, ext = os.path.splitext(filename)

        # Allow bare file name that's just a date (e.g. 2023-04-18, 20230418, or 2023)
        bare = name.strip()
        # YYYY-MM-DD
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}', bare):
            return datetime.strptime(bare, '%Y-%m-%d')
        # YYYYMMDD
        if re.fullmatch(r'\d{8}', bare):
            return datetime.strptime(bare, '%Y%m%d')
        # YYYY only
        if re.fullmatch(r'\d{4}', bare):
            return datetime.strptime(bare, '%Y')

        # Check for title.yyyymmdd(.txt) format
        m = re.match(r'^(.*\.)?(\d{8})$', name)
        if m:
            date = datetime.strptime(m.group(2), '%Y%m%d')
            return date

        # Check for yyyymmdd.title(.txt) format (e.g., 20221006.Mr. Beast Gets Questioned by the IRS.txt)
        m = re.match(r'^(\d{8})\.', name)
        if m:
            date = datetime.strptime(m.group(1), '%Y%m%d')
            return date

        # Split by underscore to check underscore-led formats
        parts = filename.split('_')
        date_str = parts[0]

        # Hyphenated formats: YYYY-MM-DD or YYYY-title
        if '-' in date_str:
            hyphen_parts = date_str.split('-')
            if len(hyphen_parts) == 3:  # YYYY-MM-DD
                return datetime.strptime(date_str, '%Y-%m-%d')
            elif len(hyphen_parts) == 2 and len(hyphen_parts[0]) == 4:  # YYYY-title
                return datetime.strptime(hyphen_parts[0], '%Y')

        # Four-digit year
        if len(date_str) == 4 and date_str.isdigit():
            return datetime.strptime(date_str, '%Y')

        # Fallback: try YYYY-MM-DD in base name
        try:
            return datetime.strptime(date_str, '%Y-%m-%d')
        except Exception:
            pass

        # Fallback: look for 8-digit number anywhere in base name
        m2 = re.search(r'(\d{8})', name)
        if m2:
            return datetime.strptime(m2.group(1), '%Y%m%d')
    except (ValueError, IndexError):
        pass
    return None

def read_transcript(filepath):
    """Read and return transcript text"""
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()

def find_phrase_context(text, pattern, window=200):
    """Find phrase in text with surrounding context"""
    matches = []
    for match in re.finditer(pattern, text.lower()):
        start = max(0, match.start() - window)
        end = min(len(text), match.end() + window)
        context = text[start:end]
        # Add ellipsis if we're not at the start/end of the text
        if start > 0:
            context = '...' + context
        if end < len(text):
            context = context + '...'
        matches.append(context)
    return matches

def process_directory(directory, phrases, verbose=True, post_inaug=False):
    """Process transcripts from speech and sotu directories only

    Args:
        directory: Path to directory containing transcript files
        phrases: Dictionary of phrase patterns to search for
        verbose: Whether to print progress (default True)
        post_inaug: If True, only include transcripts dated after 2025-01-20
    """
    results = []
    categories_found = set()
    
    # Pre-compile patterns for better performance
    compiled = compile_patterns(phrases)
    
    # First, count total files for progress reporting
    total_files = 0
    for root, _, files in os.walk(directory):
        total_files += sum(1 for f in files if f.endswith('.txt'))
    
    if verbose:
        print(f"Found {total_files} .txt files to process with {len(phrases)} patterns...")
    
    processed = 0
    for root, _, files in os.walk(directory):
        category = os.path.basename(root)
        categories_found.add(category)
        for file in files:
            if file.endswith('.txt'):
                filepath = os.path.join(root, file)
                try:
                    date = get_date_from_filename(file)
                    if post_inaug and date and date <= datetime(2025, 1, 20):
                        processed += 1
                        continue
                    text = read_transcript(filepath)
                    counts = count_phrases(text, phrases, compiled)

                    results.append({
                        'date': date,
                        'file': file,
                        'category': category,
                        'text_length': len(text.split()),
                        'text': text,  # Store full text for context analysis
                        **counts
                    })
                    processed += 1
                    if verbose and processed % 500 == 0:
                        print(f"  Processed {processed}/{total_files} files... (current: {category}/{file[:50]})")
                except Exception as e:
                    print(f"\nError processing {filepath}: {str(e)}")
    
    if verbose:
        print(f"\nCompleted processing {processed} files.")
        print("Categories found in directory:")
        for cat in sorted(categories_found):
            print(f"- {cat}")
    
    return pd.DataFrame(results)

def process_json(json_file, phrases):
    """Process transcripts from a JSON file with schema: {filename: {"transcript": text, ...}, ...}"""
    import json
    
    results = []
    
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        for filename, file_data in data.items():
            if 'transcript' in file_data:
                try:
                    date = get_date_from_filename(filename)
                    text = file_data['transcript']
                    counts = count_phrases(text, phrases)
                    
                    results.append({
                        'date': date,
                        'file': filename,
                        'category': 'json',  # Default category for JSON files
                        'text_length': len(text.split()),
                        'text': text,  # Store full text for context analysis
                        **counts
                    })
                except Exception as e:
                    print(f"Error processing {filename}: {str(e)}")
            else:
                print(f"Warning: No 'transcript' field found in {filename}")
    
    except Exception as e:
        print(f"Error reading JSON file {json_file}: {str(e)}")
        return pd.DataFrame()
    
    return pd.DataFrame(results)


def plot_phrase_frequency_over_time(
    df_category,
    df_non_category,
    phrase,
    window=30,
    start_date=None,
    end_date=None,
    show_monthly_grid=False,
    show_weekly_grid=False,
    log_scale=False,
    show_moving_average=True
):
    """Plot the frequency of a phrase over time with separate lines for category and non-category

    Args:
        df_category: DataFrame for category.
        df_non_category: DataFrame for non-category.
        phrase: Phrase to plot.
        window: Rolling window for moving average.
        start_date: Filter start date.
        end_date: Filter end date.
        show_monthly_grid: Show monthly grid.
        show_weekly_grid: Show weekly grid.
        log_scale: Use log scale for y-axis.
        show_moving_average: If False, do not plot moving average lines.
    """
    from IPython.display import HTML, display

    plt.figure(figsize=(15, 6))

    # Filter data to start from specified date if provided
    if start_date is not None:
        start_date = pd.Timestamp(start_date)
        df_category = df_category[df_category['date'] >= start_date].copy()
        if df_non_category is not None:
            df_non_category = df_non_category[df_non_category['date'] >= start_date].copy()
    else:
        df_category = df_category.copy()
        if df_non_category is not None:
            df_non_category = df_non_category.copy()

    # Filter data to end at specified date if provided
    if end_date is not None:
        end_date = pd.Timestamp(end_date)
        df_category = df_category[df_category['date'] <= end_date].copy()
        if df_non_category is not None:
            df_non_category = df_non_category[df_non_category['date'] <= end_date].copy()

    # Process category data
    df_category[f'{phrase}_freq'] = (df_category[phrase] / df_category['text_length']) * 1000
    category_series = df_category.set_index('date')[f'{phrase}_freq']
    category_rolling = category_series.rolling(window=window, min_periods=1).mean()

    # Process non-category data only if it exists
    if df_non_category is not None and not df_non_category.empty:
        df_non_category[f'{phrase}_freq'] = (df_non_category[phrase] / df_non_category['text_length']) * 1000
        non_category_series = df_non_category.set_index('date')[f'{phrase}_freq']
        non_category_rolling = non_category_series.rolling(window=window, min_periods=1).mean()
    else:
        non_category_series = pd.Series(dtype=float)
        non_category_rolling = pd.Series(dtype=float)

    # Store original values for y-axis labels
    original_category_series = category_series.copy()
    original_category_rolling = category_rolling.copy()
    original_non_category_series = non_category_series.copy()
    original_non_category_rolling = non_category_rolling.copy()

    # Apply log scale if requested
    if log_scale:
        # Add small constant to avoid log(0)
        category_series = np.log10(category_series + 0.1)
        category_rolling = np.log10(category_rolling + 0.1)
        if not non_category_series.empty:
            non_category_series = np.log10(non_category_series + 0.1)
            non_category_rolling = np.log10(non_category_rolling + 0.1)

    # Plot category data
    plt.scatter(
        category_series.index,
        category_series.values,
        alpha=0.3,
        color='red',
        label='Category Transcripts'
    )
    if show_moving_average:
        plt.plot(
            category_rolling.index,
            category_rolling.values,
            'r-',
            linewidth=2,
            label=f'Category {window}-day Average'
        )

    # Plot non-category data only if it exists
    if not non_category_series.empty:
        plt.scatter(
            non_category_series.index,
            non_category_series.values,
            alpha=0.3,
            color='blue',
            label='Non-Category Transcripts'
        )
        if show_moving_average:
            plt.plot(
                non_category_rolling.index,
                non_category_rolling.values,
                'b-',
                linewidth=2,
                label=f'Non-Category {window}-day Average'
            )

    title = f'Frequency of "{phrase}" Over Time'
    ylabel = 'Occurrences per 1000 words'
    if log_scale:
        title += ' (Log Scale)'

    plt.title(title)
    plt.xlabel('Date')
    plt.ylabel(ylabel)
    plt.legend()

    # Only show default grid if neither weekly nor monthly grid is enabled
    if not show_weekly_grid and not show_monthly_grid:
        plt.grid(True, alpha=0.3)

    # Add vertical line for inauguration
    plt.axvline(x=datetime(2025, 1, 20), color='k', linestyle='--', alpha=0.5, label='Inauguration')

    # Add monthly vertical lines and counts only if requested
    if show_monthly_grid:
        combined_df = df_category.copy()
        if df_non_category is not None and not df_non_category.empty:
            combined_df = pd.concat([df_category, df_non_category])

        if not combined_df.empty:
            date_range = pd.date_range(
                start=combined_df['date'].min().replace(day=1),
                end=combined_df['date'].max() + pd.DateOffset(months=1),
                freq='MS'  # Month start
            )

            # Calculate monthly statistics
            combined_df['year_month'] = combined_df['date'].dt.to_period('M')
            monthly_stats = combined_df.groupby('year_month').agg({
                phrase: 'sum',
                'date': 'count'
            }).rename(columns={'date': 'transcript_count'})

            # Calculate % of months with 1+ occurrence
            months_with_occurrence = (monthly_stats[phrase] >= 1).sum()
            total_months = len(monthly_stats)
            pct_months_with_occurrence = (months_with_occurrence / total_months * 100) if total_months > 0 else 0

            # Add monthly vertical lines and counts
            for date in date_range:
                plt.axvline(x=date, color='gray', linestyle=':', alpha=0.5)

                # Get monthly count for this date
                period = date.to_period('M')
                if period in monthly_stats.index:
                    monthly_count = monthly_stats.loc[period, phrase]
                    if monthly_count > 0:
                        # Position text at top of plot
                        ymax = min(8, max(
                            original_category_rolling.max() if not original_category_rolling.empty else 0,
                            original_non_category_rolling.max() if not original_non_category_rolling.empty else 0
                        ) * 1.1)
                        if log_scale:
                            ymax = np.log10(ymax + 0.1)
                        plt.text(date, ymax * 0.95, str(int(monthly_count)),
                                 ha='center', va='top', fontsize=8,
                                 bbox=dict(boxstyle='round,pad=0.2', facecolor='white', alpha=0.7))

            # Add percentage to title
            title += f'\n({pct_months_with_occurrence:.1f}% of months with ≥1 occurrence)'
            plt.title(title)

    # Add weekly vertical lines and counts only if requested
    if show_weekly_grid:
        combined_df = df_category.copy()
        if df_non_category is not None and not df_non_category.empty:
            combined_df = pd.concat([df_category, df_non_category])

        if not combined_df.empty:
            # Find the first Monday on or before the minimum date
            min_date = combined_df['date'].min()
            days_since_monday = min_date.weekday()  # Monday is 0
            first_monday = min_date - pd.Timedelta(days=days_since_monday)

            # Create weekly date range (Mondays)
            date_range = pd.date_range(
                start=first_monday,
                end=combined_df['date'].max() + pd.DateOffset(weeks=1),
                freq='W-MON'  # Weekly on Monday
            )

            # Calculate weekly statistics
            combined_df['year_week'] = combined_df['date'].dt.to_period('W-MON')
            weekly_stats = combined_df.groupby('year_week').agg({
                phrase: 'sum',
                'date': 'count'
            }).rename(columns={'date': 'transcript_count'})

            # Calculate % of weeks with 1+ occurrence
            weeks_with_occurrence = (weekly_stats[phrase] >= 1).sum()
            total_weeks = len(weekly_stats)
            pct_weeks_with_occurrence = (weeks_with_occurrence / total_weeks * 100) if total_weeks > 0 else 0

            # Add weekly vertical lines and counts
            for date in date_range:
                plt.axvline(x=date, color='lightblue', linestyle=':', alpha=0.5)

                # Get weekly count for this date
                period = date.to_period('W-MON')
                if period in weekly_stats.index:
                    weekly_count = weekly_stats.loc[period, phrase]
                    if weekly_count > 0:
                        # Position text at top of plot
                        ymax = min(8, max(
                            original_category_rolling.max() if not original_category_rolling.empty else 0,
                            original_non_category_rolling.max() if not original_non_category_rolling.empty else 0
                        ) * 1.1)
                        if log_scale:
                            ymax = np.log10(ymax + 0.1)
                        plt.text(date, ymax * 0.85, str(int(weekly_count)),
                                 ha='center', va='top', fontsize=7,
                                 bbox=dict(boxstyle='round,pad=0.2', facecolor='lightblue', alpha=0.7))

            # Add percentage to title
            title += f'\n({pct_weeks_with_occurrence:.1f}% of weeks with ≥1 occurrence)'
            plt.title(title)

    # Auto-adjust y-axis limit and set custom labels for log scale
    if log_scale:
        ymax = max(
            original_category_rolling.max() if not original_category_rolling.empty else 0,
            original_non_category_rolling.max() if not original_non_category_rolling.empty else 0
        ) * 1.1  # Add 10% padding

        # Set y-axis limits in log scale
        plt.ylim(np.log10(0.1), np.log10(ymax + 0.1))

        # Create custom y-axis labels showing original values
        original_ticks = [0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50]
        log_ticks = [np.log10(tick) for tick in original_ticks if tick <= ymax]
        tick_labels = [str(tick) if tick >= 1 else f"{tick:.1f}" for tick in original_ticks if tick <= ymax]

        plt.yticks(log_ticks, tick_labels)
    else:
        ymax = min(8, max(
            original_category_rolling.max() if not original_category_rolling.empty else 0,
            original_non_category_rolling.max() if not original_non_category_rolling.empty else 0
        ) * 1.1)  # Add 10% padding
        plt.ylim(0, ymax)

    plt.tight_layout()

    # Save plot to base64 string for embedding in HTML
    import io
    import base64

    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    buf.seek(0)
    plot_data = base64.b64encode(buf.read()).decode()
    plt.close()  # Close the figure to free memory

    # Create collapsible HTML
    html = f"""
    <details>
        <summary style="cursor: pointer; font-weight: bold; font-size: 14px; padding: 5px;">
            📊 {phrase} - Frequency Over Time
        </summary>
        <div style="margin-top: 10px;">
            <img src="data:image/png;base64,{plot_data}" style="max-width: 100%; height: auto;">
        </div>
    </details>
    """

    display(HTML(html))

def plot_phrase_frequency_over_time_all(
    df_category,
    df_non_category=None,
    phrases=None,
    log_scale=False,
    show_monthly_grid=False,
    show_weekly_grid=False,
    window=30,
    start_date=None,
    end_date=None,
    show_moving_average=True
):
    if phrases is None:
        print("No phrases provided")
        return

    phrase_freqs = {}
    for phrase in phrases.keys():
        # Calculate average frequency across all data
        category_freq = (df_category[phrase].sum() / df_category['text_length'].sum()) * 1000 if not df_category.empty and df_category['text_length'].sum() > 0 else 0
        non_category_freq = (df_non_category[phrase].sum() / df_non_category['text_length'].sum()) * 1000 if df_non_category is not None and not df_non_category.empty and df_non_category['text_length'].sum() > 0 else 0
        phrase_freqs[phrase] = (category_freq + non_category_freq) / 2

    for phrase in phrase_freqs:
        plot_phrase_frequency_over_time(
            df_category,
            df_non_category if df_non_category is not None else None,
            phrase,
            log_scale=log_scale,
            show_monthly_grid=show_monthly_grid,
            show_weekly_grid=show_weekly_grid,
            window=window,
            start_date=start_date,
            end_date=end_date,
            show_moving_average=show_moving_average
        )

def plot_phrases_individual(
    df,
    phrase,
    show_word_counts=False,
    spread_appearances_evenly=False,
    show_filenames=False,  # NEW FLAG
):
    """
    Plot the frequency of a phrase over time, or spread each appearance evenly in time order.

    Args:
        df: DataFrame containing the data.
        phrase: The phrase/column to plot.
        show_word_counts: Whether to plot word counts as secondary bars.
        spread_appearances_evenly: If True, plot each appearance evenly along the x-axis in chronological order.
        show_filenames: If True, show filenames as x-axis labels (rotated), instead of dates.
    """

    # Get raw counts and dates
    counts = df[phrase]
    dates = df['date']
    filenames = df['file'] if 'file' in df.columns else None

    # Calculate statistics
    avg = counts.mean()
    appearances_with_phrase = (counts > 0).sum()
    total_appearances = len(counts)
    percentage = (appearances_with_phrase / total_appearances) * 100 if total_appearances > 0 else 0

    from IPython.display import HTML, display
    import uuid

    plot_id = str(uuid.uuid4())

    fig, ax1 = plt.subplots(figsize=(15, 6))

    # SPREAD EVENLY OPTION
    if spread_appearances_evenly:
        # Sort by date
        df_sorted = df.sort_values('date').reset_index(drop=True)
        counts_sorted = df_sorted[phrase].to_numpy()
        word_counts_sorted = df_sorted['text_length'].to_numpy()
        dates_sorted = df_sorted['date'].to_numpy()
        filenames_sorted = df_sorted['file'].to_numpy() if 'file' in df_sorted.columns else None

        # Each x value is equally spaced, label only occasional ticks
        xvals = np.arange(len(df_sorted))
        bars = ax1.bar(xvals, counts_sorted, alpha=0.7, color='blue', label='Appearances', width=1.0)

        # Add horizontal line for average
        ax1.axhline(y=avg, color='red', linestyle='--', alpha=0.7,
                    label=f'Average ({avg:.1f} appearances)')

        if show_filenames and filenames_sorted is not None:
            label_step = max(1, len(filenames_sorted)//15)
            # Truncate or wrap long names, but keep them readable
            def wrap(s, width=18):
                if len(s) <= width:
                    return s
                import textwrap
                return '\n'.join(textwrap.wrap(s, width=width))
            tick_labels = [wrap(filenames_sorted[idx]) for idx in range(len(filenames_sorted)) if idx % label_step == 0]
            ax1.set_xticks(xvals[::label_step])
            ax1.set_xticklabels(tick_labels, rotation=90, fontsize=7, ha='center', va='top')  # Smaller font size
            ax1.set_xlabel('File (chronological order)')
        else:
            # X-axis labels: Only show every Nth to avoid crowding and convert dates to pandas Timestamp for formatting
            label_step = max(1, len(dates_sorted)//15)

            # Helper to ensure formatting works for all date-like types
            def safe_strftime(date):
                import pandas as pd
                if pd.isnull(date):
                    return ""
                # Convert numpy.datetime64 or other date-like to pandas.Timestamp first
                try:
                    date_ts = pd.Timestamp(date)
                    return date_ts.strftime('%Y-%m-%d')
                except Exception:
                    return str(date)

            tick_labels = [safe_strftime(dates_sorted[idx]) for idx in range(len(dates_sorted)) if idx % label_step == 0]
            ax1.set_xticks(xvals[::label_step])
            ax1.set_xticklabels(tick_labels, rotation=45, ha='right')
            ax1.set_xlabel('Appearances (chronological order)')
    else:
        # Normal time series bar plot
        if show_word_counts:
            bar_width = 1.6
            offset = bar_width / 2
            bars = ax1.bar([d - pd.Timedelta(days=offset) for d in dates], counts,
                           alpha=0.7, color='blue', label='Appearances', width=bar_width)
        else:
            bars = ax1.bar(dates, counts, alpha=0.7, color='blue', label='Appearances', width=1.6)

        ax1.axhline(y=avg, color='red', linestyle='--', alpha=0.7,
                    label=f'Average ({avg:.1f} appearances)')

        if show_filenames and filenames is not None:
            label_step = max(1, len(filenames)//15)
            def wrap(s, width=18):
                if len(s) <= width:
                    return s
                import textwrap
                return '\n'.join(textwrap.wrap(s, width=width))
            tick_labels = [wrap(filenames.iloc[idx]) for idx in range(len(filenames)) if idx % label_step == 0]
            ax1.set_xticks(dates[::label_step])
            ax1.set_xticklabels(tick_labels, rotation=70, fontsize=8, ha='center', va='top')  # Smaller font size
            ax1.set_xlabel('File')
        else:
            # Ensure dates are formatted robustly
            def safe_strftime(date):
                import pandas as pd
                if pd.isnull(date):
                    return ""
                try:
                    date_ts = pd.Timestamp(date)
                    return date_ts.strftime('%Y-%m-%d')
                except Exception:
                    return str(date)
            ax1.set_xticks(dates)
            ax1.set_xticklabels([safe_strftime(d) for d in dates], rotation=45, ha='right')
            ax1.set_xlabel('Date')

    ax1.set_title(f'Appearances of "{phrase}"  ({percentage:.1f}% of files)')
    ax1.set_ylabel('Number of Appearances', color='blue')
    ax1.tick_params(axis='y', labelcolor='blue')
    ax1.grid(True, alpha=0.3)

    # Add word counts on secondary axis if requested
    if show_word_counts:
        ax2 = ax1.twinx()
        if spread_appearances_evenly:
            bars2 = ax2.bar(xvals + 0.45, word_counts_sorted,
                            alpha=0.7, color='lightcoral', label='Word Count', width=0.9)
        else:
            word_counts = df['text_length']
            bar_width = 1.6  # 2x wide
            offset = bar_width / 2
            bars2 = ax2.bar([d + pd.Timedelta(days=offset) for d in dates], word_counts,
                            alpha=0.7, color='lightcoral', label='Word Count', width=bar_width)
        ax2.set_ylabel('Word Count', color='lightcoral')
        ax2.tick_params(axis='y', labelcolor='lightcoral')

        # Combine legends
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
    else:
        ax1.legend()

    plt.tight_layout()

    import io
    import base64
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', dpi=100)
    buf.seek(0)
    plot_data = base64.b64encode(buf.read()).decode()
    plt.close()

    html = f"""
    <details>
        <summary style="cursor: pointer; font-weight: bold; font-size: 16px; margin: 10px 0;">
            📊 {phrase} - {percentage:.1f}% of appearances ({appearances_with_phrase}/{total_appearances})
        </summary>
        <div style="margin: 10px 0;">
            <img src="data:image/png;base64,{plot_data}" style="max-width: 100%; height: auto;">
        </div>
    </details>
    """

    display(HTML(html))

def plot_phrases_individual_all(df, phrases, show_word_counts=False, spread_appearances_evenly=False, show_filenames=False):
    for phrase in phrases.keys():
        plot_phrases_individual(
            df,
            phrase,
            show_word_counts=show_word_counts,
            spread_appearances_evenly=spread_appearances_evenly,
            show_filenames=show_filenames
        )




def plot_length_distribution(df_category):
    """Plot the distribution of speech lengths"""
    expected_length = df_category['text_length'].mean()
    print(f"Expected length from category data: {expected_length:.0f} words")

    # Show histogram of category lengths
    plt.figure(figsize=(12, 6))
    plt.hist(df_category['text_length'], bins=30, alpha=0.7, color='skyblue', edgecolor='black')
    plt.axvline(x=expected_length, color='red', linestyle='--', label=f'Mean Length ({int(expected_length):,} words)')
    plt.xlabel('Speech Length (words)')
    plt.ylabel('Number of Speeches')
    plt.title(f'Distribution of Speech Lengths in Category')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()


def analyze_files_phrase_occurrences(df, phrases):
    """Analyze phrase occurrences across a list of files"""
    # Filter dataframe to only include the specified files
    from IPython.display import display
    if df.empty:
        print("No matching files found.")
        return
    
    print(f"Found {len(df)} matching files:")
    print()
    
    # Display the file paths
    for filename in df['file']:
        print(f"  {filename}")
    
    print()
    
    # Create data for the table
    all_data = []
    
    for idx, row in df.iterrows():
        filename = row['file']
        word_count = row['text_length']
        
        # Add row for each phrase occurrence
        for phrase in phrases.keys():
            count = int(row[phrase])
            all_data.append({
                'File': filename,
                'Word Count': word_count,
                'Phrase': phrase,
                'Count': count
            })
    # Create DataFrame and pivot for better display
    analysis_df = pd.DataFrame(all_data)
    
    if not analysis_df.empty:
        # Create pivot table with phrases as rows and files as columns
        pivot_df = analysis_df.pivot_table(
            index='Phrase', 
            columns='File', 
            values='Count', 
            fill_value=0
        ).astype(int)
        
        # Add word count row to the pivot table
        word_count_row = df[['file', 'text_length']].drop_duplicates().set_index('file')['text_length']
        word_count_row.name = 'Word Count'
        pivot_df = pd.concat([pd.DataFrame([word_count_row]), pivot_df])
        
        # If more than 1 file, add average column
        if len(pivot_df.columns) > 1:
            # Calculate average for word count row
            avg_word_count = pivot_df.iloc[0].mean()
            
            # Calculate average for phrase rows
            phrase_rows = pivot_df.iloc[1:]
            avg_phrase_counts = phrase_rows.mean(axis=1)
            
            # Add average column
            pivot_df['Average'] = pd.concat([pd.Series([avg_word_count], index=['Word Count']), avg_phrase_counts])
        
        # Sort by total occurrences (sum across all files) - excluding word count row and average column
        phrase_rows = pivot_df.iloc[1:]  # Skip word count row
        # Exclude average column from sorting calculation if it exists
        sort_columns = [col for col in phrase_rows.columns if col != 'Average']
        phrase_rows['Total'] = phrase_rows[sort_columns].sum(axis=1)
        phrase_rows = phrase_rows.sort_values('Total', ascending=False)
        phrase_rows = phrase_rows.drop('Total', axis=1)  # Remove the total column after sorting
        
        # Combine word count row with sorted phrase rows
        pivot_df = pd.concat([pivot_df.iloc[:1], phrase_rows])
        
        print("\nPhrase occurrences across selected files (sorted by total occurrences):")
        # Style the table with wrapped column headers
        styled_df = pivot_df.style.set_table_styles([
            {'selector': 'th.col_heading', 
             'props': [('max-width', '150px'), 
                      ('word-wrap', 'break-word'),
                      ('white-space', 'normal'),
                      ('text-align', 'center')]},
            {'selector': 'td', 
             'props': [('text-align', 'center')]}
        ])
        
        display(styled_df)
        
        # Summary statistics
        print(f"\nSummary:")
        print(f"Total files processed: {len([col for col in pivot_df.columns if col != 'Average'])}")
        print(f"Total word count: {df['text_length'].sum():,}")
        
    else:
        print("\nNo data found for the specified files.")


def get_recent_contexts(df, phrase, phrases, n=10):
    """Get the n most recent contexts for a phrase"""
    # Create a list to store matches with their dates
    all_matches = []
    
    # Look through speeches from newest to oldest
    for _, row in df.sort_values('date', ascending=False).iterrows():
        matches = find_phrase_context(row['text'], phrases[phrase])
        for match in matches:
            all_matches.append({
                'title': row['file'],
                'date': row['date'],
                'category': row['category'],
                'context': match
            })
        if len(all_matches) >= n:
            break
    
    return pd.DataFrame(all_matches[:n])

def get_recent_contexts_all(df, phrases, n=10):
    """Get the n most recent contexts for each phrase"""
    for phrase in phrases.keys():
        contexts = get_recent_contexts(df, phrase, phrases, n=n)
        if not contexts.empty:
            print(f"\n=== Recent usage of '{phrase}' ===\n")
            for _, row in contexts.iterrows():
                print(f"Title: {row['title']}")
                if(row['date'] is not None):
                    print(f"Date: {row['date'].strftime('%Y-%m-%d')} ({row['category']})")
                else:
                    print(f"Date: Unknown ({row['category']})")
                print(f"Context: {row['context']}\n")

def calculate_confidence_interval(successes: int, n: int, confidence: float = 0.95, 
                                   method: str = 'wilson') -> tuple:
    """
    Calculate confidence interval for a proportion.
    
    Args:
        successes: Number of successes
        n: Total number of trials
        confidence: Confidence level (default 0.95)
        method: 'wilson' (recommended), 'agresti-coull', or 'clopper-pearson' (exact)
        
    Returns:
        Tuple of (lower_bound, upper_bound, point_estimate) as proportions
    """
    if n == 0:
        return (0.0, 1.0, 0.5)
    
    p = successes / n
    alpha = 1 - confidence
    z = stats.norm.ppf(1 - alpha/2)
    
    if method == 'wilson':
        # Wilson score interval - best for small samples
        denominator = 1 + z**2/n
        center = (p + z**2/(2*n)) / denominator
        margin = z * np.sqrt((p*(1-p)/n + z**2/(4*n**2))) / denominator
        lower = max(0, center - margin)
        upper = min(1, center + margin)
        
    elif method == 'agresti-coull':
        # Agresti-Coull interval - simple and robust
        n_tilde = n + z**2
        p_tilde = (successes + z**2/2) / n_tilde
        margin = z * np.sqrt(p_tilde * (1 - p_tilde) / n_tilde)
        lower = max(0, p_tilde - margin)
        upper = min(1, p_tilde + margin)
        
    elif method == 'clopper-pearson':
        # Clopper-Pearson exact interval - conservative
        if successes == 0:
            lower = 0.0
        else:
            lower = stats.beta.ppf(alpha/2, successes, n - successes + 1)
        if successes == n:
            upper = 1.0
        else:
            upper = stats.beta.ppf(1 - alpha/2, successes + 1, n - successes)
    else:
        raise ValueError(f"Unknown method: {method}")
    
    return (lower, upper, p)


def apply_multiple_testing_correction(p_values: list, method: str = 'bonferroni') -> list:
    """
    Apply multiple hypothesis testing correction.
    
    Args:
        p_values: List of p-values to correct
        method: 'bonferroni', 'holm', 'fdr_bh' (Benjamini-Hochberg)
        
    Returns:
        List of corrected p-values
    """
    from scipy.stats import false_discovery_control
    
    n = len(p_values)
    if n == 0:
        return []
    
    p_array = np.array(p_values)
    
    if method == 'bonferroni':
        # Simple Bonferroni correction
        corrected = np.minimum(p_array * n, 1.0)
        
    elif method == 'holm':
        # Holm-Bonferroni step-down method
        sorted_indices = np.argsort(p_array)
        sorted_p = p_array[sorted_indices]
        corrected = np.zeros(n)
        
        for i, idx in enumerate(sorted_indices):
            corrected[idx] = min(sorted_p[i] * (n - i), 1.0)
        
        # Enforce monotonicity
        corrected_sorted = corrected[sorted_indices]
        for i in range(1, n):
            corrected_sorted[i] = max(corrected_sorted[i], corrected_sorted[i-1])
        corrected[sorted_indices] = corrected_sorted
        
    elif method == 'fdr_bh':
        # Benjamini-Hochberg FDR control
        sorted_indices = np.argsort(p_array)
        sorted_p = p_array[sorted_indices]
        corrected = np.zeros(n)
        
        for i, idx in enumerate(sorted_indices):
            corrected[idx] = sorted_p[i] * n / (i + 1)
        
        # Enforce monotonicity (from right to left)
        corrected_sorted = corrected[sorted_indices]
        for i in range(n - 2, -1, -1):
            corrected_sorted[i] = min(corrected_sorted[i], corrected_sorted[i+1])
        corrected[sorted_indices] = np.minimum(corrected_sorted, 1.0)
        
    else:
        raise ValueError(f"Unknown method: {method}")
    
    return corrected.tolist()


def get_adjusted_confidence_level(num_hypotheses: int, family_confidence: float = 0.95,
                                   method: str = 'bonferroni') -> float:
    """
    Get adjusted confidence level for individual tests in multiple testing.
    
    Args:
        num_hypotheses: Number of hypotheses being tested
        family_confidence: Desired family-wise confidence level
        method: Correction method
        
    Returns:
        Adjusted confidence level for individual tests
    """
    alpha = 1 - family_confidence
    
    # Guard against division by zero
    if num_hypotheses <= 0:
        return family_confidence
    
    if method == 'bonferroni':
        adjusted_alpha = alpha / num_hypotheses
    elif method == 'sidak':
        adjusted_alpha = 1 - (1 - alpha) ** (1 / num_hypotheses)
    else:
        adjusted_alpha = alpha / num_hypotheses  # Default to Bonferroni
    
    return 1 - adjusted_alpha


def get_per_appearance_frequency(dfs_dict, phrases, show_confidence_interval=False,
                                  ci_method='wilson', multiple_testing_correction=None,
                                  family_confidence=0.95, return_df=False):
    """
    Calculate percentage of appearances containing each phrase at least once.
    
    Args:
        dfs_dict: Dictionary mapping dataset names to DataFrames
        phrases: Dictionary of phrase patterns
        show_confidence_interval: Whether to show confidence intervals
        ci_method: Method for CI calculation ('wilson', 'agresti-coull', 'clopper-pearson')
        multiple_testing_correction: Correction method ('bonferroni', 'holm', 'fdr_bh', or None)
        family_confidence: Family-wise confidence level (default 0.95)
        return_df: If True, return the DataFrame instead of displaying
        
    Returns:
        DataFrame if return_df=True, else None (displays the table)
    """
    num_phrases = len(phrases)
    
    # Return early if no phrases
    if num_phrases == 0:
        if return_df:
            return pd.DataFrame()
        return None
    
    # Adjust confidence level if using multiple testing correction
    if multiple_testing_correction and show_confidence_interval:
        individual_confidence = get_adjusted_confidence_level(
            num_phrases, family_confidence, multiple_testing_correction
        )
    else:
        individual_confidence = family_confidence
    
    phrase_percentages = {}
    for phrase in phrases.keys():
        phrase_data = {}
        for name, df in dfs_dict.items():
            n = len(df)
            successes = (df[phrase] >= 1).sum()
            pct = round((successes / n) * 100, 2) if n > 0 else 0
            
            if show_confidence_interval and n > 0:
                lower, upper, _ = calculate_confidence_interval(
                    successes, n, individual_confidence, ci_method
                )
                ci_lower = lower * 100
                ci_upper = upper * 100
                phrase_data[name] = f"{pct}% ({ci_lower:.1f}-{ci_upper:.1f}%)"
            else:
                phrase_data[name] = pct
        
        phrase_percentages[phrase] = phrase_data

    # Create DataFrame and sort by the first column
    phrase_df = pd.DataFrame.from_dict(phrase_percentages, orient='index')
    first_col = phrase_df.columns[0]
    
    if not show_confidence_interval:
        phrase_df = phrase_df.sort_values(first_col, ascending=False)
    else:
        # For confidence intervals, sort by extracting numeric value
        phrase_df['_sort_key'] = phrase_df[first_col].astype(str).str.extract(r'(\d+\.?\d*)').astype(float)
        phrase_df = phrase_df.sort_values('_sort_key', ascending=False)
        phrase_df = phrase_df.drop('_sort_key', axis=1)
    
    phrase_df.index.name = 'Phrase'

    # Create summary rows
    summary_values = []
    for idx in ['# of files', 'Average word count']:
        row = []
        for name, df in dfs_dict.items():
            if idx == '# of files':
                row.append(len(df))
            else:  # Average word count
                row.append(round(df['text_length'].mean(), 2) if len(df) > 0 else 0)
        summary_values.append(row)
    
    # Create summary DataFrame
    summary_part = pd.DataFrame(summary_values, index=['# of files', 'Average word count'], columns=phrase_df.columns)
    
    # Combine summary and phrase data
    combined_df = pd.concat([summary_part, phrase_df])

    if return_df:
        return combined_df
    
    title = "Percentage of Appearances Containing Each Phrase"
    if show_confidence_interval:
        conf_pct = int(individual_confidence * 100)
        title += f" (with {conf_pct}% CIs"
        if multiple_testing_correction:
            title += f", {multiple_testing_correction} corrected for {num_phrases} tests"
        title += ")"
    print(title + ":")
    display(combined_df)


def calculate_poisson_predictions(df, avg_length, phrases):
    """Calculate predictions with likelihoods using Poisson distribution"""
    predictions = []
    
    for phrase in phrases.keys():
        # Calculate rate per word
        total_occurrences = df[phrase].sum()
        total_words = df['text_length'].sum()
        rate_per_word = total_occurrences / total_words
        
        # Expected occurrences in SOTU
        expected = rate_per_word * avg_length
        
        # Calculate likelihoods using Poisson PMF
        def poisson_ge_k(lambda_, k):
            return 1 - stats.poisson.cdf(k-1, lambda_)
        
        predictions.append({
            'Phrase': phrase,
            'Expected': expected,
            'Historical Rate': rate_per_word * 1000,  # per 1000 words
            'Total Historical': total_occurrences,
            'P(≥1)': poisson_ge_k(expected, 1)
        })
    
    return pd.DataFrame(predictions)


def calculate_negative_binomial_predictions(df, avg_length, phrases):
    """Calculate predictions using Negative Binomial distribution"""
    predictions = []
    
    for phrase in phrases.keys():
        # Calculate rate per word and variance
        total_occurrences = df[phrase].sum()
        total_words = df['text_length'].sum()
        rate_per_word = total_occurrences / total_words
        
        # Expected occurrences in speech of avg_length
        expected = rate_per_word * avg_length
        
        # Calculate sample variance to estimate overdispersion
        counts_per_speech = df[phrase] / df['text_length'] * avg_length
        sample_variance = counts_per_speech.var()
        
        if sample_variance > expected and expected > 0:  # Check for overdispersion
            # Calculate r parameter for negative binomial using method of moments
            r = max(0.1, expected**2 / (sample_variance - expected))
            p = r/(r + expected)
        else:
            # If no overdispersion, fallback to quasi-Poisson
            r = 100
            p = r/(r + expected)
        
        # Calculate likelihoods using Negative Binomial CDF
        def nb_ge_k(r, p, k):
            return 1 - stats.nbinom.cdf(k-1, r, p)
        
        predictions.append({
            'Phrase': phrase,
            'Expected': expected,
            'Dispersion (r)': r,
            'Sample Variance': sample_variance,
            'P(≥1)': nb_ge_k(r, p, 1),
            'P(≥3)': nb_ge_k(r, p, 3)
        })
    
    return pd.DataFrame(predictions)


def format_prediction_table(df):
    """Format prediction table with percentages"""
    formatted = df.copy()
    formatted = formatted.round(3)
    
    # Format probability columns as percentages
    formatted['P(≥1)'] = formatted['P(≥1)'].map('{:.1%}'.format)
    
    # Round other numeric columns
    formatted['Expected'] = formatted['Expected'].round(2)
    if 'Dispersion (r)' in formatted.columns:
        formatted['Dispersion (r)'] = formatted['Dispersion (r)'].round(2)
        formatted['Sample Variance'] = formatted['Sample Variance'].round(2)
    
    return formatted.sort_values('Expected', ascending=False)
