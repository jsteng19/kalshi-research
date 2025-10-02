import pandas as pd

def filter_netanyahu_news(input_csv_path, output_csv_path):
    # Keywords to identify relevant entries
    keywords = [
        "speech", "interview", "remarks", "press conference", 
        "foreign", "international", "UN", "US", "address", "statement",
        "meeting", "visit", "event", "forum"
    ]
    
    # Netanyahu-specific keywords that must be present
    netanyahu_keywords = ["Netanyahu", "PM", "Prime Minister"]

    # Read the CSV in chunks to handle large files
    chunk_size = 10000  # Adjust as needed
    filtered_rows = []
    filtered_out_rows = []

    for chunk in pd.read_csv(input_csv_path, chunksize=chunk_size):
        # Filter rows where the title contains any of the keywords (case-insensitive)
        keyword_filter = chunk['title'].str.contains('|'.join(keywords), case=False, na=False)
        # AND must contain Netanyahu, PM, or Prime Minister
        netanyahu_filter = chunk['title'].str.contains('|'.join(netanyahu_keywords), case=False, na=False)
        
        # Combined filter for included rows
        combined_filter = keyword_filter & netanyahu_filter
        
        filtered_chunk = chunk[combined_filter]
        filtered_out_chunk = chunk[~combined_filter]
        
        filtered_rows.append(filtered_chunk)
        filtered_out_rows.append(filtered_out_chunk)

    # Concatenate all filtered chunks and save to CSV files
    filtered_df = pd.concat(filtered_rows)
    filtered_df.to_csv(output_csv_path, index=False)
    
    # Save filtered out events to a separate file
    filtered_out_df = pd.concat(filtered_out_rows)
    filtered_out_path = output_csv_path.replace('.csv', '_filtered_out.csv')
    filtered_out_df.to_csv(filtered_out_path, index=False)
    
    print(f"Filtered in: {len(filtered_df)} rows -> {output_csv_path}")
    print(f"Filtered out: {len(filtered_out_df)} rows -> {filtered_out_path}")

if __name__ == "__main__":
    input_path = "/Users/jstenger/Documents/repos/kalshi-research/data-netanyahu/news_urls.csv"
    output_path = "/Users/jstenger/Documents/repos/kalshi-research/data-netanyahu/filtered_netanyahu_news.csv"
    filter_netanyahu_news(input_path, output_path)
