import os
from dotenv import load_dotenv
import csv
import argparse
import sys
from datetime import datetime
from googleapiclient.discovery import build

def get_video_links(youtube, channel_name, since_date=None):
    request = youtube.search().list(
        q=channel_name,
        type='channel',
        part='id'
    )
    response = request.execute()
    if not response['items']:
        raise ValueError(f"No channel found for name: {channel_name}")
    channel_id = response['items'][0]['id']['channelId']

    video_links = []
    next_page_token = None

    # Convert since_date to RFC 3339 format if provided
    published_after = None
    if since_date:
        published_after = since_date.strftime('%Y-%m-%dT00:00:00Z')

    while True:
        search_params = {
            'channelId': channel_id,
            'maxResults': 50,
            'order': 'date',
            'type': 'video',
            'part': 'id,snippet',
            'pageToken': next_page_token
        }
        
        if published_after:
            search_params['publishedAfter'] = published_after
        
        request = youtube.search().list(**search_params)
        response = request.execute()

        for item in response['items']:
            video_links.append(f"https://www.youtube.com/watch?v={item['id']['videoId']}")

        next_page_token = response.get('nextPageToken')

        if not next_page_token:
            break

    return video_links

def save_to_csv(video_links, file_path):
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        for link in video_links:
            writer.writerow([link])

def main():
    parser = argparse.ArgumentParser(description="Extract YouTube video links from a channel and save to CSV.")
    parser.add_argument("channel_name", help="Name of the YouTube channel to search for")
    parser.add_argument("output_file", help="File to save the output CSV file")
    parser.add_argument("--since", help="Get videos published on or after this date (format: YYYY-MM-DD)", type=str)
    args = parser.parse_args()

    load_dotenv()
    api_key = os.getenv('YOUTUBE_API_KEY')
    if not api_key:
        print("Error: YOUTUBE_API_KEY environment variable not set.", file=sys.stderr)
        sys.exit(1)
    youtube = build('youtube', 'v3', developerKey=api_key)

    since_date = None
    if args.since:
        try:
            since_date = datetime.strptime(args.since, '%Y-%m-%d')
        except ValueError:
            print(f"Error: Invalid date format '{args.since}'. Use YYYY-MM-DD.", file=sys.stderr)
            sys.exit(1)

    try:
        video_links = get_video_links(youtube, args.channel_name, since_date)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    output_file = os.path.join(args.output_file)
    save_to_csv(video_links, output_file)
    print(f"Saved {len(video_links)} video links to {output_file}")

if __name__ == "__main__":
    main()