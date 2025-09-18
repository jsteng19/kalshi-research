import os
import re

def process_transcript(input_file, output_file, speaker_patterns=None):
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.read().split('\n\n')  # Split by double newlines to get paragraphs
    
    # Skip the first line (source URL)
    processed_lines = []
    
    # Determine if this is a press briefing from the folder path
    # Use os.path.normpath to normalize path separators for the current OS
    normalized_path = os.path.normpath(input_file)
    path_parts = normalized_path.lower().split(os.sep)
    is_press_briefing = 'press briefing' in path_parts
    
    # Debug info
    print(f"\nProcessing file: {input_file}")
    print(f"Is press briefing: {is_press_briefing}")
    print(f"Path parts: {path_parts}")
    
    # Default speaker patterns if not provided
    if speaker_patterns is None:
        # For press briefings, also look for "Question:" sections
        if is_press_briefing:
            speaker_patterns = [
                r'Karoline\s+Leavitt\s*:\s*',  # More flexible whitespace handling
            ]
        else:
            speaker_patterns = [r'Donald\s+Trump\s*:\s*']
    
    for line in lines[1:]:  # Skip the URL line
        line = line.strip()
        if not line:  # Skip empty lines
            continue
            
        # Try to match any of the speaker patterns
        matched = False
        for pattern in speaker_patterns:
            if re.match(pattern, line, re.IGNORECASE):  # Make case insensitive
                # Remove speaker prefix and clean up
                content = re.sub(pattern, '', line, flags=re.IGNORECASE).strip()
                # Remove audience reactions in square brackets
                content = re.sub(r'\[.*?\]', '', content).strip()
                if content:  # Only add non-empty lines
                    processed_lines.append(content)
                    matched = True
                break
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Write processed content
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n\n'.join(processed_lines))

def process_all_transcripts(data_prefix="data", output_dir="processed-transcripts", speaker_patterns=None):
    """
    Process all transcripts with configurable data directory and speaker patterns.
    
    Args:
        data_prefix (str): Base data directory (e.g., "data" for Trump, "data-harris" for Harris)
        speaker_patterns (list): List of regex patterns for speaker identification
    """
    # Define base directories using os.path.join for cross-platform compatibility
    raw_dir = os.path.join(data_prefix, 'raw-transcripts')
    processed_dir = os.path.join(data_prefix, output_dir)
    
    # Create processed-transcripts directory
    os.makedirs(processed_dir, exist_ok=True)
    
    print(f"Processing directory: {raw_dir}")
    # Walk through raw-transcripts directory
    for root, dirs, files in os.walk(raw_dir):
        for file in files:
            if file.endswith('.txt'):
                # Get relative path components using os.path functions
                rel_path = os.path.relpath(root, raw_dir)
                
                # Construct input and output paths
                input_file = os.path.join(root, file)
                output_dir = os.path.join(processed_dir, rel_path)
                output_file = os.path.join(output_dir, file)
                
                # Process the transcript
                try:
                    process_transcript(input_file, output_file, speaker_patterns)
                    print(f"Processed: {input_file}")
                except Exception as e:
                    print(f"Error processing {input_file}: {str(e)}")

def process_new_transcripts(data_prefix="data", output_dir="processed-transcripts", speaker_patterns=None):
    """Process only transcripts that exist in raw-transcripts but not in processed-transcripts"""
    # Define base directories using os.path.join for cross-platform compatibility
    raw_dir = os.path.join(data_prefix, 'raw-transcripts')
    processed_dir = os.path.join(data_prefix, output_dir)
    
    # Create processed-transcripts directory if it doesn't exist
    os.makedirs(processed_dir, exist_ok=True)
    
    # Keep track of processed files
    processed_files = set()
    
    # Build a set of all processed files with their relative paths
    for root, dirs, files in os.walk(processed_dir):
        rel_path = os.path.relpath(root, processed_dir)
        for file in files:
            if file.endswith('.txt'):
                processed_files.add(os.path.join(rel_path, file))
    
    # Process only new files
    new_files_processed = 0
    for root, dirs, files in os.walk(raw_dir):
        rel_path = os.path.relpath(root, raw_dir)
        for file in files:
            if file.endswith('.txt'):
                rel_file_path = os.path.join(rel_path, file)
                # Skip if file has already been processed
                if rel_file_path in processed_files:
                    continue
                
                # Construct input and output paths
                input_file = os.path.join(raw_dir, rel_file_path)
                output_file = os.path.join(processed_dir, rel_file_path)
                
                # Process the transcript
                try:
                    process_transcript(input_file, output_file, speaker_patterns)
                    print(f"Processed new transcript: {input_file}")
                    new_files_processed += 1
                except Exception as e:
                    print(f"Error processing {input_file}: {str(e)}")
    
    if new_files_processed == 0:
        print("No new transcripts to process.")
    else:
        print(f"Processed {new_files_processed} new transcript(s).")

# Convenience functions for specific politicians
def process_trump_transcripts():
    """Process Trump transcripts with Trump-specific speaker patterns"""
    trump_patterns = [r'Donald\s+Trump\s*:\s*']
    process_all_transcripts("data", trump_patterns)

# Convenience functions for specific politicians
def process_vance_transcripts():
    """Process Trump transcripts with Trump-specific speaker patterns"""
    vance_patterns = [r'J.D.\s+Vance\s*:\s*']
    process_all_transcripts("data", "vance-processed-transcripts", vance_patterns)

def process_harris_transcripts():
    """Process Harris transcripts with Harris-specific speaker patterns"""
    harris_patterns = [
        r'Kamala\s+Harris\s*:\s*',
        r'Vice\s+President\s+Harris\s*:\s*',
        r'VP\s+Harris\s*:\s*',
        r'Harris\s*:\s*'
    ]
    process_all_transcripts("data-harris", harris_patterns)

if __name__ == '__main__':
    process_all_transcripts()  # Process all to test the changes 