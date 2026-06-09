import os
import re

def process_transcript(input_file, output_file, speaker_patterns=None, multi_line=False, generic_speaker_pattern=None):
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
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
    
    processed_lines = []
    
    if multi_line:
        # Multi-line mode: collect all lines for a speaker until next speaker attribution
        lines = content.split('\n')
        # Skip the first line (source URL)
        lines = lines[1:]
        
        # Generic pattern to detect ANY speaker attribution (e.g., "NAME:" or "NAME NAME:")
        # This matches names with hyphens, commas, periods, and multiple words followed by a colon
        # Examples: "SANDERS:", "OCASIO-CORTEZ:", "JOEL PRITIKIN, PRESIDENT, AMERICAN UNIV. COLLEGE REPUBLICANS:"
        if generic_speaker_pattern is None:
             # Updated to allow digits (for timestamps)
             generic_speaker_pattern = r'^[A-Z][A-Z0-9\s.,\-()]+:\s*'
        
        any_speaker_pattern = generic_speaker_pattern
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if not line:  # Skip empty lines
                i += 1
                continue
            
            # Check if this line starts with a speaker pattern we want to capture
            matched = False
            for pattern in speaker_patterns:
                if re.match(pattern, line, re.IGNORECASE):
                    # Extract speaker content from this line
                    speaker_content = re.sub(pattern, '', line, flags=re.IGNORECASE).strip()
                    content_parts = [speaker_content] if speaker_content else []
                    
                    # Look ahead to collect subsequent lines until next speaker
                    i += 1
                    while i < len(lines):
                        next_line = lines[i].strip()
                        if not next_line:  # Skip empty lines but continue collecting
                            i += 1
                            continue
                        
                        # Check if this is ANY speaker attribution (not just ones we want)
                        # This ensures we stop at OTHER speakers too (e.g., MODERATOR, REPORTER, etc.)
                        if re.match(any_speaker_pattern, next_line):
                            break
                        
                        # Add this line to current speaker's content
                        content_parts.append(next_line)
                        i += 1
                    
                    # Join all parts with spaces (remove linebreaks)
                    full_content = ' '.join(content_parts)
                    # Remove audience reactions in square brackets
                    full_content = re.sub(r'\[.*?\]', '', full_content).strip()
                    if full_content:
                        processed_lines.append(full_content)
                    
                    matched = True
                    break
            
            if not matched:
                i += 1
    else:
        # Original single-line mode: split by double newlines to get paragraphs
        lines = content.split('\n\n')
        
        for line in lines[1:]:  # Skip the URL line
            line = line.strip()
            if not line:  # Skip empty lines
                continue
                
            # Try to match any of the speaker patterns
            matched = False
            for pattern in speaker_patterns:
                if re.match(pattern, line, re.IGNORECASE):  # Make case insensitive
                    # Remove speaker prefix and clean up
                    speaker_content = re.sub(pattern, '', line, flags=re.IGNORECASE).strip()
                    # Remove audience reactions in square brackets
                    speaker_content = re.sub(r'\[.*?\]', '', speaker_content).strip()
                    if speaker_content:  # Only add non-empty lines
                        processed_lines.append(speaker_content)
                        matched = True
                    break
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Write processed content
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n\n'.join(processed_lines))

def process_all_transcripts(data_prefix="data", output_dir="processed-transcripts", speaker_patterns=None, multi_line=False, generic_speaker_pattern=None, raw_dir_name="raw-transcripts"):
    """
    Process all transcripts with configurable data directory and speaker patterns.
    
    Args:
        data_prefix (str): Base data directory (e.g., "data" for Trump, "data/harris" for Harris)
        output_dir (str): Output directory name within data_prefix
        speaker_patterns (list): List of regex patterns for speaker identification
        multi_line (bool): If True, collect multiple lines per speaker until next speaker attribution
        generic_speaker_pattern (str): Regex pattern to identify any speaker (for boundaries)
        raw_dir_name (str): Name of the raw transcripts directory
    """
    # Define base directories using os.path.join for cross-platform compatibility
    raw_dir = os.path.join(data_prefix, raw_dir_name)
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
                    process_transcript(input_file, output_file, speaker_patterns, multi_line, generic_speaker_pattern)
                    print(f"Processed: {input_file}")
                except Exception as e:
                    print(f"Error processing {input_file}: {str(e)}")

def process_new_transcripts(data_prefix="data", output_dir="processed-transcripts", speaker_patterns=None, multi_line=False, generic_speaker_pattern=None, raw_dir_name="raw-transcripts"):
    """
    Process only transcripts that exist in raw-transcripts but not in processed-transcripts
    
    Args:
        data_prefix (str): Base data directory (e.g., "data" for Trump, "data/harris" for Harris)
        output_dir (str): Output directory name within data_prefix
        speaker_patterns (list): List of regex patterns for speaker identification
        multi_line (bool): If True, collect multiple lines per speaker until next speaker attribution
        generic_speaker_pattern (str): Regex pattern to identify any speaker (for boundaries)
        raw_dir_name (str): Name of the raw transcripts directory
    """
    # Define base directories using os.path.join for cross-platform compatibility
    raw_dir = os.path.join(data_prefix, raw_dir_name)
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
                    process_transcript(input_file, output_file, speaker_patterns, multi_line, generic_speaker_pattern)
                    print(f"Processed new transcript: {input_file}")
                    new_files_processed += 1
                except Exception as e:
                    print(f"Error processing {input_file}: {str(e)}")
    
    if new_files_processed == 0:
        print("No new transcripts to process.")
    else:
        print(f"Processed {new_files_processed} new transcript(s).")

# Convenience functions for specific politicians
def process_trump_transcripts(multi_line=False):
    """Process Trump transcripts with Trump-specific speaker patterns"""
    trump_patterns = [r'Donald\s+Trump\s*:\s*']
    process_all_transcripts("data", "processed-transcripts", trump_patterns, multi_line)

def process_vance_transcripts(multi_line=False):
    """Process Vance transcripts with Vance-specific speaker patterns"""
    vance_patterns = [r'J.D.\s+Vance\s*:\s*']
    process_all_transcripts("data", "vance-processed-transcripts", vance_patterns, multi_line)

def process_harris_transcripts(multi_line=False):
    """Process Harris transcripts with Harris-specific speaker patterns"""
    harris_patterns = [
        r'Kamala\s+Harris\s*:\s*',
        r'Vice\s+President\s+Harris\s*:\s*',
        r'VP\s+Harris\s*:\s*',
        r'Harris\s*:\s*'
    ]
    process_all_transcripts("data/harris", "processed-transcripts", harris_patterns, multi_line)

def process_amodei_transcripts(multi_line=True):
    """Process Amodei transcripts with Amodei-specific speaker patterns"""
    amodei_patterns = [
        r'Dario\s+Amodei.*:\s*',  # Matches "Dario Amodei:" and "Dario Amodei (timestamp):"
        r'^Dario\s+Amodei\s*$'    # Matches "Dario Amodei" on its own line
    ]
    
    # Generic pattern to identify ANY speaker (including interviewers and headers)
    # Matches:
    # 1. Lines starting with Capital Letter... ending with Colon (standard + timestamped)
    # 2. Lines with Title Case Name (e.g. "Lex Fridman", "Scaling Laws")
    generic_pattern = r'(^[A-Z][\w\s.,\-()]+:\s*)|(^[A-Z][a-z]+(\s+[A-Z][a-z]+)+$)'
    
    process_new_transcripts("data/amodei", "processed-transcripts", amodei_patterns, multi_line, generic_speaker_pattern=generic_pattern, raw_dir_name="raw")

if __name__ == '__main__':
    # process_all_transcripts()
    process_amodei_transcripts() 