import os
import re

def process_transcript(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.read().split('\n\n')  # Split by double newlines to get paragraphs
    
    # Skip the first line (source URL)
    processed_lines = []
    
    # Determine if this is a press briefing from the folder path
    is_press_briefing = '/press briefing/' in input_file.lower()
    speaker = 'Karoline Leavitt:' if is_press_briefing else 'Donald Trump:'
    
    for line in lines[1:]:
        # Check if line starts with the speaker name and extract the content
        if line.startswith(speaker):
            # Remove speaker prefix and clean up
            content = line.replace(speaker, '').strip()
            # Remove audience reactions in square brackets
            content = re.sub(r'\[.*?\]', '', content).strip()
            if content:  # Only add non-empty lines
                processed_lines.append(content)
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Write processed content
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n\n'.join(processed_lines))

def process_all_transcripts():
    # Create processed-transcripts directory
    os.makedirs('data/processed-transcripts', exist_ok=True)
    
    # Walk through raw-transcripts directory
    for root, dirs, files in os.walk('data/raw-transcripts'):
        for file in files:
            if file.endswith('.txt'):
                # Get relative path components
                rel_path = os.path.relpath(root, 'data/raw-transcripts')
                
                # Construct input and output paths
                input_file = os.path.join(root, file)
                output_dir = os.path.join('data/processed-transcripts', rel_path)
                output_file = os.path.join(output_dir, file)
                
                # Process the transcript
                try:
                    process_transcript(input_file, output_file)
                    print(f"Processed: {input_file}")
                except Exception as e:
                    print(f"Error processing {input_file}: {str(e)}")

if __name__ == '__main__':
    process_all_transcripts() 