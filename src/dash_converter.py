import os
import re
import argparse

def convert_dashes_in_file(filepath):
    """Converts em and en dashes to hyphens in a single text file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Replace em dashes (—) and en dashes (–) with hyphens (-)
        content = re.sub(r'[—–]', '-', content)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"Successfully converted dashes in: {filepath}")
    except Exception as e:
        print(f"Error processing file {filepath}: {e}")

def convert_dashes_in_directory(directory_path):
    """
    Iterates through all .txt files in a given directory and converts 
    em and en dashes to hyphens.
    """
    if not os.path.isdir(directory_path):
        print(f"Error: Directory not found at {directory_path}")
        return

    for filename in os.listdir(directory_path):
        if filename.endswith(".txt"):
            filepath = os.path.join(directory_path, filename)
            convert_dashes_in_file(filepath)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert em and en dashes to hyphens in text files within a directory.")
    parser.add_argument("directory", help="The target directory containing the text files.")
    args = parser.parse_args()

    convert_dashes_in_directory(args.directory) 