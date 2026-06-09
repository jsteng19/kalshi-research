import os
import unicodedata
import re
import argparse
import sys
import csv

def convert_to_ascii(text):
    """Convert text to ASCII, replacing common Unicode characters with ASCII equivalents"""
    # First replace Unicode apostrophes/quotes with ASCII versions
    # Do this before NFKD normalization to prevent apostrophes from being stripped
    quote_replacements = {
        ''': "'",
        ''': "'",
        '′': "'",
        '"': '"',
        '"': '"',
        '‛': "'",
        '’': "'",
        '´': "'",
        '`': "'",
        '′': "'"
    }
    
    for old, new in quote_replacements.items():
        text = text.replace(old, new)
    
    # Normalize unicode characters
    text = unicodedata.normalize('NFKD', text)
    
    # Replace common Unicode characters with ASCII equivalents
    replacements = {
        '–': '-',
        '—': '--',
        '…': '...',
        '•': '*',
        '°': ' degrees ',
        '×': 'x',
        '÷': '/',
        '≠': '!=',
        '≤': '<=',
        '≥': '>=',
        '±': '+/-',
        '∞': 'infinity',
        '√': 'sqrt',
        '∆': 'delta',
        '∑': 'sum',
        '∏': 'product',
        '∂': 'd',
        '∃': 'exists',
        '∀': 'for all',
        '∈': 'in',
        '∉': 'not in',
        '∋': 'contains',
        '∌': 'does not contain',
        '∩': 'intersection',
        '∪': 'union',
        '∅': 'empty set',
        '∼': '~',
        '≈': '~=',
        '≡': '===',
        '≢': '!==',
        '⊂': 'subset of',
        '⊃': 'superset of',
        '⊆': 'subset or equal to',
        '⊇': 'superset or equal to',
        '⊕': 'xor',
        '⊗': 'tensor product',
        '⊥': 'perpendicular to',
        '⋅': '.',
        '⌈': 'ceil',
        '⌉': 'ceil',
        '⌊': 'floor',
        '⌋': 'floor',
        '〈': '<',
        '〉': '>',
        '♭': 'b',
        '♮': 'natural',
        '♯': '#',
        '½': '1/2',
        '⅓': '1/3',
        '⅔': '2/3',
        '¼': '1/4',
        '¾': '3/4',
        '⅕': '1/5',
        '⅖': '2/5',
        '⅗': '3/5',
        '⅘': '4/5',
        '⅙': '1/6',
        '⅚': '5/6',
        '⅛': '1/8',
        '⅜': '3/8',
        '⅝': '5/8',
        '⅞': '7/8',
    }
    
    for old, new in replacements.items():
        text = text.replace(old, new)
    
    # Convert to ASCII, preserving apostrophes
    ascii_text = ''
    for char in text:
        if char == "'" or ord(char) < 128:
            ascii_text += char
    
    # Normalize whitespace
    ascii_text = re.sub(r'\s+', ' ', ascii_text)
    
    return ascii_text.strip()

def process_directory(target_dir):
    """Convert all .txt files in the given directory to ASCII"""
    if not os.path.isdir(target_dir):
        print(f"Error: Directory not found at {target_dir}")
        return

    for filename in os.listdir(target_dir):
        if filename.endswith('.txt'):
            filepath = os.path.join(target_dir, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    text = f.read()
                ascii_text = convert_to_ascii(text)
                with open(filepath, 'w', encoding='ascii') as f:
                    f.write(ascii_text)
                print(f"Processed: {filename}")
            except Exception as e:
                print(f"Error processing {filename}: {str(e)}")

def process_csv(csv_path):
    """Convert the status_text column in a CSV file to ASCII and save in place."""
    if not os.path.isfile(csv_path):
        print(f"Error: File not found at {csv_path}")
        return

    # Read CSV and process
    rows = []
    with open(csv_path, 'r', encoding='utf-8', newline='') as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        if not fieldnames or 'post_date' not in fieldnames or 'status_text' not in fieldnames:
            print("Error: CSV must have columns 'post_date' and 'status_text'")
            return
        for row in reader:
            orig_text = row.get('status_text', '')
            row['status_text'] = convert_to_ascii(orig_text)
            rows.append(row)

    # Write back to the same file (in place)
    with open(csv_path, 'w', encoding='utf-8', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Processed CSV: {csv_path}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Convert all .txt files in a directory to ASCII, or convert the status_text column in a CSV file to ASCII."
    )
    parser.add_argument(
        "path",
        help="The target directory containing .txt files, or a CSV file with columns post_date,status_text."
    )
    args = parser.parse_args()
    if os.path.isdir(args.path):
        process_directory(args.path)
    elif os.path.isfile(args.path) and args.path.lower().endswith('.csv'):
        process_csv(args.path)
    else:
        print("Error: Path must be a directory (for .txt files) or a .csv file with post_date,status_text columns.")