import os
import unicodedata
import re

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

def process_sotu_files():
    """Convert all SOTU files to ASCII"""
    sotu_dir = 'data/processed-transcripts/sotu'
    
    # Create output directory if it doesn't exist
    os.makedirs(sotu_dir, exist_ok=True)
    
    # Process each file
    for filename in os.listdir(sotu_dir):
        if filename.endswith('.txt'):
            filepath = os.path.join(sotu_dir, filename)
            
            # Read the file
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    text = f.read()
                
                # Convert to ASCII
                ascii_text = convert_to_ascii(text)
                
                # Write back to the same file
                with open(filepath, 'w', encoding='ascii') as f:
                    f.write(ascii_text)
                
                print(f"Processed: {filename}")
                
            except Exception as e:
                print(f"Error processing {filename}: {str(e)}")

if __name__ == '__main__':
    process_sotu_files() 