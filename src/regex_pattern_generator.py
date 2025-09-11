import re
from typing import List, Dict, Union
import inflect


def generate_regex_patterns(phrases: Union[List[str], Dict[str, str]]) -> Dict[str, str]:
    """
    Generate regex patterns programmatically from a list of phrases.
    
    Rules:
    - Any phrases separated by a slash should all be matched by a single pattern
    - Only singular, plural, and possessive forms are allowable
    - Trailing punctuation is allowed
    - The phrase is allowed to be part of a hyphenated compound word
    - For multi-word phrases, any combination of inner hyphenation and commas should be allowed
    
    Args:
        phrases: Either a list of phrases or a dict mapping phrase names to phrase strings.
                If a phrase contains slashes (/), all alternatives will be included in one pattern.
    
    Returns:
        Dictionary mapping phrase names to their regex patterns
    """
    
    if isinstance(phrases, list):
        # Convert list to dict using the phrase as both key and value
        phrase_dict = {phrase: phrase for phrase in phrases}
    else:
        phrase_dict = phrases.copy()
    
    patterns = {}
    
    for phrase_name, phrase_value in phrase_dict.items():
        pattern = _generate_single_pattern(phrase_value)
        patterns[phrase_name] = pattern
    
    return patterns


def _generate_single_pattern(phrase: str) -> str:
    """
    Generate a regex pattern for a single phrase or slash-separated alternatives.
    
    Args:
        phrase: The phrase to generate a pattern for (may contain slash-separated alternatives)
    
    Returns:
        The regex pattern string
    """
    # Split by slash to handle alternatives
    alternatives = [alt.strip() for alt in phrase.split('/')]
    
    # Generate patterns for each alternative
    alt_patterns = []
    for alt in alternatives:
        alt_pattern = _generate_phrase_pattern(alt)
        alt_patterns.append(alt_pattern)
    
    # Combine alternatives with OR operator
    if len(alt_patterns) == 1:
        combined_pattern = alt_patterns[0]
    else:
        combined_pattern = '|'.join(alt_patterns)
    
    # Wrap in word boundary and compound word logic
    full_pattern = rf'\b(?:\w+-)*(?:{combined_pattern})(?:-\w+)*(?=\W|$)[.,!?;:]*'
    
    return full_pattern


def _generate_phrase_pattern(phrase: str) -> str:
    """
    Generate a regex pattern for a single phrase (no slash alternatives).
    
    Args:
        phrase: Single phrase to generate pattern for
    
    Returns:
        Pattern string for this phrase with all its forms
    """
    # Clean and normalize the phrase
    phrase = phrase.strip()
    
    # Split into words
    words = phrase.split()
    
    if len(words) == 1:
        # Single word - generate singular, plural, possessive forms
        return _generate_word_forms(words[0])
    else:
        # Multi-word phrase - handle spaces, hyphens, commas between words
        word_patterns = []
        for word in words:
            word_forms = _generate_word_forms(word)
            word_patterns.append(word_forms)
        
        # Join with flexible spacing that allows hyphens and commas
        flexible_space = r'[\s,\-]+'
        phrase_pattern = flexible_space.join(word_patterns)
        
        return phrase_pattern


def _generate_word_forms(word: str) -> str:
    """
    Generate all forms (singular, plural, possessive) for a single word.
    
    Args:
        word: The word to generate forms for
    
    Returns:
        Regex pattern matching all forms of the word
    """
    # Clean the word and convert to lowercase for pattern generation
    clean_word = word.lower().strip("'\".,!?;:")
    
    # Handle special cases and contractions
    if clean_word.endswith("'s"):
        # Already possessive, extract base word
        base_word = clean_word[:-2]
    else:
        base_word = clean_word
    
    # Generate forms
    forms = []
    
    # Base form (singular)
    forms.append(base_word)
    
    # Plural form
    plural = _make_plural(base_word)
    if plural != base_word:
        forms.append(plural)
    
    # Possessive forms
    forms.append(f"{base_word}'s")
    forms.append(f"{base_word}s'")  # Plural possessive
    
    # If plural is different, add its possessive
    if plural != base_word:
        forms.append(f"{plural}'")
    
    # Remove duplicates while preserving order
    unique_forms = []
    seen = set()
    for form in forms:
        if form not in seen:
            unique_forms.append(form)
            seen.add(form)
    
    # Join with OR operator
    return '|'.join(unique_forms)


# Initialize inflect engine for pluralization
_inflect_engine = inflect.engine()


def _make_plural(word: str) -> str:
    """
    Generate the plural form of a word using the inflect library,
    with special handling for proper nouns.
    
    Args:
        word: The singular word
    
    Returns:
        The plural form of the word
    """
    word_lower = word.lower()
    
    # For proper nouns (capitalized words) or words that look like surnames,
    # use simple pluralization to avoid inflect's Latin/Italian rules
    if _is_likely_proper_noun(word):
        if word_lower.endswith(('s', 'ss', 'sh', 'ch', 'x', 'z')):
            return word_lower + 'es'
        else:
            return word_lower + 's'
    
    # Use inflect for common nouns
    plural = _inflect_engine.plural(word_lower)
    return plural if plural else word_lower + 's'  # Fallback to simple 's' if inflect fails


def _is_likely_proper_noun(word: str) -> bool:
    """
    Determine if a word is likely a proper noun (name).
    
    Args:
        word: The word to check
        
    Returns:
        True if the word appears to be a proper noun
    """
    # Check if word is capitalized (likely proper noun)
    if word and word[0].isupper():
        return True
    
    # Check for common surname patterns that inflect handles poorly
    word_lower = word.lower()
    problematic_endings = ['is', 'us', 'os']  # Endings that inflect treats as Latin/Italian
    
    return any(word_lower.endswith(ending) for ending in problematic_endings)


# Example usage and test function
def test_generator():
    """Test the regex pattern generator with the provided examples."""
    
    test_phrases = {
        'Melania': 'Melania',
        'Newscum': 'Newscum', 
        'Epstein': 'Epstein',
        'Sleepy Joe': 'Sleepy Joe',
        'Football': 'Football',
        'Nobel Prize': 'Nobel Prize/Peace Prize',
        'Cracker Barrel': 'Cracker Barrel',
        'TikTok': 'TikTok',
        'Marijuana': 'Marijuana/Weed/Cannabis',
        'UFC': 'UFC',
        'Barack Hussein Obama': 'Barack Hussein Obama',
        'Predict': 'Predict/Prediction',
        'Ghislaine': 'Ghislaine/Maxwell',
        'Buttigieg': 'Buttigieg',
        'Crypto': 'Crypto/Bitcoin',
        'DeSantis': 'DeSantis',
        'Elon': 'Elon/Musk',
        'Ozempic': 'Fat Shot/Ozempic',
        "McDonald's": "McDonald's",
        'Taylor Swift': 'Taylor Swift'
    }
    
    patterns = generate_regex_patterns(test_phrases)
    
    print("Generated Patterns:")
    print("=" * 50)
    for name, pattern in patterns.items():
        print(f"'{name}': r'{pattern}',")
    
    return patterns


if __name__ == "__main__":
    test_generator() 