#!/usr/bin/env python3
"""
Comprehensive test suite for regex_pattern_generator.py

Tests the following rules:
1. Any phrases separated by a slash should all be matched by a single pattern
2. Only singular, plural, and possessive forms are allowable
3. Trailing punctuation is allowed
4. The phrase is allowed to be part of a hyphenated compound word
5. For multi-word phrases, any combination of inner hyphenation and commas should be allowed
"""

import re
import sys
from regex_pattern_generator import generate_regex_patterns


def test_singular_plural_possessive_forms():
    """Test Rule 2: Only singular, plural, and possessive forms are allowable."""
    
    print("\n" + "=" * 60)
    print("TEST: Singular, Plural, and Possessive Forms")
    print("=" * 60)
    
    test_phrases = {
        'Trump': 'Trump',
        'Policy': 'Policy',
        'Crisis': 'Crisis',
    }
    
    patterns = generate_regex_patterns(test_phrases)
    
    test_cases = {
        'Trump': [
            # Should match: singular, plural, possessive
            ("Trump", True),
            ("trump", True),  # Case insensitive
            ("Trumps", True),  # Plural
            ("Trump's", True),  # Singular possessive
            ("Trumps'", True),  # Plural possessive
            
            # Should NOT match: other forms
            ("Trumping", False),  # Gerund
            ("Trumped", False),  # Past tense
            ("Trumpian", False),  # Adjective form
            ("Trumpet", False),  # Different word
        ],
        'Policy': [
            ("policy", True),
            ("policies", True),  # Plural with 'ies'
            ("policy's", True),
            ("policies'", True),
            ("Policy's", True),
            
            ("policying", False),  # Not a valid form
            ("policied", False),  # Not a valid form
        ],
        'Crisis': [
            ("crisis", True),
            ("crises", True),  # Irregular plural
            ("crisis's", True),
            ("crises'", True),
            
            ("crisises", False),  # Wrong plural form
        ]
    }
    
    return run_test_cases(patterns, test_cases)


def test_trailing_punctuation():
    """Test Rule 3: Trailing punctuation is allowed."""
    
    print("\n" + "=" * 60)
    print("TEST: Trailing Punctuation")
    print("=" * 60)
    
    patterns = generate_regex_patterns({'Biden': 'Biden'})
    pattern = re.compile(patterns['Biden'], re.IGNORECASE)
    
    test_cases = [
        ("Biden.", True),
        ("Biden,", True),
        ("Biden!", True),
        ("Biden?", True),
        ("Biden;", True),
        ("Biden:", True),
        ("Biden...", True),  # Multiple punctuation
        ("Biden!?", True),
        
        # Leading punctuation is acceptable (finds the word within the text)
        (".Biden", True),
        ("Bi.den", False),  # Punctuation within the word should not match
    ]
    
    passed = 0
    failed = 0
    
    for text, expected in test_cases:
        match = pattern.search(text)
        result = bool(match)
        if result == expected:
            status = "✓ PASS"
            passed += 1
        else:
            status = "✗ FAIL"
            failed += 1
        
        print(f"{status}: '{text}' -> Expected: {expected}, Got: {result}")
        if match:
            print(f"       Matched: '{match.group()}'")
    
    return failed == 0


def test_hyphenated_compounds():
    """Test Rule 4: The phrase is allowed to be part of a hyphenated compound word."""
    
    print("\n" + "=" * 60)
    print("TEST: Hyphenated Compound Words")
    print("=" * 60)
    
    patterns = generate_regex_patterns({
        'Trump': 'Trump',
        'Climate': 'Climate'
    })
    
    test_cases = {
        'Trump': [
            # Compounds before the word
            ("anti-Trump", True),
            ("pro-Trump", True),
            ("never-Trump", True),
            ("super-anti-Trump", True),
            
            # Compounds after the word
            ("Trump-era", True),
            ("Trump-style", True),
            ("Trump-related", True),
            
            # Both before and after
            ("anti-Trump-movement", True),
            ("pro-Trump-rally", True),
            
            # Should still work with possessives and plurals
            ("anti-Trump's", True),
            ("pro-Trumps", True),
        ],
        'Climate': [
            ("anti-climate", True),
            ("climate-related", True),
            ("pro-climate-action", True),
        ]
    }
    
    return run_test_cases(patterns, test_cases)


def test_multi_word_flexibility():
    """Test Rule 5: For multi-word phrases, any combination of inner hyphenation and commas should be allowed."""
    
    print("\n" + "=" * 60)
    print("TEST: Multi-word Phrase Flexibility")
    print("=" * 60)
    
    patterns = generate_regex_patterns({
        'Climate Change': 'Climate Change',
        'Social Security': 'Social Security',
        'Supreme Court': 'Supreme Court'
    })
    
    test_cases = {
        'Climate Change': [
            # Normal spacing
            ("Climate Change", True),
            ("climate change", True),
            
            # Hyphenation
            ("Climate-Change", True),
            ("climate-change", True),
            
            # Comma separation
            ("Climate, Change", True),
            ("Climate,Change", True),
            
            # Mixed
            ("Climate-change", True),
            ("climate, Change", True),
            
            # With plurals/possessives
            ("Climate Changes", True),
            ("Climates Change", True),
            ("Climate Change's", True),
            
            # In compounds
            ("anti-Climate-Change", True),
            ("Climate-Change-deniers", True),
            
            # Should NOT match
            ("Climate and Change", False),
            ("Climate or Change", False),
            ("Climate the Change", False),
        ],
        'Social Security': [
            ("Social Security", True),
            ("Social-Security", True),
            ("Social, Security", True),
            ("Social,Security", True),
            ("social-security", True),
            ("Social Security's", True),
            ("anti-Social-Security", True),
        ],
        'Supreme Court': [
            ("Supreme Court", True),
            ("Supreme-Court", True),
            ("Supreme, Court", True),
            ("supreme court", True),
            ("Supreme Court's", True),
            ("Supreme Courts", True),
            ("pro-Supreme-Court", True),
        ]
    }
    
    return run_test_cases(patterns, test_cases)


def test_slash_alternatives():
    """Test Rule 1: Any phrases separated by a slash should all be matched by a single pattern."""
    
    print("\n" + "=" * 60)
    print("TEST: Slash-separated Alternatives")
    print("=" * 60)
    
    patterns = generate_regex_patterns({
        'Marijuana': 'Marijuana/Weed/Cannabis',
        'Nobel Prize': 'Nobel Prize/Peace Prize',
        'Crypto': 'Crypto/Bitcoin',
        'Elon': 'Elon/Musk'
    })
    
    test_cases = {
        'Marijuana': [
            # All alternatives should match
            ("Marijuana", True),
            ("Weed", True),
            ("Cannabis", True),
            
            # Case insensitive
            ("marijuana", True),
            ("weed", True),
            ("cannabis", True),
            
            # Plurals and possessives
            ("Marijuanas", True),
            ("Weeds", True),
            ("Cannabis's", True),
            
            # Compounds
            ("anti-marijuana", True),
            ("pro-weed", True),
            ("cannabis-related", True),
        ],
        'Nobel Prize': [
            ("Nobel Prize", True),
            ("Peace Prize", True),
            ("nobel prize", True),
            ("peace prize", True),
            ("Nobel-Prize", True),
            ("Peace-Prize", True),
            ("Nobel Prize's", True),
            ("Peace Prizes", True),
        ],
        'Crypto': [
            ("Crypto", True),
            ("Bitcoin", True),
            ("crypto", True),
            ("bitcoin", True),
            ("Cryptos", True),  # This might fail with current implementation
            ("Bitcoins", True),
            ("Crypto's", True),
            ("Bitcoin's", True),
            ("anti-crypto", True),
            ("pro-Bitcoin", True),
        ],
        'Elon': [
            ("Elon", True),
            ("Musk", True),
            ("elon", True),
            ("musk", True),
            ("Elon's", True),
            ("Musk's", True),
            ("Musks", True),
            ("anti-Elon", True),
            ("pro-Musk", True),
        ]
    }
    
    return run_test_cases(patterns, test_cases)


def test_edge_cases():
    """Test edge cases and special scenarios."""
    
    print("\n" + "=" * 60)
    print("TEST: Edge Cases")
    print("=" * 60)
    
    patterns = generate_regex_patterns({
        'McDonald\'s': 'McDonald\'s',
        'AT&T': 'AT&T',
        'COVID-19': 'COVID-19',
        'X': 'X',  # Single letter
        'I': 'I',  # Single letter that's also a word
        'Barack Hussein Obama': 'Barack Hussein Obama',  # Three-word phrase
    })
    
    test_cases = {
        'McDonald\'s': [
            ("McDonald's", True),
            ("mcdonald's", True),
            ("McDonald's'", True),  # Possessive of possessive
            ("anti-McDonald's", True),
            
            ("McDonalds", False),  # Missing apostrophe
            ("McDonald", False),  # Missing 's
        ],
        'AT&T': [
            ("AT&T", True),
            ("at&t", True),
            ("AT&T's", True),
            ("anti-AT&T", True),
            
            ("AT & T", False),  # Spaces around &
            ("ATT", False),  # Missing &
        ],
        'COVID-19': [
            ("COVID-19", True),
            ("covid-19", True),
            ("COVID-19's", True),
            ("anti-COVID-19", True),
            
            ("COVID19", False),  # Missing hyphen
            ("COVID 19", False),  # Space instead of hyphen
        ],
        'X': [
            ("X", True),
            ("x", True),
            ("X's", True),
            ("anti-X", True),
            
            ("Xbox", False),  # Part of another word
            ("eXample", False),  # In middle of word
        ],
        'I': [
            ("I", True),
            ("I's", True),
            
            ("It", False),  # Different word
            ("In", False),  # Different word
        ],
        'Barack Hussein Obama': [
            ("Barack Hussein Obama", True),
            ("barack hussein obama", True),
            ("Barack-Hussein-Obama", True),
            ("Barack, Hussein, Obama", True),
            ("Barack,Hussein,Obama", True),
            ("Barack Hussein Obama's", True),
            ("anti-Barack-Hussein-Obama", True),
            
            ("Barack Obama", False),  # Missing middle name
            ("Hussein Obama", False),  # Missing first name
            ("Barack Hussein", False),  # Missing last name
        ]
    }
    
    return run_test_cases(patterns, test_cases)


def test_word_boundary_enforcement():
    """Test that word boundaries are properly enforced."""
    
    print("\n" + "=" * 60)
    print("TEST: Word Boundary Enforcement")
    print("=" * 60)
    
    patterns = generate_regex_patterns({
        'Democrat': 'Democrat',
        'Trump': 'Trump',
        'Biden': 'Biden',
        'America': 'America'
    })
    
    test_cases = {
        'Democrat': [
            ("Democrat", True),
            ("Democrats", True),
            ("democrat", True),
            
            # Should NOT match substrings
            ("Democratic", False),
            ("Democratization", False),
            ("Undemocratic", False),
        ],
        'Trump': [
            ("Trump", True),
            ("trump", True),
            
            # Should NOT match substrings
            ("Trumpet", False),
            ("Trumpeter", False),
            ("Strumpet", False),
        ],
        'Biden': [
            ("Biden", True),
            ("biden", True),
            
            # Should NOT match substrings
            ("Bidenomics", False),  # New word derived from Biden
            ("Abidance", False),
        ],
        'America': [
            ("America", True),
            ("america", True),
            ("America's", True),
            
            # Should NOT match substrings
            ("American", False),
            ("Americans", False),  # Different word, not plural of America
            ("Americana", False),
        ]
    }
    
    return run_test_cases(patterns, test_cases)


def test_real_world_examples():
    """Test with real-world examples from political transcripts."""
    
    print("\n" + "=" * 60)
    print("TEST: Real-world Examples")
    print("=" * 60)
    
    patterns = generate_regex_patterns({
        'Sleepy Joe': 'Sleepy Joe',
        'Crooked Hillary': 'Crooked Hillary',
        'Fake News': 'Fake News',
        'Make America Great Again': 'Make America Great Again'
    })
    
    # Test in context
    test_sentences = {
        'Sleepy Joe': [
            ("I think Sleepy Joe is wrong.", True),
            ("The policies of Sleepy-Joe are terrible.", True),
            ("sleepy joe's administration", True),
            ("He called him sleepy, Joe didn't respond.", True),
            
            ("He was sleepy. Joe was awake.", False),  # Separate sentences
        ],
        'Crooked Hillary': [
            ("Crooked Hillary lost the election.", True),
            ("They support Crooked-Hillary.", True),
            ("crooked hillary's emails", True),
            
            ("The road was crooked. Hillary drove carefully.", False),
        ],
        'Fake News': [
            ("This is Fake News!", True),
            ("The fake-news media", True),
            ("FAKE NEWS!!!", True),
            ("fake, news", True),
            
            ("This fake is news to me.", False),
        ],
        'Make America Great Again': [
            ("Make America Great Again", True),
            ("make america great again", True),
            ("Make-America-Great-Again", True),
            ("Make, America, Great, Again", True),
            ("Make America Great Again's", True),
            
            ("Let's make America and Canada great again.", False),  # Extra word
        ]
    }
    
    passed = 0
    failed = 0
    failed_tests = []
    
    for phrase_name, test_list in test_sentences.items():
        print(f"\nTesting '{phrase_name}':")
        print("-" * 40)
        
        pattern = re.compile(patterns[phrase_name], re.IGNORECASE)
        
        for text, should_match in test_list:
            match = pattern.search(text)
            result = bool(match)
            
            if result == should_match:
                status = "✓ PASS"
                passed += 1
            else:
                status = "✗ FAIL"
                failed += 1
                failed_tests.append((phrase_name, text, should_match, result))
            
            print(f"{status}: '{text}'")
            if match:
                print(f"       Matched: '{match.group()}'")
    
    if failed_tests:
        print("\nFailed tests:")
        for phrase, text, expected, actual in failed_tests:
            print(f"  {phrase}: '{text}' - Expected: {expected}, Got: {actual}")
    
    return failed == 0


def run_test_cases(patterns, test_cases):
    """Helper function to run test cases and report results."""
    passed = 0
    failed = 0
    failed_tests = []
    
    for phrase_name, test_list in test_cases.items():
        print(f"\nTesting '{phrase_name}':")
        pattern_str = patterns[phrase_name]
        print(f"Pattern (first 100 chars): {pattern_str[:100]}{'...' if len(pattern_str) > 100 else ''}")
        print("-" * 40)
        
        pattern = re.compile(pattern_str, re.IGNORECASE)
        
        for text, expected in test_list:
            match = pattern.search(text)
            result = bool(match)
            
            if result == expected:
                status = "✓ PASS"
                passed += 1
            else:
                status = "✗ FAIL"
                failed += 1
                failed_tests.append((phrase_name, text, expected, result))
            
            print(f"{status}: '{text}' -> Expected: {expected}, Got: {result}")
            if match:
                print(f"       Matched: '{match.group()}'")
    
    print(f"\nResults: {passed} passed, {failed} failed")
    
    if failed_tests:
        print("\nFailed tests:")
        for phrase, text, expected, actual in failed_tests:
            print(f"  {phrase}: '{text}' - Expected: {expected}, Got: {actual}")
    
    return failed == 0


def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("REGEX PATTERN GENERATOR - COMPREHENSIVE TEST SUITE")
    print("=" * 60)
    
    all_passed = True
    
    # Run all test categories
    test_functions = [
        test_singular_plural_possessive_forms,
        test_trailing_punctuation,
        test_hyphenated_compounds,
        test_multi_word_flexibility,
        test_slash_alternatives,
        test_edge_cases,
        test_word_boundary_enforcement,
        test_real_world_examples,
    ]
    
    for test_func in test_functions:
        if not test_func():
            all_passed = False
    
    # Final summary
    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 ALL TESTS PASSED!")
        sys.exit(0)
    else:
        print("❌ SOME TESTS FAILED!")
        print("Review the failed tests above and fix the regex pattern generator.")
        sys.exit(1)


if __name__ == "__main__":
    main()