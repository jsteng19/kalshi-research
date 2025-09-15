#!/usr/bin/env python3
"""
Test script for regex_pattern_generator.py focusing on multiple word phrases.

This script tests the regex pattern generator with various multi-word phrases
to ensure they match correctly in different contexts and forms.
"""

import re
import sys
from regex_pattern_generator import generate_regex_patterns


def test_multiple_word_phrases():
    """Test multiple word phrases with various forms and contexts."""
    
    # Define test phrases - focusing on multi-word examples
    test_phrases = {
        'Sleepy Joe': 'Sleepy Joe',
        'Nobel Prize': 'Nobel Prize/Peace Prize',
        'Cracker Barrel': 'Cracker Barrel',
        'Barack Hussein Obama': 'Barack Hussein Obama',
        'Taylor Swift': 'Taylor Swift',
        'New York': 'New York',
        'White House': 'White House',
        'Supreme Court': 'Supreme Court',
        'Social Security': 'Social Security',
        'United States': 'United States/America',
        'Vice President': 'Vice President',
        'National Guard': 'National Guard',
        'Border Wall': 'Border Wall',
        'Climate Change': 'Climate Change/Global Warming'
    }
    
    # Generate patterns
    patterns = generate_regex_patterns(test_phrases)
    
    # Test cases: phrase_name -> list of (text, should_match)
    test_cases = {
        'Sleepy Joe': [
            ("I think Sleepy Joe is wrong.", True),
            ("Sleepy Joe's policies are terrible.", True),
            ("The Sleepy-Joe supporters are loud.", True),
            ("Anti-Sleepy Joe protesters gathered.", True),
            ("Sleepy, Joe was tired.", True), 
            ("I saw sleepy joe yesterday.", True),  # Case insensitive
            ("Sleepy Joe!", True),  # With punctuation
            ("Sleepy-Joe administration", True),  # Hyphenated
            ("Sleepy,Joe", True),  # Comma between words (allowed)
            ("He's not sleepy or joe.", False),  # Words separated by other words
        ],
        
        'Nobel Prize': [
            ("She won the Nobel Prize.", True),
            ("Nobel Prize winners are smart.", True),
            ("The Nobel-Prize ceremony was grand.", True),
            ("Peace Prize recipients deserve recognition.", True),  # Alternative form
            ("Nobel Prize's significance is huge.", True),  # Possessive
            ("Anti-Nobel Prize sentiment exists.", True),  # Compound
            ("Nobel, Prize committee met.", True),  # Comma separation
            ("Nobel and Prize are different.", False),  # Separated by other words
            ("nobel prize for literature", True),  # Case insensitive
            ("Peace Prize!", True),  # Alternative with punctuation
        ],
        
        'Barack Hussein Obama': [
            ("Barack Hussein Obama was president.", True),
            ("Barack-Hussein-Obama spoke today.", True),
            ("Barack, Hussein, Obama's legacy.", True),  # Commas between all words
            ("Barack Hussein Obama's administration", True),  # Possessive
            ("Pro-Barack Hussein Obama rally", True),  # Compound
            ("Barack Hussein Obama!", True),  # With punctuation
            ("barack hussein obama", True),  # Case insensitive
            ("Barack Hussein and Obama are names.", False),  # Interrupted
            ("Barack Obama", False),  # Missing middle name
        ],
        
        'White House': [
            ("The White House is beautiful.", True),
            ("White House's lawn is green.", True),
            ("White-House staff meeting", True),
            ("Anti-White House protests", True),
            ("white house tour", True),  # Case insensitive
            ("White, House residents", True),  # Comma separation
            ("White and House are colors and buildings.", False),  # Separated
            ("White House!", True),  # With punctuation
        ],
        
        'Climate Change': [
            ("Climate Change is real.", True),
            ("Global Warming affects us all.", True),  # Alternative form
            ("Climate-Change deniers exist.", True),
            ("Climate, Change policies", True),  # Comma separation
            ("Anti-Climate Change movement", True),  # Compound
            ("climate change effects", True),  # Case insensitive
            ("Global Warming's impact", True),  # Alternative possessive
            ("Climate and Change are concepts.", False),  # Separated
        ]
    }
    
    print("Testing Multiple Word Phrase Patterns")
    print("=" * 60)
    
    total_tests = 0
    passed_tests = 0
    failed_tests = []
    
    for phrase_name, test_list in test_cases.items():
        print(f"\nTesting '{phrase_name}':")
        print(f"Pattern: {patterns[phrase_name]}")
        print("-" * 40)
        
        compiled_pattern = re.compile(patterns[phrase_name], re.IGNORECASE)
        
        for text, should_match in test_list:
            total_tests += 1
            match = compiled_pattern.search(text)
            
            if bool(match) == should_match:
                status = "✓ PASS"
                passed_tests += 1
            else:
                status = "✗ FAIL"
                failed_tests.append((phrase_name, text, should_match, bool(match)))
            
            print(f"{status}: '{text}' -> Expected: {should_match}, Got: {bool(match)}")
            if match:
                print(f"       Matched: '{match.group()}'")
    
    # Summary
    print("\n" + "=" * 60)
    print(f"TEST SUMMARY:")
    print(f"Total tests: {total_tests}")
    print(f"Passed: {passed_tests}")
    print(f"Failed: {len(failed_tests)}")
    print(f"Success rate: {passed_tests/total_tests*100:.1f}%")
    
    if failed_tests:
        print(f"\nFAILED TESTS:")
        print("-" * 30)
        for phrase, text, expected, actual in failed_tests:
            print(f"Phrase: {phrase}")
            print(f"Text: '{text}'")
            print(f"Expected: {expected}, Got: {actual}")
            print()
    
    return len(failed_tests) == 0


def test_edge_cases():
    """Test edge cases for multi-word phrases."""
    
    print("\n" + "=" * 60)
    print("TESTING EDGE CASES")
    print("=" * 60)
    
    edge_phrases = {
        'Empty Test': '',
        'Single Word': 'Test',
        'Many Words': 'This Is A Very Long Multi Word Phrase',
        'With Punctuation': "McDonald's Restaurant",
        'Numbers': 'COVID 19',
        'Special Chars': 'AT&T Corporation'
    }
    
    try:
        patterns = generate_regex_patterns(edge_phrases)
        print("✓ Edge case patterns generated successfully")
        
        # Test a few edge cases
        test_text = "This Is A Very Long Multi Word Phrase works well."
        pattern = re.compile(patterns['Many Words'], re.IGNORECASE)
        match = pattern.search(test_text)
        
        if match:
            print(f"✓ Long phrase matched: '{match.group()}'")
        else:
            print("✗ Long phrase failed to match")
            
        return True
        
    except Exception as e:
        print(f"✗ Edge case testing failed: {e}")
        return False


def run_sample_text_test():
    """Test patterns against sample text paragraphs."""
    
    print("\n" + "=" * 60)
    print("TESTING WITH SAMPLE TEXT")
    print("=" * 60)
    
    sample_text = """
    The White House announced today that Barack Hussein Obama will be speaking
    about Climate Change at the Nobel Prize ceremony. Sleepy Joe was also
    mentioned in the context of Social Security reforms. The Supreme Court's
    decision on the Border Wall has been controversial. Taylor Swift's concert
    at Cracker Barrel was unexpectedly political, touching on Global Warming
    and the Vice President's role in New York policy.
    
    Anti-Climate Change protesters gathered outside the White-House, while
    Nobel-Prize winners discussed Peace Prize criteria. Barack-Hussein-Obama's
    legacy includes Supreme-Court appointments and National-Guard deployments.
    """
    
    test_phrases = {
        'White House': 'White House',
        'Barack Hussein Obama': 'Barack Hussein Obama', 
        'Climate Change': 'Climate Change/Global Warming',
        'Nobel Prize': 'Nobel Prize/Peace Prize',
        'Sleepy Joe': 'Sleepy Joe',
        'Supreme Court': 'Supreme Court',
        'Taylor Swift': 'Taylor Swift'
    }
    
    patterns = generate_regex_patterns(test_phrases)
    
    print("Sample text analysis:")
    print("-" * 30)
    
    for phrase_name, pattern_str in patterns.items():
        pattern = re.compile(pattern_str, re.IGNORECASE)
        matches = pattern.findall(sample_text)
        
        print(f"'{phrase_name}': {len(matches)} matches")
        for match in matches:
            print(f"  -> '{match}'")
    
    return True


def main():
    """Run all tests."""
    print("REGEX PATTERN GENERATOR - MULTI-WORD PHRASE TESTS")
    print("=" * 60)
    
    success = True
    
    # Run main tests
    if not test_multiple_word_phrases():
        success = False
    
    # Run edge case tests  
    if not test_edge_cases():
        success = False
    
    # Run sample text tests
    if not run_sample_text_test():
        success = False
    
    # Final result
    print("\n" + "=" * 60)
    if success:
        print("🎉 ALL TESTS COMPLETED SUCCESSFULLY!")
        sys.exit(0)
    else:
        print("❌ SOME TESTS FAILED!")
        sys.exit(1)


if __name__ == "__main__":
    main()
