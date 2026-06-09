"""
Transcript Processor for Mention Markets

Uses LLM to extract just the speaker's dialogue from interview transcripts.
Similar to caption_processor.py but focused on extracting a specific speaker.

Usage:
    from src.auto_collect.transcript_processor import process_transcript, process_batch
    
    # Process single transcript
    result = process_transcript(
        transcript_text,
        speaker="Will Smith",
        model="gemini-2.0-flash"
    )
    
    # Process all transcripts in a directory
    process_batch("data/mentions/late_night/will_smith/", speaker="Will Smith")
"""

import os
import sys
import time
from pathlib import Path
from typing import Optional, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()


SYSTEM_PROMPT = """You are an expert at extracting speaker dialogue from interview transcripts.

Your task is to extract ONLY what a specific speaker said from an interview transcript.

RULES:
1. Extract ONLY the dialogue/statements from the specified speaker
2. Remove all interviewer questions and other speakers' dialogue  
3. Remove stage directions, audience reactions, [laughter], etc.
4. Keep the speaker's full statements - don't summarize or paraphrase
5. Preserve the original wording exactly
6. Separate distinct statements/topics with blank lines
7. Output ONLY the extracted dialogue, nothing else - no headers or explanations
8. If the speaker is not found in the transcript, output "SPEAKER_NOT_FOUND"

The output should read as a collection of the speaker's statements from the interview."""


USER_PROMPT_TEMPLATE = """Extract only what {speaker} said from this interview transcript.

Transcript:
{transcript}

Output only {speaker}'s dialogue:"""


def process_with_gemini(text: str, speaker: str, model: str = "gemini-2.5-flash") -> Optional[str]:
    """Process transcript using Google Gemini (google.genai SDK)."""
    try:
        from google import genai
        from google.genai import types
    except ImportError:
        print("❌ google-genai not installed. Run: pip install google-genai")
        return None
    
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print("❌ GOOGLE_API_KEY not found in environment")
        return None
    
    client = genai.Client(api_key=api_key)
    
    config = types.GenerateContentConfig(
        system_instruction=SYSTEM_PROMPT,
        temperature=0.1,
        top_p=0.95,
        max_output_tokens=100000,
    )
    
    prompt = USER_PROMPT_TEMPLATE.format(speaker=speaker, transcript=text)
    start_time = time.time()

    try:
        response = client.models.generate_content(
            model=model,
            contents=prompt,
            config=config,
        )
    except Exception as e:
        # Try fallback model
        if "not found" in str(e).lower():
            print(f"   ⚠️ Model '{model}' not found, trying gemini-2.0-flash...")
            model = "gemini-2.0-flash"
            response = client.models.generate_content(
                model=model,
                contents=prompt,
                config=config,
            )
        else:
            print(f"❌ Gemini error: {e}")
            return None

    elapsed = time.time() - start_time

    if hasattr(response, "usage_metadata") and response.usage_metadata:
        usage = response.usage_metadata
        input_tokens = getattr(usage, "prompt_token_count", 0) or 0
        output_tokens = getattr(usage, "candidates_token_count", 0) or 0
        if "flash" in model:
            cost = (input_tokens / 1_000_000) * 0.075 + (output_tokens / 1_000_000) * 0.30
        else:
            cost = (input_tokens / 1_000_000) * 1.25 + (output_tokens / 1_000_000) * 5.00
        print(f"   ✅ {elapsed:.1f}s | {input_tokens:,}+{output_tokens:,} tokens | ${cost:.4f}")

    if not getattr(response, "candidates", None) or not response.candidates:
        print(f"   ⚠️ Model returned no candidates")
        return None
    first_candidate = response.candidates[0]
    if not getattr(first_candidate, "content", None) or not first_candidate.content:
        # Check for finish_reason (e.g. SAFETY, RECITATION)
        reason = getattr(first_candidate, "finish_reason", None)
        if reason and str(reason).upper() not in ("STOP", "END_TURN", "1", "2"):
            print(f"   ⚠️ Model returned: {reason}")
        else:
            print(f"   ⚠️ Model returned no content")
        return None

    return response.text


def process_with_openai(text: str, speaker: str, model: str = "gpt-4o-mini") -> Optional[str]:
    """Process transcript using OpenAI."""
    try:
        from openai import OpenAI
    except ImportError:
        print("❌ openai not installed. Run: pip install openai")
        return None
    
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("❌ OPENAI_API_KEY not found in environment")
        return None
    
    client = OpenAI(api_key=api_key, timeout=180.0)
    
    start_time = time.time()
    
    # Set appropriate max_tokens for model
    max_tokens = 16000 if "mini" in model else 100000
    
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": USER_PROMPT_TEMPLATE.format(speaker=speaker, transcript=text)}
            ],
            temperature=0.1,
            max_tokens=max_tokens,
        )
        
        elapsed = time.time() - start_time
        
        if response.usage:
            input_tokens = response.usage.prompt_tokens
            output_tokens = response.usage.completion_tokens
            
            if model == "gpt-4o-mini":
                cost = (input_tokens / 1_000_000) * 0.15 + (output_tokens / 1_000_000) * 0.60
            elif model == "gpt-4o":
                cost = (input_tokens / 1_000_000) * 2.50 + (output_tokens / 1_000_000) * 10.00
            else:
                cost = 0
            
            print(f"   ✅ {elapsed:.1f}s | {input_tokens:,}+{output_tokens:,} tokens | ${cost:.4f}")
        
        return response.choices[0].message.content
        
    except Exception as e:
        print(f"❌ OpenAI error: {e}")
        return None


def process_transcript(
    text: str,
    speaker: str,
    model: str = "gemini-2.5-flash",
) -> Optional[str]:
    """
    Extract speaker's dialogue from a transcript.
    
    Args:
        text: Full transcript text
        speaker: Speaker name to extract
        model: LLM model to use
        
    Returns:
        Extracted speaker dialogue or None on error
    """
    if model.startswith("gemini"):
        return process_with_gemini(text, speaker, model)
    elif model.startswith("gpt"):
        return process_with_openai(text, speaker, model)
    else:
        print(f"❌ Unknown model: {model}")
        return None


def chunk_text(text: str, chunk_size: int = 8000) -> List[str]:
    """
    Split text into chunks, trying to break at sentence boundaries.
    
    Args:
        text: Text to chunk
        chunk_size: Approximate chunk size in characters
        
    Returns:
        List of text chunks
    """
    if len(text) <= chunk_size:
        return [text]
    
    chunks = []
    current_chunk = ""
    
    # Try to split by paragraphs first, then sentences
    paragraphs = text.split('\n\n')
    
    for para in paragraphs:
        if len(current_chunk) + len(para) + 2 <= chunk_size:
            if current_chunk:
                current_chunk += "\n\n" + para
            else:
                current_chunk = para
        else:
            if current_chunk:
                chunks.append(current_chunk)
            
            # If paragraph itself is too large, split by sentences
            if len(para) > chunk_size:
                sentences = para.split('. ')
                para_chunk = ""
                for sent in sentences:
                    if len(para_chunk) + len(sent) + 2 <= chunk_size:
                        if para_chunk:
                            para_chunk += ". " + sent
                        else:
                            para_chunk = sent
                    else:
                        if para_chunk:
                            chunks.append(para_chunk + ".")
                        para_chunk = sent
                current_chunk = para_chunk
            else:
                current_chunk = para
    
    if current_chunk:
        chunks.append(current_chunk)
    
    return chunks if chunks else [text]


def process_file(
    input_path: str,
    speaker: str,
    output_path: Optional[str] = None,
    model: str = "gemini-2.5-flash",
    chunk_size: int = 8000,
    use_chunking: bool = True,
) -> bool:
    """
    Process a transcript file to extract speaker dialogue.
    Uses chunking for large transcripts to avoid token limits.
    
    Args:
        input_path: Path to transcript file
        speaker: Speaker to extract
        output_path: Output path (default: adds _processed suffix)
        model: LLM model to use
        chunk_size: Character chunk size for splitting large transcripts
        use_chunking: Whether to chunk large transcripts
        
    Returns:
        True if successful
    """
    # Read input
    with open(input_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Skip header if present (our format has header before ===)
    if '=' * 20 in content:
        parts = content.split('=' * 80)
        if len(parts) > 1:
            text = parts[-1].strip()
        else:
            text = content
    else:
        text = content
    
    # Chunk if text is large (roughly > 10k tokens ≈ 7500 chars)
    if use_chunking and len(text) > 7500:
        chunks = chunk_text(text, chunk_size)
        
        if len(chunks) > 1:
            # Process chunks in parallel
            print(f"   📦 Splitting into {len(chunks)} chunks...")
            
            chunk_failure_reasons: List[str] = []
            with ThreadPoolExecutor(max_workers=min(5, len(chunks))) as executor:
                futures = {
                    executor.submit(process_transcript, chunk, speaker, model): i
                    for i, chunk in enumerate(chunks)
                }
                
                chunk_results = {}
                for future in as_completed(futures):
                    idx = futures[future]
                    try:
                        result = future.result()
                        if result and result != "SPEAKER_NOT_FOUND":
                            chunk_results[idx] = result
                        elif result == "SPEAKER_NOT_FOUND":
                            chunk_failure_reasons.append(f"chunk {idx}: SPEAKER_NOT_FOUND")
                        else:
                            chunk_failure_reasons.append(f"chunk {idx}: no content from model")
                    except Exception as e:
                        chunk_failure_reasons.append(f"chunk {idx}: {e}")
                        print(f"   ⚠️ Chunk {idx} failed: {e}")
            
            # Combine chunks
            sorted_results = [chunk_results[i] for i in sorted(chunk_results.keys())]
            result = "\n\n".join(sorted_results) if sorted_results else ""
            if not result and chunk_failure_reasons:
                print(f"   ⚠️ All chunks failed: {'; '.join(chunk_failure_reasons[:3])}{'...' if len(chunk_failure_reasons) > 3 else ''}")
        else:
            result = process_transcript(text, speaker, model)
    else:
        # Process normally
        result = process_transcript(text, speaker, model)
    
    if not result or result == "SPEAKER_NOT_FOUND":
        if result == "SPEAKER_NOT_FOUND":
            print(f"   ⚠️ Speaker '{speaker}' not found in transcript")
        elif not result:
            print(f"   ⚠️ No dialogue extracted (model returned no content)")
        return False
    
    # Determine output path
    if output_path is None:
        input_p = Path(input_path)
        output_path = str(input_p.parent / f"{input_p.stem}_processed.txt")
    
    # Write output
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(f"Speaker: {speaker}\n")
        f.write(f"Source: {input_path}\n")
        f.write("=" * 80 + "\n\n")
        f.write(result)
    
    return True


def process_batch(
    input_dir: str,
    speaker: str,
    output_dir: Optional[str] = None,
    model: str = "gemini-2.5-flash",
    skip_existing: bool = True,
    max_workers: int = 5,
) -> Dict[str, Any]:
    """
    Process all transcript files in a directory with parallel processing.
    
    Args:
        input_dir: Directory with transcript files
        speaker: Speaker to extract
        output_dir: Output directory (default: same as input with _processed suffix)
        model: LLM model to use
        skip_existing: Skip files that already have processed versions
        max_workers: Parallel workers for processing (default: 5)
        
    Returns:
        Summary dict with counts
    """
    input_path = Path(input_dir)
    
    # If output_dir is separate, use same filename; otherwise add _processed suffix
    if output_dir and output_dir != input_dir:
        output_path = Path(output_dir)
        use_same_filename = True
    else:
        output_path = input_path
        use_same_filename = False
    
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Find transcript files (skip _processed files)
    files = [f for f in input_path.glob("*.txt") if "_processed" not in f.name]
    
    print(f"📁 Found {len(files)} transcript files")
    print(f"👤 Extracting dialogue for: {speaker}")
    print(f"🤖 Model: {model}")
    print(f"⚡ Parallel workers: {max_workers}")
    print()
    
    # Separate files into to_process and skipped
    to_process = []
    skipped_count = 0
    
    for file in files:
        if use_same_filename:
            output_file = output_path / file.name
        else:
            output_file = output_path / f"{file.stem}_processed.txt"
        
        if skip_existing and output_file.exists():
            skipped_count += 1
            continue
        
        to_process.append((file, output_file))
    
    if skipped_count > 0:
        print(f"⏭️  Skipping {skipped_count} already processed files")
    
    if not to_process:
        print("✅ All files already processed!")
        return {
            'processed': 0,
            'failed': 0,
            'skipped': skipped_count,
            'files': [],
        }
    
    print(f"🔄 Processing {len(to_process)} files in parallel...")
    print()
    
    results = {
        'processed': 0,
        'failed': 0,
        'skipped': skipped_count,
        'files': [],
    }
    
    # Process files in parallel
    def process_one(args):
        file, output_file = args
        try:
            print(f"🔄 {file.name}")
            success = process_file(str(file), speaker, str(output_file), model)
            if success:
                print(f"   ✅ {file.name}")
                return ('success', str(output_file))
            else:
                print(f"   ❌ {file.name} (failed)")
                return ('failed', None)
        except Exception as e:
            print(f"   ❌ {file.name} (error: {e})")
            return ('failed', None)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_one, args): args[0] for args in to_process}
        
        for future in as_completed(futures):
            status, output_file = future.result()
            if status == 'success':
                results['processed'] += 1
                results['files'].append(output_file)
            else:
                results['failed'] += 1
    
    print()
    print(f"✅ Processed: {results['processed']}")
    print(f"⏭️  Skipped: {results['skipped']}")
    print(f"❌ Failed: {results['failed']}")
    
    return results


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract speaker dialogue from transcripts')
    parser.add_argument('input', help='Input file or directory')
    parser.add_argument('--speaker', '-s', required=True, help='Speaker name to extract')
    parser.add_argument('--output', '-o', help='Output file or directory')
    parser.add_argument('--model', '-m', default='gemini-2.5-flash', help='LLM model')
    parser.add_argument('--batch', action='store_true', help='Process directory')
    
    args = parser.parse_args()
    
    if args.batch or os.path.isdir(args.input):
        process_batch(args.input, args.speaker, args.output, args.model)
    else:
        success = process_file(args.input, args.speaker, args.output, args.model)
        if success:
            print("✅ Done")
        else:
            print("❌ Failed")
