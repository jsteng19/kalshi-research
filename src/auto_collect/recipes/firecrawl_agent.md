## Firecrawl Agent Collection Tips

The Firecrawl `/agent` endpoint is the easiest way to find transcripts when you don't know the exact URL. It autonomously searches, navigates, and extracts content.

### When to Use Agent Mode

Use the agent when:
- You don't have the exact URL for the transcript
- The transcript might be on multiple different sites
- You need to search across YouTube, news sites, and official sources
- The content requires navigating through paginated or dynamic pages

### Quick Start

```python
from src.auto_collect.strategies.firecrawl import agent_find_transcript

result = agent_find_transcript(
    speaker="Bernie Sanders",
    event_description="The Late Show with Stephen Colbert interview",
    target_date="January 21, 2026",
    hint_urls=["youtube.com", "cbs.com"],  # Optional: guide the search
    max_credits=50,  # Cost control
    verbose=True,
)

if result and result.get('transcripts'):
    transcript = result['transcripts'][0]
    print(f"Found: {transcript['title']}")
    print(f"Source: {transcript['source_url']}")
    print(transcript['full_text'][:1000])
```

### Cost Control

The agent charges credits based on complexity. Use these tips:
- Set `max_credits` to limit spending (default: 50)
- Use `spark-1-mini` model for faster, cheaper results
- Provide `hint_urls` to narrow the search scope
- Set `strict_url_constraint=True` if you want to limit to specific domains

### Hint URLs by Event Type

**Late Night Shows:**
```python
hint_urls = ["youtube.com/@colaborshow", "cbs.com", "youtube.com"]
```

**Press Briefings:**
```python
hint_urls = ["c-span.org", "whitehouse.gov", "youtube.com/@cspan"]
```

**Speeches:**
```python
hint_urls = ["c-span.org", "youtube.com/@cspan", "congress.gov"]
```

### Advanced: Custom Schema

For custom extraction, provide your own schema:

```python
from src.auto_collect.strategies.firecrawl import FirecrawlStrategy

strategy = FirecrawlStrategy()
result = strategy.agent_collect(
    prompt="Find all mentions of 'tariff' in Scott Bessent's press briefings",
    speaker_config={'name': 'Scott Bessent'},
    schema={
        "type": "object",
        "properties": {
            "mentions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "quote": {"type": "string"},
                        "context": {"type": "string"},
                        "date": {"type": "string"},
                    }
                }
            }
        }
    },
    max_credits=100,
)
```

### Troubleshooting

**Agent times out:**
- Increase timeout or reduce scope with hint_urls
- Try a more specific prompt

**No results found:**
- Check if the event has actually happened yet
- Try broader search terms
- Remove strict URL constraints

**High credit usage:**
- Add hint_urls to narrow scope
- Use spark-1-mini instead of spark-1-pro
- Set lower max_credits limit
