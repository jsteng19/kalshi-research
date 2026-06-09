## Press Briefing Collection Tips

Government press briefings often have multiple sources:

1. **Official Sources (Best Quality)**
   - White House: https://www.whitehouse.gov/briefing-room/
   - State Department: https://www.state.gov/briefings/
   - Treasury: https://home.treasury.gov/news/press-releases
   - These have official transcripts - use Firecrawl to extract

2. **Video Sources**
   - C-SPAN is the most reliable for full, unedited briefings
   - White House YouTube channel posts official videos
   - Search: "[Agency] press briefing [date]"

3. **Transcript Extraction**
   For official government sites, use Firecrawl:
   ```python
   from src.auto_collect.strategies.firecrawl import extract_transcript_from_page
   result = extract_transcript_from_page("https://www.whitehouse.gov/briefing-room/...")
   ```

4. **Common Issues**
   - Briefings may be delayed in posting
   - Some briefings are "off camera" (audio only, harder to find)
   - Foreign government briefings may need translation
