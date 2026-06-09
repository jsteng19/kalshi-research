## Interview Collection Tips

Interviews can come from many sources:

1. **News Channel YouTube Channels**
   - CNBC, Bloomberg, Fox Business for financial figures
   - CNN, Fox News, MSNBC for political figures
   - Search: "[Person] [Network] interview [date]"

2. **Podcast Appearances**
   - Many podcasts post full episodes on YouTube
   - Check Spotify and Apple Podcasts for episode lists
   - Search: "[Person] podcast interview"

3. **Conference/Event Interviews**
   - WEF/Davos: World Economic Forum YouTube channel
   - Tech conferences: Often posted on event's YouTube
   - Search: "[Person] [Event Name] interview"

4. **Tips**
   - "Full interview" in search helps avoid clips
   - Check the guest's official social media for interview announcements
   - News aggregators may have text transcripts

5. **Fallback: Firecrawl Search**
   If you can't find video, search for text transcripts:
   ```python
   from src.auto_collect.strategies.firecrawl import FirecrawlStrategy
   strategy = FirecrawlStrategy()
   result = strategy.search_and_extract("[Person] interview transcript [date]", speaker_config)
   ```
