# Kalshi Research

This repository contains tools for analyzing political speech transcripts and making predictions for Kalshi markets.

## Features

### Multi-Politician Support
- **Trump Analysis**: Original analysis framework for Donald Trump speeches
- **Harris Analysis**: Parallel analysis framework for Kamala Harris speeches
- **Configurable Scraper**: Single scraper works for both politicians via URL and parameter configuration

### Data Collection
- Automated web scraping from Roll Call Factbase
- Support for multiple transcript types (speeches, interviews, press briefings, etc.)
- Configurable date ranges and filtering

### Statistical Analysis
- **Phrase Frequency Analysis**: Track key phrases over time
- **Poisson Modeling**: Predict phrase occurrences in future speeches
- **Negative Binomial Modeling**: Account for overdispersion in phrase usage
- **Time Series Analysis**: Visualize trends and patterns
- **Contextual Analysis**: Extract usage examples with surrounding context

## Usage

### Trump Analysis
```python
# Data Collection
# Run notebooks/run_scraper.ipynb

# Analysis  
# Run notebooks/trump_appearance.ipynb
```

### Harris Analysis
```python
# Data Collection
# Run notebooks/harris_run_scraper.ipynb

# Analysis
# Run notebooks/harris_appearance.ipynb
```

### Custom Politician Analysis
```python
from src.speech_scraper import TrumpSpeechScraper

# Configure for any politician
scraper = TrumpSpeechScraper(
    url="https://rollcall.com/factbase/[politician]/search/",
    save_path="data-[politician]/transcript-urls/urls.txt", 
    politician="[politician]"
)
```

## Data Structure

```
data/                    # Trump data
├── raw-transcripts/
├── processed-transcripts/
└── transcript-urls/

data-harris/             # Harris data  
├── raw-transcripts/
├── processed-transcripts/
└── transcript-urls/
```

## Key Files

- `src/speech_scraper.py`: Configurable web scraper for any politician
- `src/process_transcripts.py`: Text processing with politician-specific speaker patterns
- `notebooks/trump_appearance.ipynb`: Comprehensive Trump analysis
- `notebooks/harris_appearance.ipynb`: Comprehensive Harris analysis
- `notebooks/run_scraper.ipynb`: Trump data collection
- `notebooks/harris_run_scraper.ipynb`: Harris data collection

## Requirements

See `requirements.txt` for Python dependencies.

## License

See LICENSE file.
