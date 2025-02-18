# trump-speech

A data analysis project that predicts and analyzes patterns in Trump's potential 2025 State of the Union address based on:

- Pre-inauguration speeches and rallies
- Post-inauguration speeches (Jan 20, 2025 onwards) 
- Previous State of the Union addresses (2017-2020)

## Analysis Methodology

The analysis uses Poisson distribution models to:
- Calculate expected frequencies of key phrases
- Generate confidence intervals for phrase occurrences
- Compute probabilities of specific phrase thresholds (≥1, ≥3, ≥4, ≥5, ≥15 occurrences)

Additional analysis includes:
- Time series analysis of phrase frequencies
- Historical SOTU phrase patterns
- Recent contextual usage of key phrases
- Per-speech frequency distributions

Results are normalized by speech length and visualized through time series plots and frequency distributions.