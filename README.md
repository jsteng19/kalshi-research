# kalshi-research

A data analysis project for prediction markets focused on predicting phrase occurrences in political speeches and public appearances.

## Background

Prediction markets are powerful tools for aggregating information and forecasting future events by allowing participants to trade contracts based on event outcomes. [Kalshi](https://kalshi.com) is the first CFTC-regulated exchange for event contracts in the US, enabling traders to take positions on various outcomes, including the occurence of specific phrases in political speeches.

This project analyzes Donald Trump's speech patterns to inform trading on his mention markets, focusing on:
- Pre-inauguration appearances (currently back to Sept 2023)
- Post-inauguration appearances (Jan 20, 2025 onwards) 
- Previous State of the Union addresses (2017-2020)

The analysis provides a valuable reference for event contract speculators by:
- Gathering and cleaning data
- Identifying patterns in phrase usage over time
- Quantifying the frequency of specific phrases and applying statistical models
- Understanding contextual factors that influence speech content (type of appearance, phrase clustering, etc.)

## Data Collection

Speech transcripts are collected using an automated scraper that interfaces with Roll Call's ([factba.se](https://rollcall.com/factbase/)) archive. The scraper:
- Navigates through historical speech archives
- Extracts full transcripts while preserving speaker attribution
- Categorizes speeches by type (speeches, remarks, interviews, etc.) and records the date


## Analysis Methodology

### Time Series Analysis:
  - Tracks phrase frequency over time
  - Compare different types of appearances

### Previous State of the Unions

Small sample size for rare phrases, but useful for gauging stable common phrases as well as the general tone and composition of the addresses. Also used to forecast the length of future SOTU addresses.

### Forecasting probabilities with statistical models

Use the past frequency of a phrase to forecast the probability of it occurring during the expected length of a future speech. 

#### Poisson Distribution
Using the Poisson distribution, we assume each word is independent and identically distributed. This allows us to compute threshold probabilities for a phrase occurring a given number of times, which can be compared to a market's implied probability. However, this approach does not account for the clear dependence between words in a speech. The most obvious dependence is single word clustering: a word is often more likely to quickly be mentioned again after being mentioned once. The structure of a speech is also important: for example tariffs might be more likely to be mentioned in the second half of an address given they were not mentioned in the first half.

#### Other Statistical Models

Negative Binomial Model:
   - Generalizes the Poisson distribution to allow for overdispersion
   - Better handles clustered occurrences where frequency variance > mean
   - More accurate for phrases that tend to appear in bursts


We can use these models to predict the probability of a phrase occurring a given number of times in a future speech, given an assumed length.

### Per Speech Analysis

Important to compare per-word to per speech analysis.  


## Papers and research

[How to Analyze Political Attention with Minimal
Assumptions and Costs](https://www.law.berkeley.edu/files/TopicModelAJPS(2).pdf)
