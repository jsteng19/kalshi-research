import pandas as pd
import nltk
from textblob import TextBlob

class SpeechAnalyzer:
    def __init__(self, data_file):
        self.df = pd.read_csv(data_file)
        
    def basic_stats(self):
        """
        Returns basic statistics about the speeches
        """
        stats = {
            'total_speeches': len(self.df),
            'date_range': (self.df['date'].min(), self.df['date'].max()),
            'event_types': self.df['event_type'].value_counts().to_dict()
        }
        return stats
    
    def analyze_sentiment(self, speech_text):
        """
        Performs sentiment analysis on speech text
        """
        blob = TextBlob(speech_text)
        return {
            'polarity': blob.sentiment.polarity,
            'subjectivity': blob.sentiment.subjectivity
        } 