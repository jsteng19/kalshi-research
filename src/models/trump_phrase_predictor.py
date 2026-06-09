#!/usr/bin/env python3
"""
Trump Phrase Mention Probability Predictor

This script implements multiple statistical models to predict the probability 
that Trump will mention a specific phrase by the end of the current month.

Usage:
    python trump_phrase_predictor.py --phrase "fake news" --historical-pct 0.85 --days-elapsed 15
    python trump_phrase_predictor.py --phrase "witch hunt" --historical-pct 0.60 --days-elapsed 5 --current-mentions 1
"""

import argparse
import numpy as np
from scipy import stats
from typing import Dict
import datetime


def bayesian_beta_model(historical_pct: float, days_elapsed: int, days_in_month: int, 
                       current_mentions: int = 0, confidence: float = 0.8) -> Dict:
    """
    Bayesian model using Beta distribution for phrase occurrence probability.
    """
    if current_mentions > 0:
        return {
            'probability': 1.0,
            'method': 'bayesian_beta',
            'confidence_interval': (1.0, 1.0),
            'reasoning': 'Already mentioned this month'
        }
    
    # Convert confidence to effective sample size for prior
    effective_n = int(confidence * 100)
    
    # Beta prior parameters based on historical data
    alpha_prior = historical_pct * effective_n + 1
    beta_prior = (1 - historical_pct) * effective_n + 1
    
    # Days remaining as "trials" where phrase could be mentioned
    days_remaining = days_in_month - days_elapsed
    
    # Update with current month data (no mentions so far)
    alpha_posterior = alpha_prior
    beta_posterior = beta_prior + days_elapsed
    
    # Probability of at least one mention in remaining days
    daily_prob = alpha_posterior / (alpha_posterior + beta_posterior)
    prob_no_mention_remaining = (1 - daily_prob) ** days_remaining
    prob_at_least_one = 1 - prob_no_mention_remaining
    
    # Confidence interval using Beta distribution
    ci_lower = stats.beta.ppf(0.025, alpha_posterior, beta_posterior)
    ci_upper = stats.beta.ppf(0.975, alpha_posterior, beta_posterior)
    
    return {
        'probability': prob_at_least_one,
        'daily_probability': daily_prob,
        'method': 'bayesian_beta',
        'confidence_interval': (ci_lower, ci_upper),
        'prior_strength': effective_n,
        'days_remaining': days_remaining
    }


def poisson_time_decay_model(historical_pct: float, days_elapsed: int, 
                            days_in_month: int, current_mentions: int = 0,
                            decay_factor: float = 0.95) -> Dict:
    """
    Poisson process model with time-dependent intensity.
    """
    if current_mentions > 0:
        return {
            'probability': 1.0,
            'method': 'poisson_decay',
            'reasoning': 'Already mentioned this month'
        }
    
    # Convert historical percentage to initial daily rate
    initial_daily_rate = -np.log(1 - historical_pct) / days_in_month
    
    # Calculate remaining probability with decay
    days_remaining = days_in_month - days_elapsed
    total_remaining_intensity = 0
    
    for day in range(days_remaining):
        day_intensity = initial_daily_rate * (decay_factor ** (days_elapsed + day))
        total_remaining_intensity += day_intensity
    
    # Probability of at least one mention
    prob_at_least_one = 1 - np.exp(-total_remaining_intensity)
    
    return {
        'probability': prob_at_least_one,
        'method': 'poisson_decay',
        'initial_daily_rate': initial_daily_rate,
        'total_remaining_intensity': total_remaining_intensity,
        'decay_factor': decay_factor
    }


def logistic_time_model(historical_pct: float, days_elapsed: int, 
                       days_in_month: int, current_mentions: int = 0,
                       steepness: float = 2.0) -> Dict:
    """
    Logistic model where probability changes based on month progression.
    """
    if current_mentions > 0:
        return {'probability': 1.0, 'method': 'logistic_time', 'reasoning': 'Already mentioned this month'}
    
    progress = days_elapsed / days_in_month
    base_logit = np.log(historical_pct / (1 - historical_pct + 1e-10))
    time_adjustment = -steepness * progress
    adjusted_logit = base_logit + time_adjustment
    probability = 1 / (1 + np.exp(-adjusted_logit))
    
    return {
        'probability': probability,
        'method': 'logistic_time',
        'progress': progress,
        'steepness': steepness
    }


def ensemble_prediction(historical_pct: float, days_elapsed: int, 
                       days_in_month: int, current_mentions: int = 0,
                       weights: Dict[str, float] = None) -> Dict:
    """
    Combine multiple models for ensemble prediction.
    """
    if weights is None:
        weights = {'bayesian': 0.4, 'poisson': 0.3, 'logistic': 0.3}
    
    # Get predictions from all models
    models = {
        'bayesian': bayesian_beta_model(historical_pct, days_elapsed, days_in_month, current_mentions),
        'poisson': poisson_time_decay_model(historical_pct, days_elapsed, days_in_month, current_mentions),
        'logistic': logistic_time_model(historical_pct, days_elapsed, days_in_month, current_mentions)
    }
    
    # Calculate weighted ensemble probability
    ensemble_prob = sum(weights[model] * models[model]['probability'] 
                       for model in weights.keys())
    
    return {
        'ensemble_probability': ensemble_prob,
        'individual_models': models,
        'weights': weights,
        'method': 'ensemble'
    }


def analyze_phrase_probability(phrase: str, historical_pct: float, 
                              days_elapsed: int, days_in_month: int = 31,
                              current_mentions: int = 0, verbose: bool = True):
    """
    Comprehensive analysis of phrase mention probability.
    """
    if verbose:
        print(f"\n=== Phrase Probability Analysis: '{phrase}' ===")
        print(f"Historical percentage of months with 1+ mentions: {historical_pct:.1%}")
        print(f"Days elapsed in current month: {days_elapsed}/{days_in_month}")
        print(f"Current mentions this month: {current_mentions}")
        print(f"Days remaining: {days_in_month - days_elapsed}")
    
    # Get ensemble prediction
    result = ensemble_prediction(historical_pct, days_elapsed, days_in_month, current_mentions)
    
    if verbose:
        print(f"\n🎯 ENSEMBLE PREDICTION: {result['ensemble_probability']:.1%}")
        
        print("\n📊 Individual Model Results:")
        for model_name, model_result in result['individual_models'].items():
            prob = model_result['probability']
            print(f"  {model_name.capitalize():>10}: {prob:.1%}")
        
        # Add confidence interval for Bayesian model
        bayesian_result = result['individual_models']['bayesian']
        if 'confidence_interval' in bayesian_result:
            ci_lower, ci_upper = bayesian_result['confidence_interval']
            print(f"\n📈 Bayesian 95% Confidence Interval: [{ci_lower:.1%}, {ci_upper:.1%}]")
    
    return result


def get_current_month_info():
    """Get current month information for convenience."""
    now = datetime.datetime.now()
    days_in_month = (datetime.date(now.year, now.month + 1, 1) - datetime.timedelta(days=1)).day if now.month < 12 else 31
    return now.day, days_in_month


def main():
    parser = argparse.ArgumentParser(description='Predict Trump phrase mention probability')
    parser.add_argument('--phrase', required=True, help='Phrase to analyze (e.g., "fake news")')
    parser.add_argument('--historical-pct', type=float, required=True, 
                       help='Historical percentage of months with 1+ mentions (0.0-1.0)')
    parser.add_argument('--days-elapsed', type=int, 
                       help='Days elapsed in current month (default: current day)')
    parser.add_argument('--days-in-month', type=int, default=31,
                       help='Total days in month (default: 31)')
    parser.add_argument('--current-mentions', type=int, default=0,
                       help='Number of mentions so far this month (default: 0)')
    parser.add_argument('--model', choices=['bayesian', 'poisson', 'logistic', 'ensemble'], 
                       default='ensemble', help='Model to use (default: ensemble)')
    parser.add_argument('--quiet', action='store_true', help='Only output the probability')
    
    args = parser.parse_args()
    
    # Use current day if not specified
    if args.days_elapsed is None:
        current_day, current_month_days = get_current_month_info()
        args.days_elapsed = current_day
        args.days_in_month = current_month_days
        if not args.quiet:
            print(f"Using current date: day {current_day} of {current_month_days}")
    
    # Validate inputs
    if not 0 <= args.historical_pct <= 1:
        print("Error: historical-pct must be between 0.0 and 1.0")
        return
    
    if args.days_elapsed > args.days_in_month:
        print("Error: days-elapsed cannot exceed days-in-month")
        return
    
    # Run analysis
    result = analyze_phrase_probability(
        args.phrase, 
        args.historical_pct, 
        args.days_elapsed, 
        args.days_in_month,
        args.current_mentions,
        verbose=not args.quiet
    )
    
    if args.quiet:
        if args.model == 'ensemble':
            print(f"{result['ensemble_probability']:.3f}")
        else:
            print(f"{result['individual_models'][args.model]['probability']:.3f}")
    else:
        print(f"\n✅ Analysis complete for '{args.phrase}'")
        
        # Provide interpretation
        prob = result['ensemble_probability']
        if prob > 0.8:
            interpretation = "Very likely to be mentioned"
        elif prob > 0.6:
            interpretation = "Likely to be mentioned"
        elif prob > 0.4:
            interpretation = "Moderate chance of being mentioned"
        elif prob > 0.2:
            interpretation = "Low chance of being mentioned"
        else:
            interpretation = "Very unlikely to be mentioned"
        
        print(f"📝 Interpretation: {interpretation}")


if __name__ == "__main__":
    main() 