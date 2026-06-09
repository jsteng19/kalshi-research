#!/usr/bin/env python3
"""Kelly Criterion position sizing calculator for prediction markets."""

import argparse


def kelly_size(fair: float, market: float) -> float:
    """
    Calculate Kelly fraction for a binary bet.
    
    Args:
        fair: Your estimated probability of winning (0-100)
        market: Current market price (0-100)
    
    Returns:
        Optimal fraction of bankroll to bet
    """
    p = fair / 100  # probability of win
    q = 1 - p       # probability of loss
    
    # Determine if we're buying YES or NO
    if fair > market:
        # Buy YES at market price
        cost = market / 100
        profit = 1 - cost
        b = profit / cost  # odds
        kelly = (b * p - q) / b
        direction = "YES"
    else:
        # Buy NO at (1 - market) price
        cost = (100 - market) / 100
        profit = 1 - cost
        b = profit / cost
        p_no = q  # prob of NO winning
        q_no = p  # prob of NO losing
        kelly = (b * p_no - q_no) / b
        direction = "NO"
    
    return max(kelly, 0), direction


def main():
    parser = argparse.ArgumentParser(
        description="Kelly Criterion calculator for prediction markets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python kelly.py --fair 60 --market 50
  python kelly.py --fair 60 --market 50 --bankroll 1000 --fraction 0.5
        """
    )
    parser.add_argument("--fair", "-f", type=float, required=True,
                        help="Your fair value probability (0-100)")
    parser.add_argument("--market", "-m", type=float, required=True,
                        help="Current market price (0-100)")
    parser.add_argument("--bankroll", "-b", type=float, default=1000,
                        help="Your bankroll (default: 1000)")
    parser.add_argument("--fraction", "-k", type=float, default=1.0,
                        help="Kelly fraction to use, e.g., 0.5 for half-Kelly (default: 1.0)")
    
    args = parser.parse_args()
    
    # Validate inputs
    if not 0 < args.fair < 100:
        print("Error: fair value must be between 0 and 100")
        return
    if not 0 < args.market < 100:
        print("Error: market value must be between 0 and 100")
        return
    if args.fraction <= 0:
        print("Error: kelly fraction must be positive")
        return
    
    kelly, direction = kelly_size(args.fair, args.market)
    adjusted_kelly = kelly * args.fraction
    bet_size = args.bankroll * adjusted_kelly
    
    edge = abs(args.fair - args.market)

    # Cost per contract in dollars
    if direction == "YES":
        cost_per_contract = args.market / 100
    else:
        cost_per_contract = (100 - args.market) / 100

    contracts = int(bet_size / cost_per_contract) if cost_per_contract > 0 else 0

    print(f"\n{'='*40}")
    print(f"  Kelly Criterion Calculator")
    print(f"{'='*40}")
    print(f"  Fair value:     {args.fair:.1f}%")
    print(f"  Market price:   {args.market:.1f}%")
    print(f"  Edge:           {edge:.1f}%")
    print(f"  Direction:      {direction}")
    print(f"{'='*40}")
    print(f"  Full Kelly:     {kelly*100:.2f}%")
    print(f"  Your fraction:  {args.fraction:.2f}x")
    print(f"  Adjusted Kelly: {adjusted_kelly*100:.2f}%")
    print(f"{'='*40}")
    print(f"  Bankroll:       ${args.bankroll:,.2f}")
    print(f"  Bet size:       ${bet_size:,.2f}")
    print(f"  Contracts:      {contracts} {direction} @ ${cost_per_contract:.2f}")
    print(f"{'='*40}\n")


if __name__ == "__main__":
    main()


