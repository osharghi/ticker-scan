"""
Small fixed ticker list for local/manual testing, so a test run doesn't
have to fetch and filter all 503 S&P 500 tickers.

Same variable name as sp500_tickers.py (SP500_TICKERS) so it can be
swapped in as a drop-in replacement, e.g.:

    from sp500_tickers_test import SP500_TICKERS
"""

SP500_TICKERS = [
    "MSFT", "META", "TSLA", "GOOG",
]
