"""
One-off: fill in afternoon prices for site days published before the Lambda
started recording them.

The afternoon run has fired at 3:00 PM ET (12:00 PM Pacific) every weekday
since 2026-09-27, so each missing PM price is reconstructed from Tiingo's
5-minute IEX history as the price at 3:00 PM ET (close of the 2:55 PM bar).
Only days with no afternoon data are touched; a day the Lambda already
recorded is never overwritten.

Usage (after `aws sso login`, with TIINGO_API_TOKEN set):
    python backfill_pm.py <site-bucket> [cache-dir]
"""
import datetime
import json
import os
import sys
import time
from collections import defaultdict
from zoneinfo import ZoneInfo

import boto3
import requests

ET = ZoneInfo("America/New_York")
PM_HOUR, PM_MINUTE = 15, 0
IEX_HISTORY = "https://api.tiingo.com/iex/{ticker}/prices"


def fetch_bars(ticker, start, end, token, cache_dir):
    """5-minute bars for one ticker over [start, end]; cached on disk so a
    rerun (e.g. after hitting a rate limit) never refetches."""
    path = os.path.join(cache_dir, f"{ticker}_{start}_{end}.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    params = {"startDate": start, "endDate": end, "resampleFreq": "5min",
              "columns": "open,close", "token": token}
    for attempt in range(6):
        resp = requests.get(IEX_HISTORY.format(ticker=ticker), params=params, timeout=60)
        if resp.status_code == 429 or (resp.status_code == 200 and "rate limit" in resp.text.lower()[:200]):
            wait = 60 * (attempt + 1)
            print(f"  rate limited on {ticker}; waiting {wait}s")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        bars = resp.json()
        with open(path, "w") as f:
            json.dump(bars, f)
        return bars
    raise RuntimeError(f"gave up on {ticker} after repeated rate limiting")


def price_at(bars, date):
    """Price at 3:00 PM ET on `date`: close of the last bar that starts
    before 3:00 PM ET that day (normally the 2:55 PM bar)."""
    cutoff = datetime.datetime.fromisoformat(date).replace(hour=PM_HOUR, minute=PM_MINUTE, tzinfo=ET)
    best = None
    for b in bars:
        t = datetime.datetime.fromisoformat(b["date"].replace("Z", "+00:00")).astimezone(ET)
        if t.date().isoformat() == date and t < cutoff and b.get("close") is not None:
            if best is None or t > best[0]:
                best = (t, b["close"])
    return best[1] if best else None


def main(site_bucket, cache_dir="pm_cache"):
    token = os.environ["TIINGO_API_TOKEN"]
    os.makedirs(cache_dir, exist_ok=True)
    s3 = boto3.client("s3")

    dates = json.loads(s3.get_object(Bucket=site_bucket, Key="data/index.json")["Body"].read())["dates"]
    days = {}
    for d in dates:
        day = json.loads(s3.get_object(Bucket=site_bucket, Key=f"data/{d}.json")["Body"].read())
        if day.get("pm_time") is None:
            days[d] = day
    print(f"{len(days)} of {len(dates)} days need afternoon prices")

    # One request per ticker, spanning every date that ticker appears on.
    ticker_dates = defaultdict(list)
    for d, day in days.items():
        for t in day["tickers"]:
            ticker_dates[t["ticker"]].append(d)

    bars_by_ticker = {}
    for i, (ticker, ds) in enumerate(sorted(ticker_dates.items()), 1):
        bars_by_ticker[ticker] = fetch_bars(ticker, min(ds), max(ds), token, cache_dir)
        print(f"[{i}/{len(ticker_dates)}] {ticker}: {len(bars_by_ticker[ticker])} bars")

    for d in sorted(days):
        day = days[d]
        missing = []
        for t in day["tickers"]:
            t["pm_price"] = price_at(bars_by_ticker[t["ticker"]], d)
            if t["pm_price"] is None:
                missing.append(t["ticker"])
        day["pm_time"] = datetime.datetime.fromisoformat(d).replace(
            hour=PM_HOUR, minute=PM_MINUTE, tzinfo=ET).isoformat()
        day["pm_reconstructed"] = True  # from price history, not the live afternoon run
        s3.put_object(Bucket=site_bucket, Key=f"data/{d}.json",
                      Body=json.dumps(day).encode("utf-8"),
                      ContentType="application/json", CacheControl="max-age=60")
        print(f"{d}: filled {len(day['tickers']) - len(missing)}/{len(day['tickers'])}"
              + (f" (no 3pm price for {', '.join(missing)})" if missing else ""))


if __name__ == "__main__":
    main(*sys.argv[1:])
