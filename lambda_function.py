import datetime
import html
import json
import os
import smtplib
import time
import traceback
from email.message import EmailMessage
from zoneinfo import ZoneInfo

import boto3
import pandas as pd
import requests

from sp500_tickers import SP500_TICKERS

# ---------------------------------------------------------------------------
# Configuration -- all secrets/settings come from Lambda environment
# variables. Never hardcode API tokens or passwords in source.
#
# SENDER_EMAIL/SENDER_APP_PASSWORD are read strictly (crash on cold start
# if missing) because without them we can't even send a failure
# notification. Everything else is read loosely and validated inside
# lambda_handler so a misconfiguration still reaches your inbox instead
# of only CloudWatch Logs.
# ---------------------------------------------------------------------------
SENDER_EMAIL = os.environ["SENDER_EMAIL"]
SENDER_APP_PASSWORD = os.environ["SENDER_APP_PASSWORD"]
RECEIVER_EMAIL = os.environ.get("RECEIVER_EMAIL", SENDER_EMAIL)

TIINGO_TOKEN = os.environ.get("TIINGO_API_TOKEN")
STATE_BUCKET = os.environ.get("STATE_BUCKET")
STATE_PREFIX = os.environ.get("STATE_PREFIX", "ticker-scan")

TOP_N = int(os.environ.get("TOP_N", "10"))
MIN_PRICE = float(os.environ.get("MIN_PRICE", "5"))
MIN_VOLUME = int(os.environ.get("MIN_VOLUME", "100000"))

MARKET_TZ = ZoneInfo("America/New_York")
IEX_QUOTE_ENDPOINT = "https://api.tiingo.com/iex"
BATCH_SIZE = 150  # keep query strings well under URL length limits

s3 = boto3.client("s3")


def _today_state_key():
    today = datetime.datetime.now(MARKET_TZ).strftime("%Y-%m-%d")
    return f"{STATE_PREFIX}/{today}.json"


def _get_with_retries(url, params, retries=4, backoff_seconds=2):
    """Tiingo's IEX endpoint occasionally returns a transient 502; retry
    with backoff before giving up on this batch."""
    last_error = None
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            last_error = e
            print(f"Request failed (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(backoff_seconds * (attempt + 1))
    raise last_error


def fetch_quotes(tickers):
    """Fetch current IEX top-of-book quotes for a batch of tickers.

    A batch that keeps failing after retries is skipped (logged) rather
    than aborting the whole run -- a partial scan/check is more useful
    than none for an unattended daily job.
    """
    quotes = []
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i + BATCH_SIZE]
        try:
            resp = _get_with_retries(
                IEX_QUOTE_ENDPOINT,
                params={"tickers": ",".join(batch), "token": TIINGO_TOKEN},
            )
            quotes.extend(resp.json())
        except requests.exceptions.RequestException as e:
            print(f"Giving up on batch {i}-{i + len(batch)} after retries: {e}")

    if not quotes:
        raise RuntimeError(
            f"No quotes returned for any of {len(tickers)} requested tickers -- "
            "Tiingo may be down or the token/params are invalid."
        )
    return quotes


def _send_email(subject, html_body):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECEIVER_EMAIL
    msg.set_content("This email requires an HTML-capable client to view.")
    msg.add_alternative(html_body, subtype="html")

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(SENDER_EMAIL, SENDER_APP_PASSWORD)
        server.send_message(msg)
    print(f"Email sent: {subject}")


def _notify_failure(mode, error):
    """Best-effort failure email. Swallows its own errors (e.g. SMTP itself
    being broken) so a secondary failure doesn't mask the original one --
    that case is still visible in CloudWatch Logs and via the re-raise in
    lambda_handler."""
    tb = traceback.format_exc()
    print(f"FAILURE in mode='{mode}': {error}\n{tb}")
    try:
        _send_email(
            f"Ticker scan FAILED ({mode})",
            (
                f"<h3>The '{html.escape(mode)}' run failed</h3>"
                f"<p><b>{html.escape(type(error).__name__)}:</b> {html.escape(str(error))}</p>"
                f"<pre style='white-space:pre-wrap;font-size:12px'>{html.escape(tb)}</pre>"
            ),
        )
    except Exception as email_error:
        print(f"Also failed to send failure notification email: {email_error}")


def _rows_to_html_table(rows, columns):
    header = "".join(f"<th style='text-align:left;padding:4px 10px'>{c}</th>" for c in columns)
    body_rows = "".join(
        "<tr>" + "".join(f"<td style='padding:4px 10px'>{row[c]}</td>" for c in columns) + "</tr>"
        for row in rows
    )
    return (
        "<table style='border-collapse:collapse;font-family:sans-serif;font-size:14px'>"
        f"<thead><tr>{header}</tr></thead><tbody>{body_rows}</tbody></table>"
    )


# ---------------------------------------------------------------------------
# Morning: scan the S&P 500 for the biggest movers since market open
# ---------------------------------------------------------------------------
def run_morning_scan(tickers=None):
    quotes = fetch_quotes(tickers or SP500_TICKERS)
    df = pd.DataFrame(quotes)

    df = df.dropna(subset=["open", "tngoLast"])
    df = df[df["open"] > 0]
    df["pct_change_since_open"] = (df["tngoLast"] - df["open"]) / df["open"] * 100

    df = df[df["tngoLast"] >= MIN_PRICE]
    if "volume" in df.columns:
        df = df[df["volume"].fillna(0) >= MIN_VOLUME]

    top = df.sort_values("pct_change_since_open", ascending=False).head(TOP_N)

    if top.empty:
        _send_email(
            "Morning ticker scan: no qualifying movers",
            "<p>No S&amp;P 500 tickers passed the price/volume filters this morning.</p>",
        )
        return {"statusCode": 200, "body": "No qualifying tickers"}

    snapshot_time = datetime.datetime.now(MARKET_TZ).isoformat()
    state = {
        "snapshot_time": snapshot_time,
        "tickers": [
            {
                "ticker": r["ticker"],
                "open": r["open"],
                "morning_price": r["tngoLast"],
                "pct_change_since_open": r["pct_change_since_open"],
            }
            for _, r in top.iterrows()
        ],
    }
    s3.put_object(
        Bucket=STATE_BUCKET,
        Key=_today_state_key(),
        Body=json.dumps(state).encode("utf-8"),
        ContentType="application/json",
    )

    rows = [
        {
            "Ticker": r["ticker"],
            "Open": f"${r['open']:.2f}",
            "Now": f"${r['tngoLast']:.2f}",
            "Change since open": f"{r['pct_change_since_open']:+.2f}%",
        }
        for _, r in top.iterrows()
    ]
    html = f"<h3>Top {len(rows)} S&amp;P 500 movers since open</h3>" + _rows_to_html_table(
        rows, ["Ticker", "Open", "Now", "Change since open"]
    )
    _send_email(f"Morning scan: top {len(rows)} movers since open", html)
    return {"statusCode": 200, "body": f"Scanned {len(df)} tickers, emailed top {len(rows)}"}


# ---------------------------------------------------------------------------
# Afternoon: re-check this morning's top movers and report gain/loss since then
# ---------------------------------------------------------------------------
def run_afternoon_check():
    try:
        obj = s3.get_object(Bucket=STATE_BUCKET, Key=_today_state_key())
        state = json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        _send_email(
            "Afternoon check: no morning scan found",
            "<p>No morning snapshot was found in S3 for today, so there's nothing to compare.</p>",
        )
        return {"statusCode": 200, "body": "No morning snapshot found"}

    morning_tickers = state["tickers"]
    ticker_list = [t["ticker"] for t in morning_tickers]
    quotes_by_ticker = {q["ticker"]: q for q in fetch_quotes(ticker_list)}

    rows = []
    for t in morning_tickers:
        q = quotes_by_ticker.get(t["ticker"])
        if not q or q.get("tngoLast") is None:
            continue
        current_price = q["tngoLast"]
        pct_since_morning = (current_price - t["morning_price"]) / t["morning_price"] * 100
        pct_since_open = (current_price - t["open"]) / t["open"] * 100
        rows.append({
            "Ticker": t["ticker"],
            "Open": f"${t['open']:.2f}",
            "Morning price": f"${t['morning_price']:.2f}",
            "Now": f"${current_price:.2f}",
            "_sort": pct_since_morning,
            "Change since morning email": f"{pct_since_morning:+.2f}%",
            "Change since open": f"{pct_since_open:+.2f}%",
        })

    rows.sort(key=lambda r: r["_sort"], reverse=True)
    for r in rows:
        del r["_sort"]

    html = f"<h3>Afternoon check-in (morning snapshot: {state['snapshot_time']})</h3>" + _rows_to_html_table(
        rows, ["Ticker", "Open", "Morning price", "Now", "Change since morning email", "Change since open"]
    )
    _send_email("Afternoon check: how this morning's movers are doing", html)
    return {"statusCode": 200, "body": f"Checked {len(rows)} tickers"}


def lambda_handler(event, context):
    mode = (event or {}).get("mode", "morning")
    print(f"Running in '{mode}' mode at {datetime.datetime.now(MARKET_TZ).isoformat()}")

    try:
        if not TIINGO_TOKEN:
            raise RuntimeError("TIINGO_API_TOKEN environment variable is not set")
        if not STATE_BUCKET:
            raise RuntimeError("STATE_BUCKET environment variable is not set")

        if mode == "morning":
            return run_morning_scan(tickers=(event or {}).get("tickers"))
        elif mode == "afternoon":
            return run_afternoon_check()
        else:
            raise ValueError(f"Unknown mode '{mode}'. Expected 'morning' or 'afternoon'.")
    except Exception as e:
        _notify_failure(mode, e)
        raise  # keep the invocation marked as failed in Lambda/CloudWatch metrics
