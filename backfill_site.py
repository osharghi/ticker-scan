"""
One-off: copy existing morning snapshots from the private state bucket into
the public website bucket, so the site starts with history instead of empty.

Afternoon prices were never stored before the website existed, so backfilled
days show the morning chart only.

Usage (after `aws sso login`):
    python backfill_site.py <state-bucket> <site-bucket> [state-prefix]
"""
import json
import sys

import boto3

SITE_DATA_PREFIX = "data"


def main(state_bucket, site_bucket, state_prefix="ticker-scan"):
    s3 = boto3.client("s3")

    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=state_bucket, Prefix=f"{state_prefix}/"
    ):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".json")]

    dates = []
    for key in sorted(keys):
        date = key.rsplit("/", 1)[-1].removesuffix(".json")
        site_key = f"{SITE_DATA_PREFIX}/{date}.json"
        try:
            # Never overwrite a day the Lambda already published -- it may
            # hold afternoon prices that the state snapshot doesn't have.
            s3.head_object(Bucket=site_bucket, Key=site_key)
            print(f"skip {date} (already on site)")
            dates.append(date)
            continue
        except s3.exceptions.ClientError:
            pass
        state =json.loads(s3.get_object(Bucket=state_bucket, Key=key)["Body"].read())
        day = {
            "date": date,
            "am_time": state["snapshot_time"],
            "pm_time": None,
            "tickers": [
                {
                    "ticker": t["ticker"],
                    "open": t["open"],
                    "am_price": t["morning_price"],
                    "pm_price": None,
                }
                for t in state["tickers"]
            ],
        }
        s3.put_object(
            Bucket=site_bucket,
            Key=site_key,
            Body=json.dumps(day).encode("utf-8"),
            ContentType="application/json",
            CacheControl="max-age=60",
        )
        dates.append(date)
        print(f"backfilled {date} ({len(day['tickers'])} tickers)")

    index_key = f"{SITE_DATA_PREFIX}/index.json"
    try:
        existing = json.loads(s3.get_object(Bucket=site_bucket, Key=index_key)["Body"].read())["dates"]
    except s3.exceptions.NoSuchKey:
        existing = []
    all_dates = sorted(set(existing) | set(dates), reverse=True)
    s3.put_object(
        Bucket=site_bucket,
        Key=index_key,
        Body=json.dumps({"dates": all_dates}).encode("utf-8"),
        ContentType="application/json",
        CacheControl="max-age=60",
    )
    print(f"index now lists {len(all_dates)} days")


if __name__ == "__main__":
    main(*sys.argv[1:])
