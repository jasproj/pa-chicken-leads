#!/usr/bin/env python3
"""
Weekly rank tracker: Powerstream + competitor visibility on target keywords.
Runs on GitHub Actions (see .github/workflows/rank_tracker.yml). Logs to Supabase.
"""

import os
import sys
import json
import time
import urllib.request

KEYWORDS = [
    # P1 - agricultural cluster
    "agricultural solar installer pennsylvania",
    "poultry house solar panels pennsylvania",
    "solar panels for chicken house",
    "barn roof solar panels pa",
    "solar panels dairy farm pennsylvania",
    "farm solar lancaster county",
    "REAP grant solar pennsylvania",
    # P1 - local installer cluster
    "solar installer lancaster pa",
    "solar companies lancaster pa",
    # P2 - expansion
    "solar installer chester county pa",
    "solar installer york pa",
    "solar installer berks county pa",
    "solar installer lebanon county pa",
    "commercial solar installer pennsylvania",
    # P2 - education
    "pennsylvania SREC price",
    "pa net metering rules",
    "solar tax credit pennsylvania 2026",
]

DOMAINS = [
    "powerstreamsolarelectric.com",
    "powerstreamelectric.com",
    "paradisesolarenergy.com",
    "belmontsolar.com",
    "twilightrenewables.com",
    "trifectasolar.com",
    "solarrooflease.com",
]

SERPER_URL = "https://google.serper.dev/search"
LOCATION = "Lancaster, Pennsylvania, United States"


def serp(keyword, api_key):
    body = json.dumps({"q": keyword, "location": LOCATION, "num": 100}).encode()
    req = urllib.request.Request(
        SERPER_URL, data=body,
        headers={"X-API-KEY": api_key, "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def positions(result):
    found = {}
    for item in result.get("organic", []):
        link = item.get("link", "")
        pos = item.get("position")
        for d in DOMAINS:
            if d in link and d not in found:
                found[d] = (pos, link)
    return found


def supabase_insert(rows, url, key):
    req = urllib.request.Request(
        f"{url}/rest/v1/rank_checks",
        data=json.dumps(rows).encode(),
        headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        },
        method="POST",
    )
    urllib.request.urlopen(req, timeout=30).read()


def main():
    api_key = os.environ["SERPER_API_KEY"]
    sb_url = os.environ["SUPABASE_URL"].rstrip("/")
    sb_key = os.environ["SUPABASE_KEY"]

    all_rows = []
    for kw in KEYWORDS:
        try:
            result = serp(kw, api_key)
        except Exception as e:
            print(f"SERP failed for '{kw}': {e}", file=sys.stderr)
            continue
        found = positions(result)
        features = {
            "local_pack": bool(result.get("places")),
            "ads": bool(result.get("ads")),
        }
        for d in DOMAINS:
            pos, link = found.get(d, (None, None))
            all_rows.append({
                "keyword": kw,
                "domain": d,
                "position": pos,
                "url": link,
                "serp_features": features,
            })
        hit = ", ".join(f"{d}:{p}" for d, (p, _) in found.items()) or "none tracked"
        print(f"{kw} -> {hit}")
        time.sleep(1.5)

    if all_rows:
        supabase_insert(all_rows, sb_url, sb_key)
        print(f"Logged {len(all_rows)} rows to Supabase.")
    else:
        print("No rows collected.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
