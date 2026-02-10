#!/usr/bin/env python3
"""
Scheduled Threads scraper for PM2.
Scrapes all monitored accounts and caches results as JSON files.

Run via PM2 with cron_restart for periodic execution:
    pm2 start ecosystem.config.cjs

Or manually:
    uv run python3 scrape_scheduled.py
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from scraper.threads_scraper import ThreadsScraper
from scraper.parser import ThreadsParser

# Monitored accounts (mirrors threads-monitor.ts)
MONITORED_ACCOUNTS = {
    "taiwan": [
        "pttgossiping",
        "ctinews",
        "newtalk_news",
        "twreporter",
        "pts.news",
    ],
    "news": ["nytimes", "washingtonpost", "reuters", "apnews"],
}

CACHE_DIR = os.path.join(os.path.dirname(__file__), "data", "cache")
LIMIT_PER_ACCOUNT = 10


def scrape_all():
    os.makedirs(CACHE_DIR, exist_ok=True)

    settings = {
        "timeout": 15,
        "use_offline": False,
        "use_proxies": False,
    }
    scraper = ThreadsScraper(settings=settings)
    parser = ThreadsParser()

    all_posts = []
    seen_ids = set()

    all_accounts = []
    for category, accounts in MONITORED_ACCOUNTS.items():
        for acct in accounts:
            all_accounts.append((category, acct))

    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting scheduled scrape of {len(all_accounts)} accounts", flush=True)

    for category, username in all_accounts:
        try:
            raw_items = scraper.fetch_user_threads(username=username, limit=LIMIT_PER_ACCOUNT)
            parsed = [parser.parse_item(item, default_username=username) for item in raw_items]
            valid = [p for p in parsed if p and p.get("id")]

            for post in valid:
                if post["id"] not in seen_ids:
                    seen_ids.add(post["id"])
                    post["_category"] = category
                    all_posts.append(post)

            print(f"  @{username}: {len(valid)} posts", flush=True)
            time.sleep(2)  # Rate limit between accounts

        except Exception as e:
            print(f"  @{username}: ERROR - {e}", file=sys.stderr, flush=True)

    # Write cache file
    cache_data = {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "total_posts": len(all_posts),
        "posts": all_posts,
    }

    cache_path = os.path.join(CACHE_DIR, "latest.json")
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache_data, f, ensure_ascii=False, indent=2)

    # Also write a timestamped backup (keep last 10)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(CACHE_DIR, f"scrape_{ts}.json")
    with open(backup_path, "w", encoding="utf-8") as f:
        json.dump(cache_data, f, ensure_ascii=False, indent=2)

    # Cleanup old backups (keep last 10)
    backups = sorted(
        [f for f in os.listdir(CACHE_DIR) if f.startswith("scrape_") and f.endswith(".json")],
        reverse=True,
    )
    for old in backups[10:]:
        os.remove(os.path.join(CACHE_DIR, old))

    print(f"[{datetime.now(timezone.utc).isoformat()}] Done. {len(all_posts)} posts cached to {cache_path}", flush=True)


if __name__ == "__main__":
    scrape_all()
