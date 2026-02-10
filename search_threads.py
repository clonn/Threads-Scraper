#!/usr/bin/env python3
"""
Standalone Threads search script.
Called by the Node.js backend to search Threads for keywords.

Usage:
    python3 search_threads.py --keywords "keyword1" "keyword2" --limit 10
    python3 search_threads.py --username "zuck" --limit 20

Output: JSON array of posts to stdout
"""
import argparse
import json
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from scraper.threads_scraper import ThreadsScraper
from scraper.parser import ThreadsParser


def main():
    parser = argparse.ArgumentParser(description="Search Threads posts")
    parser.add_argument(
        "--keywords",
        nargs="+",
        help="Keywords to search for",
        default=[],
    )
    parser.add_argument(
        "--username",
        type=str,
        help="Username to fetch posts from",
        default="",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Max number of posts to return",
    )
    args = parser.parse_args()

    if not args.keywords and not args.username:
        print(json.dumps([]), flush=True)
        sys.exit(0)

    settings = {
        "timeout": 15,
        "use_offline": False,
        "use_proxies": False,
    }

    scraper = ThreadsScraper(settings=settings)
    threads_parser = ThreadsParser()
    all_results = []

    try:
        if args.username:
            raw_items = scraper.fetch_user_threads(
                username=args.username, limit=args.limit
            )
            parsed = [
                threads_parser.parse_item(item, default_username=args.username)
                for item in raw_items
            ]
            all_results.extend([p for p in parsed if p])

        for keyword in args.keywords:
            raw_items = scraper.search_threads(keyword=keyword, limit=args.limit)
            parsed = [
                threads_parser.parse_item(item, default_username="")
                for item in raw_items
            ]
            all_results.extend([p for p in parsed if p])

        # Deduplicate by id
        seen = set()
        unique = []
        for item in all_results:
            item_id = item.get("id", "")
            if item_id and item_id not in seen:
                seen.add(item_id)
                unique.append(item)
            elif not item_id:
                unique.append(item)

        print(json.dumps(unique[: args.limit], ensure_ascii=False, indent=2), flush=True)

    except Exception as e:
        print(json.dumps({"error": str(e)}), file=sys.stderr, flush=True)
        print(json.dumps([]), flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
