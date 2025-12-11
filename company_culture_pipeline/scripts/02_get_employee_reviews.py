#!/usr/bin/env python3
"""
Script to collect employee reviews from multiple platforms

IMPORTANT NOTES:
- Employee review sites have strict anti-scraping policies
- This script uses DuckDuckGo search (free, no API key required)
- Alternative: Manual collection using the helper functions provided
- Legal: Check terms of service and local laws before scraping

Supported platforms:
- Glassdoor, Indeed, Comparably, Kununu, AmbitionBox
"""

import csv  # Only for reading input companies.csv
import os
import json
import time
import random
from urllib.parse import quote_plus
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# Configuration
INPUT_CSV = "data/tmp/companies_with_sites.csv"
OUTPUT_DIR = "data/raw_reviews"
REVIEWS_JSON = f"{OUTPUT_DIR}/all_reviews.json"
PROGRESS_JSON = f"{OUTPUT_DIR}/reviews_progress.json"

# DuckDuckGo search settings
DDG_SEARCH_URL = "https://html.duckduckgo.com/html/"
MIN_DELAY = 4.0
MAX_DELAY = 8.0
SAVE_EVERY = 5

# User agent
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Global state for adaptive rate limiting
consecutive_rate_limits = 0
backoff_multiplier = 1.0


def search_review_site_duckduckgo(
    company_name: str, location: str, platform: str, retry_count: int = 0
):
    """
    Search for company reviews on any platform using DuckDuckGo

    Args:
        company_name: Company name to search for (from CSV column 1)
        location: Location/nationality (from CSV column 2)
        platform: One of 'glassdoor', 'indeed', 'comparably', 'kununu', 'ambitionbox'
        retry_count: Current retry attempt

    Returns:
        URL to reviews page or None
    """
    global consecutive_rate_limits, backoff_multiplier

    # Create search index: company name + location
    search_index = f"{company_name} {location}"

    # Platform-specific search queries and URL patterns
    platform_config = {
        "glassdoor": {
            "query": f"{search_index} glassdoor employee reviews",
            "domains": ["glassdoor.com", "glassdoor.co.uk", "glassdoor.ca"],
            "paths": ["/Reviews/", "/reviews/", "-Reviews-"],
        },
        "indeed": {
            "query": f"{search_index} indeed employee reviews",
            "domains": ["indeed.com", "indeed.co.uk", "indeed.ca"],
            "paths": ["/cmp/", "/companies/", "/reviews"],
        },
        "comparably": {
            "query": f"{search_index} comparably employee reviews",
            "domains": ["comparably.com"],
            "paths": ["/companies/", "/reviews"],
        },
        "kununu": {
            "query": f"{search_index} kununu employee reviews",
            "domains": ["kununu.com", "kununu.de", "kununu.at"],
            "paths": ["/bewertung/", "/reviews/", "/en/"],
        },
        "ambitionbox": {
            "query": f"{search_index} ambitionbox employee reviews",
            "domains": ["ambitionbox.com"],
            "paths": ["/reviews/", "/company/"],
        },
    }

    if platform not in platform_config:
        return None

    config = platform_config[platform]
    search_query = config["query"]

    # Adaptive delay
    base_delay = random.uniform(MIN_DELAY, MAX_DELAY)
    delay = base_delay * backoff_multiplier
    time.sleep(delay)

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    data = {"q": search_query, "b": "", "kl": "us-en"}

    try:
        response = requests.post(DDG_SEARCH_URL, headers=headers, data=data, timeout=15)

        if response.status_code == 202:
            consecutive_rate_limits += 1
            backoff_multiplier = min(3.0, 1.0 + (consecutive_rate_limits * 0.3))

            if retry_count < 2:
                wait_time = 15 + (retry_count * 10)
                print(
                    f"   ⚠️  Rate limited, waiting {wait_time}s... (backoff: {backoff_multiplier:.1f}x)"
                )
                time.sleep(wait_time)
                return search_review_site_duckduckgo(
                    company_name, location, platform, retry_count + 1
                )
            else:
                return None

        if response.status_code != 200:
            return None

        # Success - reduce backoff
        if consecutive_rate_limits > 0:
            consecutive_rate_limits = max(0, consecutive_rate_limits - 1)
            backoff_multiplier = max(1.0, backoff_multiplier - 0.1)

        soup = BeautifulSoup(response.text, "html.parser")
        results = soup.find_all("a", class_="result__a")

        if not results:
            return None

        # Find review page matching platform criteria
        for result in results[:10]:
            url = result.get("href", "")
            url_lower = url.lower()

            # Check if URL matches platform's domains and paths
            domain_match = any(domain in url_lower for domain in config["domains"])
            path_match = any(path.lower() in url_lower for path in config["paths"])

            if domain_match and path_match:
                # Additional validation for specific platforms
                if platform == "ambitionbox":
                    if (
                        "ambition-box-reviews" in url_lower
                        or "ambitionbox-reviews" in url_lower
                    ):
                        continue
                if platform == "comparably":
                    if "/companies/" not in url_lower:
                        continue
                return url

        return None

    except Exception as e:
        return None


def generate_manual_search_urls(company_name: str, location: str):
    """Generate search URLs for manual review collection"""
    search_index = f"{company_name} {location}"
    encoded_name = quote_plus(search_index)

    return {
        "glassdoor_search": f"https://www.glassdoor.com/Search/results.htm?keyword={encoded_name}",
        "indeed_search": f"https://www.indeed.com/cmp/{quote_plus(company_name).replace(' ', '-')}/reviews",
        "comparably_search": f"https://www.google.com/search?q={encoded_name}+comparably+employee+reviews",
        "kununu_search": f"https://www.google.com/search?q={encoded_name}+kununu+employee+reviews",
        "ambitionbox_search": f"https://www.google.com/search?q={encoded_name}+ambitionbox+employee+reviews",
    }


def save_progress(results, filepath):
    """Save current progress to JSON"""
    if results:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)


def load_progress():
    """Load previous progress if exists"""
    if os.path.exists(PROGRESS_JSON):
        with open(PROGRESS_JSON, encoding="utf-8") as f:
            data = json.load(f)
            # Use company_name as key (backward compatible with old format)
            return {
                item.get("company_name", item.get("original_name", "")): item
                for item in data
            }
    return {}


def collect_reviews_for_company(
    company_name: str,
    location: str,
    method: str = "duckduckgo",
    platforms_to_search=None,
):
    """
    Collect review links for a company from multiple platforms

    Args:
        company_name: Name of the company (from CSV column 1)
        location: Location/nationality (from CSV column 2)
        method: 'duckduckgo' or 'manual'
        platforms_to_search: List of platforms to search

    Returns:
        Dictionary with URLs for all review platforms
    """
    # Default to all 5 platforms
    if platforms_to_search is None:
        platforms_to_search = [
            "glassdoor",
            "indeed",
            "comparably",
            "kununu",
            "ambitionbox",
        ]

    result = {
        "company_name": company_name,
        "location": location,
        "search_index": f"{company_name} {location}",
        "glassdoor_url": "",
        "indeed_url": "",
        "comparably_url": "",
        "kununu_url": "",
        "ambitionbox_url": "",
        "method": method,
    }

    if method == "duckduckgo":
        # Search each platform using company name + location
        found_count = 0
        for platform in platforms_to_search:
            url = search_review_site_duckduckgo(company_name, location, platform)
            if url:
                result[f"{platform}_url"] = url
                print(f"   ✓ {platform.capitalize()}: {url}")
                found_count += 1

        if found_count == 0:
            print(f"   ⚠️  No review pages found")
        else:
            print(f"   → Found on {found_count}/{len(platforms_to_search)} platforms")

    # Always generate manual search URLs as fallback
    manual_urls = generate_manual_search_urls(company_name, location)
    for platform in ["glassdoor", "indeed", "comparably", "kununu", "ambitionbox"]:
        result[f"{platform}_search_url"] = manual_urls[f"{platform}_search"]

    return result


def main():
    print("=" * 70)
    print("EMPLOYEE REVIEW URL COLLECTOR")
    print("Using DuckDuckGo Search (No API key required)")
    print("=" * 70)
    print()

    # Load previous progress
    previous_progress = load_progress()
    if previous_progress:
        print(f"📁 Found previous progress with {len(previous_progress)} companies")
        print(f"   Resuming from where we left off...\n")

    # Method
    method = "duckduckgo"
    print("✓ Using DuckDuckGo (free, no API key needed)")

    # Choose platforms to search
    print()
    print("Which review platforms do you want to search?")
    print("  1. Glassdoor + Indeed only (RECOMMENDED - most reliable)")
    print(
        "  2. All 5 platforms (slower, less reliable for Comparably/Kununu/AmbitionBox)"
    )
    print()
    choice = input("Enter choice (1/2) [default: 2]: ").strip() or "2"

    if choice == "1":
        platforms_to_search = ["glassdoor", "indeed"]
        print("→ Searching Glassdoor + Indeed only")
    else:
        platforms_to_search = [
            "glassdoor",
            "indeed",
            "comparably",
            "kununu",
            "ambitionbox",
        ]
        print("→ Searching all 5 platforms")

    print()
    print("=" * 70)
    print()

    # Load companies
    if not os.path.exists(INPUT_CSV):
        print(f"❌ ERROR: {INPUT_CSV} not found")
        print(f"   Please run 01_resolve_websites.py first")
        return

    companies = []
    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        companies = list(reader)

    total = len(companies)
    remaining = sum(1 for c in companies if c["name"] not in previous_progress)

    # Estimate time
    avg_delay = (MIN_DELAY + MAX_DELAY) / 2
    searches_per_company = len(platforms_to_search)
    estimated_minutes = (remaining * avg_delay * searches_per_company) / 60

    print(f"Processing {total} companies...")
    if previous_progress:
        print(f"Already completed: {total - remaining}")
        print(f"Remaining: {remaining}")
    print(f"Searching {searches_per_company} platform(s) per company:")
    platform_names = [p.capitalize() for p in platforms_to_search]
    print(f"  • {' • '.join(platform_names)}")
    print(
        f"Estimated time: {estimated_minutes:.1f} minutes ({avg_delay:.1f}s avg delay)"
    )
    print(f"Progress will be saved every {SAVE_EVERY} companies\n")

    all_results = []
    found_count = 0
    skipped = 0

    for idx, company in enumerate(companies, 1):
        company_name = company["name"]
        location = company.get("location", "US")

        # Check if already processed
        if company_name in previous_progress:
            result = previous_progress[company_name]
            all_results.append(result)
            if result.get("glassdoor_url") or result.get("indeed_url"):
                found_count += 1
            skipped += 1
            print(f"[{idx}/{total}] {company_name}")
            print(f"   ✓ Already processed (skipped)")
            continue

        print(f"[{idx}/{total}] {company_name}")

        result = collect_reviews_for_company(
            company_name, location, method, platforms_to_search
        )
        all_results.append(result)

        if any(result.get(f"{p}_url") for p in platforms_to_search):
            found_count += 1

        # Save progress periodically
        if idx % SAVE_EVERY == 0:
            save_progress(all_results, PROGRESS_JSON)
            print(f"   💾 Progress saved ({found_count}/{idx} found)")

    # Final save
    save_progress(all_results, PROGRESS_JSON)

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Save results as JSON
    with open(REVIEWS_JSON, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)

    # Calculate statistics for searched platforms
    platform_stats = {}
    platform_display_names = {
        "glassdoor": "Glassdoor",
        "indeed": "Indeed",
        "comparably": "Comparably",
        "kununu": "Kununu",
        "ambitionbox": "AmbitionBox",
    }

    for platform in platforms_to_search:
        count = sum(1 for r in all_results if r.get(f"{platform}_url"))
        platform_stats[platform_display_names[platform]] = count

    # Count companies with at least one URL
    companies_with_reviews = sum(
        1 for r in all_results if any(r.get(f"{p}_url") for p in platforms_to_search)
    )

    print()
    print("=" * 70)
    print("RESULTS:")
    print(f"  Total companies: {total}")
    print(f"  Companies with reviews found: {companies_with_reviews}")
    if skipped > 0:
        print(f"  (Resumed from progress: {skipped})")
    print()
    print("  Platform Breakdown:")
    for platform, count in platform_stats.items():
        percentage = (count / total * 100) if total > 0 else 0
        print(f"    • {platform:12s}: {count:3d} ({percentage:5.1f}%)")
    print()
    print(f"  Success rate: {(companies_with_reviews/total*100):.1f}%")
    print()
    print(f"✓ Results saved:")
    print(f"  - JSON: {REVIEWS_JSON}")
    print(f"  - Progress: {PROGRESS_JSON}")
    print("=" * 70)
    print()

    print("💡 NEXT STEPS:")
    print("1. Check the JSON for companies with review URLs")
    print("2. For companies without URLs, use the search URLs to find them manually")
    print("3. Run script 03 to extract actual review content from these URLs")
    print()


if __name__ == "__main__":
    main()
