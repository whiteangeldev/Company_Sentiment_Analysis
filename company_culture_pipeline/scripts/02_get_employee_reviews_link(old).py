#!/usr/bin/env python3
"""
Script to collect employee reviews from Glassdoor and Indeed

IMPORTANT NOTES:
- Employee review sites have strict anti-scraping policies
- This script uses SerpAPI which requires an API key and has costs after free tier
- Alternative: Manual collection using the helper functions provided
- Legal: Check terms of service and local laws before scraping

Methods supported:
1. SerpAPI (recommended) - Requires API key, has free tier
2. Manual search - Generates URLs for manual review collection
"""

import csv
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
REVIEWS_CSV = f"{OUTPUT_DIR}/reviews_summary.csv"
PROGRESS_JSON = f"{OUTPUT_DIR}/reviews_progress.json"

# DuckDuckGo search settings
DDG_SEARCH_URL = "https://html.duckduckgo.com/html/"
MIN_DELAY = 4.0
MAX_DELAY = 8.0
SAVE_EVERY = 5

# SerpAPI settings (optional backup method)
SERPAPI_KEY = os.getenv("SERPAPI_KEY")
SERPAPI_ENDPOINT = "https://serpapi.com/search"

# User agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
]

# Global state for adaptive rate limiting
consecutive_rate_limits = 0
backoff_multiplier = 1.0


def clean_company_name(name: str) -> str:
    """
    Extract core company name from full name with location/division info
    Example: "FNAC - Fujifilm Greenwood SC - Primary" -> "Fujifilm"
    Example: "ID Logistics West Jefferson OH Premier - Primary" -> "ID Logistics"
    """
    # Common division/department indicators to remove
    division_keywords = [
        "PRIMARY",
        "PRODUCTION",
        "DIRECT",
        "INDIRECT",
        "PAYROLL",
        "DAILY",
        "ROOT",
        "CTO",
        "PACKAGING",
        "SANITATION",
        "MAIN WAREHOUSE",
        "FINISHING",
    ]

    # US State codes (helps identify location info)
    us_states = [
        "AL",
        "AK",
        "AZ",
        "AR",
        "CA",
        "CO",
        "CT",
        "DE",
        "FL",
        "GA",
        "HI",
        "ID",
        "IL",
        "IN",
        "IA",
        "KS",
        "KY",
        "LA",
        "ME",
        "MD",
        "MA",
        "MI",
        "MN",
        "MS",
        "MO",
        "MT",
        "NE",
        "NV",
        "NH",
        "NJ",
        "NM",
        "NY",
        "NC",
        "ND",
        "OH",
        "OK",
        "OR",
        "PA",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VT",
        "VA",
        "WA",
        "WV",
        "WI",
        "WY",
    ]

    # If name has " - ", split and analyze parts
    if " - " in name:
        parts = [p.strip() for p in name.split(" - ")]

        # Remove division indicators
        parts = [
            p
            for p in parts
            if not any(keyword in p.upper() for keyword in division_keywords)
        ]

        # If we have multiple parts, prefer the longest one that's not just a location
        if len(parts) > 1:
            # Sort by length, take the longest that doesn't end with a state code
            candidates = []
            for part in parts:
                words = part.split()
                # Skip if last word is a state code (likely just location)
                if words and words[-1].upper() in us_states:
                    continue
                candidates.append(part)

            if candidates:
                # Take the longest candidate
                name = max(candidates, key=len)
            elif parts:
                # Fallback to first part
                name = parts[0]
        elif parts:
            name = parts[0]

    # Remove location info in parentheses
    if "(" in name:
        name = name.split("(")[0].strip()

    # Remove trailing location info (city state pattern)
    words = name.split()
    if len(words) >= 2 and words[-1].upper() in us_states:
        # Remove state code and possibly city name
        name = " ".join(words[:-1])
        # If second-to-last looks like city, remove it too
        words = name.split()
        if len(words) >= 2 and words[-1][0].isupper():
            # Keep at least company name
            if len(words) > 2:
                name = " ".join(words[:-1])

    # Remove common business suffixes
    for suffix in [
        " LLC",
        " Inc",
        " Inc.",
        " Ltd",
        " Limited",
        " Corporation",
        " Corp",
        " Corp.",
        " Co",
        " Co.",
    ]:
        if name.upper().endswith(suffix.upper()):
            name = name[: -len(suffix)].strip()

    # Remove trailing commas and extra spaces
    name = name.strip().strip(",").strip()

    # Final cleanup
    if not name and " - " in name:
        # Fallback: just use first part
        name = name.split(" - ")[0].strip()

    return name if name else "Unknown"


def search_review_site_duckduckgo(
    company_name: str, platform: str, retry_count: int = 0
):
    """
    Generic function to search for company reviews on any platform using DuckDuckGo

    Args:
        company_name: Company name to search for
        platform: One of 'glassdoor', 'indeed', 'comparably', 'kununu', 'ambitionbox'
        retry_count: Current retry attempt

    Returns:
        URL to reviews page or None
    """
    global consecutive_rate_limits, backoff_multiplier

    # Platform-specific search queries and URL patterns
    platform_config = {
        "glassdoor": {
            "query": f"{company_name} glassdoor reviews",
            "domains": ["glassdoor.com", "glassdoor.co.uk", "glassdoor.ca"],
            "paths": ["/Reviews/", "/reviews/", "-Reviews-"],
        },
        "indeed": {
            "query": f"{company_name} indeed company reviews",
            "domains": ["indeed.com", "indeed.co.uk", "indeed.ca"],
            "paths": ["/cmp/", "/companies/", "/reviews"],
        },
        "comparably": {
            "query": f"{company_name} comparably reviews",
            "domains": ["comparably.com"],
            "paths": ["/companies/", "/reviews"],
        },
        "kununu": {
            "query": f"{company_name} kununu reviews",
            "domains": ["kununu.com", "kununu.de", "kununu.at"],
            "paths": ["/bewertung/", "/reviews/", "/en/"],
        },
        "ambitionbox": {
            "query": f"{company_name} ambitionbox reviews",
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
        "User-Agent": random.choice(USER_AGENTS),
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
                    company_name, platform, retry_count + 1
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

            # Check if URL matches any of the platform's domains
            domain_match = any(domain in url_lower for domain in config["domains"])
            if not domain_match:
                continue

            # Check if URL contains review-specific paths
            path_match = any(path.lower() in url_lower for path in config["paths"])
            if not path_match:
                continue

            # Platform-specific validation to avoid wrong matches
            if platform == "ambitionbox":
                # Exclude AmbitionBox's own reviews page
                if (
                    "ambition-box-reviews" in url_lower
                    or "ambitionbox-reviews" in url_lower
                ):
                    continue
                # Must contain the company name (rough check)
                if "reviews/" not in url_lower and "/reviews" not in url_lower:
                    continue

            if platform == "comparably":
                # Must have /companies/ in the path
                if "/companies/" not in url_lower:
                    continue

            return url

        return None

    except Exception as e:
        return None


def generate_manual_search_urls(company_name: str):
    """
    Generate search URLs for manual review collection
    This is the fallback method when APIs aren't available
    """
    clean_name = clean_company_name(company_name)
    encoded_name = quote_plus(clean_name)

    return {
        "company_name": company_name,
        "clean_name": clean_name,
        "glassdoor_search": f"https://www.glassdoor.com/Search/results.htm?keyword={encoded_name}",
        "indeed_search": f"https://www.indeed.com/cmp/{encoded_name.replace(' ', '-')}/reviews",
        "google_glassdoor": f"https://www.google.com/search?q={encoded_name}+glassdoor+reviews",
        "google_indeed": f"https://www.google.com/search?q={encoded_name}+indeed+reviews",
        "source": "manual",
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
            return {item["original_name"]: item for item in data}
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
        company_name: Name of the company
        location: Location (US, UK, etc.)
        method: 'duckduckgo' or 'manual'
        platforms_to_search: List of platforms to search (default: glassdoor, indeed only)

    Returns:
        Dictionary with URLs for all review platforms
    """
    clean_name = clean_company_name(company_name)

    # Default to main platforms (Glassdoor and Indeed work best)
    if platforms_to_search is None:
        platforms_to_search = ["glassdoor", "indeed"]

    # All possible platforms for result structure
    all_platforms = ["glassdoor", "indeed", "comparably", "kununu", "ambitionbox"]

    result = {
        "original_name": company_name,
        "clean_name": clean_name,
        "location": location,
        "glassdoor_url": "",
        "indeed_url": "",
        "comparably_url": "",
        "kununu_url": "",
        "ambitionbox_url": "",
        "method": method,
    }

    if method == "duckduckgo":
        # Search each platform
        found_count = 0
        for platform in platforms_to_search:
            url = search_review_site_duckduckgo(clean_name, platform)
            if url:
                result[f"{platform}_url"] = url
                print(f"   ✓ {platform.capitalize()}: {url}")
                found_count += 1

        if found_count == 0:
            print(f"   ⚠️  No review pages found")
        elif len(platforms_to_search) > 1:
            print(f"   → Found on {found_count}/{len(platforms_to_search)} platforms")

    # Always generate manual search URLs as fallback
    manual_urls = generate_manual_search_urls(company_name)
    result["glassdoor_search_url"] = manual_urls["glassdoor_search"]
    result["indeed_search_url"] = manual_urls["indeed_search"]
    # Add search URLs for other platforms too
    encoded_name = quote_plus(clean_name)
    result["comparably_search_url"] = (
        f"https://www.google.com/search?q={encoded_name}+comparably+reviews"
    )
    result["kununu_search_url"] = (
        f"https://www.google.com/search?q={encoded_name}+kununu+reviews"
    )
    result["ambitionbox_search_url"] = (
        f"https://www.google.com/search?q={encoded_name}+ambitionbox+reviews"
    )

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

    # Check method - DuckDuckGo by default
    method = "duckduckgo"
    print("✓ Using DuckDuckGo (free, no API key needed)")
    print()

    # Choose platforms to search
    print("Which review platforms do you want to search?")
    print("  1. Glassdoor + Indeed only (RECOMMENDED - most reliable)")
    print(
        "  2. All 5 platforms (slower, less reliable for Comparably/Kununu/AmbitionBox)"
    )
    print()
    choice = input("Enter choice (1/2) [default: 1]: ").strip() or "1"

    if choice == "2":
        platforms_to_search = [
            "glassdoor",
            "indeed",
            "comparably",
            "kununu",
            "ambitionbox",
        ]
        print("→ Searching all 5 platforms")
    else:
        platforms_to_search = ["glassdoor", "indeed"]
        print("→ Searching Glassdoor + Indeed only")

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

    # Estimate time based on selected platforms
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

        if result["glassdoor_url"] or result["indeed_url"]:
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

    # Save results as CSV
    with open(REVIEWS_CSV, "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "original_name",
            "clean_name",
            "location",
            "glassdoor_url",
            "indeed_url",
            "comparably_url",
            "kununu_url",
            "ambitionbox_url",
            "glassdoor_search_url",
            "indeed_search_url",
            "comparably_search_url",
            "kununu_search_url",
            "ambitionbox_search_url",
            "method",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_results)

    # Calculate statistics for searched platforms only
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
        1
        for r in all_results
        if any(
            [
                r.get("glassdoor_url"),
                r.get("indeed_url"),
                r.get("comparably_url"),
                r.get("kununu_url"),
                r.get("ambitionbox_url"),
            ]
        )
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
    print(f"  - CSV:  {REVIEWS_CSV}")
    print(f"  - Progress: {PROGRESS_JSON}")
    print("=" * 70)
    print()

    print("💡 NEXT STEPS:")
    print("1. Check the CSV for companies with review URLs")
    print("2. For companies without URLs, use the search URLs to find them manually")
    print("3. Run script 03 to extract actual review content from these URLs")
    print()


if __name__ == "__main__":
    main()
