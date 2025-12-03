import csv
import os
import time
import random
from urllib.parse import urlparse, quote_plus
import requests
from bs4 import BeautifulSoup

INPUT_CSV = "companies.csv"
OUTPUT_CSV = "data/tmp/companies_with_sites.csv"
PROGRESS_CSV = "data/tmp/companies_progress.csv"  # Save progress as we go

# DuckDuckGo search settings
DDG_SEARCH_URL = "https://html.duckduckgo.com/html/"
MIN_DELAY = 4.0  # Minimum delay between requests (increased)
MAX_DELAY = 8.0  # Maximum delay between requests (increased)
SAVE_EVERY = 5  # Save progress every N companies

# Rotate through different user agents to appear more human-like
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# Global state for adaptive rate limiting
consecutive_rate_limits = 0
backoff_multiplier = 1.0


def is_valid_homepage(url):
    if not url:
        return False
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    if any(
        bad in domain
        for bad in [
            "glassdoor",
            "indeed",
            "linkedin",
            "ziprecruiter",
            "amazonaws",
            "facebook",
            "yelp",
            "map",
            "google",
        ]
    ):
        return False
    return True


def search_official_site(query: str, retry_count=0):
    """Search for company website using DuckDuckGo HTML search with adaptive rate limiting"""
    global consecutive_rate_limits, backoff_multiplier

    search_query = f"{query} official site"

    # Adaptive delay based on recent rate limiting
    base_delay = random.uniform(MIN_DELAY, MAX_DELAY)
    delay = base_delay * backoff_multiplier
    time.sleep(delay)

    # Randomly select a user agent
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    # DuckDuckGo HTML search uses POST with form data
    data = {"q": search_query, "b": "", "kl": "us-en"}

    try:
        response = requests.post(DDG_SEARCH_URL, headers=headers, data=data, timeout=15)

        if response.status_code == 202:
            # Rate limited - increase backoff and retry
            consecutive_rate_limits += 1
            backoff_multiplier = min(3.0, 1.0 + (consecutive_rate_limits * 0.3))

            if retry_count < 2:  # Allow up to 2 retries
                wait_time = 15 + (retry_count * 10)  # Progressive wait: 15s, 25s
                print(
                    f"   ⚠️  Rate limited (#{consecutive_rate_limits}), waiting {wait_time}s... (backoff: {backoff_multiplier:.1f}x)"
                )
                time.sleep(wait_time)
                return search_official_site(query, retry_count=retry_count + 1)
            else:
                print(f"   ⚠️  Rate limited (max retries reached)")
                return ""

        if response.status_code != 200:
            print(f"   ⚠️  Search returned status {response.status_code}")
            return ""

        # Success! Reset rate limit counter and gradually reduce backoff
        if consecutive_rate_limits > 0:
            consecutive_rate_limits = max(0, consecutive_rate_limits - 1)
            backoff_multiplier = max(1.0, backoff_multiplier - 0.1)

        # Parse the HTML
        soup = BeautifulSoup(response.text, "html.parser")

        # Find search result links
        results = soup.find_all("a", class_="result__a")

        if not results:
            print(f"   ⚠️  No results found for '{query}'")
            return ""

        # Check each result URL
        for result in results[:10]:  # Check first 10 results
            url = result.get("href", "")
            if url and is_valid_homepage(url):
                print(f"   ✓ Found: {url}")
                return url

        print(f"   ⚠️  No valid homepage found (filtered out non-official sites)")
        return ""

    except requests.exceptions.Timeout:
        print(f"   ⚠️  Search timed out for '{query}'")
        return ""
    except requests.exceptions.RequestException as e:
        print(f"   ⚠️  Request error: {str(e)[:50]}")
        return ""
    except Exception as e:
        print(f"   ⚠️  Error: {str(e)[:50]}")
        return ""


def save_progress(rows, filepath):
    """Save current progress to CSV"""
    if rows:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)


def load_progress():
    """Load previous progress if exists"""
    if os.path.exists(PROGRESS_CSV):
        with open(PROGRESS_CSV, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return {row["name"]: row.get("website_url", "") for row in reader}
    return {}


def main():
    print("=" * 70)
    print("COMPANY WEBSITE RESOLVER")
    print("Using DuckDuckGo Search (No API key required)")
    print("=" * 70)
    print()

    # Load previous progress
    previous_progress = load_progress()
    if previous_progress:
        print(f"📁 Found previous progress with {len(previous_progress)} companies")
        print(f"   Resuming from where we left off...\n")

    rows = []
    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        companies = list(reader)

    total = len(companies)
    found = 0
    skipped = 0

    # Estimate time for remaining companies
    remaining = sum(
        1
        for c in companies
        if c["name"] not in previous_progress or not previous_progress[c["name"]]
    )
    avg_delay = (MIN_DELAY + MAX_DELAY) / 2
    estimated_minutes = (remaining * avg_delay) / 60

    print(f"Processing {total} companies...")
    if previous_progress:
        print(f"Already completed: {total - remaining}")
        print(f"Remaining: {remaining}")
    print(
        f"Estimated time: {estimated_minutes:.1f} minutes (with {avg_delay:.1f}s average delay)"
    )
    print(f"Progress will be saved every {SAVE_EVERY} companies\n")

    for idx, row in enumerate(companies, 1):
        company_name = row["name"]

        # Check if we already have this company
        if company_name in previous_progress and previous_progress[company_name]:
            website = previous_progress[company_name]
            row["website_url"] = website
            rows.append(row)
            found += 1
            skipped += 1
            print(f"[{idx}/{total}] {company_name}")
            print(f"   ✓ Already found: {website} (skipped)")
            continue

        print(f"[{idx}/{total}] {company_name}")
        website = search_official_site(company_name)
        row["website_url"] = website
        rows.append(row)

        if website:
            found += 1

        # Save progress periodically
        if idx % SAVE_EVERY == 0:
            save_progress(rows, PROGRESS_CSV)
            print(f"   💾 Progress saved ({found}/{idx} found so far)")

    print()
    print("=" * 70)

    os.makedirs("data/tmp", exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    # Final save
    save_progress(rows, PROGRESS_CSV)

    print(f"RESULTS:")
    print(f"  Total companies: {total}")
    print(f"  Websites found: {found}")
    if skipped > 0:
        print(f"  (Resumed from progress: {skipped})")
        print(f"  Newly searched: {total - skipped}")
    print(f"  Not found: {total - found}")
    print(f"  Success rate: {(found/total*100):.1f}%")
    print()
    print(f"✓ Output saved → {OUTPUT_CSV}")
    print(f"✓ Progress saved → {PROGRESS_CSV}")
    print("=" * 70)


if __name__ == "__main__":
    main()
