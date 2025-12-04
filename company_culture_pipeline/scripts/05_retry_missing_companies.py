#!/usr/bin/env python3
"""
Script to retry searching for companies with missing review URLs

This script identifies companies with all blank URLs and attempts to search
for them again using the same DuckDuckGo search method.
"""

import json
import os
import sys

# Import the search functions from script 02
sys.path.insert(0, os.path.dirname(__file__))

# We'll import the necessary functions
import time
import random
from urllib.parse import quote_plus
import requests
from bs4 import BeautifulSoup

# Configuration
DATA_DIR = "data/raw_reviews"
ALL_REVIEWS_JSON = f"{DATA_DIR}/all_reviews.json"
BACKUP_JSON = f"{DATA_DIR}/all_reviews_retry_backup.json"

# DuckDuckGo search settings
DDG_SEARCH_URL = "https://html.duckduckgo.com/html/"
MIN_DELAY = 4.0
MAX_DELAY = 8.0

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Review platform fields
REVIEW_FIELDS = [
    "glassdoor_url",
    "indeed_url",
    "comparably_url",
    "kununu_url",
    "ambitionbox_url"
]

# Platform configurations
PLATFORM_CONFIG = {
    "glassdoor": {
        "query_template": "{} glassdoor reviews",
        "domains": ["glassdoor.com", "glassdoor.co.uk", "glassdoor.ca"],
        "paths": ["/Reviews/", "/reviews/", "-Reviews-"],
    },
    "indeed": {
        "query_template": "{} indeed company reviews",
        "domains": ["indeed.com", "indeed.co.uk", "indeed.ca"],
        "paths": ["/cmp/", "/companies/", "/reviews"],
    },
    "comparably": {
        "query_template": "{} comparably reviews",
        "domains": ["comparably.com"],
        "paths": ["/companies/", "/reviews"],
    },
    "kununu": {
        "query_template": "{} kununu reviews",
        "domains": ["kununu.com", "kununu.de", "kununu.at"],
        "paths": ["/bewertung/", "/reviews/", "/en/"],
    },
    "ambitionbox": {
        "query_template": "{} ambitionbox reviews",
        "domains": ["ambitionbox.com"],
        "paths": ["/reviews/", "/company/"],
    },
}


def load_json(filepath):
    """Load JSON file"""
    with open(filepath, encoding="utf-8") as f:
        return json.load(f)


def save_json(filepath, data):
    """Save data to JSON file"""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def has_all_blank_urls(company):
    """Check if all 5 review URLs are blank"""
    return all(not company.get(field, "") for field in REVIEW_FIELDS)


def search_duckduckgo(query, platform):
    """Search DuckDuckGo for a platform's review page"""
    
    if platform not in PLATFORM_CONFIG:
        return None
    
    config = PLATFORM_CONFIG[platform]
    
    # Add delay
    delay = random.uniform(MIN_DELAY, MAX_DELAY)
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
    
    data = {"q": query, "b": "", "kl": "us-en"}
    
    try:
        response = requests.post(DDG_SEARCH_URL, headers=headers, data=data, timeout=15)
        
        if response.status_code != 200:
            return None
        
        soup = BeautifulSoup(response.text, "html.parser")
        results = soup.find_all("a", class_="result__a")
        
        if not results:
            return None
        
        # Find matching URL
        for result in results[:10]:
            url = result.get("href", "")
            url_lower = url.lower()
            
            domain_match = any(domain in url_lower for domain in config["domains"])
            path_match = any(path.lower() in url_lower for path in config["paths"])
            
            if domain_match and path_match:
                # Additional validation for specific platforms
                if platform == "ambitionbox":
                    if "ambition-box-reviews" in url_lower or "ambitionbox-reviews" in url_lower:
                        continue
                if platform == "comparably":
                    if "/companies/" not in url_lower:
                        continue
                return url
        
        return None
        
    except Exception as e:
        print(f"      Error searching: {str(e)}")
        return None


def retry_company_search(company):
    """Retry searching for all platforms for a company"""
    clean_name = company.get("clean_name", company["original_name"])
    
    print(f"\n🔍 Searching: {company['original_name']}")
    print(f"   Clean name: {clean_name}")
    
    found_any = False
    
    for platform in ["glassdoor", "indeed", "comparably", "kununu", "ambitionbox"]:
        query = PLATFORM_CONFIG[platform]["query_template"].format(clean_name)
        print(f"   • {platform.capitalize()}...", end=" ", flush=True)
        
        url = search_duckduckgo(query, platform)
        
        if url:
            company[f"{platform}_url"] = url
            print(f"✓ Found")
            found_any = True
        else:
            print("✗ Not found")
    
    return company, found_any


def main():
    print("=" * 70)
    print("RETRY SEARCH FOR COMPANIES WITH MISSING URLs")
    print("=" * 70)
    print()
    
    # Load data
    if not os.path.exists(ALL_REVIEWS_JSON):
        print(f"❌ ERROR: {ALL_REVIEWS_JSON} not found")
        return
    
    all_reviews = load_json(ALL_REVIEWS_JSON)
    print(f"✓ Loaded {len(all_reviews)} companies")
    
    # Find companies with blank URLs
    blank_companies = [(i, c) for i, c in enumerate(all_reviews) if has_all_blank_urls(c)]
    
    if not blank_companies:
        print("✓ No companies with blank URLs found!")
        return
    
    print(f"\n📋 Found {len(blank_companies)} companies with all blank URLs:")
    for idx, company in blank_companies:
        print(f"   {idx+1}. {company['original_name']}")
    
    print(f"\n⏱️  Estimated time: {len(blank_companies) * 5 * 6 / 60:.1f} minutes")
    print(f"   (5 platforms × ~6 seconds per search)")
    
    # Ask for confirmation
    print()
    response = input("Continue with retry search? (y/n): ").strip().lower()
    if response != 'y':
        print("❌ Cancelled")
        return
    
    # Create backup
    print(f"\n💾 Creating backup...")
    save_json(BACKUP_JSON, all_reviews)
    print(f"   ✓ Backup saved: {BACKUP_JSON}")
    
    # Retry searches
    print("\n" + "=" * 70)
    print("SEARCHING...")
    print("=" * 70)
    
    updated_count = 0
    
    for idx, company in blank_companies:
        updated_company, found_any = retry_company_search(company)
        all_reviews[idx] = updated_company
        
        if found_any:
            updated_count += 1
    
    # Save results
    print("\n" + "=" * 70)
    print("RESULTS:")
    print(f"  Total retried: {len(blank_companies)}")
    print(f"  Found URLs for: {updated_count} companies")
    print(f"  Still blank: {len(blank_companies) - updated_count} companies")
    print("=" * 70)
    
    if updated_count > 0:
        print("\n💾 Saving updated results...")
        save_json(ALL_REVIEWS_JSON, all_reviews)
        print(f"   ✓ Saved: {ALL_REVIEWS_JSON}")
        print(f"   ✓ Backup: {BACKUP_JSON}")
    else:
        print("\nℹ️  No updates to save")
    
    print("\n✅ Complete!")


if __name__ == "__main__":
    main()

