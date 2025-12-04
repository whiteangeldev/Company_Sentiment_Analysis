#!/usr/bin/env python3
"""
Script to retry failed companies with alternative/shorter company names

For companies where the full name is too specific, this script tries
shorter, more common variations that are more likely to appear on review sites.
"""

import json
import os
import time
import random
from urllib.parse import quote_plus
import requests
from bs4 import BeautifulSoup

# Configuration
DATA_DIR = "data/raw_reviews"
ALL_REVIEWS_JSON = f"{DATA_DIR}/all_reviews.json"
PROGRESS_JSON = f"{DATA_DIR}/reviews_progress.json"
BACKUP_JSON = f"{DATA_DIR}/all_reviews_alt_backup.json"

# DuckDuckGo settings
DDG_SEARCH_URL = "https://html.duckduckgo.com/html/"
MIN_DELAY = 4.0
MAX_DELAY = 8.0

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Alternative names to try for specific companies
ALTERNATIVE_NAMES = {
    "Shell International Petroleum Company - SIPC (DX) ": [
        "Shell",  # Main brand
        "Shell International",  # Already found for other Shell companies
    ],
    "Walgreens Boots Alliance Services Limited": [
        "Walgreens Boots Alliance",  # Parent company
        "Boots",  # UK brand
        "Walgreens",  # US brand
    ],
    "Gamma Telecom Ltd": [
        "Gamma",  # Shorter name
        "Gamma Communications",  # Full company name variant
    ],
    "Caraffi Limited - N/A": [
        "Caraffi Group",
    ],
    "Simplifai Systems Limited": [
        "Simplifai",
    ],
    "Pracyva": [
        # Small company, probably no alternatives will work
    ],
}

REVIEW_FIELDS = [
    "glassdoor_url",
    "indeed_url",
    "comparably_url",
    "kununu_url",
    "ambitionbox_url"
]

PLATFORM_CONFIG = {
    "glassdoor": {
        "query": "{} glassdoor reviews",
        "domains": ["glassdoor.com", "glassdoor.co.uk", "glassdoor.ca"],
        "paths": ["/Reviews/", "/reviews/", "-Reviews-"],
    },
    "indeed": {
        "query": "{} indeed company reviews",
        "domains": ["indeed.com", "indeed.co.uk", "indeed.ca"],
        "paths": ["/cmp/", "/companies/", "/reviews"],
    },
    "comparably": {
        "query": "{} comparably reviews",
        "domains": ["comparably.com"],
        "paths": ["/companies/", "/reviews"],
    },
    "kununu": {
        "query": "{} kununu reviews",
        "domains": ["kununu.com", "kununu.de", "kununu.at"],
        "paths": ["/bewertung/", "/reviews/", "/en/"],
    },
    "ambitionbox": {
        "query": "{} ambitionbox reviews",
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
        return ""
    
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
            return ""
        
        soup = BeautifulSoup(response.text, "html.parser")
        results = soup.find_all("a", class_="result__a")
        
        if not results:
            return ""
        
        # Find matching URL
        for result in results[:10]:
            url = result.get("href", "")
            url_lower = url.lower()
            
            domain_match = any(domain in url_lower for domain in config["domains"])
            path_match = any(path.lower() in url_lower for path in config["paths"])
            
            if domain_match and path_match:
                if platform == "ambitionbox":
                    if "ambition-box-reviews" in url_lower or "ambitionbox-reviews" in url_lower:
                        continue
                if platform == "comparably":
                    if "/companies/" not in url_lower:
                        continue
                return url
        
        return ""
        
    except Exception as e:
        return ""


def try_alternative_names(company, platforms):
    """Try alternative names for a company"""
    original_name = company["original_name"]
    
    if original_name not in ALTERNATIVE_NAMES:
        print(f"   ⚠️  No alternative names configured for this company")
        return company, 0
    
    alternative_names = ALTERNATIVE_NAMES[original_name]
    
    if not alternative_names:
        print(f"   ⚠️  No alternative names available to try")
        return company, 0
    
    print(f"   Trying {len(alternative_names)} alternative name(s): {', '.join(alternative_names)}")
    
    best_found_count = 0
    best_company = company.copy()
    
    for alt_name in alternative_names:
        print(f"\n   → Trying: '{alt_name}'")
        found_count = 0
        temp_company = company.copy()
        
        for platform in platforms:
            query = PLATFORM_CONFIG[platform]["query"].format(alt_name)
            print(f"      • {platform.capitalize():12s}", end=" ", flush=True)
            
            url = search_duckduckgo(query, platform)
            
            if url:
                # Only update if we don't already have a URL for this platform
                if not temp_company.get(f"{platform}_url", ""):
                    temp_company[f"{platform}_url"] = url
                    found_count += 1
                    print(f"✓ Found")
                else:
                    print(f"(already had)")
            else:
                print(f"✗ Not found")
        
        if found_count > best_found_count:
            best_found_count = found_count
            best_company = temp_company
            best_company["clean_name"] = alt_name  # Update the clean name
            print(f"      → Best so far: {found_count} URL(s) found")
        
        # If we found URLs for all platforms, stop trying
        if best_found_count == len(platforms):
            print(f"      ✓ Found all platforms, stopping search")
            break
    
    return best_company, best_found_count


def main():
    print("=" * 70)
    print("RETRY WITH ALTERNATIVE COMPANY NAMES")
    print("Try shorter names for companies that failed")
    print("=" * 70)
    print()
    
    # Load data
    if not os.path.exists(ALL_REVIEWS_JSON):
        print(f"❌ ERROR: {ALL_REVIEWS_JSON} not found")
        return
    
    all_reviews = load_json(ALL_REVIEWS_JSON)
    print(f"✓ Loaded {len(all_reviews)} companies")
    
    # Find companies with blank URLs that have alternative names
    blank_companies = [(i, c) for i, c in enumerate(all_reviews) 
                      if has_all_blank_urls(c) and c["original_name"] in ALTERNATIVE_NAMES]
    
    if not blank_companies:
        print("✓ No companies to retry with alternative names")
        return
    
    print(f"\n📋 Found {len(blank_companies)} companies to retry:")
    for idx, company in blank_companies:
        alt_names = ALTERNATIVE_NAMES.get(company["original_name"], [])
        print(f"   {idx+1}. {company['original_name']}")
        if alt_names:
            print(f"       → Will try: {', '.join(alt_names)}")
    
    platforms = ["glassdoor", "indeed", "comparably", "kununu", "ambitionbox"]
    avg_delay = (MIN_DELAY + MAX_DELAY) / 2
    
    # Calculate total alternatives to try
    total_searches = sum(len(ALTERNATIVE_NAMES.get(c["original_name"], [])) 
                        for _, c in blank_companies)
    estimated_minutes = (total_searches * len(platforms) * avg_delay) / 60
    
    print(f"\n⏱️  Estimated time: {estimated_minutes:.1f} minutes")
    print()
    
    response = input("Continue with alternative name search? (y/n): ").strip().lower()
    if response != 'y':
        print("❌ Cancelled")
        return
    
    # Create backup
    print(f"\n💾 Creating backup...")
    save_json(BACKUP_JSON, all_reviews)
    print(f"   ✓ Backup saved: {BACKUP_JSON}")
    
    # Retry with alternative names
    print("\n" + "=" * 70)
    print("SEARCHING WITH ALTERNATIVE NAMES...")
    print("=" * 70)
    
    updated_count = 0
    total_urls_found = 0
    
    for idx, company in blank_companies:
        print(f"\n🔍 [{company['original_name']}]")
        updated_company, found_count = try_alternative_names(company, platforms)
        all_reviews[idx] = updated_company
        
        if found_count > 0:
            updated_count += 1
            total_urls_found += found_count
            print(f"\n   ✅ Found {found_count} URL(s) total")
        else:
            print(f"\n   ❌ No URLs found with any alternative name")
    
    # Results
    print("\n" + "=" * 70)
    print("SUMMARY:")
    print(f"  Companies retried: {len(blank_companies)}")
    print(f"  Companies with URLs found: {updated_count}")
    print(f"  Total URLs found: {total_urls_found}")
    print(f"  Companies still blank: {len(blank_companies) - updated_count}")
    print("=" * 70)
    print()
    
    if updated_count > 0:
        print("💾 Saving updated results...")
        save_json(ALL_REVIEWS_JSON, all_reviews)
        save_json(PROGRESS_JSON, all_reviews)
        
        print()
        print("✅ Update complete!")
        print(f"   ✓ Updated: {ALL_REVIEWS_JSON}")
        print(f"   ✓ Updated: {PROGRESS_JSON}")
        print(f"   ✓ Backup: {BACKUP_JSON}")
    else:
        print("ℹ️  No URLs found - no updates saved")
    
    print()
    print("=" * 70)


if __name__ == "__main__":
    main()

