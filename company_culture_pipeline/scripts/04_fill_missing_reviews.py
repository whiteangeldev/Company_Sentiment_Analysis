#!/usr/bin/env python3
"""
Script to fill missing review URLs in all_reviews.json

This script identifies companies where all 5 review platform URLs are blank
and searches for them using DuckDuckGo (same method as script 02).
"""

import json
import os
import time
import random
from typing import Dict, List
from urllib.parse import quote_plus
import requests
from bs4 import BeautifulSoup

# Configuration
DATA_DIR = "data/raw_reviews"
ALL_REVIEWS_JSON = f"{DATA_DIR}/all_reviews.json"
PROGRESS_JSON = f"{DATA_DIR}/reviews_progress.json"
BACKUP_JSON = f"{DATA_DIR}/all_reviews_backup.json"

# DuckDuckGo search settings
DDG_SEARCH_URL = "https://html.duckduckgo.com/html/"
MIN_DELAY = 4.0
MAX_DELAY = 8.0

# User agent
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Review platform URL fields
REVIEW_FIELDS = [
    "glassdoor_url",
    "indeed_url",
    "comparably_url",
    "kununu_url",
    "ambitionbox_url"
]

# Platform-specific search configurations
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


def search_review_site_duckduckgo(company_name: str, platform: str) -> str:
    """
    Search for company reviews on a platform using DuckDuckGo
    
    Args:
        company_name: Company name to search for
        platform: Platform to search ('glassdoor', 'indeed', etc.)
    
    Returns:
        URL to reviews page or empty string if not found
    """
    if platform not in PLATFORM_CONFIG:
        return ""
    
    config = PLATFORM_CONFIG[platform]
    search_query = config["query"].format(company_name)
    
    # Add delay to avoid rate limiting
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
    
    data = {"q": search_query, "b": "", "kl": "us-en"}
    
    try:
        response = requests.post(DDG_SEARCH_URL, headers=headers, data=data, timeout=15)
        
        if response.status_code != 200:
            return ""
        
        soup = BeautifulSoup(response.text, "html.parser")
        results = soup.find_all("a", class_="result__a")
        
        if not results:
            return ""
        
        # Find review page matching platform criteria
        for result in results[:10]:
            url = result.get("href", "")
            url_lower = url.lower()
            
            # Check if URL matches platform's domains and paths
            domain_match = any(domain in url_lower for domain in config["domains"])
            path_match = any(path.lower() in url_lower for path in config["paths"])
            
            if domain_match and path_match:
                # Platform-specific validation
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


def generate_manual_search_urls(company_name: str) -> Dict[str, str]:
    """Generate manual search URLs as fallback"""
    encoded_name = quote_plus(company_name)
    
    return {
        "glassdoor_search_url": f"https://www.glassdoor.com/Search/results.htm?keyword={encoded_name}",
        "indeed_search_url": f"https://www.indeed.com/cmp/{encoded_name.replace(' ', '-')}/reviews",
        "comparably_search_url": f"https://www.google.com/search?q={encoded_name}+comparably+reviews",
        "kununu_search_url": f"https://www.google.com/search?q={encoded_name}+kununu+reviews",
        "ambitionbox_search_url": f"https://www.google.com/search?q={encoded_name}+ambitionbox+reviews",
    }


def load_json(filepath: str) -> List[Dict]:
    """Load JSON file and return data"""
    if not os.path.exists(filepath):
        print(f"❌ ERROR: {filepath} not found")
        return []
    
    with open(filepath, encoding="utf-8") as f:
        return json.load(f)


def save_json(filepath: str, data: List[Dict]):
    """Save data to JSON file"""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"✓ Saved: {filepath}")


def has_all_blank_urls(company: Dict) -> bool:
    """Check if all 5 review URLs are blank"""
    return all(not company.get(field, "") for field in REVIEW_FIELDS)


def search_for_company(company: Dict, platforms: List[str]) -> tuple[Dict, int]:
    """
    Search for review URLs for a company across all platforms
    
    Args:
        company: Company dictionary
        platforms: List of platforms to search
    
    Returns:
        Tuple of (updated_company_dict, urls_found_count)
    """
    clean_name = company.get("clean_name", company["original_name"])
    found_count = 0
    
    print(f"\n🔍 [{company['original_name']}]")
    print(f"   Searching as: {clean_name}")
    
    for platform in platforms:
        print(f"   • {platform.capitalize():12s}", end=" ", flush=True)
        
        url = search_review_site_duckduckgo(clean_name, platform)
        
        if url:
            company[f"{platform}_url"] = url
            print(f"✓ Found")
            found_count += 1
        else:
            print(f"✗ Not found")
    
    # Generate manual search URLs as fallback
    manual_urls = generate_manual_search_urls(clean_name)
    company.update(manual_urls)
    
    return company, found_count


def main():
    print("=" * 70)
    print("FILL MISSING REVIEW URLs")
    print("Search for companies with blank URLs using DuckDuckGo")
    print("=" * 70)
    print()

    # Load data
    print("📁 Loading data files...")
    all_reviews = load_json(ALL_REVIEWS_JSON)

    if not all_reviews:
        print("❌ Could not load all_reviews.json")
        return

    print(f"   ✓ Loaded {len(all_reviews)} companies from all_reviews.json")
    print()

    # Find companies with all blank URLs
    blank_indices = [(i, c) for i, c in enumerate(all_reviews) if has_all_blank_urls(c)]
    
    if len(blank_indices) == 0:
        print("✓ No companies with blank URLs found. Nothing to update.")
        return
    
    print(f"🔍 Found {len(blank_indices)} companies with all 5 URLs blank:")
    for idx, company in blank_indices:
        print(f"   {idx+1}. {company['original_name']}")
    print()

    # Estimate time
    platforms_to_search = ["glassdoor", "indeed", "comparably", "kununu", "ambitionbox"]
    avg_delay = (MIN_DELAY + MAX_DELAY) / 2
    estimated_minutes = (len(blank_indices) * len(platforms_to_search) * avg_delay) / 60
    
    print(f"⏱️  Will search {len(platforms_to_search)} platforms per company:")
    print(f"   • {' • '.join([p.capitalize() for p in platforms_to_search])}")
    print(f"   Estimated time: {estimated_minutes:.1f} minutes ({avg_delay:.1f}s avg delay)")
    print()

    # Ask for confirmation
    response = input("Continue with search? (y/n): ").strip().lower()
    if response != 'y':
        print("❌ Cancelled")
        return

    # Create backup
    print("\n💾 Creating backup of all_reviews.json...")
    save_json(BACKUP_JSON, all_reviews)
    print(f"   ✓ Backup saved: {BACKUP_JSON}")

    # Search for missing companies
    print("\n" + "=" * 70)
    print("SEARCHING...")
    print("=" * 70)
    
    updated_count = 0
    total_urls_found = 0

    for idx, company in blank_indices:
        updated_company, found_count = search_for_company(company, platforms_to_search)
        all_reviews[idx] = updated_company
        
        if found_count > 0:
            updated_count += 1
            total_urls_found += found_count
            print(f"   → Found {found_count} URL(s)")
        else:
            print(f"   → No URLs found")

    # Save results
    print("\n" + "=" * 70)
    print("SUMMARY:")
    print(f"  Total companies: {len(all_reviews)}")
    print(f"  Companies searched: {len(blank_indices)}")
    print(f"  Companies with URLs found: {updated_count}")
    print(f"  Total URLs found: {total_urls_found}")
    print(f"  Companies still blank: {len(blank_indices) - updated_count}")
    print("=" * 70)
    print()

    if updated_count > 0:
        # Save updated data
        print("💾 Saving updated all_reviews.json...")
        save_json(ALL_REVIEWS_JSON, all_reviews)
        
        # Also update progress file
        print("💾 Saving updated reviews_progress.json...")
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

