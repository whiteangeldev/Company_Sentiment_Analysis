#!/usr/bin/env python3
"""
Simple retry script for failed company websites - no overcomplicated strategies
"""
import csv
import time
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import json
from pathlib import Path
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

MAX_PAGES = 50
TIMEOUT = 30
MAX_RETRIES = 3  # Retry each request up to 3 times

SECTION_KEYWORDS = {
    "about": ["about", "who-we-are", "our-company"],
    "careers": ["career", "jobs", "join-us"],
    "values": ["mission", "vision", "values", "culture"],
    "leadership": ["team", "leadership", "management"],
    "blog": ["blog", "news", "press"],
}


def classify_section(url_path: str):
    path = url_path.lower()
    for section, keywords in SECTION_KEYWORDS.items():
        if any(k in path for k in keywords):
            return section
    return "other"


def extract_text(html: str):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "header", "footer"]):
        tag.decompose()
    text = soup.get_text(" ", strip=True)
    return " ".join(text.split())


def fetch_page(url, session):
    """Simple, reliable page fetch with retries"""
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(
                url,
                headers=HEADERS,
                timeout=TIMEOUT,
                verify=False,  # Skip SSL issues - these are simple sites
                allow_redirects=True
            )
            if r.status_code == 200:
                return r, None
            elif r.status_code == 403 or r.status_code == 429:
                # Rate limited, wait longer
                time.sleep(3 * (attempt + 1))
            else:
                time.sleep(1)
        except requests.exceptions.Timeout:
            if attempt == MAX_RETRIES - 1:
                return None, "Timeout after retries"
            time.sleep(2)
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                return None, f"Error: {str(e)[:60]}"
            time.sleep(2)
    return None, "Max retries reached"


def scrape_site(company_id, company_name, base_url):
    visited = set()
    queue = [base_url]
    scraped_pages = []
    errors = []

    domain = urlparse(base_url).netloc
    session = requests.Session()

    while queue and len(visited) < MAX_PAGES:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)

        response, error = fetch_page(url, session)
        
        if error:
            if len(visited) == 1:  # Only log first page errors
                print(f"   ❌ {error}")
            errors.append({"url": url, "error": error})
            continue
        
        if len(visited) == 1:
            print(f"   ✅ Connected")

        if "text/html" not in response.headers.get("Content-Type", ""):
            continue

        text = extract_text(response.text)
        if len(text) < 200:
            continue

        section = classify_section(urlparse(url).path)
        scraped_pages.append({
            "company_id": company_id,
            "company_name": company_name,
            "url": url,
            "section": section,
            "text": text
        })

        if len(scraped_pages) == 1:
            print(f"   📄 Extracted {len(text)} chars")

        # Find more internal links
        soup = BeautifulSoup(response.text, "html.parser")
        for a in soup.find_all("a", href=True):
            link = urljoin(url, a["href"])
            if domain in urlparse(link).netloc:
                if link not in visited and link not in queue:
                    queue.append(link)

        time.sleep(1)  # Simple 1-second delay

    session.close()
    return scraped_pages, errors


def load_existing_data(output_path):
    """Load existing scraped data if it exists"""
    if output_path.exists():
        try:
            with open(output_path, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️  Could not load existing data: {e}")
            return []
    return []


def save_data(output_path, all_data):
    """Save data to JSON file"""
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_data, f, indent=2, ensure_ascii=False)


def save_failed_csv(failed_path, failed_companies):
    """Save failed companies as CSV in same format as input"""
    with open(failed_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "location", "website_url"])
        writer.writeheader()
        for company in failed_companies:
            writer.writerow({
                "name": company["company_name"],
                "location": company.get("location", "US"),
                "website_url": company["url"]
            })


def load_failed_companies(input_path):
    """Load failed companies from either CSV or JSON"""
    if not input_path.exists():
        return []
    
    # Try JSON first (old format)
    try:
        with open(input_path, encoding="utf-8") as f:
            data = json.load(f)
            if data and isinstance(data[0], dict):
                # Convert from JSON format to standard format
                companies = []
                for item in data:
                    companies.append({
                        "company_id": item.get("company_id"),
                        "company_name": item.get("company_name", "Unknown"),
                        "location": item.get("location", "US"),
                        "url": item.get("url", ""),
                        "previous_errors": item.get("errors", [])
                    })
                return companies
    except:
        pass
    
    # Try CSV
    try:
        with open(input_path, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            companies = []
            for idx, row in enumerate(reader, 1):
                companies.append({
                    "company_id": str(idx),
                    "company_name": row.get("name", "Unknown"),
                    "location": row.get("location", "US"),
                    "url": row.get("website_url", ""),
                    "previous_errors": []
                })
            return companies
    except:
        pass
    
    return []


def main():
    # Try both CSV and JSON as input
    input_csv = Path("data/scraped_websites/failed_companies.csv")
    input_json = Path("data/scraped_websites/failed_companies.json")
    output_path = Path("data/scraped_websites/website_text.json")
    failed_csv_path = Path("data/scraped_websites/failed_companies.csv")
    
    print("=" * 70)
    print("RETRY FAILED COMPANY SCRAPER - Simple & Reliable")
    print("=" * 70)
    print()

    # Load failed companies from CSV or JSON
    failed_companies = []
    if input_csv.exists():
        print(f"✓ Loading from CSV: {input_csv}")
        failed_companies = load_failed_companies(input_csv)
    elif input_json.exists():
        print(f"✓ Loading from JSON: {input_json}")
        failed_companies = load_failed_companies(input_json)
    else:
        print(f"❌ ERROR: No failed companies file found")
        print(f"   Expected: {input_csv} or {input_json}")
        return

    if not failed_companies:
        print("✓ No failed companies to retry!")
        return

    # Filter out companies that already succeeded (empty errors in JSON format)
    truly_failed = []
    already_succeeded = []
    for company in failed_companies:
        prev_errors = company.get("previous_errors", [])
        if isinstance(prev_errors, list) and len(prev_errors) == 0 and "previous_errors" in company:
            already_succeeded.append(company)
        else:
            truly_failed.append(company)
    
    if already_succeeded:
        print(f"✓ Found {len(already_succeeded)} companies with empty errors (already succeeded)")
    print(f"✓ Companies to retry: {len(truly_failed)}")
    print()

    # Load existing successful scrapes
    all_data = load_existing_data(output_path)
    print(f"✓ Existing successful scrapes in database: {len(all_data)}")
    print(f"✓ Max pages per site: {MAX_PAGES}")
    print()

    scraped_count = 0
    still_failed = []

    for idx, company_info in enumerate(truly_failed, 1):
        company_id = company_info.get("company_id")
        company_name = company_info.get("company_name", "Unknown")
        location = company_info.get("location", "US")
        base_url = company_info.get("url", "").strip()

        if not base_url:
            still_failed.append(company_info)
            continue

        print(f"[{idx}/{len(truly_failed)}] {company_name}")
        print(f"   URL: {base_url}")

        pages, errors = scrape_site(company_id, company_name, base_url)
        
        if pages:
            all_data.extend(pages)
            scraped_count += 1
            print(f"   ✅ SUCCESS! Scraped {len(pages)} pages")
            
            # Save immediately after each successful company
            save_data(output_path, all_data)
            print(f"   💾 Saved")
        else:
            # Still failed
            still_failed.append({
                "company_name": company_name,
                "location": location,
                "url": base_url,
                "errors": errors,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            })
            print(f"   ❌ Still failed")
            if errors:
                error_summary = errors[0]["error"] if errors else "Unknown"
                print(f"   📝 Error: {error_summary}")

        print()

    # Save still-failed companies as CSV
    if still_failed:
        save_failed_csv(failed_csv_path, still_failed)

    print("=" * 70)
    print("RETRY SUMMARY:")
    print(f"  Companies attempted: {len(truly_failed)}")
    print(f"  Now successful: {scraped_count}")
    print(f"  Still failed: {len(still_failed)}")
    print(f"  Total pages in database: {len(all_data)}")
    print()
    print(f"  ✓ Output JSON: {output_path}")
    if still_failed:
        print(f"  ⚠️  Failed companies CSV: {failed_csv_path}")
        print(f"     (You can retry again using this CSV as input)")
    if scraped_count > 0:
        print(f"\n  🎉 Successfully recovered {scraped_count} companies!")
    print("=" * 70)


if __name__ == "__main__":
    main()
