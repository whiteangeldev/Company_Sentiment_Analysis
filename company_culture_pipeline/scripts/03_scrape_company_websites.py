import csv
import time
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import json
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException
import os

MAX_PAGES = 50  # limit per website
TIMEOUT = 30
MAX_RETRIES = 3  # Retry each request up to 3 times
PAGE_LOAD_WAIT = 5  # Seconds to wait for page to fully load

# Keywords to classify page type
SECTION_KEYWORDS = {
    "about": ["about", "who-we-are", "our-company"],
    "careers": ["career", "jobs", "join-us"],
    "values": ["mission", "vision", "values", "culture"],
    "leadership": ["team", "leadership", "management"],
    "blog": ["blog", "news", "press"],
}


def init_browser():
    """Initialize Selenium Chrome browser with headless options"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-setuid-sandbox")
    chrome_options.add_argument("--remote-debugging-port=9222")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    
    # Specify Chromium binary (snap installation)
    chrome_options.binary_location = "/snap/bin/chromium"

    try:
        # Use locally installed ChromeDriver that matches Chromium version
        chromedriver_path = os.path.expanduser("~/.local/bin/chromedriver")
        service = Service(chromedriver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(TIMEOUT)
        return driver
    except Exception as e:
        print(f"❌ Failed to initialize browser: {e}")
        return None


def classify_section(url_path: str):
    path = url_path.lower()
    for section, keywords in SECTION_KEYWORDS.items():
        if any(k in path for k in keywords):
            return section
    return "other"


def extract_text(html: str):
    soup = BeautifulSoup(html, "html.parser")

    # Remove scripts, styles, nav, footer
    for tag in soup(["script", "style", "nav", "header", "footer"]):
        tag.decompose()

    text = soup.get_text(" ", strip=True)
    return " ".join(text.split())


def fetch_page(url, driver):
    """Fetch page using Selenium browser with retries"""
    for attempt in range(MAX_RETRIES):
        try:
            driver.get(url)
            # Wait for page to load
            time.sleep(PAGE_LOAD_WAIT)

            # Get page source
            html = driver.page_source

            # Check if we got meaningful content
            if html and len(html) > 500:
                return html, None
            else:
                if attempt == MAX_RETRIES - 1:
                    return None, "Page loaded but content too small"
                time.sleep(2)

        except TimeoutException:
            if attempt == MAX_RETRIES - 1:
                return None, "Timeout after retries"
            time.sleep(2)
        except WebDriverException as e:
            if attempt == MAX_RETRIES - 1:
                return None, f"WebDriver error: {str(e)[:60]}"
            time.sleep(2)
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                return None, f"Error: {str(e)[:60]}"
            time.sleep(2)
    return None, "Max retries reached"


def scrape_site(company_id, company_name, base_url, driver):
    visited = set()
    queue = [base_url]
    scraped_pages = []
    errors = []

    domain = urlparse(base_url).netloc

    while queue and len(visited) < MAX_PAGES:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)

        html, error = fetch_page(url, driver)

        if error:
            if len(visited) == 1:  # Only log first page errors
                print(f"   ❌ {error}")
            errors.append({"url": url, "error": error})
            # Don't stop on errors, keep trying other pages
            continue

        if len(visited) == 1:
            print(f"   ✅ Connected (browser)")

        # Extract text
        text = extract_text(html)
        if len(text) < 200:
            continue  # too small

        section = classify_section(urlparse(url).path)

        scraped_pages.append(
            {
                "company_id": company_id,
                "company_name": company_name,
                "url": url,
                "section": section,
                "text": text,
            }
        )

        if len(scraped_pages) == 1:
            print(f"   📄 Extracted {len(text)} chars")

        # Find more internal links
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            link = urljoin(url, a["href"])
            if domain in urlparse(link).netloc:
                if link not in visited and link not in queue:
                    queue.append(link)

        time.sleep(1)  # Simple 1-second delay

    return scraped_pages, errors


def load_existing_data(output_path):
    """Load existing scraped data if it exists"""
    if output_path.exists():
        try:
            with open(output_path, encoding="utf-8") as f:
                data = json.load(f)
                # Get set of already scraped company names
                scraped_companies = set(item["company_name"] for item in data)
                return data, scraped_companies
        except Exception as e:
            print(f"⚠️  Could not load existing data: {e}")
            return [], set()
    return [], set()


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
            writer.writerow(
                {
                    "name": company["company_name"],
                    "location": company.get("location", "US"),
                    "website_url": company["url"],
                }
            )


def main():
    # Updated paths to match project structure
    input_csv = "data/tmp/companies_with_sites.csv"
    output_path = Path("data/scraped_websites/website_text.json")
    failed_csv_path = Path("data/scraped_websites/failed_companies.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("COMPANY WEBSITE SCRAPER - Browser-Based Scraping")
    print("=" * 70)
    print()

    if not Path(input_csv).exists():
        print(f"❌ ERROR: {input_csv} not found")
        print(f"   Please run 01_resolve_websites.py first")
        return

    # Initialize browser
    print("🌐 Initializing browser...")
    driver = init_browser()
    if not driver:
        print("❌ Failed to initialize browser. Exiting.")
        return
    print("✓ Browser initialized (headless Chrome)\n")

    # Load existing data to resume from where we left off
    all_data, scraped_companies = load_existing_data(output_path)
    if scraped_companies:
        print(
            f"📁 Found existing data with {len(scraped_companies)} companies already scraped"
        )
        print(f"   Resuming from where we left off...\n")

    with open(input_csv, newline="", encoding="utf-8") as f:
        companies = list(csv.DictReader(f))

    print(f"✓ Loaded {len(companies)} companies from CSV")
    print(f"✓ Max pages per site: {MAX_PAGES}")

    remaining = len(
        [
            c
            for c in companies
            if c.get("name", "Unknown") not in scraped_companies
            and c.get("website_url", "").strip()
        ]
    )
    if remaining > 0:
        print(f"✓ Remaining to scrape: {remaining}")
    print()

    scraped_count = 0
    skipped_count = 0
    failed_count = 0
    failed_companies = []

    try:
        for idx, row in enumerate(companies, 1):
            base_url = row.get("website_url", "").strip()
            company_name = row.get("name", "Unknown")
            location = row.get("location", "US")

            if not base_url:
                skipped_count += 1
                continue

            # Skip if already scraped
            if company_name in scraped_companies:
                print(f"[{idx}/{len(companies)}] {company_name}")
                print(f"   ✓ Already scraped (skipped)")
                continue

            # Use index as company_id if not present
            company_id = row.get("company_id", str(idx))

            print(f"[{idx}/{len(companies)}] {company_name}")
            print(f"   URL: {base_url}")

            pages, errors = scrape_site(company_id, company_name, base_url, driver)

            if pages:
                all_data.extend(pages)
                scraped_count += 1
                print(f"   ✓ Scraped {len(pages)} pages")

                # Save immediately after each company
                save_data(output_path, all_data)
                print(f"   💾 Saved")
            else:
                # Track failed company
                failed_count += 1
                failed_info = {
                    "company_name": company_name,
                    "location": location,
                    "url": base_url,
                    "errors": errors,
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                }
                failed_companies.append(failed_info)

                # Save failed companies as CSV
                save_failed_csv(failed_csv_path, failed_companies)

                print(f"   ⚠️  No pages scraped - saved to failed CSV")
                if errors:
                    error_summary = errors[0]["error"] if errors else "Unknown"
                    print(f"   📝 Error: {error_summary}")

    finally:
        # Always close the browser
        print("\n🌐 Closing browser...")
        driver.quit()

    print()
    print("=" * 70)
    print("SUMMARY:")
    print(f"  Companies successfully scraped: {scraped_count}")
    print(f"  Companies failed: {failed_count}")
    print(f"  Companies skipped (no URL): {skipped_count}")
    print(f"  Total pages in database: {len(all_data)}")
    print()
    print(f"  ✓ Output JSON: {output_path}")
    if failed_count > 0:
        print(f"  ⚠️  Failed companies CSV: {failed_csv_path}")
        print(f"     (You can retry these by using this CSV as input)")
    print("=" * 70)


if __name__ == "__main__":
    main()
