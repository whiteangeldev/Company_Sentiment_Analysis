#!/usr/bin/env python3
"""
Script to scrape actual review content from Glassdoor and Indeed pages

⚠️ IMPORTANT REALITY CHECK:
1. Glassdoor and Indeed have VERY strong anti-scraping protection
2. Most reviews require login to view
3. Both sites use heavy JavaScript rendering
4. Automated scraping will get blocked within 2-3 requests
5. This violates their Terms of Service

PRACTICAL APPROACHES:
A. Selenium (browser automation) - Slower but more reliable
B. Manual collection - Most reliable, legal
C. API services - Paid but legal (Bright Data, ScrapingBee, etc.)

This script provides Selenium-based scraping with realistic expectations.
"""

import csv
import json
import os
import time
import random
from datetime import datetime

# Check if selenium is installed
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.common.exceptions import TimeoutException, NoSuchElementException

    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    print("⚠️  Selenium not installed. Install with: pip install selenium")

# Configuration
INPUT_CSV = "data/raw_reviews/reviews_summary.csv"
OUTPUT_DIR = "data/raw_reviews"
REVIEWS_JSON = f"{OUTPUT_DIR}/review_content.json"
REVIEWS_CSV = f"{OUTPUT_DIR}/review_content.csv"

# Scraping settings
MAX_REVIEWS_PER_COMPANY = 10  # Limit reviews per company
PAGE_LOAD_TIMEOUT = 15
SCROLL_DELAY = 2.0


def setup_selenium_driver():
    """Set up Selenium WebDriver with stealth options"""
    if not SELENIUM_AVAILABLE:
        return None

    chrome_options = Options()

    # Headless mode (runs without opening browser window)
    # Comment out the next line if you want to see the browser
    chrome_options.add_argument("--headless")

    # Stealth options to avoid detection
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")

    # Random user agent
    user_agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    ]
    chrome_options.add_argument(f"user-agent={random.choice(user_agents)}")

    # Exclude automation flags
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)

    try:
        driver = webdriver.Chrome(options=chrome_options)
        # Override navigator.webdriver
        driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        return driver
    except Exception as e:
        print(f"❌ Error setting up Chrome driver: {e}")
        print("   Make sure ChromeDriver is installed:")
        print("   - macOS: brew install chromedriver")
        print("   - Or download from: https://chromedriver.chromium.org/")
        return None


def scrape_glassdoor_reviews(driver, url, max_reviews=10):
    """
    Scrape reviews from Glassdoor using Selenium

    Note: This will likely fail due to:
    - Login walls
    - CAPTCHA challenges
    - Anti-bot detection

    Returns list of review dictionaries or None
    """
    try:
        print(f"      Loading Glassdoor page...")
        driver.get(url)
        time.sleep(random.uniform(3, 5))

        # Wait for page to load
        try:
            WebDriverWait(driver, PAGE_LOAD_TIMEOUT).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
        except TimeoutException:
            print(f"      ⚠️  Page load timeout")
            return None

        # Check for common blocking indicators
        page_source = driver.page_source.lower()
        if "captcha" in page_source or "access denied" in page_source:
            print(f"      ❌ Blocked by CAPTCHA or access denied")
            return None

        # Try to find review elements (these selectors may be outdated)
        reviews = []

        # Glassdoor review selectors (as of 2024 - may change!)
        review_selectors = [
            "li[data-test='emp-review']",
            "li.empReview",
            "div.review",
            "article.review",
        ]

        review_elements = None
        for selector in review_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    review_elements = elements
                    break
            except:
                continue

        if not review_elements:
            print(
                f"      ⚠️  No review elements found (may need login or selectors outdated)"
            )
            return None

        print(f"      Found {len(review_elements)} review elements")

        for idx, element in enumerate(review_elements[:max_reviews]):
            try:
                review = extract_glassdoor_review_data(element)
                if review:
                    reviews.append(review)
            except Exception as e:
                continue

        return reviews if reviews else None

    except Exception as e:
        print(f"      ⚠️  Error: {str(e)[:80]}")
        return None


def extract_glassdoor_review_data(element):
    """Extract review data from a Glassdoor review element"""
    try:
        review = {
            "rating": "",
            "title": "",
            "pros": "",
            "cons": "",
            "advice": "",
            "job_title": "",
            "location": "",
            "date": "",
        }

        # These selectors are examples and will likely need updating
        try:
            rating_elem = element.find_element(By.CSS_SELECTOR, "[class*='rating']")
            review["rating"] = (
                rating_elem.get_attribute("aria-label") or rating_elem.text
            )
        except:
            pass

        try:
            title_elem = element.find_element(By.CSS_SELECTOR, "h2, [class*='summary']")
            review["title"] = title_elem.text
        except:
            pass

        try:
            pros_elem = element.find_element(By.CSS_SELECTOR, "[class*='pros'], .pros")
            review["pros"] = pros_elem.text.replace("Pros", "").strip()
        except:
            pass

        try:
            cons_elem = element.find_element(By.CSS_SELECTOR, "[class*='cons'], .cons")
            review["cons"] = cons_elem.text.replace("Cons", "").strip()
        except:
            pass

        return review if (review["title"] or review["pros"] or review["cons"]) else None

    except:
        return None


def scrape_indeed_reviews(driver, url, max_reviews=10):
    """
    Scrape reviews from Indeed using Selenium

    Similar limitations as Glassdoor
    """
    try:
        print(f"      Loading Indeed page...")
        driver.get(url)
        time.sleep(random.uniform(3, 5))

        try:
            WebDriverWait(driver, PAGE_LOAD_TIMEOUT).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
        except TimeoutException:
            print(f"      ⚠️  Page load timeout")
            return None

        # Check for blocking
        page_source = driver.page_source.lower()
        if "captcha" in page_source or "access denied" in page_source:
            print(f"      ❌ Blocked by CAPTCHA or access denied")
            return None

        reviews = []

        # Indeed review selectors (may be outdated)
        review_selectors = [
            "div.cmp-Review",
            "[data-tn-component='reviews']",
            "div.review",
        ]

        review_elements = None
        for selector in review_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    review_elements = elements
                    break
            except:
                continue

        if not review_elements:
            print(f"      ⚠️  No review elements found")
            return None

        print(f"      Found {len(review_elements)} review elements")

        for element in review_elements[:max_reviews]:
            try:
                review = extract_indeed_review_data(element)
                if review:
                    reviews.append(review)
            except:
                continue

        return reviews if reviews else None

    except Exception as e:
        print(f"      ⚠️  Error: {str(e)[:80]}")
        return None


def extract_indeed_review_data(element):
    """Extract review data from an Indeed review element"""
    try:
        review = {
            "rating": "",
            "title": "",
            "review_text": "",
            "job_title": "",
            "location": "",
            "date": "",
        }

        # Example selectors - will need updating
        try:
            rating_elem = element.find_element(By.CSS_SELECTOR, "[class*='rating']")
            review["rating"] = (
                rating_elem.get_attribute("aria-label") or rating_elem.text
            )
        except:
            pass

        try:
            title_elem = element.find_element(By.CSS_SELECTOR, "h2, [class*='title']")
            review["title"] = title_elem.text
        except:
            pass

        try:
            text_elem = element.find_element(
                By.CSS_SELECTOR, "[class*='review-text'], p"
            )
            review["review_text"] = text_elem.text
        except:
            pass

        return review if (review["title"] or review["review_text"]) else None

    except:
        return None


def scrape_company_reviews(
    company_name, glassdoor_url, indeed_url, driver, method="selenium"
):
    """Scrape reviews for a single company"""
    result = {
        "company_name": company_name,
        "glassdoor_url": glassdoor_url,
        "indeed_url": indeed_url,
        "glassdoor_reviews": [],
        "indeed_reviews": [],
        "glassdoor_count": 0,
        "indeed_count": 0,
        "status": "pending",
        "scraped_at": datetime.now().isoformat(),
    }

    if method == "selenium":
        if not driver:
            result["status"] = "no_driver"
            return result

        # Try Glassdoor
        if glassdoor_url:
            print(f"    → Scraping Glassdoor...")
            reviews = scrape_glassdoor_reviews(
                driver, glassdoor_url, MAX_REVIEWS_PER_COMPANY
            )
            if reviews:
                result["glassdoor_reviews"] = reviews
                result["glassdoor_count"] = len(reviews)
                print(f"      ✓ Got {len(reviews)} reviews")

            # Delay between sites
            time.sleep(random.uniform(3, 6))

        # Try Indeed
        if indeed_url:
            print(f"    → Scraping Indeed...")
            reviews = scrape_indeed_reviews(driver, indeed_url, MAX_REVIEWS_PER_COMPANY)
            if reviews:
                result["indeed_reviews"] = reviews
                result["indeed_count"] = len(reviews)
                print(f"      ✓ Got {len(reviews)} reviews")

        total_reviews = result["glassdoor_count"] + result["indeed_count"]
        result["status"] = "success" if total_reviews > 0 else "no_reviews"

    return result


def export_to_csv(all_results, output_path):
    """Export reviews to flattened CSV format"""
    rows = []

    for company_result in all_results:
        company_name = company_result["company_name"]

        # Glassdoor reviews
        for review in company_result.get("glassdoor_reviews", []):
            row = {
                "company_name": company_name,
                "platform": "Glassdoor",
                "rating": review.get("rating", ""),
                "title": review.get("title", ""),
                "pros": review.get("pros", ""),
                "cons": review.get("cons", ""),
                "advice": review.get("advice", ""),
                "job_title": review.get("job_title", ""),
                "location": review.get("location", ""),
                "date": review.get("date", ""),
                "scraped_at": company_result.get("scraped_at", ""),
            }
            rows.append(row)

        # Indeed reviews
        for review in company_result.get("indeed_reviews", []):
            row = {
                "company_name": company_name,
                "platform": "Indeed",
                "rating": review.get("rating", ""),
                "title": review.get("title", ""),
                "pros": "",
                "cons": "",
                "advice": "",
                "review_text": review.get("review_text", ""),
                "job_title": review.get("job_title", ""),
                "location": review.get("location", ""),
                "date": review.get("date", ""),
                "scraped_at": company_result.get("scraped_at", ""),
            }
            rows.append(row)

    if rows:
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            fieldnames = [
                "company_name",
                "platform",
                "rating",
                "title",
                "pros",
                "cons",
                "advice",
                "review_text",
                "job_title",
                "location",
                "date",
                "scraped_at",
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)


def main():
    print("=" * 70)
    print("EMPLOYEE REVIEW CONTENT SCRAPER")
    print("=" * 70)
    print()

    if not SELENIUM_AVAILABLE:
        print("❌ Selenium is not installed")
        print()
        print("Install it with:")
        print("  pip install selenium")
        print()
        print("Also install ChromeDriver:")
        print("  macOS: brew install chromedriver")
        print("  Or: https://chromedriver.chromium.org/downloads")
        return

    print("⚠️  IMPORTANT DISCLAIMER:")
    print()
    print("Web scraping Glassdoor and Indeed:")
    print("  • Violates their Terms of Service")
    print("  • Will likely get blocked after 2-5 companies")
    print("  • May require CAPTCHA solving")
    print("  • Most content requires login")
    print()
    print("This script is for EDUCATIONAL purposes only!")
    print("For production use:")
    print("  → Manual collection (most reliable)")
    print("  → Official APIs (if available)")
    print("  → Data providers (Bright Data, etc.)")
    print()

    proceed = input("Continue anyway? (yes/no): ").lower().strip()
    if proceed != "yes":
        print("Aborted.")
        return

    print()
    print("=" * 70)
    print()

    # Load companies with review URLs
    if not os.path.exists(INPUT_CSV):
        print(f"❌ ERROR: {INPUT_CSV} not found")
        print(f"   Please run 02_get_employee_reviews.py first")
        return

    companies = []
    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("glassdoor_url") or row.get("indeed_url"):
                companies.append(row)

    if not companies:
        print("❌ No companies with review URLs found")
        return

    total = len(companies)
    print(f"Found {total} companies with review URLs\n")

    # Limit to first N companies for testing
    max_companies = int(
        input(f"How many companies to scrape? [max {total}, default 3]: ").strip()
        or "3"
    )
    companies = companies[: min(max_companies, total)]

    print(f"\nScraping {len(companies)} companies...")
    print(f"This may take a while and will likely get blocked!\n")

    # Setup Selenium driver
    print("Setting up browser...")
    driver = setup_selenium_driver()
    if not driver:
        return

    print("✓ Browser ready\n")

    all_results = []
    success_count = 0

    try:
        for idx, company in enumerate(companies, 1):
            company_name = company["original_name"]
            glassdoor_url = company.get("glassdoor_url", "")
            indeed_url = company.get("indeed_url", "")

            print(f"[{idx}/{len(companies)}] {company_name}")

            result = scrape_company_reviews(
                company_name, glassdoor_url, indeed_url, driver, method="selenium"
            )
            all_results.append(result)

            if result["status"] == "success":
                success_count += 1

            # Longer delay between companies to avoid detection
            if idx < len(companies):
                delay = random.uniform(8, 15)
                print(f"    Waiting {delay:.1f}s before next company...\n")
                time.sleep(delay)

    finally:
        # Clean up
        if driver:
            driver.quit()

    # Save results
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(REVIEWS_JSON, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)

    export_to_csv(all_results, REVIEWS_CSV)

    total_reviews = sum(r["glassdoor_count"] + r["indeed_count"] for r in all_results)

    print("=" * 70)
    print("RESULTS:")
    print(f"  Companies processed: {len(companies)}")
    print(f"  Successfully scraped: {success_count}")
    print(f"  Total reviews collected: {total_reviews}")
    print()
    print(f"✓ Results saved:")
    print(f"  - JSON: {REVIEWS_JSON}")
    print(f"  - CSV:  {REVIEWS_CSV}")
    print("=" * 70)
    print()

    if success_count < len(companies) * 0.3:
        print("⚠️  LOW SUCCESS RATE (< 30%)")
        print()
        print("This is expected when scraping review sites.")
        print()
        print("RECOMMENDED ALTERNATIVES:")
        print("  1. Manual collection (use 03b_manual_review_template.py)")
        print("  2. Data providers:")
        print("     - Bright Data: https://brightdata.com/")
        print("     - ScrapingBee: https://www.scrapingbee.com/")
        print("  3. Official APIs (contact Glassdoor/Indeed for access)")
        print()


if __name__ == "__main__":
    main()
