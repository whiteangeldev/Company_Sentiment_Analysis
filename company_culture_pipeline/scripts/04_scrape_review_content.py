#!/usr/bin/env python3
"""
Script to scrape employee reviews from Indeed using ScraperAPI

Features:
- Multi-page scraping (up to 5 pages per company) - OPTIMIZED FOR RATE LIMITS
- Bypasses Cloudflare using ScraperAPI
- Extracts topic, text, and ratings
- Handles pagination automatically
- Uses ScraperAPI free tier (1,000 calls/month = 100 companies × 5 pages = 500 calls)
- Smart rate limiting with 10-15s delays between pages
- Exponential backoff on errors

Platform: Indeed only
Note: Glassdoor skipped - requires premium ScraperAPI features (unreliable with free tier)

Rate Limiting Protection:
- 5 pages per company (reduced from 10)
- 10-15 seconds between pages
- 15-20 seconds between companies
- 2 second base delay after each API call
- Exponential backoff on connection errors
"""

import csv
import json
import os
import time
import random
import re
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse

from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    WebDriverException,
    NoSuchElementException,
)

# Load environment variables from .env file
load_dotenv()

# Configuration
INPUT_JSON = "data/raw_reviews/all_reviews.json"
OUTPUT_DIR = "data/raw_reviews"
REVIEWS_OUTPUT = f"{OUTPUT_DIR}/scraped_reviews.json"
PROGRESS_FILE = f"{OUTPUT_DIR}/scraping_progress.json"
FAILED_FILE = f"{OUTPUT_DIR}/failed_reviews.csv"

# Scraping settings - OPTIMIZED TO AVOID RATE LIMITING
MAX_REVIEWS_PER_COMPANY = 200  # Max reviews to scrape per company
MAX_PAGES_PER_COMPANY = (
    5  # Reduced from 10 to avoid rate limits (5 pages × ~20 reviews = 100 reviews)
)
MAX_RETRIES = 5  # Retry attempts per URL
PAGE_LOAD_TIMEOUT = 60
SCROLL_DELAY = 4.0
WAIT_TIMEOUT = 45

# Rate limiting protection - delays between requests
DELAY_BETWEEN_PAGES = (10, 15)  # Seconds to wait between page requests (min, max)
DELAY_BETWEEN_PLATFORMS = (15, 20)  # Seconds between platform switches
DELAY_AFTER_API_CALL = 2  # Base delay after every API call

# ScraperAPI configuration
SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY", "")
USE_SCRAPERAPI = len(SCRAPERAPI_KEY) > 0
SCRAPERAPI_PLATFORMS = ["glassdoor", "indeed"]  # Only use for these platforms

# User agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/121.0",
]


def init_browser():
    """Initialize Undetected Chrome browser to bypass Cloudflare"""
    try:
        options = uc.ChromeOptions()

        # Keep headless commented - undetected works better in headed mode
        # options.add_argument("--headless=new")

        # Basic options
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")

        # Random user agent
        user_agent = random.choice(USER_AGENTS)
        options.add_argument(f"--user-agent={user_agent}")

        # Initialize undetected-chromedriver
        # use_subprocess=True helps with stability
        driver = uc.Chrome(
            options=options,
            use_subprocess=True,
            version_main=None,  # Auto-detect Chrome version
        )

        driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)

        print("   ✓ Initialized undetected Chrome browser")
        return driver

    except Exception as e:
        print(f"   ❌ Failed to initialize browser: {e}")
        return None


def simplify_glassdoor_url(url):
    """
    Simplify complex Glassdoor URLs by removing location parameters
    Complex URLs often fail, simpler ones work better
    """
    import re

    # Extract company ID from complex URL
    # Example: Reviews-EI_IE4258.0,22_IL.23,32_IM358.htm -> E4258.htm
    match = re.search(r"IE(\d+)", url)
    if match and "glassdoor.com" in url:
        company_id = match.group(1)
        # Get company name from URL
        name_match = re.search(r"/Reviews/([^/]+?)-(?:Reviews|Greenwood|Portland)", url)
        if name_match:
            company_name = name_match.group(1)
            simple_url = f"https://www.glassdoor.com/Reviews/{company_name}-Reviews-E{company_id}.htm"
            print(f"      📝 Simplified URL: Using main company page")
            return simple_url, company_name, company_id

    return url, None, None


def generate_glassdoor_page_urls(base_url, max_pages=10):
    """
    Generate paginated Glassdoor URLs
    Example: Page 1: Reviews-E4258.htm, Page 2: Reviews-E4258_P2.htm
    """
    import re

    urls = []

    # First simplify the URL if needed
    simplified, company_name, company_id = simplify_glassdoor_url(base_url)

    if not company_id:
        # Can't parse, just return base URL
        print(f"      ⚠️  Could not parse Glassdoor URL for pagination")
        return [base_url]

    # Generate page URLs
    for page in range(1, max_pages + 1):
        if page == 1:
            url = f"https://www.glassdoor.com/Reviews/{company_name}-Reviews-E{company_id}.htm"
        else:
            url = f"https://www.glassdoor.com/Reviews/{company_name}-Reviews-E{company_id}_P{page}.htm"
        urls.append(url)

    return urls


def generate_indeed_page_urls(base_url, max_pages=10):
    """
    Generate paginated Indeed URLs
    Example: Page 1: /reviews, Page 2: /reviews?start=20, Page 3: /reviews?start=40
    """
    urls = []

    # Indeed shows ~20 reviews per page
    reviews_per_page = 20

    for page in range(max_pages):
        if page == 0:
            urls.append(base_url)
        else:
            start = page * reviews_per_page
            separator = "&" if "?" in base_url else "?"
            url = f"{base_url}{separator}start={start}"
            urls.append(url)

    return urls


def scrape_with_scraperapi(url, render=True, retry=0, max_retries=2):
    """
    Scrape a URL using ScraperAPI to bypass Cloudflare

    Args:
        url: Target URL to scrape
        render: Whether to render JavaScript (default True for SPA sites)
        retry: Current retry attempt
        max_retries: Maximum number of retries for 500 errors

    Returns:
        HTML content as string, or None if failed
    """
    if not SCRAPERAPI_KEY:
        print("      ⚠️  ScraperAPI key not found in environment")
        return None

    try:
        # Simplify Glassdoor URLs to improve success rate
        original_url = url
        if "glassdoor.com" in url:
            simplified_result = simplify_glassdoor_url(url)
            if isinstance(simplified_result, tuple):
                url, company_name, company_id = simplified_result
            else:
                url = simplified_result

        # Validate URL
        if not url or not url.startswith("http"):
            print(f"      ❌ Invalid URL generated: {url}")
            return None

        # Keep params minimal to avoid 400 errors
        params = {
            "api_key": SCRAPERAPI_KEY,
            "url": url,
            "render": "true",
        }

        api_url = "http://api.scraperapi.com"

        # Debug output on first try
        if retry == 0 and "glassdoor" in url:
            print(f"      🔍 URL: {url[:80]}...")

        # Add exponential backoff delay between retries
        if retry > 0:
            wait_time = retry * 5  # Increased from 3 to 5 seconds per retry
            print(
                f"      ⏳ Waiting {wait_time}s before retry {retry}/{max_retries}..."
            )
            time.sleep(wait_time)

        response = requests.get(api_url, params=params, timeout=90)

        if response.status_code == 200:
            print(f"      ✓ ScraperAPI success (status: {response.status_code})")
            # Add delay after successful API call to avoid rate limiting
            time.sleep(DELAY_AFTER_API_CALL)
            return response.text

        elif response.status_code == 500 and retry < max_retries:
            # Retry on 500 errors (server issues)
            print(
                f"      ⚠️  ScraperAPI 500 error - retrying ({retry + 1}/{max_retries})..."
            )
            return scrape_with_scraperapi(url, render, retry + 1, max_retries)

        elif response.status_code == 400:
            print(f"      ⚠️  ScraperAPI 400: Bad Request - URL might be malformed")
            print(f"      URL: {url[:100]}...")
            # Don't retry 400s - they won't succeed
            return None

        elif response.status_code == 403:
            print(f"      ⚠️  ScraperAPI 403: API key issue or credits exhausted")
            return None

        elif response.status_code == 404:
            print(f"      ⚠️  ScraperAPI 404: Target URL not found")
            return None

        else:
            print(f"      ⚠️  ScraperAPI returned status: {response.status_code}")
            if retry < max_retries and response.status_code >= 500:
                return scrape_with_scraperapi(url, render, retry + 1, max_retries)
            return None

    except requests.Timeout:
        print(f"      ⚠️  ScraperAPI timeout (90s)")
        if retry < max_retries:
            return scrape_with_scraperapi(url, render, retry + 1, max_retries)
        return None

    except Exception as e:
        error_msg = str(e)
        print(f"      ❌ ScraperAPI error: {error_msg[:60]}")

        # If connection error, add extra delay before retry (likely rate limited)
        if "Max retries exceeded" in error_msg or "Connection" in error_msg:
            if retry < max_retries:
                wait_time = (retry + 1) * 10  # Longer wait for connection errors
                print(
                    f"      ⏳ Connection issue - waiting {wait_time}s before retry..."
                )
                time.sleep(wait_time)
                return scrape_with_scraperapi(url, render, retry + 1, max_retries)
        elif retry < max_retries:
            return scrape_with_scraperapi(url, render, retry + 1, max_retries)
        return None


def parse_glassdoor_html(html, max_reviews=10):
    """Parse Glassdoor reviews from HTML - simple format"""
    reviews = []

    try:
        soup = BeautifulSoup(html, "html.parser")

        # Try multiple selectors for Glassdoor reviews
        review_selectors = [
            'li[class*="review"]',
            'li[class*="Review"]',
            'div[class*="review"]',
            'article[class*="review"]',
            '[data-test*="review"]',
        ]

        review_elements = []
        for selector in review_selectors:
            review_elements = soup.select(selector)
            if review_elements and len(review_elements) > 3:
                break

        if not review_elements:
            print("      ⚠️  No review elements found in HTML")
            return reviews

        print(f"      Found {len(review_elements[:max_reviews])} review elements")

        for idx, element in enumerate(review_elements[:max_reviews], 1):
            try:
                # Extract topic/title
                topic = ""
                topic_selectors = [
                    '[class*="reviewTitle"]',
                    '[class*="review-title"]',
                    '[class*="Summary"]',
                    "h2",
                    "h3",
                    '[data-test*="title"]',
                ]
                for selector in topic_selectors:
                    topic_elem = element.select_one(selector)
                    if topic_elem:
                        topic = topic_elem.get_text(strip=True)
                        if topic and len(topic) > 3:
                            break

                # Extract review text - try multiple approaches
                review_text = ""

                # Approach 1: Try to find pros and cons separately and combine
                pros = ""
                cons = ""

                # Look for pros/cons in the element
                for sub_elem in element.select(
                    '[class*="fullWidth"], [class*="reviewBodyCell"]'
                ):
                    text = sub_elem.get_text(strip=True)
                    if "Pros" in text[:30]:
                        pros = text.replace("Pros", "").replace("pros", "").strip()
                    elif "Cons" in text[:30]:
                        cons = text.replace("Cons", "").replace("cons", "").strip()

                if pros or cons:
                    review_text = (
                        f"Pros: {pros}\n\nCons: {cons}"
                        if pros and cons
                        else (pros or cons)
                    )

                # Approach 2: Try specific text selectors
                if not review_text:
                    text_selectors = [
                        '[class*="reviewText"]',
                        '[class*="review-text"]',
                        '[class*="description"]',
                        'span[class*="cont"]',
                    ]
                    for selector in text_selectors:
                        text_elem = element.select_one(selector)
                        if text_elem:
                            review_text = text_elem.get_text(strip=True)
                            if review_text and len(review_text) > 30:
                                break

                # Approach 3: Get all paragraph text
                if not review_text:
                    paragraphs = element.select("p")
                    if paragraphs:
                        review_text = " ".join(
                            [
                                p.get_text(strip=True)
                                for p in paragraphs
                                if p.get_text(strip=True)
                            ]
                        )

                # Extract rating
                rating = None
                try:
                    rating_elem = element.select_one('[class*="rating"]')
                    if rating_elem:
                        rating_text = (
                            rating_elem.get("aria-label", "") or rating_elem.get_text()
                        )
                        rating_match = re.search(r"(\d+\.?\d*)", rating_text)
                        if rating_match:
                            rating = float(rating_match.group(1))
                except:
                    pass

                # More lenient content check - accept if we have topic OR text
                if (review_text and len(review_text) > 30) or (
                    topic and len(topic) > 5
                ):
                    reviews.append(
                        {
                            "topic": topic or None,
                            "text": review_text or topic,  # Use topic as fallback
                            "rating": rating,
                            "platform": "glassdoor",
                            "scraped_at": datetime.now().isoformat(),
                            "method": "scraperapi",
                        }
                    )

            except Exception as e:
                print(f"      ⚠️  Error parsing review {idx}: {str(e)[:40]}")
                continue

        print(f"      ✓ Parsed {len(reviews)} reviews from HTML")

    except Exception as e:
        print(f"      ❌ HTML parsing error: {str(e)[:60]}")

    return reviews


def parse_indeed_html(html, max_reviews=10):
    """Parse Indeed reviews from HTML - simple format"""
    reviews = []

    try:
        soup = BeautifulSoup(html, "html.parser")

        # Try multiple selectors for Indeed reviews
        review_selectors = [
            '[data-tn-component="reviews"]',
            '[class*="review-item"]',
            '[class*="ReviewItem"]',
            'div[itemprop="review"]',
            '[class*="review"]',
        ]

        review_elements = []
        for selector in review_selectors:
            review_elements = soup.select(selector)
            if review_elements and len(review_elements) > 3:
                break

        if not review_elements:
            print("      ⚠️  No review elements found in HTML")
            return reviews

        print(f"      Found {len(review_elements[:max_reviews])} review elements")

        for idx, element in enumerate(review_elements[:max_reviews], 1):
            try:
                # Extract topic/title
                topic = ""
                topic_selectors = [
                    '[class*="review-title"]',
                    '[class*="ReviewTitle"]',
                    '[itemprop="name"]',
                    "h2",
                    "h3",
                    '[data-tn-component*="reviewTitle"]',
                ]
                for selector in topic_selectors:
                    topic_elem = element.select_one(selector)
                    if topic_elem:
                        topic = topic_elem.get_text(strip=True)
                        if topic and len(topic) > 3:
                            break

                # Extract review text
                text = ""
                text_selectors = [
                    '[itemprop="reviewBody"]',
                    '[class*="review-text"]',
                    '[class*="ReviewText"]',
                    "p",
                ]
                for selector in text_selectors:
                    text_elem = element.select_one(selector)
                    if text_elem:
                        text = text_elem.get_text(strip=True)
                        if text and len(text) > 50:
                            break

                if not text:
                    text = element.get_text(strip=True)

                # Extract rating
                rating = None
                rating_elem = element.select_one('[itemprop="ratingValue"]')
                if rating_elem:
                    try:
                        rating = float(
                            rating_elem.get("content", "") or rating_elem.get_text()
                        )
                    except:
                        pass

                # Only add if we have meaningful content
                if text and len(text) > 50:
                    reviews.append(
                        {
                            "topic": topic or None,
                            "text": text,
                            "rating": rating,
                            "platform": "indeed",
                            "scraped_at": datetime.now().isoformat(),
                            "method": "scraperapi",
                        }
                    )

            except Exception as e:
                print(f"      ⚠️  Error parsing review {idx}: {str(e)[:40]}")
                continue

        print(f"      ✓ Parsed {len(reviews)} reviews from HTML")

    except Exception as e:
        print(f"      ❌ HTML parsing error: {str(e)[:60]}")

    return reviews


def scrape_glassdoor_reviews(driver, url, max_reviews=10):
    """Scrape reviews from Glassdoor"""
    reviews = []

    try:
        driver.get(url)
        time.sleep(SCROLL_DELAY * 3)  # Wait for page load

        # Selectors for Glassdoor (may need updates as site changes)
        review_selectors = [
            "li[class*='Review']",
            "div[class*='Review']",
            "article[class*='review']",
            "[data-test='employer-review']",
        ]

        review_elements = []
        for selector in review_selectors:
            try:
                review_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if review_elements:
                    break
            except:
                continue

        if not review_elements:
            print("      ⚠️  No reviews found on page")
            return reviews

        print(f"      Found {len(review_elements[:max_reviews])} review elements")

        for idx, element in enumerate(review_elements[:max_reviews], 1):
            try:
                # Extract review title/topic
                topic = ""
                topic_selectors = [
                    "[class*='reviewTitle']",
                    "[class*='Summary']",
                    "h2",
                    "h3",
                ]

                for selector in topic_selectors:
                    try:
                        topic_elem = element.find_element(By.CSS_SELECTOR, selector)
                        topic = topic_elem.text.strip()
                        if topic and len(topic) > 3:
                            break
                    except:
                        continue

                # Try to extract review text
                review_text = ""
                text_selectors = [
                    "[class*='reviewText']",
                    "[class*='review-text']",
                    "[class*='description']",
                    "p",
                ]

                for selector in text_selectors:
                    try:
                        text_elem = element.find_element(By.CSS_SELECTOR, selector)
                        review_text = text_elem.text.strip()
                        if review_text:
                            break
                    except:
                        continue

                if not review_text:
                    review_text = element.text.strip()

                # Extract rating if available
                rating = None
                try:
                    rating_elem = element.find_element(
                        By.CSS_SELECTOR, "[class*='rating']"
                    )
                    rating_text = (
                        rating_elem.get_attribute("aria-label") or rating_elem.text
                    )
                    # Extract number from rating text
                    import re

                    rating_match = re.search(r"(\d+\.?\d*)", rating_text)
                    if rating_match:
                        rating = float(rating_match.group(1))
                except:
                    pass

                if review_text and len(review_text) > 50:  # Only meaningful reviews
                    reviews.append(
                        {
                            "topic": topic or None,
                            "text": review_text,
                            "rating": rating,
                            "platform": "glassdoor",
                            "scraped_at": datetime.now().isoformat(),
                        }
                    )
            except Exception as e:
                print(f"      ⚠️  Error extracting review {idx}: {str(e)[:50]}")
                continue

        print(f"      ✓ Extracted {len(reviews)} reviews")

    except TimeoutException:
        print("      ⚠️  Page load timeout")
    except Exception as e:
        print(f"      ⚠️  Error: {str(e)[:60]}")

    return reviews


def scrape_indeed_reviews(driver, url, max_reviews=10):
    """Scrape reviews from Indeed"""
    reviews = []

    try:
        driver.get(url)
        time.sleep(SCROLL_DELAY * 3)

        # Scroll to load more reviews
        for _ in range(3):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_DELAY)

        # Indeed review selectors
        review_selectors = [
            "[data-tn-component='reviews']",
            "[class*='review-item']",
            "[class*='ReviewItem']",
            "div[itemprop='review']",
        ]

        review_elements = []
        for selector in review_selectors:
            try:
                review_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if review_elements:
                    break
            except:
                continue

        if not review_elements:
            print("      ⚠️  No reviews found on page")
            return reviews

        print(f"      Found {len(review_elements[:max_reviews])} review elements")

        for idx, element in enumerate(review_elements[:max_reviews], 1):
            try:
                # Extract review title/topic
                topic = ""
                topic_selectors = [
                    "[class*='review-title']",
                    "[class*='ReviewTitle']",
                    "[itemprop='name']",
                    "h2",
                    "h3",
                ]

                for selector in topic_selectors:
                    try:
                        topic_elem = element.find_element(By.CSS_SELECTOR, selector)
                        topic = topic_elem.text.strip()
                        if topic and len(topic) > 3:
                            break
                    except:
                        continue

                review_text = ""

                # Try different text selectors
                text_selectors = [
                    "[itemprop='reviewBody']",
                    "[class*='review-text']",
                    "[class*='ReviewText']",
                    "p",
                ]

                for selector in text_selectors:
                    try:
                        text_elem = element.find_element(By.CSS_SELECTOR, selector)
                        review_text = text_elem.text.strip()
                        if review_text:
                            break
                    except:
                        continue

                if not review_text:
                    review_text = element.text.strip()

                # Extract rating
                rating = None
                try:
                    rating_elem = element.find_element(
                        By.CSS_SELECTOR, "[itemprop='ratingValue']"
                    )
                    rating = float(
                        rating_elem.get_attribute("content") or rating_elem.text
                    )
                except:
                    pass

                if review_text and len(review_text) > 50:
                    reviews.append(
                        {
                            "topic": topic or None,
                            "text": review_text,
                            "rating": rating,
                            "platform": "indeed",
                            "scraped_at": datetime.now().isoformat(),
                        }
                    )
            except Exception as e:
                print(f"      ⚠️  Error extracting review {idx}: {str(e)[:50]}")
                continue

        print(f"      ✓ Extracted {len(reviews)} reviews")

    except TimeoutException:
        print("      ⚠️  Page load timeout")
    except Exception as e:
        print(f"      ⚠️  Error: {str(e)[:60]}")

    return reviews


def scrape_comparably_reviews(driver, url, max_reviews=10):
    """Scrape reviews from Comparably"""
    reviews = []

    try:
        driver.get(url)
        time.sleep(SCROLL_DELAY * 3)

        # Scroll to load content
        for _ in range(3):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_DELAY)

        # Generic review selectors
        review_selectors = [
            "[class*='review']",
            "[class*='Review']",
            "article",
            "[class*='comment']",
        ]

        review_elements = []
        for selector in review_selectors:
            try:
                review_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if len(review_elements) > 3:  # Need meaningful amount
                    break
            except:
                continue

        if not review_elements:
            print("      ⚠️  No reviews found on page")
            return reviews

        print(f"      Found {len(review_elements[:max_reviews])} review elements")

        for idx, element in enumerate(review_elements[:max_reviews], 1):
            try:
                review_text = element.text.strip()

                if review_text and len(review_text) > 50:
                    reviews.append(
                        {
                            "text": review_text,
                            "rating": None,
                            "platform": "comparably",
                            "scraped_at": datetime.now().isoformat(),
                        }
                    )
            except Exception as e:
                continue

        print(f"      ✓ Extracted {len(reviews)} reviews")

    except Exception as e:
        print(f"      ⚠️  Error: {str(e)[:60]}")

    return reviews


def scrape_generic_reviews(driver, url, platform_name, max_reviews=10):
    """Generic scraper for other platforms"""
    reviews = []

    try:
        driver.get(url)
        time.sleep(SCROLL_DELAY * 3)

        # Scroll to load content
        for _ in range(2):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_DELAY)

        # Get all text content
        page_text = driver.find_element(By.TAG_NAME, "body").text

        if page_text and len(page_text) > 200:
            reviews.append(
                {
                    "text": page_text[:5000],  # Limit to first 5000 chars
                    "rating": None,
                    "platform": platform_name,
                    "scraped_at": datetime.now().isoformat(),
                    "note": "Generic page scrape",
                }
            )
            print(f"      ✓ Extracted page content ({len(page_text)} chars)")

    except Exception as e:
        print(f"      ⚠️  Error: {str(e)[:60]}")

    return reviews


def scrape_reviews_from_url(url, platform, max_reviews=10):
    """
    Scrape reviews from a URL
    - Uses ScraperAPI for Glassdoor and Indeed (bypasses Cloudflare)
    - Uses undetected Chrome for other platforms (free)

    Returns: (reviews, success, error_message)
    """
    if not url or url == "":
        return [], False, "Empty URL"

    platform_lower = platform.lower()

    # Use ScraperAPI for Glassdoor and Indeed if available
    if USE_SCRAPERAPI and platform_lower in SCRAPERAPI_PLATFORMS:
        print(f"      🔑 Using ScraperAPI for {platform}")
        try:
            html = scrape_with_scraperapi(url, render=True)

            if not html:
                return [], False, "ScraperAPI failed to fetch content"

            # Parse HTML based on platform
            if "glassdoor" in platform_lower:
                reviews = parse_glassdoor_html(html, max_reviews)
            elif "indeed" in platform_lower:
                reviews = parse_indeed_html(html, max_reviews)
            else:
                reviews = []

            if reviews:
                return reviews, True, None
            else:
                return [], False, "No reviews parsed from ScraperAPI response"

        except Exception as e:
            error_msg = f"ScraperAPI error: {str(e)[:60]}"
            print(f"      ❌ {error_msg}")
            return [], False, error_msg

    # Use regular Selenium for other platforms
    else:
        if not USE_SCRAPERAPI and platform_lower in SCRAPERAPI_PLATFORMS:
            print(f"      ⚠️  ScraperAPI not configured, skipping {platform}")
            return [], False, "ScraperAPI key not found (required for this platform)"

        print(f"      🌐 Using Selenium for {platform}")
        driver = init_browser()
        if not driver:
            return [], False, "Could not initialize browser"

        try:
            if "comparably" in platform_lower:
                reviews = scrape_comparably_reviews(driver, url, max_reviews)
            else:
                reviews = scrape_generic_reviews(driver, url, platform, max_reviews)

            driver.quit()

            if reviews:
                return reviews, True, None
            else:
                return [], False, "No reviews extracted"
        except Exception as e:
            try:
                driver.quit()
            except:
                pass
            return [], False, f"Error: {str(e)[:60]}"


def load_existing_data(output_path):
    """Load existing scraped reviews if file exists"""
    if Path(output_path).exists():
        try:
            with open(output_path, encoding="utf-8") as f:
                data = json.load(f)
                # Get set of already scraped company-platform combinations
                # Handle different field names for backwards compatibility
                scraped_keys = set()
                for item in data:
                    company = item.get("company_name") or item.get("name") or "Unknown"
                    platform = item.get("platform", "unknown")
                    scraped_keys.add(f"{company}_{platform}")
                return data, scraped_keys
        except Exception as e:
            print(f"⚠️  Could not load existing data: {e}")
            return [], set()
    return [], set()


def save_data(output_path, all_data):
    """Save data to JSON file"""
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_data, f, indent=2, ensure_ascii=False)


def save_failed_csv(failed_path, failed_items):
    """Save failed scraping attempts to CSV"""
    if not failed_items:
        return

    with open(failed_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["company_name", "platform", "url", "error", "timestamp"]
        )
        writer.writeheader()
        writer.writerows(failed_items)


def main():
    print("=" * 70)
    print("INDEED REVIEW SCRAPER - ScraperAPI (Multi-Page Scraping)")
    print("=" * 70)
    print()

    # Create output directory
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    # Check if input file exists
    if not Path(INPUT_JSON).exists():
        print(f"❌ ERROR: {INPUT_JSON} not found")
        print("   Please run 02_get_employee_reviews.py first")
        return

    # Load input data
    with open(INPUT_JSON, encoding="utf-8") as f:
        companies = json.load(f)

    print(f"✓ Loaded {len(companies)} companies from JSON")

    # Load existing data
    all_reviews, scraped_keys = load_existing_data(REVIEWS_OUTPUT)
    if scraped_keys:
        print(
            f"✓ Found {len(scraped_keys)} already scraped company-platform combinations"
        )

    print("✓ Using undetected-chromedriver (bypasses Cloudflare)")
    print(f"✓ Max reviews per company: {MAX_REVIEWS_PER_COMPANY}")
    print(
        f"✓ Max pages per company: {MAX_PAGES_PER_COMPANY} (reduced to avoid rate limits)"
    )
    print(
        f"✓ Rate limiting: {DELAY_BETWEEN_PAGES[0]}-{DELAY_BETWEEN_PAGES[1]}s between pages, {DELAY_BETWEEN_PLATFORMS[0]}-{DELAY_BETWEEN_PLATFORMS[1]}s between companies"
    )
    print(f"✓ Expected rate: ~3-5 requests/minute (safe for free tier)")
    print()

    # Stats
    success_count = 0
    failed_count = 0
    skipped_count = 0
    failed_items = []

    # Review platforms to scrape - ONLY Indeed (Glassdoor too unreliable with basic ScraperAPI)
    platforms = ["indeed"]

    if USE_SCRAPERAPI:
        print("✓ ScraperAPI enabled for Indeed")
        print("✓ Scraping from: Indeed (API) - Multiple pages per company")
        print("⚠️  Note: Glassdoor skipped (requires premium ScraperAPI features)")
    else:
        print("⚠️  ScraperAPI not configured!")
        print("   Add SCRAPERAPI_KEY to .env to enable Indeed scraping")
        print()
        print("   Quick setup:")
        print("   1. Sign up: https://www.scraperapi.com/signup")
        print("   2. Copy your API key")
        print("   3. Add to .env: SCRAPERAPI_KEY=your_actual_key")
        print()

    for idx, company in enumerate(companies, 1):
        company_name = company.get("company_name", "Unknown")
        print(f"[{idx}/{len(companies)}] {company_name}")

        company_had_success = False

        for platform in platforms:
            url_key = f"{platform}_url"
            base_url = company.get(url_key, "")

            if not base_url:
                continue

            # Check if already scraped
            scrape_key = f"{company_name}_{platform}"
            if scrape_key in scraped_keys:
                print(f"   {platform.capitalize()}: Already scraped (skipped)")
                skipped_count += 1
                continue

            print(f"   {platform.capitalize()}: Scraping multiple pages...")

            # Generate paginated URLs
            if "glassdoor" in platform.lower():
                page_urls = generate_glassdoor_page_urls(
                    base_url, MAX_PAGES_PER_COMPANY
                )
            elif "indeed" in platform.lower():
                page_urls = generate_indeed_page_urls(base_url, MAX_PAGES_PER_COMPANY)
            else:
                page_urls = [base_url]  # No pagination for other platforms

            # Scrape multiple pages
            platform_reviews = []
            pages_scraped = 0

            for page_num, url in enumerate(page_urls, 1):
                # Stop if we have enough reviews
                if len(platform_reviews) >= MAX_REVIEWS_PER_COMPANY:
                    print(f"      ✓ Reached {MAX_REVIEWS_PER_COMPANY} reviews limit")
                    break

                if page_num > 1:
                    print(f"      📄 Page {page_num}...")

                # Scrape this page
                reviews, success, error = scrape_reviews_from_url(
                    url, platform, MAX_REVIEWS_PER_COMPANY - len(platform_reviews)
                )

                if success and reviews:
                    platform_reviews.extend(reviews)
                    pages_scraped += 1
                    print(
                        f"      ✓ Page {page_num}: Got {len(reviews)} reviews (Total: {len(platform_reviews)})"
                    )
                elif page_num == 1:
                    # If first page fails, record as failed
                    failed_count += 1
                    failed_items.append(
                        {
                            "company_name": company_name,
                            "platform": platform,
                            "url": url,
                            "error": error or "Unknown error",
                            "timestamp": datetime.now().isoformat(),
                        }
                    )
                    save_failed_csv(Path(FAILED_FILE), failed_items)
                    break
                else:
                    # No more reviews on this page, stop pagination
                    if page_num > 1:
                        print(f"      ✓ No more reviews on page {page_num}, stopping")
                    break

                # Delay between page requests to avoid rate limiting
                if page_num < len(page_urls):
                    delay = random.uniform(*DELAY_BETWEEN_PAGES)
                    print(f"      ⏳ Waiting {delay:.1f}s before next page...")
                    time.sleep(delay)

            # Save all reviews from this platform
            if platform_reviews:
                # Add company context to each review
                for review in platform_reviews:
                    review["company_name"] = company_name
                    review["location"] = company.get("location", "")
                    review["url"] = base_url  # Use base URL

                all_reviews.extend(platform_reviews)
                success_count += 1
                company_had_success = True

                # Save after each platform
                save_data(REVIEWS_OUTPUT, all_reviews)
                print(
                    f"      💾 Saved {len(platform_reviews)} total reviews from {pages_scraped} pages"
                )

            # Delay between platforms/companies to avoid rate limiting
            delay = random.uniform(*DELAY_BETWEEN_PLATFORMS)
            print(f"   ⏳ Waiting {delay:.1f}s before next company...")
            time.sleep(delay)

        print()

    print("=" * 70)
    print("SUMMARY:")
    print(f"  Successful scrapes: {success_count}")
    print(f"  Failed scrapes: {failed_count}")
    print(f"  Skipped (already scraped): {skipped_count}")
    print(f"  Total reviews collected: {len(all_reviews)}")
    print()
    print(f"  ✓ Output JSON: {REVIEWS_OUTPUT}")
    if failed_count > 0:
        print(f"  ⚠️  Failed scrapes CSV: {FAILED_FILE}")
    print("=" * 70)


if __name__ == "__main__":
    main()
