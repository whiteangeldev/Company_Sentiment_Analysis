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
API_KEY_STATE_FILE = f"{OUTPUT_DIR}/api_key_state.json"  # Track which key is active


class APIKeyManager:
    """Manages multiple ScraperAPI keys and auto-rotates on 403 errors"""
    
    def __init__(self):
        self.api_keys = self._load_api_keys()
        self.current_key_index = 0
        self.failed_keys = set()
        self._load_state()
    
    def _load_api_keys(self):
        keys = []
        # Try loading SCRAPERAPI_KEY_1 through SCRAPERAPI_KEY_4
        for i in range(1, 5):
            key = os.getenv(f"SCRAPERAPI_KEY_{i}", "")
            if key and key.strip():
                keys.append(key.strip())
        
        # Fallback to single SCRAPERAPI_KEY if numbered keys not found
        if not keys:
            single_key = os.getenv("SCRAPERAPI_KEY", "")
            if single_key and single_key.strip():
                keys.append(single_key.strip())
        
        return keys
    
    def _load_state(self):
        try:
            if Path(API_KEY_STATE_FILE).exists():
                with open(API_KEY_STATE_FILE, 'r') as f:
                    state = json.load(f)
                    self.current_key_index = state.get('current_key_index', 0)
                    self.failed_keys = set(state.get('failed_keys', []))
        except Exception as e:
            pass  # Silent fail - will start fresh
    
    def _save_state(self):
        try:
            state = {
                'current_key_index': self.current_key_index,
                'failed_keys': list(self.failed_keys),
                'last_updated': datetime.now().isoformat()
            }
            with open(API_KEY_STATE_FILE, 'w') as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            pass  # Silent fail
    
    def get_current_key(self):
        if not self.api_keys:
            return None
        
        # Skip failed keys
        attempts = 0
        while attempts < len(self.api_keys):
            if self.current_key_index not in self.failed_keys:
                return self.api_keys[self.current_key_index]
            
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
            attempts += 1
        
        # All keys failed
        return None
    
    def rotate_key(self, reason="403_error"):
        if not self.api_keys or len(self.api_keys) == 1:
            return False
        
        # Mark current key as failed
        self.failed_keys.add(self.current_key_index)
        old_index = self.current_key_index
        
        # Try to find next available key
        for _ in range(len(self.api_keys)):
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
            if self.current_key_index not in self.failed_keys:
                print(f"      🔄 API Key Rotated: #{old_index + 1} → #{self.current_key_index + 1} (Reason: {reason})")
                self._save_state()
                return True
        
        # All keys failed
        print(f"      ❌ All {len(self.api_keys)} API keys exhausted!")
        return False
    
    def get_status(self):
        total_keys = len(self.api_keys)
        failed_count = len(self.failed_keys)
        active_count = total_keys - failed_count
        
        return {
            'total_keys': total_keys,
            'active_keys': active_count,
            'failed_keys': failed_count,
            'current_key': self.current_key_index + 1 if total_keys > 0 else 0
        }


# Initialize global API key manager
api_key_manager = APIKeyManager()
SCRAPERAPI_KEY = api_key_manager.get_current_key() or ""
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


def scrape_with_scraperapi(url, render=True, retry=0, max_retries=5, try_alternative_params=False):
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
        # Get current API key from manager (supports rotation)
        current_key = api_key_manager.get_current_key()
        if not current_key:
            print(f"      ❌ No API keys available")
            return None
        
        # Build params - try alternative params on retry if needed
        params = {
            "api_key": current_key,
            "url": url,
        }
        
        # Try different strategies on retry
        if try_alternative_params and retry >= 2:
            # Strategy 1: Try without render (faster, less resource-intensive)
            params["render"] = "false"
            print(f"      🔄 Trying without render parameter...")
        elif retry >= 3:
            # Strategy 2: Try with country code for international sites
            if "uk.indeed.com" in url:
                params["country_code"] = "uk"
                print(f"      🔄 Trying with UK country code...")
            params["render"] = "true"
        else:
            params["render"] = "true"

        api_url = "http://api.scraperapi.com"

        # Debug output on first try
        if retry == 0 and "glassdoor" in url:
            print(f"      🔍 URL: {url[:80]}...")

        # Add exponential backoff delay between retries (longer waits for 500 errors)
        if retry > 0:
            # Exponential backoff: 10s, 20s, 30s, 45s, 60s
            wait_time = min(10 * retry + (retry - 1) * 5, 60)
            print(
                f"      ⏳ Waiting {wait_time}s before retry {retry}/{max_retries}..."
            )
            time.sleep(wait_time)

        response = requests.get(api_url, params=params, timeout=120)  # Increased timeout

        if response.status_code == 200:
            print(f"      ✓ ScraperAPI success (status: {response.status_code})")
            # Add delay after successful API call to avoid rate limiting
            time.sleep(DELAY_AFTER_API_CALL)
            return response.text

        elif response.status_code == 500:
            # Enhanced 500 error handling
            if retry < max_retries:
                print(
                    f"      ⚠️  ScraperAPI 500 error - retrying ({retry + 1}/{max_retries})..."
                )
                
                # Try rotating API key on 500 errors (if multiple keys available)
                if retry >= 2 and len(api_key_manager.api_keys) > 1:
                    if api_key_manager.rotate_key(reason="500_server_error"):
                        print(f"      🔄 Rotated API key due to persistent 500 errors")
                        time.sleep(5)  # Brief pause after key rotation
                
                # Try alternative params after a few retries
                use_alt_params = retry >= 2
                return scrape_with_scraperapi(url, render, retry + 1, max_retries, use_alt_params)
            else:
                print(f"      ❌ ScraperAPI 500 error - max retries ({max_retries}) exceeded")
                return None

        elif response.status_code == 400:
            print(f"      ⚠️  ScraperAPI 400: Bad Request - URL might be malformed")
            print(f"      URL: {url[:100]}...")
            # Don't retry 400s - they won't succeed
            return None

        elif response.status_code == 403:
            print(f"      ⚠️  ScraperAPI 403: API key credits exhausted")
            
            # Try to rotate to next API key and retry
            if api_key_manager.rotate_key(reason="403_credits_exhausted") and retry < max_retries:
                print(f"      🔄 Retrying with new API key...")
                time.sleep(3)  # Brief pause before retry
                return scrape_with_scraperapi(url, render, retry + 1, max_retries)
            
            return None

        elif response.status_code == 404:
            print(f"      ⚠️  ScraperAPI 404: Target URL not found")
            # Try to fix common URL issues before giving up
            if retry == 0:
                # Try fixing URL if it's missing /reviews
                if "indeed.com/cmp/" in url and not url.endswith("/reviews") and "/reviews" not in url:
                    fixed_url = url.rstrip("/") + "/reviews"
                    print(f"      🔄 Trying fixed URL (added /reviews)...")
                    time.sleep(2)
                    # Recursively try with fixed URL (but don't increment retry to avoid double counting)
                    fixed_result = scrape_with_scraperapi(fixed_url, render, 0, max_retries, try_alternative_params)
                    if fixed_result and fixed_result != "NO_MORE_PAGES":
                        return fixed_result
            # Return special value - caller will decide if it's an error (first page) or expected (subsequent pages)
            return "NO_MORE_PAGES"

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


def clean_review_text(text):
    import re
    
    if not text:
        return text
    
    # List of truncation indicators to remove
    truncation_phrases = [
        'Show more',
        'Read more', 
        'Show full review',
        'Read full review',
        'See more',
        'View more',
        'Continue reading',
        'Expand review',
    ]
    
    # Remove truncation indicators (case-insensitive)
    cleaned = text
    for phrase in truncation_phrases:
        # Remove the phrase and surrounding whitespace/punctuation
        pattern = re.compile(r'\s*' + re.escape(phrase) + r'\s*\.{0,3}\s*', re.IGNORECASE)
        cleaned = pattern.sub(' ', cleaned)
    
    # Clean up excessive whitespace
    cleaned = ' '.join(cleaned.split())
    
    # Remove trailing ellipsis if text is truncated
    cleaned = re.sub(r'\s*\.{2,}\s*$', '', cleaned)
    
    # Remove "..." in the middle if followed by limited text (likely truncation)
    cleaned = re.sub(r'\.\.\.\s*$', '', cleaned)
    
    return cleaned.strip()


def parse_indeed_html(html, max_reviews=10):
    reviews = []

    try:
        soup = BeautifulSoup(html, "html.parser")

        # Try multiple selectors for Indeed reviews (Updated 2024/2025)
        review_selectors = [
            # Modern Indeed selectors (2024/2025)
            '[data-testid="review-card"]',
            '[data-testid="review"]',
            '[id*="cmp-review-"]',
            'div[class*="css-"][id*="review"]',  # Indeed uses CSS-in-JS
            
            # Legacy selectors (fallback)
            '[data-tn-component="reviews"]',
            '[class*="review-item"]',
            '[class*="ReviewItem"]',
            'div[itemprop="review"]',
            '[class*="review"]',
        ]

        review_elements = []
        matched_selector = None
        for selector in review_selectors:
            review_elements = soup.select(selector)
            # FIXED: Accept ANY reviews found (not just >3)
            if review_elements and len(review_elements) >= 1:
                matched_selector = selector
                break

        if not review_elements:
            print("      ⚠️  No review elements found in HTML")
            return reviews

        print(f"      Found {len(review_elements)} review elements (using selector: {matched_selector[:40]}...)")

        for idx, element in enumerate(review_elements[:max_reviews], 1):
            try:
                # Extract topic/title
                topic = ""
                topic_selectors = [
                    '[data-testid="review-title"]',
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

                # Extract review text - with expanded content support
                text = ""
                
                # Strategy 1: Look for expanded/full content in hidden elements
                full_text_selectors = [
                    '[class*="expanded"]',  # Expanded content
                    '[class*="full-text"]',  # Full text container
                    '[class*="full-review"]',  # Full review
                    '[style*="display:none"]',  # Hidden content
                    '[class*="collapsed"]',  # Collapsed content
                ]
                
                for selector in full_text_selectors:
                    full_elem = element.select_one(selector)
                    if full_elem:
                        potential_text = full_elem.get_text(separator=' ', strip=True)
                        if potential_text and len(potential_text) > len(text):
                            text = potential_text
                            break
                
                # Strategy 2: Standard text extraction
                if not text or len(text) < 50:  # If we don't have good text yet
                    text_selectors = [
                        '[data-testid="review-text"]',
                        '[itemprop="reviewBody"]',
                        '[class*="review-text"]',
                        '[class*="ReviewText"]',
                        '[class*="reviewText"]',
                        "p",
                        "span",
                    ]
                    for selector in text_selectors:
                        text_elem = element.select_one(selector)
                        if text_elem:
                            # Try to get full text from all child elements (including hidden ones)
                            potential_text = text_elem.get_text(separator=' ', strip=True)
                            if potential_text and len(potential_text) > len(text):
                                text = potential_text
                            # FIXED: Reduced from 50 to 20 characters
                            if text and len(text) > 20:
                                break

                # Strategy 3: Fallback to all text in element
                if not text:
                    text = element.get_text(separator=' ', strip=True)
                
                # POST-PROCESSING: Remove "Show more" artifacts
                if text:
                    text = clean_review_text(text)

                # Extract rating
                rating = None
                rating_selectors = [
                    '[itemprop="ratingValue"]',
                    '[data-testid="rating"]',
                    '[class*="rating"]',
                ]
                for selector in rating_selectors:
                    rating_elem = element.select_one(selector)
                    if rating_elem:
                        try:
                            rating = float(
                                rating_elem.get("content", "") 
                                or rating_elem.get("aria-label", "").split()[0]
                                or rating_elem.get_text()
                            )
                            break
                        except:
                            pass

                # FIXED: Reduced minimum text length from 50 to 20
                if text and len(text) > 20:
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


def parse_indeed_html_fallback(html, max_reviews=10, company_name="unknown"):
    reviews = []
    seen_texts = set()  # Track duplicates
    
    try:
        soup = BeautifulSoup(html, "html.parser")
        
        # Look for divs/sections that might contain reviews
        # Reviews typically have certain patterns: rating + text + date
        all_containers = soup.find_all(['div', 'article', 'section', 'li'])
        
        for container in all_containers[:max_reviews * 5]:  # Check more containers
            try:
                text = container.get_text(separator=' ', strip=True)
                
                # Skip if we've seen this text before (avoid duplicates)
                text_signature = text[:100].lower()
                if text_signature in seen_texts:
                    continue
                
                # Check if it looks like a review:
                # - Reasonable length (between 30 and 2000 chars)
                # - Contains common review keywords
                review_keywords = [
                    'work', 'company', 'job', 'management', 'employee', 
                    'culture', 'team', 'salary', 'benefit', 'environment',
                    'position', 'manager', 'experience', 'staff', 'coworker',
                    'colleague', 'workplace', 'supervisor', 'boss', 'pay',
                    'overtime', 'shift', 'schedule', 'hour', 'training',
                    'promotion', 'career', 'hired', 'interview', 'quit'
                ]
                
                if 30 <= len(text) <= 2000:
                    # Check if text contains review-like language
                    text_lower = text.lower()
                    keyword_matches = sum(1 for kw in review_keywords if kw in text_lower)
                    
                    # If we find multiple review keywords, it's likely a review
                    # Lowered threshold from 2 to 1 for broader matching
                    if keyword_matches >= 1:
                        # Try to extract a title/topic from headers within this container
                        topic = None
                        for header in container.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b']):
                            header_text = header.get_text(strip=True)
                            if 3 < len(header_text) < 100:
                                topic = header_text
                                break
                        
                        seen_texts.add(text_signature)
                        
                        # Clean the text to remove "Show more..." artifacts
                        cleaned_text = clean_review_text(text)
                        
                        reviews.append({
                            "topic": topic,
                            "text": cleaned_text,
                            "rating": None,
                            "platform": "indeed",
                            "scraped_at": datetime.now().isoformat(),
                            "method": "scraperapi_fallback",
                        })
                        
                        if len(reviews) >= max_reviews:
                            break
                            
            except Exception as e:
                continue
        
        if reviews:
            print(f"      🔄 Fallback parser found {len(reviews)} potential reviews")
        
    except Exception as e:
        print(f"      ⚠️  Fallback parser error: {str(e)[:60]}")
    
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


def scrape_reviews_from_url(url, platform, max_reviews=10, company_name="unknown"):
    """
    Scrape reviews from a URL
    - Uses ScraperAPI for Glassdoor and Indeed (bypasses Cloudflare)
    - Uses undetected Chrome for other platforms (free)
    - Includes fallback parser for difficult pages

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

            # Handle special "NO_MORE_PAGES" return value for expected 404s
            if html == "NO_MORE_PAGES":
                return [], False, "No more pages (404)"  # Expected for subsequent pages
            
            if not html:
                return [], False, "ScraperAPI failed to fetch content"

            # Parse HTML based on platform
            if "glassdoor" in platform_lower:
                reviews = parse_glassdoor_html(html, max_reviews)
            elif "indeed" in platform_lower:
                reviews = parse_indeed_html(html, max_reviews)
            else:
                reviews = []

            # NEW: Try fallback parser if primary parser found nothing
            if not reviews and "indeed" in platform_lower:
                print(f"      🔄 Primary parser found no reviews, trying fallback parser...")
                reviews = parse_indeed_html_fallback(html, max_reviews, company_name)
            
            if reviews:
                return reviews, True, None
            else:
                # Save HTML for debugging if no reviews found
                try:
                    debug_dir = Path("data/raw_reviews/debug_html")
                    debug_dir.mkdir(exist_ok=True)
                    debug_file = debug_dir / f"{company_name.replace(' ', '_')[:50]}.html"
                    
                    # Add diagnostic information at the top of the HTML
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(html, "html.parser")
                    
                    # Gather diagnostics
                    diagnostics = f"""
<!-- DEBUG DIAGNOSTICS FOR: {company_name} -->
<!-- Scraped at: {datetime.now().isoformat()} -->
<!-- URL: {url} -->
<!-- HTML Length: {len(html)} bytes -->
<!-- Total divs: {len(soup.find_all('div'))} -->
<!-- Total articles: {len(soup.find_all('article'))} -->
<!-- Total sections: {len(soup.find_all('section'))} -->
<!-- Contains 'review': {str(html.lower().count('review'))} times -->
<!-- Contains 'employee': {str(html.lower().count('employee'))} times -->
<!-- Contains 'rating': {str(html.lower().count('rating'))} times -->
<!-- Page title: {soup.title.string if soup.title else 'No title'} -->
-->

"""
                    
                    with open(debug_file, "w", encoding="utf-8") as f:
                        f.write(diagnostics)
                        f.write(html[:100000])  # Save first 100KB
                    print(f"      💾 Saved HTML to {debug_file.name} for debugging")
                    print(f"      📊 HTML stats: {len(html)} bytes, {len(soup.find_all('div'))} divs, 'review' appears {html.lower().count('review')} times")
                except Exception as e:
                    print(f"      ⚠️  Could not save debug HTML: {str(e)[:40]}")
                
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
    
    # Display API key status
    if USE_SCRAPERAPI:
        status = api_key_manager.get_status()
        print(f"✓ ScraperAPI Keys: {status['total_keys']} total, {status['active_keys']} active, using key #{status['current_key']}")
        if status['failed_keys'] > 0:
            print(f"   ⚠️  {status['failed_keys']} key(s) already exhausted")
    else:
        print("⚠️  ScraperAPI not configured - Limited scraping ability")

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
                    url, platform, MAX_REVIEWS_PER_COMPANY - len(platform_reviews), company_name
                )

                if success and reviews:
                    platform_reviews.extend(reviews)
                    pages_scraped += 1
                    print(
                        f"      ✓ Page {page_num}: Got {len(reviews)} reviews (Total: {len(platform_reviews)})"
                    )
                elif page_num == 1:
                    # If first page fails, record as failed (unless it's expected 404)
                    if error and "No more pages (404)" in error:
                        # First page 404 means URL is invalid
                        failed_count += 1
                        failed_items.append(
                            {
                                "company_name": company_name,
                                "platform": platform,
                                "url": url,
                                "error": "Page not found (404) - URL may be invalid",
                                "timestamp": datetime.now().isoformat(),
                            }
                        )
                    else:
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
                    # Check if it's an expected 404 (no more pages) or other error
                    if error and "No more pages (404)" in error:
                        print(f"      ✓ No more pages (404), stopping pagination")
                    else:
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
