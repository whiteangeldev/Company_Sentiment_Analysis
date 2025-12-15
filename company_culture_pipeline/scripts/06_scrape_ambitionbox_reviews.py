#!/usr/bin/env python3
"""
Script to scrape employee reviews from AmbitionBox platform only

Features:
- AmbitionBox-only scraping
- Bypasses Cloudflare using ScraperAPI
- Extracts topic, text, and ratings
- Uses ScraperAPI free tier
- Smart rate limiting with delays between requests
- Exponential backoff on errors

Input: reviews_link.json (with company_id, company_name, location, and ambition_url)
Output: scraped_reviews_ambitionbox.json (with company_id, company_name, location, url, platform, topic, text, rating)

Rate Limiting Protection:
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
INPUT_JSON = "data/raw_reviews/reviews_link.json"
OUTPUT_DIR = "data/raw_reviews"
REVIEWS_OUTPUT = f"{OUTPUT_DIR}/scraped_reviews_ambitionbox.json"
PROGRESS_FILE = f"{OUTPUT_DIR}/scraping_progress_ambitionbox.json"
FAILED_FILE = f"{OUTPUT_DIR}/failed_reviews_ambitionbox.csv"

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
    """Manages multiple ScraperAPI keys and auto-rotates only on 403 errors (credits exhausted)"""
    
    def __init__(self):
        self.api_keys = self._load_api_keys()
        self.current_key_index = 0  # Always start with key 1 (index 0)
        self.failed_keys = set()  # Track keys that are fully exhausted (403 errors)
        self._load_state()
        # Ensure we start with key 1 if it's available
        if self.api_keys and 0 not in self.failed_keys:
            self.current_key_index = 0
    
    def _load_api_keys(self):
        keys = []
        # Try loading SCRAPERAPI_KEY_1 through SCRAPERAPI_KEY_4
        # Key 1 (index 0) is the primary key
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
                    # Only load failed keys, but always start with key 1 if available
                    self.failed_keys = set(state.get('failed_keys', []))
                    # If key 1 is not failed, use it; otherwise use saved index
                    if 0 not in self.failed_keys and len(self.api_keys) > 0:
                        self.current_key_index = 0
                    else:
                        self.current_key_index = state.get('current_key_index', 0)
        except Exception as e:
            pass  # Silent fail - will start fresh with key 1
    
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
        """Get the current active API key"""
        if not self.api_keys:
            return None
        
        # If current key is not failed, return it
        if self.current_key_index not in self.failed_keys:
            return self.api_keys[self.current_key_index]
        
        # Current key is failed, find next available key
        attempts = 0
        while attempts < len(self.api_keys):
            if self.current_key_index not in self.failed_keys:
                return self.api_keys[self.current_key_index]
            
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
            attempts += 1
        
        # All keys failed
        return None
    
    def rotate_key(self, reason="403_credits_exhausted"):
        """
        Rotate to next API key only when current key is fully exhausted (403 error)
        Only call this when you get a 403 error indicating credits are exhausted
        """
        if not self.api_keys or len(self.api_keys) == 1:
            return False
        
        # Mark current key as failed/exhausted
        old_index = self.current_key_index
        self.failed_keys.add(self.current_key_index)
        
        # Find next available key
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
# Always starts with SCRAPERAPI_KEY_1 (index 0)
# Only rotates to next key when current key gets 403 error (credits exhausted)
api_key_manager = APIKeyManager()
SCRAPERAPI_KEY = api_key_manager.get_current_key() or ""
USE_SCRAPERAPI = len(SCRAPERAPI_KEY) > 0
SCRAPERAPI_PLATFORMS = ["ambitionbox"]  # Use ScraperAPI for AmbitionBox only

# User agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/121.0",
]


def generate_ambitionbox_page_urls(base_url, max_pages=5):
    """
    Generate paginated AmbitionBox URLs
    Example: Page 1: /reviews/company-reviews, Page 2: /reviews/company-reviews?page=2
    """
    urls = []
    
    # Remove existing page parameter if present
    if '?page=' in base_url:
        base_url = base_url.split('?page=')[0]
    elif '&page=' in base_url:
        base_url = base_url.split('&page=')[0]
    
    for page in range(1, max_pages + 1):
        if page == 1:
            urls.append(base_url)
        else:
            separator = "&" if "?" in base_url else "?"
            url = f"{base_url}{separator}page={page}"
            urls.append(url)
    
    return urls


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


def scrape_with_scraperapi(url, render=True, retry=0, max_retries=5, try_alternative_params=False):
    if not SCRAPERAPI_KEY:
        print("      ⚠️  ScraperAPI key not found in environment")
        return None

    try:
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
            # 500 errors are server-side issues on ScraperAPI's end, NOT API key problems
            # Common causes:
            # - ScraperAPI server overload/temporary issues
            # - Target site (Indeed) blocking ScraperAPI requests
            # - Rate limiting on ScraperAPI infrastructure
            # Solution: Wait longer and retry with same key, or try different parameters
            if retry < max_retries:
                print(
                    f"      ⚠️  ScraperAPI 500 error (server-side issue) - retrying ({retry + 1}/{max_retries})..."
                )
                print(f"      💡 Note: 500 errors indicate ScraperAPI server issues, not API key problems")
                
                # Try alternative params after a few retries (render=false is faster, less resource-intensive)
                use_alt_params = retry >= 2
                
                # Longer exponential backoff for 500 errors (server needs time to recover)
                # Wait: 15s, 30s, 60s, 90s, 120s
                wait_time = min(15 * (2 ** retry), 120)
                print(f"      ⏳ Waiting {wait_time}s before retry (server recovery time)...")
                time.sleep(wait_time)
                
                return scrape_with_scraperapi(url, render, retry + 1, max_retries, use_alt_params)
            else:
                print(f"      ❌ ScraperAPI 500 error - max retries ({max_retries}) exceeded")
                print(f"      💡 Possible causes: ScraperAPI server issues, Indeed blocking, or rate limiting")
                return None

        elif response.status_code == 400:
            print(f"      ⚠️  ScraperAPI 400: Bad Request - URL might be malformed")
            print(f"      URL: {url[:100]}...")
            # Don't retry 400s - they won't succeed
            return None

        elif response.status_code == 403:
            # 403 = Forbidden = API key credits exhausted
            # This is the ONLY case where we rotate to the next key
            print(f"      ⚠️  ScraperAPI 403: API key credits exhausted")
            
            # Rotate to next API key only when current key is fully exhausted
            if api_key_manager.rotate_key(reason="403_credits_exhausted") and retry < max_retries:
                print(f"      🔄 Retrying with new API key...")
                time.sleep(3)  # Brief pause before retry
                return scrape_with_scraperapi(url, render, retry + 1, max_retries)
            
            return None

        elif response.status_code == 404:
            print(f"      ⚠️  ScraperAPI 404: Target URL not found")
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


def clean_review_text(text):
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


def parse_ambitionbox_html(html, max_reviews=10):
    """
    Parse AmbitionBox reviews from HTML
    Focuses on <div id="reviews-section"> which contains all actual reviews
    """
    reviews = []
    
    try:
        soup = BeautifulSoup(html, "html.parser")
        
        # Find the reviews-section div
        reviews_section = soup.find('div', id='reviews-section')
        if not reviews_section:
            print("      ⚠️  reviews-section not found in HTML")
            return reviews
        
        # Find all review elements using schema.org/Review pattern
        review_elements = reviews_section.find_all('span', {'itemtype': 'https://schema.org/Review'})
        
        if not review_elements:
            print("      ⚠️  No review elements found in reviews-section")
            return reviews
        
        print(f"      Found {len(review_elements)} review elements in reviews-section")
        
        # Extract data from each review
        for idx, review_elem in enumerate(review_elements[:max_reviews], 1):
            try:
                # Get review ID to find the corresponding div with visible content
                review_id = review_elem.get('id', '')
                
                # Find the div with the same ID (contains visible review content)
                review_div = None
                if review_id:
                    review_div = reviews_section.find('div', id=review_id)
                
                # Use review_div if found, otherwise use review_elem
                container = review_div if review_div else review_elem
                
                # Extract topic from h2 with itemprop="name"
                topic = None
                title_elem = container.find('h2', {'itemprop': 'name'})
                if not title_elem:
                    title_elem = review_elem.find('h2', {'itemprop': 'name'})
                if title_elem:
                    topic = title_elem.get_text(strip=True)
                
                # Extract rating from meta tag (in review_elem, not container)
                rating = None
                rating_meta = review_elem.find('meta', {'itemprop': 'ratingValue'})
                if rating_meta:
                    try:
                        rating = float(rating_meta.get('content', ''))
                    except:
                        pass
                
                # Extract date - prefer meta tag, fallback to span
                date = None
                date_meta = review_elem.find('meta', {'itemprop': 'datePublished'})
                if date_meta:
                    date_text = date_meta.get('content', '').strip()
                    if date_text and re.match(r'\d{4}-\d{2}-\d{2}', date_text):
                        try:
                            from datetime import datetime
                            dt = datetime.strptime(date_text, '%Y-%m-%d')
                            date = dt.strftime('%d %b %Y')
                        except:
                            date = date_text
                
                # Fallback: look for date span with "updated on" in container
                if not date:
                    all_spans = container.find_all('span')
                    for span in all_spans:
                        classes = span.get('class', [])
                        class_str = ' '.join(classes) if isinstance(classes, list) else str(classes) if classes else ''
                        if 'text-secondary-text' in class_str:
                            span_text = span.get_text(strip=True)
                            if 'updated on' in span_text.lower():
                                date_match = re.search(r'updated\s+on\s+(.+)', span_text, re.IGNORECASE)
                                if date_match:
                                    date = re.sub(r'<!--\s*-->', '', date_match.group(1)).strip()
                                    break
                
                # Extract text: combine Likes and Dislikes from container
                text_parts = []
                
                # Find Likes section - look for h3 with text "Likes"
                likes_h3 = None
                for h3 in container.find_all('h3'):
                    h3_text = h3.get_text(strip=True)
                    if h3_text == 'Likes':
                        likes_h3 = h3
                        break
                
                if likes_h3:
                    likes_p = likes_h3.find_next_sibling('p')
                    if likes_p:
                        likes_text = likes_p.get_text(strip=True)
                        if likes_text and len(likes_text) > 3:
                            text_parts.append(f"Likes: {likes_text}")
                
                # Find Dislikes section - look for h3 with text "Dislikes"
                dislikes_h3 = None
                for h3 in container.find_all('h3'):
                    h3_text = h3.get_text(strip=True)
                    if h3_text == 'Dislikes':
                        dislikes_h3 = h3
                        break
                
                if dislikes_h3:
                    dislikes_p = dislikes_h3.find_next_sibling('p')
                    if dislikes_p:
                        dislikes_text = dislikes_p.get_text(strip=True)
                        if dislikes_text and len(dislikes_text) > 3:
                            text_parts.append(f"Dislikes: {dislikes_text}")
                
                # Combine text
                text = ' '.join(text_parts) if text_parts else None
                
                # Clean up text
                if text:
                    text = clean_review_text(text)
                
                # Only add if we have meaningful text
                if text and len(text) > 20:
                    reviews.append({
                        "topic": topic,
                        "text": text,
                        "rating": rating,
                        "date": date,
                    })
            
            except Exception as e:
                print(f"      ⚠️  Error parsing review {idx}: {str(e)[:40]}")
                continue
        
        print(f"      ✓ Parsed {len(reviews)} reviews from reviews-section")
    
    except Exception as e:
        print(f"      ❌ HTML parsing error: {str(e)[:60]}")
    
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
                    "topic": None,
                    "text": page_text[:5000],  # Limit to first 5000 chars
                    "rating": None,
                }
            )
            print(f"      ✓ Extracted page content ({len(page_text)} chars)")

    except Exception as e:
        print(f"      ⚠️  Error: {str(e)[:60]}")

    return reviews


def scrape_reviews_from_url(url, platform, max_reviews=10, company_name="unknown"):
    """
    Scrape reviews from an AmbitionBox URL
    - Uses ScraperAPI for AmbitionBox if available
    - Falls back to Selenium if ScraperAPI is not configured

    Returns: (reviews, success, error_message)
    """
    if not url or url == "":
        return [], False, "Empty URL"

    platform_lower = platform.lower()
    if "ambitionbox" not in platform_lower and "ambition" not in platform_lower:
        return [], False, f"Platform {platform} not supported (AmbitionBox only)"

    # Use ScraperAPI for AmbitionBox if available
    if USE_SCRAPERAPI and platform_lower in SCRAPERAPI_PLATFORMS:
        print(f"      🔑 Using ScraperAPI for {platform}")
        try:
            html = scrape_with_scraperapi(url, render=True)

            if html == "NO_MORE_PAGES":
                return [], False, "No more pages (404)"
            
            if not html:
                return [], False, "ScraperAPI failed to fetch content"

            # Parse HTML for AmbitionBox
            reviews = parse_ambitionbox_html(html, max_reviews)
            
            if reviews:
                return reviews, True, None
            else:
                # Save HTML for debugging if no reviews found
                try:
                    debug_dir = Path("data/raw_reviews/debug_html")
                    debug_dir.mkdir(exist_ok=True)
                    debug_file = debug_dir / f"{company_name.replace(' ', '_')[:50]}_{platform}.html"
                    
                    # Add diagnostic information at the top of the HTML
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(html, "html.parser")
                    
                    # Gather diagnostics
                    diagnostics = f"""
<!-- DEBUG DIAGNOSTICS FOR: {company_name} -->
<!-- Platform: {platform} -->
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

    # Fallback to Selenium if ScraperAPI is not configured
    else:
        if not USE_SCRAPERAPI:
            print(f"      ⚠️  ScraperAPI not configured, using Selenium for AmbitionBox")

        print(f"      🌐 Using Selenium for AmbitionBox")
        driver = init_browser()
        if not driver:
            return [], False, "Could not initialize browser"

        try:
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
                # Use company_id if available, otherwise fallback to company_name
                scraped_keys = set()
                # Map company_id -> list of reviews (to check for missing dates)
                reviews_by_company = {}
                for item in data:
                    company_id = item.get("company_id")
                    company_name = item.get("company_name", "Unknown")
                    platform = item.get("platform", "unknown")
                    if company_id:
                        key = f"{company_id}_{platform}"
                        scraped_keys.add(key)
                        if key not in reviews_by_company:
                            reviews_by_company[key] = []
                        reviews_by_company[key].append(item)
                    else:
                        key = f"{company_name}_{platform}"
                        scraped_keys.add(key)
                        if key not in reviews_by_company:
                            reviews_by_company[key] = []
                        reviews_by_company[key].append(item)
                return data, scraped_keys, reviews_by_company
        except Exception as e:
            print(f"⚠️  Could not load existing data: {e}")
            return [], set(), {}
    return [], set(), {}


def has_missing_dates(reviews):
    """Check if any reviews in the list are missing the date field"""
    if not reviews:
        return True
    for review in reviews:
        if "date" not in review or review.get("date") is None:
            return True
    return False


def merge_reviews_with_dates(existing_reviews, new_reviews, company_id, platform):
    """
    Merge new reviews with existing reviews, matching by text and updating dates.
    Returns updated list of reviews with dates added to existing ones.
    """
    # Create a map of existing reviews by text signature (first 100 chars)
    existing_map = {}
    for review in existing_reviews:
        if review.get("company_id") == company_id and review.get("platform") == platform:
            text = review.get("text", "")
            if text:
                # Use first 100 chars as signature for matching
                signature = text[:100].lower().strip()
                existing_map[signature] = review
    
    # Update existing reviews with dates from new reviews
    updated_count = 0
    for new_review in new_reviews:
        text = new_review.get("text", "")
        if text:
            signature = text[:100].lower().strip()
            if signature in existing_map:
                existing_review = existing_map[signature]
                # Update date if it's missing in existing review
                if ("date" not in existing_review or existing_review.get("date") is None) and new_review.get("date"):
                    existing_review["date"] = new_review.get("date")
                    updated_count += 1
    
    return updated_count


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
    print("EMPLOYEE REVIEW SCRAPER - AmbitionBox Platform Only")
    print("=" * 70)
    print()

    # Create output directory
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    # Check if input file exists
    if not Path(INPUT_JSON).exists():
        print(f"❌ ERROR: {INPUT_JSON} not found")
        print("   Please ensure reviews_link.json exists")
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
        print("⚠️  ScraperAPI not configured - Indeed scraping will be limited")

    # Load existing data
    all_reviews, scraped_keys, reviews_by_company = load_existing_data(REVIEWS_OUTPUT)
    if scraped_keys:
        print(
            f"✓ Found {len(scraped_keys)} already scraped company-platform combinations"
        )
        # Check how many have missing dates
        missing_dates_count = 0
        for key in scraped_keys:
            if key in reviews_by_company and has_missing_dates(reviews_by_company[key]):
                missing_dates_count += 1
        if missing_dates_count > 0:
            print(f"   📅 {missing_dates_count} company(ies) have reviews missing date field - will re-scrape to add dates")

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

    # Review platform to scrape - AmbitionBox only
    platform = "ambitionbox"
    url_key = "ambition_url"

    if USE_SCRAPERAPI:
        print("✓ ScraperAPI enabled for AmbitionBox")
        print("✓ Scraping from: AmbitionBox (via ScraperAPI)")
    else:
        print("⚠️  ScraperAPI not configured!")
        print("   Add SCRAPERAPI_KEY to .env to enable fast scraping with API")
        print("   Will fall back to Selenium (slower)")
        print()

    for idx, company in enumerate(companies, 1):
        company_id = company.get("company_id", idx)
        company_name = company.get("company_name", "Unknown")
        location = company.get("location", "")
        print(f"[{idx}/{len(companies)}] {company_name} (ID: {company_id})")

        base_url = company.get(url_key, "")

        if not base_url or base_url.strip() == "":
            print(f"   AmbitionBox: No URL provided (skipped)")
            continue

        # Check if already scraped
        scrape_key = f"{company_id}_{platform}"
        needs_rescrape = False
        if scrape_key in scraped_keys:
            # Check if reviews are missing dates
            existing_reviews = reviews_by_company.get(scrape_key, [])
            if has_missing_dates(existing_reviews):
                print(f"   AmbitionBox: Already scraped but missing dates - re-scraping to add dates...")
                needs_rescrape = True
            else:
                print(f"   AmbitionBox: Already scraped with dates (skipped)")
                skipped_count += 1
                continue
        else:
            print(f"   AmbitionBox: Scraping...")

        # Generate paginated URLs for AmbitionBox
        page_urls = generate_ambitionbox_page_urls(base_url, MAX_PAGES_PER_COMPANY)

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
            # Add company context to each review with required fields
            for review in platform_reviews:
                # Ensure all required fields are present
                review["company_id"] = company_id
                review["company_name"] = company_name
                review["location"] = location
                review["url"] = base_url  # Use base URL
                review["platform"] = platform
                # Ensure topic, text, rating, date exist (may be None)
                if "topic" not in review:
                    review["topic"] = None
                if "text" not in review:
                    review["text"] = review.get("review_text", "")
                if "rating" not in review:
                    review["rating"] = None
                if "date" not in review:
                    review["date"] = None
                # Remove any extra fields not in the required list
                allowed_fields = ["company_id", "company_name", "location", "url", "platform", "topic", "text", "rating", "date"]
                review_copy = {k: v for k, v in review.items() if k in allowed_fields}
                review.clear()
                review.update(review_copy)

            # If this is a re-scrape to add dates, merge dates with existing reviews
            if needs_rescrape:
                existing_reviews = reviews_by_company.get(scrape_key, [])
                updated_count = merge_reviews_with_dates(existing_reviews, platform_reviews, company_id, platform)
                if updated_count > 0:
                    print(f"      📅 Updated {updated_count} existing review(s) with date field")
                else:
                    print(f"      ⚠️  Could not match new reviews with existing ones to update dates")
                # Don't add new reviews, just update existing ones in all_reviews
                # The existing reviews are already in all_reviews, we just updated them
            else:
                # New scrape - add all reviews
                all_reviews.extend(platform_reviews)
            
            success_count += 1

            # Save after scraping
            save_data(REVIEWS_OUTPUT, all_reviews)
            if needs_rescrape:
                print(f"      💾 Updated existing reviews with dates")
            else:
                print(
                    f"      💾 Saved {len(platform_reviews)} total reviews from {pages_scraped} pages"
                )
        elif needs_rescrape:
            # Re-scrape found no reviews - still save to ensure file is updated
            print(f"      ⚠️  No reviews found during re-scrape - existing reviews unchanged")
            save_data(REVIEWS_OUTPUT, all_reviews)

        # Delay between companies to avoid rate limiting
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
