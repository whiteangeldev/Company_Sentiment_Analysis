#!/usr/bin/env python3
"""
Script to scrape employee reviews from Indeed platform only

Features:
- Indeed-only scraping
- Multi-page scraping (up to 5 pages per company) - OPTIMIZED FOR RATE LIMITS
- Bypasses Cloudflare using ScraperAPI
- Extracts topic, text, and ratings
- Handles pagination automatically
- Uses ScraperAPI free tier (1,000 calls/month = 100 companies × 5 pages = 500 calls)
- Smart rate limiting with 10-15s delays between pages
- Exponential backoff on errors

Input: reviews_link.json (with company_id, company_name, location, and indeed_url)
Output: scraped_reviews_indeed.json (with company_id, company_name, location, url, platform, topic, text, rating)

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
from bs4 import BeautifulSoup, NavigableString
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
REVIEWS_OUTPUT = f"{OUTPUT_DIR}/scraped_reviews_indeed.json"
PROGRESS_FILE = f"{OUTPUT_DIR}/scraping_progress_indeed.json"
FAILED_FILE = f"{OUTPUT_DIR}/failed_reviews_indeed.csv"

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
SCRAPERAPI_PLATFORMS = ["indeed"]  # Use ScraperAPI for Indeed only

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

        elif response.status_code == 401:
            # 401 = Unauthorized = API key is invalid/malformed/doesn't exist
            # This is different from 403 (credits exhausted) - the key itself is wrong
            print(f"      ⚠️  ScraperAPI 401: API key is invalid or unauthorized")
            print(f"      💡 Possible causes: Invalid key format, key deleted, or key doesn't exist")
            
            # Rotate to next API key (invalid key should be skipped)
            if api_key_manager.rotate_key(reason="401_invalid_key") and retry < max_retries:
                print(f"      🔄 Retrying with new API key...")
                time.sleep(3)  # Brief pause before retry
                return scrape_with_scraperapi(url, render, retry + 1, max_retries)
            
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
                # Extract topic/title - Look in the review element AND its siblings/parent
                # Indeed structure: Title is often in h3[data-testid="title"] as a sibling of review text
                topic = None
                
                # Patterns that indicate this is NOT a valid title (questions, prompts, etc.)
                invalid_patterns = [
                    r'^what is',
                    r'^what are',
                    r'^how (is|are|do|does)',
                    r'^tell us',
                    r'^describe',
                    r'^explain',
                    r'^can you',
                    r'^would you',
                    r'^do you',
                ]
                
                def is_valid_title(text):
                    """Check if text is a valid title (not a question or prompt)"""
                    if not text or len(text) < 3 or len(text) > 150:
                        return False
                    text_lower = text.lower().strip()
                    # Don't accept questions
                    if text.strip().endswith('?'):
                        return False
                    # Don't accept question patterns
                    if any(re.search(pattern, text_lower) for pattern in invalid_patterns):
                        return False
                    return True
                
                # Strategy 1: Look for title in the review element itself
                primary_title_selectors = [
                    '[data-testid="review-title"]',
                    '[data-testid="title"]',
                    '[data-testid="titleSpan"]',
                    '[data-testid="reviewTitle"]',
                    '[itemprop="name"]',
                ]
                
                for selector in primary_title_selectors:
                    topic_elem = element.select_one(selector)
                    if topic_elem:
                        topic_text = topic_elem.get_text(strip=True)
                        if is_valid_title(topic_text):
                            topic = topic_text
                            break
                
                # Strategy 2: Look for title in parent container (title is often a sibling)
                if not topic:
                    parent = element.parent
                    if parent:
                        # Look for h3[data-testid="title"] in parent (common Indeed structure)
                        title_elem = parent.select_one('h3[data-testid="title"]')
                        if title_elem:
                            topic_text = title_elem.get_text(strip=True)
                            if is_valid_title(topic_text):
                                topic = topic_text
                        
                        # Also check for other title selectors in parent
                        if not topic:
                            for selector in primary_title_selectors:
                                title_elem = parent.select_one(selector)
                                if title_elem:
                                    topic_text = title_elem.get_text(strip=True)
                                    if is_valid_title(topic_text):
                                        topic = topic_text
                                        break
                        
                        # Also check all children of parent for title elements (in case structure is nested)
                        if not topic:
                            all_title_elems = parent.find_all(['h2', 'h3', 'h4'], attrs={'data-testid': 'title'})
                            if all_title_elems:
                                # Use the first valid title found
                                for title_elem in all_title_elems:
                                    topic_text = title_elem.get_text(strip=True)
                                    if is_valid_title(topic_text):
                                        topic = topic_text
                                        break
                
                # Strategy 3: Look for title in previous siblings (skip whitespace nodes)
                if not topic:
                    # Check previous siblings for title elements
                    current = element.previous_sibling
                    checked = 0
                    while current and checked < 10:  # Increased limit
                        # Skip NavigableString (whitespace) nodes
                        if isinstance(current, NavigableString):
                            current = current.previous_sibling if hasattr(current, 'previous_sibling') else None
                            continue
                        
                        if hasattr(current, 'select_one'):
                            # Check for h3[data-testid="title"] (most common Indeed structure)
                            title_elem = current.select_one('h3[data-testid="title"]')
                            if title_elem:
                                topic_text = title_elem.get_text(strip=True)
                                if is_valid_title(topic_text):
                                    topic = topic_text
                                    break
                            
                            # Also check if the sibling itself is a title element
                            if hasattr(current, 'get'):
                                if current.get('data-testid') == 'title':
                                    topic_text = current.get_text(strip=True)
                                    if is_valid_title(topic_text):
                                        topic = topic_text
                                        break
                        
                        # Also check all children of the sibling for title elements
                        if hasattr(current, 'find_all'):
                            title_children = current.find_all(['h2', 'h3', 'h4'], attrs={'data-testid': 'title'})
                            if title_children:
                                for title_child in title_children:
                                    topic_text = title_child.get_text(strip=True)
                                    if is_valid_title(topic_text):
                                        topic = topic_text
                                        break
                                if topic:
                                    break
                        
                        current = current.previous_sibling if hasattr(current, 'previous_sibling') else None
                        checked += 1
                
                # Strategy 4: Look for heading elements with review-related classes in parent
                if not topic:
                    parent = element.parent
                    if parent:
                        heading_selectors = [
                            'h2[class*="review"]',
                            'h3[class*="review"]',
                            'h2[class*="title"]',
                            'h3[class*="title"]',
                            '[class*="review-title"]',
                            '[class*="ReviewTitle"]',
                            '[class*="reviewTitle"]',
                            '[data-tn-component*="reviewTitle"]',
                        ]
                        for selector in heading_selectors:
                            topic_elem = parent.select_one(selector)
                            if topic_elem:
                                topic_text = topic_elem.get_text(strip=True)
                                if is_valid_title(topic_text) and len(topic_text.split()) < 20:
                                    topic = topic_text
                                    break
                
                # Strategy 5: Look for heading elements (h2, h3) in parent but validate carefully
                if not topic:
                    parent = element.parent
                    if parent:
                        for heading_tag in ['h2', 'h3', 'h4']:
                            heading_elem = parent.select_one(heading_tag)
                            if heading_elem:
                                topic_text = heading_elem.get_text(strip=True)
                                # More strict: short headings only, not questions
                                if is_valid_title(topic_text) and len(topic_text.split()) < 15:
                                    topic = topic_text
                                    break
                
                # Strategy 6: Search all ancestors (parent, grandparent, etc.) for title elements
                if not topic:
                    ancestor = element.parent
                    levels_checked = 0
                    while ancestor and levels_checked < 3:  # Check up to 3 levels up
                        # Look for h3[data-testid="title"] in ancestor
                        title_elem = ancestor.select_one('h3[data-testid="title"]')
                        if title_elem:
                            topic_text = title_elem.get_text(strip=True)
                            if is_valid_title(topic_text):
                                topic = topic_text
                                break
                        
                        # Also check for any heading with data-testid="title"
                        all_titles = ancestor.find_all(['h2', 'h3', 'h4'], attrs={'data-testid': 'title'})
                        if all_titles:
                            for title_elem in all_titles:
                                topic_text = title_elem.get_text(strip=True)
                                if is_valid_title(topic_text):
                                    topic = topic_text
                                    break
                            if topic:
                                break
                        
                        ancestor = ancestor.parent if hasattr(ancestor, 'parent') else None
                        levels_checked += 1
                
                # Strategy 7: Last resort - find the closest h3[data-testid="title"] anywhere in the document
                # that appears before this review element (to avoid matching wrong titles)
                if not topic:
                    # Get all title elements in the document
                    all_title_elems = soup.find_all(['h2', 'h3', 'h4'], attrs={'data-testid': 'title'})
                    # Find the one that's closest to our review element (check if it's in the same parent chain)
                    for title_elem in all_title_elems:
                        # Check if this title is in the same parent or ancestor chain
                        title_parent = title_elem.parent
                        review_parent = element.parent
                        
                        # Check if they share a common parent
                        if title_parent == review_parent:
                            topic_text = title_elem.get_text(strip=True)
                            if is_valid_title(topic_text):
                                topic = topic_text
                                break
                        
                        # Check if title's parent contains our review element
                        if title_parent and element in title_parent.descendants:
                            topic_text = title_elem.get_text(strip=True)
                            if is_valid_title(topic_text):
                                topic = topic_text
                                break
                
                # DO NOT extract from text content, strong/bold text, or first lines
                # These often contain questions like "what is...", "can you..." which are NOT titles

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

                # Strategy 3: Fallback to all text in element, but exclude title elements
                if not text:
                    # Get all text but exclude elements that might be titles
                    all_elements = element.find_all(['p', 'span', 'div', 'li'])
                    text_parts = []
                    for elem in all_elements:
                        # Skip if it's a title element
                        is_title = False
                        for title_selector in ['[data-testid*="title"]', 'h2', 'h3', 'h4', '[class*="title"]']:
                            if elem.select_one(title_selector):
                                is_title = True
                                break
                        if not is_title:
                            elem_text = elem.get_text(strip=True)
                            if elem_text and len(elem_text) > 10:
                                text_parts.append(elem_text)
                    
                    if text_parts:
                        text = ' '.join(text_parts)
                    else:
                        text = element.get_text(separator=' ', strip=True)
                
                # POST-PROCESSING: Clean up text and remove question patterns
                if text:
                    # Remove common question patterns from the beginning of text
                    # These are NOT titles, they're prompts/questions
                    question_patterns = [
                        r'^what is the best part of working at the company\?',
                        r'^what is the most stressful part about working at the company\?',
                        r'^what is the work environment and culture like at the company\?',
                        r'^what (do|does|did) you (like|dislike|think)',
                        r'^how (is|are|do|does)',
                        r'^can you',
                        r'^would you',
                        r'^tell us',
                    ]
                    for pattern in question_patterns:
                        text = re.sub(pattern, '', text, flags=re.IGNORECASE).strip()
                    
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
                    "[class*='title']",
                    "[class*='titleSpan']",
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


def scrape_reviews_from_url(url, platform, max_reviews=10, company_name="unknown"):
    """
    Scrape reviews from an Indeed URL
    - Uses ScraperAPI for Indeed if available
    - Falls back to Selenium if ScraperAPI is not configured
    - Includes fallback parser for difficult pages

    Returns: (reviews, success, error_message)
    """
    if not url or url == "":
        return [], False, "Empty URL"

    platform_lower = platform.lower()
    if "indeed" not in platform_lower:
        return [], False, f"Platform {platform} not supported (Indeed only)"

    # Use ScraperAPI for Indeed if available
    if USE_SCRAPERAPI and platform_lower in SCRAPERAPI_PLATFORMS:
        print(f"      🔑 Using ScraperAPI for {platform}")
        try:
            html = scrape_with_scraperapi(url, render=True)

            # Handle special "NO_MORE_PAGES" return value for expected 404s
            if html == "NO_MORE_PAGES":
                return [], False, "No more pages (404)"  # Expected for subsequent pages
            
            if not html:
                return [], False, "ScraperAPI failed to fetch content"

            # Parse HTML for Indeed
            reviews = parse_indeed_html(html, max_reviews)
            # Try fallback parser if primary parser found nothing
            if not reviews:
                print(f"      🔄 Primary parser found no reviews, trying fallback parser...")
                reviews = parse_indeed_html_fallback(html, max_reviews, company_name)
            
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
            print(f"      ⚠️  ScraperAPI not configured, using Selenium for Indeed")

        print(f"      🌐 Using Selenium for Indeed")
        driver = init_browser()
        if not driver:
            return [], False, "Could not initialize browser"

        try:
            reviews = scrape_indeed_reviews(driver, url, max_reviews)
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
                for item in data:
                    company_id = item.get("company_id")
                    company_name = item.get("company_name", "Unknown")
                    platform = item.get("platform", "unknown")
                    if company_id:
                        scraped_keys.add(f"{company_id}_{platform}")
                    else:
                        scraped_keys.add(f"{company_name}_{platform}")
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
    print("EMPLOYEE REVIEW SCRAPER - Indeed Platform Only")
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

    # Review platform to scrape - Indeed only
    platform = "indeed"
    url_key = "indeed_url"

    if USE_SCRAPERAPI:
        print("✓ ScraperAPI enabled for Indeed")
        print("✓ Scraping from: Indeed (via ScraperAPI)")
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
            print(f"   Indeed: No URL provided (skipped)")
            continue

        # Check if already scraped
        scrape_key = f"{company_id}_{platform}"
        if scrape_key in scraped_keys:
            print(f"   Indeed: Already scraped (skipped)")
            skipped_count += 1
            continue

        print(f"   Indeed: Scraping...")

        # Generate paginated URLs for Indeed
        page_urls = generate_indeed_page_urls(base_url, MAX_PAGES_PER_COMPANY)

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
                # Ensure topic, text, rating exist (may be None)
                if "topic" not in review:
                    review["topic"] = None
                if "text" not in review:
                    review["text"] = review.get("review_text", "")
                if "rating" not in review:
                    review["rating"] = None
                # Remove any extra fields not in the required list
                allowed_fields = ["company_id", "company_name", "location", "url", "platform", "topic", "text", "rating"]
                review_copy = {k: v for k, v in review.items() if k in allowed_fields}
                review.clear()
                review.update(review_copy)

            all_reviews.extend(platform_reviews)
            success_count += 1

            # Save after scraping
            save_data(REVIEWS_OUTPUT, all_reviews)
            print(
                f"      💾 Saved {len(platform_reviews)} total reviews from {pages_scraped} pages"
            )

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
