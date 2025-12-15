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


def parse_ambitionbox_html(html, max_reviews=10):
    """
    Parse AmbitionBox reviews from HTML
    
    AmbitionBox uses Next.js/React with Tailwind CSS, so reviews may be:
    - In dynamically loaded sections
    - In script tags with JSON data
    - In divs with Tailwind utility classes
    - Loaded via API calls (may not be in initial HTML)
    """
    reviews = []
    
    try:
        soup = BeautifulSoup(html, "html.parser")
        
        # First, try to find reviews in JSON script tags (common in Next.js apps)
        script_tags = soup.find_all('script', type='application/json')
        for script in script_tags:
            try:
                import json
                data = json.loads(script.string)
                # Look for review data in JSON
                if isinstance(data, dict):
                    # Common patterns for review data
                    for key in ['reviews', 'data', 'items', 'results']:
                        if key in data and isinstance(data[key], list):
                            for item in data[key][:max_reviews]:
                                if isinstance(item, dict):
                                    review_text = item.get('text') or item.get('content') or item.get('description') or item.get('review')
                                    if review_text and len(str(review_text)) > 50:
                                        reviews.append({
                                            "topic": item.get('title') or item.get('heading') or None,
                                            "text": str(review_text),
                                            "rating": item.get('rating') or item.get('score') or None,
                                        })
            except:
                pass
        
        # If we found reviews in JSON, return them
        if reviews:
            print(f"      ✓ Found {len(reviews)} reviews in JSON data")
            return reviews[:max_reviews]
        
        # Otherwise, try to find review containers using various strategies
        # Strategy 1: Look for divs with substantial text that might be reviews
        all_divs = soup.find_all('div')
        potential_reviews = []
        
        for div in all_divs:
            # Skip script, style, and other non-content elements
            if div.find('script') or div.find('style'):
                continue
            
            text = div.get_text(separator=' ', strip=True)
            # Look for substantial text blocks (100-5000 chars) that might be reviews
            if 100 <= len(text) <= 5000:
                text_lower = text.lower()
                # Check if it contains review-like keywords
                review_keywords = ['work', 'company', 'management', 'team', 'experience', 'employee', 
                                  'job', 'career', 'colleague', 'boss', 'manager', 'salary', 'benefit']
                # Check if it's NOT navigation/UI text
                skip_keywords = ['login', 'sign up', 'filter', 'sort', 'search', 'navigation', 
                               'menu', 'footer', 'header', 'cookie', 'privacy policy']
                
                if (any(kw in text_lower for kw in review_keywords) and 
                    not any(sk in text_lower for sk in skip_keywords) and
                    len(text.split()) > 20):  # At least 20 words
                    potential_reviews.append(div)
        
        # Strategy 2: Look for sections with class patterns that might contain reviews
        # AmbitionBox uses Tailwind, so look for common card/container patterns
        section_selectors = [
            'section',
            'div[class*="mb-"]',  # Margin bottom (common for spacing between reviews)
            'div[class*="p-"]',   # Padding (common for card content)
            'div[class*="border"]',  # Borders (common for cards)
            'div[class*="rounded"]',  # Rounded corners (common for cards)
        ]
        
        for selector in section_selectors:
            elements = soup.select(selector)
            for elem in elements:
                text = elem.get_text(separator=' ', strip=True)
                if 100 <= len(text) <= 5000:
                    text_lower = text.lower()
                    if (any(kw in text_lower for kw in ['work', 'company', 'management', 'team', 'experience']) and
                        not any(sk in text_lower for sk in ['login', 'sign up', 'filter', 'navigation']) and
                        len(text.split()) > 20):
                        if elem not in potential_reviews:
                            potential_reviews.append(elem)
        
        # Remove duplicates and limit
        seen_texts = set()
        unique_reviews = []
        for elem in potential_reviews[:max_reviews * 3]:  # Get more candidates to filter
            text = elem.get_text(separator=' ', strip=True)
            text_hash = hash(text[:200])  # Use first 200 chars as hash
            if text_hash not in seen_texts:
                seen_texts.add(text_hash)
                unique_reviews.append(elem)
        
        print(f"      Found {len(unique_reviews)} potential review containers")
        
        # Extract review data from each potential container
        for idx, element in enumerate(unique_reviews[:max_reviews], 1):
            try:
                topic = None
                text = ""
                rating = None
                
                # Extract all text from the element
                all_text = element.get_text(separator=' ', strip=True)
                all_text = ' '.join(all_text.split())  # Normalize whitespace
                
                # Skip if too short or too long
                if len(all_text) < 100 or len(all_text) > 5000:
                    continue
                
                # Try to extract topic from headings
                headings = element.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
                for heading in headings[:1]:  # Just first heading
                    heading_text = heading.get_text(strip=True)
                    if heading_text and 5 <= len(heading_text) <= 100 and not heading_text.endswith('?'):
                        topic = heading_text
                        # Remove topic from main text if it appears at the start
                        if all_text.startswith(topic):
                            all_text = all_text[len(topic):].strip()
                        break
                
                # Extract rating from SVG stars or numeric patterns
                # Look for rating in aria-labels or nearby text
                rating_elements = element.find_all(['svg', 'span', 'div'], 
                                                   attrs={'aria-label': True})
                for rating_elem in rating_elements:
                    aria_label = rating_elem.get('aria-label', '')
                    if 'rating' in aria_label.lower() or 'star' in aria_label.lower():
                        rating_match = re.search(r'(\d+\.?\d*)', aria_label)
                        if rating_match:
                            rating = float(rating_match.group(1))
                            if rating > 5:
                                rating = rating / 2
                            break
                
                # If no rating found, look for numeric patterns near "star" or "rating"
                if not rating:
                    rating_text = element.get_text()
                    rating_match = re.search(r'(\d+\.?\d*)\s*(?:star|rating|/5|out of 5)', rating_text, re.IGNORECASE)
                    if rating_match:
                        rating = float(rating_match.group(1))
                        if rating > 5:
                            rating = rating / 2
                
                # Clean up the text
                text = all_text
                
                # Remove common question patterns at the start
                question_patterns = [
                    r'^what is the best part',
                    r'^what is the most stressful',
                    r'^what is the work environment',
                    r'^how (is|are|do|does)',
                    r'^can you', r'^would you', r'^tell us',
                ]
                for pattern in question_patterns:
                    text = re.sub(pattern, '', text, flags=re.IGNORECASE).strip()
                
                text = clean_review_text(text)
                
                # Final validation: ensure it looks like a review
                if text and len(text) > 50:
                    text_lower = text.lower()
                    # Skip if it's clearly not a review
                    skip_patterns = [
                        'apparel group', 'private limited', 'company name',
                        'jobseekers also viewed', 'similar companies',
                        'write a review', 'login to continue',
                    ]
                    if any(pattern in text_lower for pattern in skip_patterns):
                        continue
                    
                    reviews.append({
                        "topic": topic,
                        "text": text,
                        "rating": rating,
                    })
            
            except Exception as e:
                print(f"      ⚠️  Error parsing review {idx}: {str(e)[:40]}")
                continue
        
        print(f"      ✓ Parsed {len(reviews)} reviews from HTML")
    
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
        if scrape_key in scraped_keys:
            print(f"   AmbitionBox: Already scraped (skipped)")
            skipped_count += 1
            continue

        print(f"   AmbitionBox: Scraping...")

        # No pagination for AmbitionBox
        page_urls = [base_url]

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
                company_had_success = True

                # Save after each platform
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
