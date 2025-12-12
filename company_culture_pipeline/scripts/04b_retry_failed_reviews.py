#!/usr/bin/env python3
"""
Retry scraping failed companies from failed_reviews.csv
Uses the same logic as 04_scrape_review_content.py but only for failed companies
"""

import csv
import json
import time
import random
import os
from pathlib import Path
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
OUTPUT_DIR = "data/raw_reviews"
FAILED_CSV = f"{OUTPUT_DIR}/failed_reviews.csv"
REVIEWS_OUTPUT = f"{OUTPUT_DIR}/scraped_reviews.json"
RETRY_OUTPUT = f"{OUTPUT_DIR}/retry_results.json"
API_KEY_STATE_FILE = f"{OUTPUT_DIR}/api_key_state.json"

# Scraping settings
MAX_PAGES_PER_COMPANY = 5
DELAY_AFTER_API_CALL = 2

# Import APIKeyManager from original script
class APIKeyManager:
    """Manages multiple ScraperAPI keys and auto-rotates on 403 errors"""
    
    def __init__(self):
        self.api_keys = self._load_api_keys()
        self.current_key_index = 0
        self.failed_keys = set()
        self._load_state()
    
    def _load_api_keys(self):
        keys = []
        for i in range(1, 5):
            key = os.getenv(f"SCRAPERAPI_KEY_{i}", "")
            if key and key.strip():
                keys.append(key.strip())
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
        except Exception:
            pass
    
    def _save_state(self):
        try:
            state = {
                'current_key_index': self.current_key_index,
                'failed_keys': list(self.failed_keys),
                'last_updated': datetime.now().isoformat()
            }
            with open(API_KEY_STATE_FILE, 'w') as f:
                json.dump(state, f, indent=2)
        except Exception:
            pass
    
    def get_current_key(self):
        if not self.api_keys:
            return None
        attempts = 0
        while attempts < len(self.api_keys):
            if self.current_key_index not in self.failed_keys:
                return self.api_keys[self.current_key_index]
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
            attempts += 1
        return None
    
    def rotate_key(self, reason="403_error"):
        if not self.api_keys or len(self.api_keys) == 1:
            return False
        self.failed_keys.add(self.current_key_index)
        old_index = self.current_key_index
        for _ in range(len(self.api_keys)):
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
            if self.current_key_index not in self.failed_keys:
                print(f"      🔄 API Key Rotated: #{old_index + 1} → #{self.current_key_index + 1} (Reason: {reason})")
                self._save_state()
                return True
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

# Initialize API key manager
api_key_manager = APIKeyManager()


def scrape_with_scraperapi(url, retry=0, max_retries=5, try_alternative_params=False):
    """Scrape URL using ScraperAPI with enhanced 500 error handling"""
    current_key = api_key_manager.get_current_key()
    if not current_key:
        return None
    
    try:
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
        
        # Add exponential backoff delay between retries (longer waits for 500 errors)
        if retry > 0:
            # Exponential backoff: 10s, 20s, 30s, 45s, 60s
            wait_time = min(10 * retry + (retry - 1) * 5, 60)
            print(f"      ⏳ Waiting {wait_time}s before retry {retry}/{max_retries}...")
            time.sleep(wait_time)
        
        response = requests.get("http://api.scraperapi.com", params=params, timeout=120)  # Increased timeout
        
        if response.status_code == 200:
            print(f"      ✓ ScraperAPI success (status: {response.status_code})")
            time.sleep(DELAY_AFTER_API_CALL)
            return response.text
        
        elif response.status_code == 500:
            # Enhanced 500 error handling
            if retry < max_retries:
                print(f"      ⚠️  ScraperAPI 500 error - retrying ({retry + 1}/{max_retries})...")
                
                # Try rotating API key on 500 errors (if multiple keys available)
                if retry >= 2 and len(api_key_manager.api_keys) > 1:
                    if api_key_manager.rotate_key(reason="500_server_error"):
                        print(f"      🔄 Rotated API key due to persistent 500 errors")
                        time.sleep(5)  # Brief pause after key rotation
                
                # Try alternative params after a few retries
                use_alt_params = retry >= 2
                return scrape_with_scraperapi(url, retry + 1, max_retries, use_alt_params)
            else:
                print(f"      ❌ ScraperAPI 500 error - max retries ({max_retries}) exceeded")
                return None
        
        elif response.status_code == 403:
            print(f"      ⚠️  ScraperAPI 403: API key credits exhausted")
            if api_key_manager.rotate_key(reason="403_credits_exhausted") and retry < max_retries:
                print(f"      🔄 Retrying with new API key...")
                time.sleep(3)
                return scrape_with_scraperapi(url, retry + 1, max_retries, try_alternative_params)
            return None
        
        elif response.status_code == 400:
            print(f"      ⚠️  ScraperAPI 400: Bad Request - URL might be malformed")
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
                    fixed_result = scrape_with_scraperapi(fixed_url, 0, max_retries, try_alternative_params)
                    if fixed_result and fixed_result != "NO_MORE_PAGES":
                        return fixed_result
            # Return special value - caller will decide if it's an error (first page) or expected (subsequent pages)
            return "NO_MORE_PAGES"
        
        else:
            print(f"      ⚠️  ScraperAPI returned status: {response.status_code}")
            # Retry on other 5xx errors
            if retry < max_retries and response.status_code >= 500:
                return scrape_with_scraperapi(url, retry + 1, max_retries, try_alternative_params)
            return None
    
    except requests.Timeout:
        print(f"      ⚠️  ScraperAPI timeout (120s)")
        if retry < max_retries:
            return scrape_with_scraperapi(url, retry + 1, max_retries, try_alternative_params)
        return None
    
    except Exception as e:
        error_msg = str(e)
        print(f"      ❌ Error: {error_msg[:60]}")
        
        # If connection error, add extra delay before retry (likely rate limited)
        if "Max retries exceeded" in error_msg or "Connection" in error_msg:
            if retry < max_retries:
                wait_time = (retry + 1) * 10  # Longer wait for connection errors
                print(f"      ⏳ Connection issue - waiting {wait_time}s before retry...")
                time.sleep(wait_time)
                return scrape_with_scraperapi(url, retry + 1, max_retries, try_alternative_params)
        elif retry < max_retries:
            return scrape_with_scraperapi(url, retry + 1, max_retries, try_alternative_params)
        return None


def clean_review_text(text):
    """Remove 'Show more...' artifacts"""
    import re
    if not text:
        return text
    
    truncation_phrases = [
        'Show more', 'Read more', 'Show full review', 'Read full review',
        'See more', 'View more', 'Continue reading', 'Expand review',
    ]
    
    cleaned = text
    for phrase in truncation_phrases:
        pattern = re.compile(r'\s*' + re.escape(phrase) + r'\s*\.{0,3}\s*', re.IGNORECASE)
        cleaned = pattern.sub(' ', cleaned)
    
    cleaned = ' '.join(cleaned.split())
    cleaned = re.sub(r'\s*\.{2,}\s*$', '', cleaned)
    cleaned = re.sub(r'\.\.\.\s*$', '', cleaned)
    
    return cleaned.strip()


def parse_indeed_html(html, max_reviews=100):
    """Parse Indeed reviews from HTML"""
    reviews = []
    
    try:
        soup = BeautifulSoup(html, "html.parser")
        
        review_selectors = [
            '[data-testid="review-card"]',
            '[data-testid="review"]',
            '[id*="cmp-review-"]',
            'div[class*="css-"][id*="review"]',
            '[data-tn-component="reviews"]',
            '[class*="review-item"]',
            '[class*="ReviewItem"]',
            'div[itemprop="review"]',
            '[class*="review"]',
        ]
        
        review_elements = []
        for selector in review_selectors:
            review_elements = soup.select(selector)
            if review_elements and len(review_elements) >= 1:
                break
        
        if not review_elements:
            return reviews
        
        print(f"      Found {len(review_elements)} review elements")
        
        for element in review_elements[:max_reviews]:
            try:
                # Extract topic
                topic = ""
                for selector in ['[data-testid="review-title"]', '[class*="review-title"]', 
                                '[itemprop="name"]', "h2", "h3"]:
                    topic_elem = element.select_one(selector)
                    if topic_elem:
                        topic = topic_elem.get_text(strip=True)
                        if topic and len(topic) > 3:
                            break
                
                # Extract text - multi-stage strategy
                text = ""
                
                # Stage 1: Hidden content
                for selector in ['[class*="expanded"]', '[class*="full-text"]', 
                                '[class*="full-review"]']:
                    full_elem = element.select_one(selector)
                    if full_elem:
                        potential_text = full_elem.get_text(separator=' ', strip=True)
                        if potential_text and len(potential_text) > len(text):
                            text = potential_text
                            break
                
                # Stage 2: Standard extraction
                if not text or len(text) < 50:
                    for selector in ['[data-testid="review-text"]', '[itemprop="reviewBody"]',
                                    '[class*="review-text"]', "p", "span"]:
                        text_elem = element.select_one(selector)
                        if text_elem:
                            potential_text = text_elem.get_text(separator=' ', strip=True)
                            if potential_text and len(potential_text) > len(text):
                                text = potential_text
                            if text and len(text) > 20:
                                break
                
                # Stage 3: Fallback
                if not text:
                    text = element.get_text(separator=' ', strip=True)
                
                # Clean text
                if text:
                    text = clean_review_text(text)
                
                # Extract rating
                rating = None
                for selector in ['[itemprop="ratingValue"]', '[data-testid="rating"]', 
                                '[class*="rating"]']:
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
                
                if text and len(text) > 20:
                    reviews.append({
                        "topic": topic or None,
                        "text": text,
                        "rating": rating,
                        "platform": "indeed",
                        "scraped_at": datetime.now().isoformat(),
                        "method": "scraperapi_retry",
                    })
            
            except Exception:
                continue
        
        print(f"      ✓ Parsed {len(reviews)} reviews from HTML")
    
    except Exception as e:
        print(f"      ❌ HTML parsing error: {str(e)[:60]}")
    
    return reviews


def scrape_company(company_name, url):
    """Scrape a single company"""
    print(f"\n{company_name}")
    print(f"   URL: {url}")
    print(f"   🔑 Using ScraperAPI for indeed")
    
    # Fix URL if missing /reviews
    if "indeed.com/cmp/" in url and not url.endswith("/reviews") and "/reviews" not in url:
        url = url.rstrip("/") + "/reviews"
        print(f"   📝 Fixed URL (added /reviews): {url}")
    
    all_reviews = []
    
    # Scrape multiple pages
    for page in range(1, MAX_PAGES_PER_COMPANY + 1):
        if page > 1:
            # Fix pagination URL construction - check if URL already has query params
            separator = "&" if "?" in url else "?"
            page_url = f"{url}{separator}start={(page-1)*20}"
            print(f"      📄 Page {page}...")
        else:
            page_url = url
        
        html = scrape_with_scraperapi(page_url)
        
        # Handle special "NO_MORE_PAGES" return value for expected 404s
        if html == "NO_MORE_PAGES":
            if page == 1:
                # 404 on first page means URL is invalid
                return None, "Page not found (404) - URL may be invalid"
            else:
                # 404 on subsequent pages means no more pages (expected)
                print(f"      ✓ No more pages (404), stopping")
                break
        
        if not html:
            if page == 1:
                return None, "ScraperAPI failed to fetch content"
            else:
                break  # No more pages
        
        reviews = parse_indeed_html(html)
        
        if not reviews:
            if page == 1:
                return None, "No reviews found on page"
            else:
                print(f"      ✓ No more reviews, stopping")
                break
        
        all_reviews.extend(reviews)
        print(f"      ✓ Page {page}: Got {len(reviews)} reviews (Total: {len(all_reviews)})")
        
        # Add company info to reviews
        for review in reviews:
            review['company_name'] = company_name
        
        if len(reviews) < 10:  # Last page probably
            break
        
        # Delay between pages
        if page < MAX_PAGES_PER_COMPANY:
            delay = random.uniform(10, 15)
            print(f"      ⏳ Waiting {delay:.1f}s before next page...")
            time.sleep(delay)
    
    if all_reviews:
        return all_reviews, None
    else:
        return None, "No reviews found"


def main():
    print("=" * 70)
    print("RETRY FAILED REVIEWS - ScraperAPI")
    print("=" * 70)
    print()
    
    # Check if failed CSV exists
    if not Path(FAILED_CSV).exists():
        print(f"❌ ERROR: {FAILED_CSV} not found")
        print("   No failed companies to retry")
        return
    
    # Load failed companies
    failed_companies = []
    with open(FAILED_CSV, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('company_name') and row.get('url'):
                failed_companies.append(row)
    
    print(f"✓ Loaded {len(failed_companies)} failed companies")
    
    # Display API key status
    status = api_key_manager.get_status()
    if status['total_keys'] > 0:
        print(f"✓ ScraperAPI Keys: {status['total_keys']} total, {status['active_keys']} active, using key #{status['current_key']}")
        if status['failed_keys'] > 0:
            print(f"   ⚠️  {status['failed_keys']} key(s) already exhausted")
    else:
        print("❌ No ScraperAPI keys configured!")
        return
    
    print()
    
    # Load existing reviews to append
    existing_reviews = []
    if Path(REVIEWS_OUTPUT).exists():
        try:
            with open(REVIEWS_OUTPUT, encoding='utf-8') as f:
                existing_reviews = json.load(f)
            print(f"✓ Loaded {len(existing_reviews)} existing reviews")
        except:
            pass
    
    # Scrape failed companies
    successful = 0
    still_failed = []
    new_reviews = []
    
    for idx, company in enumerate(failed_companies, 1):
        company_name = company['company_name']
        url = company['url']
        
        print(f"\n[{idx}/{len(failed_companies)}] {company_name}")
        
        reviews, error = scrape_company(company_name, url)
        
        if reviews:
            successful += 1
            new_reviews.extend(reviews)
            print(f"   ✅ Success! Got {len(reviews)} reviews")
        else:
            still_failed.append({
                'company_name': company_name,
                'platform': 'indeed',
                'url': url,
                'error': error,
                'timestamp': datetime.now().isoformat()
            })
            print(f"   ❌ Failed: {error}")
        
        # Delay between companies
        if idx < len(failed_companies):
            delay = random.uniform(15, 20)
            print(f"   ⏳ Waiting {delay:.1f}s before next company...")
            time.sleep(delay)
    
    # Save results
    print("\n" + "=" * 70)
    print("SAVING RESULTS")
    print("=" * 70)
    
    # Save retry results separately
    if new_reviews:
        with open(RETRY_OUTPUT, 'w', encoding='utf-8') as f:
            json.dump(new_reviews, f, indent=2, ensure_ascii=False)
        print(f"✓ Saved {len(new_reviews)} new reviews to {RETRY_OUTPUT}")
        
        # Append to main reviews file
        all_reviews = existing_reviews + new_reviews
        with open(REVIEWS_OUTPUT, 'w', encoding='utf-8') as f:
            json.dump(all_reviews, f, indent=2, ensure_ascii=False)
        print(f"✓ Updated main reviews file: {len(all_reviews)} total reviews")
    
    # Update failed CSV
    if still_failed:
        with open(FAILED_CSV, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['company_name', 'platform', 'url', 'error', 'timestamp'])
            writer.writeheader()
            writer.writerows(still_failed)
        print(f"✓ Updated failed CSV: {len(still_failed)} companies still failing")
    else:
        # All succeeded - clear the file
        with open(FAILED_CSV, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['company_name', 'platform', 'url', 'error', 'timestamp'])
            writer.writeheader()
        print(f"✓ All companies succeeded! Cleared failed CSV")
    
    # Summary
    print("\n" + "=" * 70)
    print("RETRY SUMMARY")
    print("=" * 70)
    print(f"  Companies attempted: {len(failed_companies)}")
    print(f"  Now successful: {successful} ({successful/len(failed_companies)*100:.1f}%)")
    print(f"  Still failing: {len(still_failed)} ({len(still_failed)/len(failed_companies)*100:.1f}%)")
    print(f"  New reviews collected: {len(new_reviews)}")
    print("=" * 70)


if __name__ == "__main__":
    main()

