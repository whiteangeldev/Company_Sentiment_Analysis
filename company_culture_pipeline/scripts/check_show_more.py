#!/usr/bin/env python3
"""
Quick script to check how many reviews have "Show more..." truncation issues
"""

import json
from pathlib import Path
import re

def analyze_truncation():
    """Analyze existing reviews for truncation issues"""
    
    print("=" * 70)
    print("REVIEW TRUNCATION ANALYSIS")
    print("=" * 70)
    print()
    
    reviews_file = Path("data/raw_reviews/scraped_reviews.json")
    
    if not reviews_file.exists():
        print(f"❌ Reviews file not found: {reviews_file}")
        return
    
    with open(reviews_file, encoding='utf-8') as f:
        reviews = json.load(f)
    
    print(f"📊 Total reviews loaded: {len(reviews)}")
    print()
    
    # Truncation patterns to look for
    truncation_patterns = [
        r'show more',
        r'read more',
        r'see more',
        r'view more',
        r'continue reading',
        r'expand review',
        r'\.\.\.$',  # Ends with ...
    ]
    
    truncated_count = 0
    truncated_reviews = []
    
    # Text length statistics
    lengths = []
    
    for review in reviews:
        text = review.get('text', '')
        lengths.append(len(text))
        
        # Check for truncation indicators
        is_truncated = False
        for pattern in truncation_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                is_truncated = True
                break
        
        if is_truncated:
            truncated_count += 1
            truncated_reviews.append({
                'company': review.get('company_name', 'Unknown'),
                'text_preview': text[:100] + '...' if len(text) > 100 else text,
                'length': len(text),
                'platform': review.get('platform', 'unknown')
            })
    
    # Calculate statistics
    if lengths:
        avg_length = sum(lengths) / len(lengths)
        min_length = min(lengths)
        max_length = max(lengths)
    else:
        avg_length = min_length = max_length = 0
    
    print("=" * 70)
    print("TRUNCATION STATISTICS")
    print("=" * 70)
    print()
    print(f"Reviews with truncation: {truncated_count} ({truncated_count/len(reviews)*100:.1f}%)")
    print(f"Complete reviews: {len(reviews) - truncated_count} ({(len(reviews)-truncated_count)/len(reviews)*100:.1f}%)")
    print()
    
    print("=" * 70)
    print("TEXT LENGTH STATISTICS")
    print("=" * 70)
    print()
    print(f"Average review length: {avg_length:.0f} characters")
    print(f"Shortest review: {min_length} characters")
    print(f"Longest review: {max_length} characters")
    print()
    
    # Show some examples
    if truncated_reviews:
        print("=" * 70)
        print("EXAMPLES OF TRUNCATED REVIEWS (First 5)")
        print("=" * 70)
        print()
        
        for idx, review in enumerate(truncated_reviews[:5], 1):
            print(f"{idx}. {review['company']} ({review['platform']})")
            print(f"   Length: {review['length']} chars")
            print(f"   Preview: {review['text_preview']}")
            print()
    
    # Impact analysis
    print("=" * 70)
    print("IMPACT ANALYSIS")
    print("=" * 70)
    print()
    
    if truncated_count > len(reviews) * 0.3:  # More than 30%
        print("⚠️  HIGH IMPACT: Over 30% of reviews are truncated")
        print("   Recommendation: Re-run scraper with Show More fix")
        print("   Expected improvement: 3x longer reviews on average")
    elif truncated_count > len(reviews) * 0.1:  # 10-30%
        print("⚠️  MEDIUM IMPACT: 10-30% of reviews are truncated")
        print("   Recommendation: Re-run scraper for better quality")
        print("   Expected improvement: 2x longer reviews on average")
    elif truncated_count > 0:
        print("✅ LOW IMPACT: Less than 10% truncated")
        print("   Optional: Re-run scraper if you want perfect quality")
        print("   Expected improvement: Minor (most reviews are complete)")
    else:
        print("✅ EXCELLENT: No truncation detected!")
        print("   All reviews appear to be complete")
    
    print()
    print("=" * 70)
    print("NEXT STEPS")
    print("=" * 70)
    print()
    
    if truncated_count > 0:
        print("1. The 'Show More' fix has been implemented in:")
        print("   company_culture_pipeline/scripts/04_scrape_review_content.py")
        print()
        print("2. To get full-text reviews:")
        print("   a) Add new ScraperAPI key (if needed)")
        print("   b) Re-run: python3 scripts/04_scrape_review_content.py")
        print()
        print("3. The scraper will automatically:")
        print("   - Click 'Show more' buttons via JavaScript")
        print("   - Extract hidden/collapsed content")
        print("   - Clean up any remaining artifacts")
        print()
        print("4. After re-scraping, run this script again to verify improvement")
    else:
        print("✅ No action needed - reviews are already complete!")
    
    print()
    print("=" * 70)


if __name__ == "__main__":
    analyze_truncation()

