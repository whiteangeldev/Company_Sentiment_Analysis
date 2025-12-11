#!/usr/bin/env python3
"""
Quick test script to verify undetected-chromedriver works
"""
import undetected_chromedriver as uc
import time

def test_browser():
    print("Testing undetected-chromedriver...")
    print("-" * 50)
    
    try:
        # Initialize browser
        print("1. Initializing browser...")
        options = uc.ChromeOptions()
        options.add_argument("--window-size=1920,1080")
        
        driver = uc.Chrome(options=options, use_subprocess=True, version_main=None)
        print("   ✓ Browser initialized successfully")
        
        # Test with a Cloudflare-protected site
        print("\n2. Testing Glassdoor (Cloudflare protected)...")
        driver.get("https://www.glassdoor.com/Reviews/index.htm")
        time.sleep(5)
        
        # Check if we got through
        if "cloudflare" in driver.page_source.lower() and "checking your browser" in driver.page_source.lower():
            print("   ⚠️  Cloudflare challenge detected - may need more time")
        else:
            print("   ✓ Successfully loaded Glassdoor!")
        
        print(f"   Page title: {driver.title[:50]}...")
        
        # Clean up
        driver.quit()
        print("\n✅ Test completed successfully!")
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    test_browser()

