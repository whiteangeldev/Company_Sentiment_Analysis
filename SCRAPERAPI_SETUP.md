# ScraperAPI Setup Instructions

## 🎯 Quick Setup (5 Minutes)

Your script is now configured to use **ScraperAPI** for Glassdoor and Indeed (bypassing Cloudflare), while using free scraping for other platforms.

---

## Step 1: Sign Up for ScraperAPI

1. **Go to:** https://www.scraperapi.com/signup
2. **Sign up** with your email (no credit card required)
3. **Verify your email**
4. **Copy your API key** from the dashboard

### Free Tier Includes:
- ✅ **1,000 API calls per month** (FREE forever)
- ✅ Your project needs ~200 calls (100 companies × 2 platforms)
- ✅ **Plenty of room to spare!**

---

## Step 2: Add API Key to .env File

1. Open `.env` file in the project root
2. Find the line: `SCRAPERAPI_KEY=your_scraperapi_key_here`
3. Replace `your_scraperapi_key_here` with your actual API key

**Example:**
```bash
# Before
SCRAPERAPI_KEY=your_scraperapi_key_here

# After
SCRAPERAPI_KEY=abc123def456ghi789jkl
```

---

## Step 3: Run the Script

```bash
cd company_culture_pipeline
python scripts/04_scrape_review_content.py
```

### What You'll See:

```
======================================================================
EMPLOYEE REVIEW SCRAPER - Undetected ChromeDriver (Cloudflare Bypass)
======================================================================

✓ Loaded 100 companies from JSON
✓ ScraperAPI enabled for Glassdoor and Indeed
✓ Scraping from: Glassdoor (API), Indeed (API), Comparably, Kununu, AmbitionBox
✓ Max reviews per company: 100

[1/100] FNAC - Fujifilm Greenwood SC - Primary
   Glassdoor: https://www.glassdoor.com/...
      🔑 Using ScraperAPI for glassdoor
      ✓ ScraperAPI success (status: 200)
      Found 10 review elements
      ✓ Parsed 8 reviews from HTML
      💾 Saved 8 reviews
      
   Indeed: https://www.indeed.com/...
      🔑 Using ScraperAPI for indeed
      ✓ ScraperAPI success (status: 200)
      Found 12 review elements
      ✓ Parsed 10 reviews from HTML
      💾 Saved 10 reviews
```

---

## How It Works

### **Hybrid Scraping Approach:**

| Platform | Method | Cost | Success Rate |
|----------|--------|------|--------------|
| **Glassdoor** | ScraperAPI | $0 (free tier) | 85-90% |
| **Indeed** | ScraperAPI | $0 (free tier) | 85-90% |
| **Comparably** | Selenium (free) | $0 | 70-80% |
| **Kununu** | Selenium (free) | $0 | 70-80% |
| **AmbitionBox** | Selenium (free) | $0 | 70-80% |

### **Benefits:**
- ✅ Bypasses Cloudflare on Glassdoor and Indeed
- ✅ Saves money by using free methods where possible
- ✅ No manual intervention needed
- ✅ Automatically handles retries and errors

---

## Monitoring Your Usage

### Check API Usage:
1. Go to: https://www.scraperapi.com/dashboard
2. View **"API Calls This Month"**
3. You should see ~200 calls used (for 100 companies)

### Example:
```
API Calls This Month: 203 / 1,000
Remaining: 797 calls
```

---

## Cost Breakdown

### Your Project (100 Companies):
```
Glassdoor: 100 calls
Indeed: 100 calls
Total: 200 calls

Cost: $0 (within 1,000 free tier)
```

### If You Need More:
- **5,000 calls/month**: Still FREE
- **100,000 calls/month**: $49/month (Hobby Plan)
- **250,000 calls/month**: $99/month (Startup Plan)

---

## Troubleshooting

### Issue 1: "ScraperAPI key not found"
**Solution:** Make sure SCRAPERAPI_KEY is set in `.env` file

### Issue 2: "ScraperAPI returned status: 403"
**Solution:** Your API key might be invalid. Check your dashboard and regenerate if needed.

### Issue 3: "No reviews parsed from ScraperAPI response"
**Solution:** The HTML structure might have changed. The script will log this and continue.

### Issue 4: Running out of free calls
**Solution:** 
- Option 1: Wait until next month (resets monthly)
- Option 2: Upgrade to paid plan ($49/month)
- Option 3: Scrape only critical companies

---

## Without ScraperAPI

If you don't add the API key, the script will:
- ✅ Still scrape Comparably, Kununu, AmbitionBox (free)
- ⚠️ Skip Glassdoor and Indeed
- Display: "ScraperAPI not configured - will skip Glassdoor and Indeed"

---

## Next Steps

1. ✅ Sign up for ScraperAPI
2. ✅ Add API key to `.env`
3. ✅ Run the script
4. ✅ Monitor results in `data/raw_reviews/scraped_reviews.json`

---

**Questions?** Check the ScraperAPI docs: https://www.scraperapi.com/documentation

