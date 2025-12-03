#!/usr/bin/env python3
"""
Create a manual review collection template

This is the RECOMMENDED approach for collecting employee reviews legally and reliably.
Generates an Excel/CSV template for manually copying reviews.
"""

import csv
import os

INPUT_CSV = "data/raw_reviews/reviews_summary.csv"
OUTPUT_CSV = "data/raw_reviews/manual_review_template.csv"


def create_template():
    """Create template with instructions for manual review collection"""
    
    if not os.path.exists(INPUT_CSV):
        print(f"❌ ERROR: {INPUT_CSV} not found")
        print(f"   Please run 02_get_employee_reviews.py first")
        return
    
    # Load companies
    companies = []
    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        companies = list(reader)
    
    # Create template rows
    template_rows = []
    
    for company in companies:
        company_name = company["original_name"]
        glassdoor_url = company.get("glassdoor_url", "")
        glassdoor_search = company.get("glassdoor_search_url", "")
        
        # Add 10 blank review rows per company (or adjust as needed)
        for i in range(1, 11):
            row = {
                "company_name": company_name if i == 1 else "",  # Only show on first row
                "review_number": i,
                "glassdoor_url": glassdoor_url if i == 1 else "",
                "glassdoor_search_url": glassdoor_search if i == 1 else "",
                "rating": "",
                "review_title": "",
                "pros": "",
                "cons": "",
                "advice_to_management": "",
                "job_title": "",
                "location": "",
                "employment_status": "",  # Current/Former
                "date": "",
                "helpful_count": "",
                "notes": "",
            }
            template_rows.append(row)
    
    # Save template
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "company_name",
            "review_number",
            "glassdoor_url",
            "glassdoor_search_url",
            "rating",
            "review_title",
            "pros",
            "cons",
            "advice_to_management",
            "job_title",
            "location",
            "employment_status",
            "date",
            "helpful_count",
            "notes",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(template_rows)
    
    print("=" * 70)
    print("MANUAL REVIEW COLLECTION TEMPLATE CREATED")
    print("=" * 70)
    print()
    print(f"✓ Template saved to: {OUTPUT_CSV}")
    print()
    print(f"📋 INSTRUCTIONS:")
    print()
    print("1. Open the CSV file in Excel or Google Sheets")
    print()
    print("2. For each company:")
    print("   • Visit the glassdoor_url (or use glassdoor_search_url to find it)")
    print("   • Read the first 5-10 most helpful reviews")
    print("   • Copy the review details into the template")
    print()
    print("3. Fields to collect:")
    print("   • rating - Star rating (1-5)")
    print("   • review_title - Title of the review")
    print("   • pros - What employees like")
    print("   • cons - What employees dislike")
    print("   • advice_to_management - Suggestions (if available)")
    print("   • job_title - Reviewer's position")
    print("   • location - Review location")
    print("   • employment_status - Current or Former employee")
    print("   • date - Review date")
    print("   • helpful_count - Number of helpful votes")
    print()
    print("4. Tips:")
    print("   • Focus on most helpful/recent reviews")
    print("   • 5-10 reviews per company is usually sufficient")
    print("   • Copy exact text to preserve authenticity")
    print("   • Note any patterns or common themes")
    print()
    print("5. After collection:")
    print("   • Save the completed CSV")
    print("   • Use it for sentiment analysis")
    print("   • Keep original for reference")
    print()
    print("=" * 70)
    print()
    print(f"Total companies: {len(companies)}")
    print(f"Template rows: {len(template_rows)} (10 per company)")
    print()
    print("💡 TIP: You can reduce review_number rows if you need fewer reviews")
    print("        per company. Just delete rows or adjust the script.")
    print()


if __name__ == "__main__":
    create_template()

