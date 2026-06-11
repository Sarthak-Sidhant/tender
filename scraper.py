#!/usr/bin/env python3
"""
eprocure.gov.in CPPP Tender Scraper (Direct Scrape via Pre-generated Hashes)
Uses pre-generated search hashes to directly scrape tender records without CAPTCHA.
Supports:
- Concurrency (ThreadPoolExecutor) for faster downloads
- Resiliency (Retries with Exponential Backoff)
- Rate limiting / Jittered delays
- State persistence (SQLite-based checkpointing & resumption)
- Dual storage: Saves to both SQLite database and directory-based JSON files
"""

import os
import sys
import time
import json
import random
import logging
import sqlite3
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("scraper.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("TenderScraper")

# Global Configuration
DB_FILE = "tenders.db"
DB_LOCK = threading.Lock()
MAX_THREADS = 5  # Adjust based on system/network limits (keep moderate to avoid IP bans)
MAX_RETRIES = 5
BACKOFF_FACTOR = 2
MIN_DELAY = 0.5
MAX_DELAY = 1.5

def clean_filename(name):
    """Replaces characters that are invalid in folder/file names."""
    for char in ['/', '\\', ':', '*', '?', '"', '<', '>', '|']:
        name = name.replace(char, '_')
    return name.strip()

def init_db():
    """Initializes the SQLite database with WAL mode and necessary tables."""
    with DB_LOCK:
        conn = sqlite3.connect(DB_FILE, timeout=30.0)
        # Enable Write-Ahead Logging for better concurrency
        conn.execute("PRAGMA journal_mode=WAL;")
        cursor = conn.cursor()
        
        # Check if we need to migrate/recreate because of new schema
        cursor.execute("PRAGMA table_info(tenders)")
        cols = [row[1] for row in cursor.fetchall()]
        if cols and 'internal_id' not in cols:
            cursor.execute("DROP TABLE tenders")
            cursor.execute("DROP INDEX IF EXISTS idx_tenders_org_status")
        
        # Table for storing tenders
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tenders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                internal_id TEXT UNIQUE,
                serial_number TEXT,
                title TEXT,
                reference_number TEXT,
                tender_id TEXT,
                e_published_date TEXT,
                bid_submission_closing_date TEXT,
                tender_opening_date TEXT,
                organisation_name TEXT,
                status TEXT,
                detail_url TEXT,
                corrigendum_url TEXT,
                scraped_at TEXT
            )
        """)
        
        # Index on organisation and status for faster lookups
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tenders_org_status ON tenders (organisation_name, status)")
        
        # Table for tracking progress checkpoints
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS progress (
                organisation TEXT,
                status TEXT,
                last_scraped_at TEXT,
                pages_scraped INTEGER,
                record_count INTEGER,
                completed INTEGER,
                PRIMARY KEY (organisation, status)
            )
        """)
        conn.commit()
        conn.close()

def get_completed_tasks():
    """Retrieves already completed status combinations to resume scraping."""
    completed = set()
    with DB_LOCK:
        conn = sqlite3.connect(DB_FILE, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute("SELECT organisation, status FROM progress WHERE completed = 1")
        for row in cursor.fetchall():
            completed.add((row[0], row[1]))
        conn.close()
    return completed

def update_progress(org, status, pages, records, completed=0):
    """Updates progress status in the DB."""
    now_str = datetime.now().isoformat()
    with DB_LOCK:
        conn = sqlite3.connect(DB_FILE, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO progress (organisation, status, last_scraped_at, pages_scraped, record_count, completed)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (org, status, now_str, pages, records, completed))
        conn.commit()
        conn.close()

def save_tenders_to_db(records, status):
    """Saves parsed records to the SQLite database."""
    if not records:
        return
    now_str = datetime.now().isoformat()
    with DB_LOCK:
        conn = sqlite3.connect(DB_FILE, timeout=30.0)
        cursor = conn.cursor()
        for r in records:
            cursor.execute("""
                INSERT OR REPLACE INTO tenders (
                    internal_id, serial_number, title, reference_number, tender_id,
                    e_published_date, bid_submission_closing_date, tender_opening_date,
                    organisation_name, status, detail_url, corrigendum_url, scraped_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                r.get("internal_id"),
                r.get("serial_number"),
                r.get("title"),
                r.get("reference_number"),
                r.get("tender_id"),
                r.get("e_published_date"),
                r.get("bid_submission_closing_date"),
                r.get("tender_opening_date"),
                r.get("organisation_name"),
                status,
                r.get("detail_url"),
                r.get("corrigendum_url"),
                now_str
            ))
        conn.commit()
        conn.close()

def parse_tenders_from_html(html_content):
    """Parses CPPP HTML search results and returns list of tender dicts."""
    soup = BeautifulSoup(html_content, "html.parser")
    table = soup.find("table", {"id": "table"})
    if not table:
        return []

    records = []
    rows = table.find_all("tr")
    
    for row in rows:
        if row.find("th") or not row.find("td"):
            continue
            
        tds = row.find_all("td")
        if len(tds) < 7:
            continue
            
        sl_no_raw = tds[0].get_text(strip=True)
        try:
            sl_no = int(sl_no_raw.rstrip("."))
        except ValueError:
            sl_no = sl_no_raw
            
        e_published_date = tds[1].get_text(strip=True)
        bid_closing_date = tds[2].get_text(strip=True)
        tender_opening_date = tds[3].get_text(strip=True)
        
        title_td = tds[4]
        a_tag = title_td.find("a")
        
        title = ""
        detail_url = ""
        ref_no = ""
        tender_id = ""
        internal_id = ""
        
        if a_tag:
            title = a_tag.get_text(strip=True)
            detail_url = a_tag.get("href", "")
            if detail_url and detail_url.startswith("/"):
                detail_url = "https://eprocure.gov.in" + detail_url
                
            full_td_text = title_td.get_text(strip=True)
            remaining_text = full_td_text.replace(title, "").strip("/")
            
            parts = [p.strip() for p in remaining_text.split("/") if p.strip()]
            if parts:
                tender_id = parts[-1]
                ref_no = "/".join(parts[:-1]) if len(parts) > 1 else ""
                if not ref_no and len(parts) == 1:
                    ref_no = parts[0]
                    
            # Extract internal_id from detail_url
            if detail_url and "/tendersfullview/" in detail_url:
                try:
                    import base64
                    payload = detail_url.split("/tendersfullview/")[-1]
                    url_parts = payload.split("A13h1")
                    if url_parts:
                        # Helper for padded b64 decoding
                        def decode_b64(s):
                            s = s.strip()
                            s += "=" * ((4 - len(s) % 4) % 4)
                            return base64.b64decode(s).decode("utf-8", errors="ignore").strip()
                        
                        internal_id = decode_b64(url_parts[0])
                except Exception:
                    pass
        else:
            raw_text = title_td.get_text(strip=True)
            parts = [p.strip() for p in raw_text.split("/") if p.strip()]
            if parts:
                title = parts[0]
                tender_id = parts[-1]
                ref_no = "/".join(parts[1:-1]) if len(parts) > 2 else ""

        if not internal_id:
            internal_id = tender_id

        org_name = tds[5].get_text(strip=True)
        
        corrigendum_td = tds[6]
        corr_a_tag = corrigendum_td.find("a")
        corrigendum_url = ""
        if corr_a_tag:
            corrigendum_url = corr_a_tag.get("href", "")
            if corrigendum_url and corrigendum_url.startswith("/"):
                corrigendum_url = "https://eprocure.gov.in" + corrigendum_url
        else:
            corrigendum_url = corrigendum_td.get_text(strip=True)
            if corrigendum_url == "--":
                corrigendum_url = None
                
        record = {
            "serial_number": sl_no,
            "title": title,
            "reference_number": ref_no,
            "tender_id": tender_id,
            "internal_id": internal_id,
            "e_published_date": e_published_date,
            "bid_submission_closing_date": bid_closing_date,
            "tender_opening_date": tender_opening_date,
            "organisation_name": org_name,
            "detail_url": detail_url,
            "corrigendum_url": corrigendum_url
        }
        records.append(record)
        
    return records

def fetch_page_with_retry(session, url, headers):
    """Fetches a URL with exponential backoff on retryable failures."""
    retries = 0
    while retries < MAX_RETRIES:
        try:
            res = session.get(url, headers=headers, timeout=20)
            
            # If rate limited (429) or server error (5xx), apply backoff
            if res.status_code == 429:
                sleep_time = (BACKOFF_FACTOR ** retries) + random.uniform(1, 3)
                logger.warning(f"Rate limited (429). Retrying in {sleep_time:.2f}s...")
                time.sleep(sleep_time)
                retries += 1
                continue
            elif res.status_code >= 500:
                sleep_time = (BACKOFF_FACTOR ** retries) + random.uniform(1, 3)
                logger.warning(f"Server Error {res.status_code}. Retrying in {sleep_time:.2f}s...")
                time.sleep(sleep_time)
                retries += 1
                continue
                
            return res
        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            sleep_time = (BACKOFF_FACTOR ** retries) + random.uniform(1, 3)
            logger.warning(f"Request error ({e}). Retrying in {sleep_time:.2f}s...")
            time.sleep(sleep_time)
            retries += 1
            
    logger.error(f"Failed to fetch {url} after {MAX_RETRIES} attempts.")
    return None

def scrape_organisation(org, info, status_name, headers):
    """Scrapes all pages of tenders for a single organisation/status combination."""
    b64_hash = info["hash"]
    session = requests.Session()
    # Configure required session cookies
    session.cookies.set("cookieWorked", "yes", domain="eprocure.gov.in", path="/")
    
    all_records = []
    page_num = 0
    consecutive_no_new = 0
    failed_attempts = 0
    
    logger.info(f"Starting scrape: '{org}' [{status_name}]")
    
    while True:
        if page_num == 1:
            page_num += 1
            continue
            
        url = f"https://eprocure.gov.in/cppp/tendersearch/cpppdata/{b64_hash}?page={page_num}"
        
        # Apply jittered delay between requests to be gentle on the server
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
        
        res = fetch_page_with_retry(session, url, headers)
        if not res or res.status_code != 200:
            failed_attempts += 1
            logger.warning(f"Failed to fetch page {page_num} for '{org}' (status: {res.status_code if res else 'Timeout'}). skipping page.")
            if failed_attempts >= 5:
                logger.error(f"Too many page failures ({failed_attempts}) for '{org}'. Stopping pagination.")
                break
            page_num += 1
            continue
            
        records = parse_tenders_from_html(res.text)
        if not records:
            logger.info(f"Reached end of records (empty page) for '{org}' at page {page_num}.")
            break
            
        # Extract new records, skipping any that are already seen
        new_records = []
        for r in records:
            if r["internal_id"] not in [x["internal_id"] for x in all_records]:
                new_records.append(r)
                
        if not new_records:
            consecutive_no_new += 1
            # Allow up to 12 consecutive pages with duplicate records before concluding we hit the end
            if consecutive_no_new >= 12:
                logger.info(f"Stopping pagination for {org}: encountered 12 consecutive pages of duplicates.")
                break
        else:
            consecutive_no_new = 0
            all_records.extend(new_records)
            logger.debug(f"[{org}] Page {page_num}: Extracted {len(new_records)} new records.")
            
        if len(records) < 10:
            logger.info(f"Reached last page (less than 10 records: {len(records)}) for '{org}' at page {page_num}.")
            break
            
        page_num += 1

    # Save Results
    if all_records:
        # Save to DB
        save_tenders_to_db(all_records, status_name)
        
        # Save to JSON File
        org_dir = clean_filename(org)
        os.makedirs(org_dir, exist_ok=True)
        json_file = os.path.join(org_dir, f"{status_name}.json")
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(all_records, f, indent=4, ensure_ascii=False)
            
        logger.info(f"Finished '{org}' [{status_name}]: Saved {len(all_records)} records (DB & JSON)")
        update_progress(org, status_name, page_num + 1, len(all_records), completed=1)
    else:
        logger.info(f"Finished '{org}' [{status_name}]: No records found.")
        update_progress(org, status_name, page_num + 1, 0, completed=1)

def main():
    # Load hashes
    active_hashes = {}
    archived_hashes = {}
    
    if os.path.exists("active_hashes.json"):
        with open("active_hashes.json", "r", encoding="utf-8") as f:
            active_hashes = json.load(f)
            
    if os.path.exists("archived_hashes.json"):
        with open("archived_hashes.json", "r", encoding="utf-8") as f:
            archived_hashes = json.load(f)
            
    all_targets = []
    for org, info in active_hashes.items():
        all_targets.append((org, info, "active"))
    for org, info in archived_hashes.items():
        all_targets.append((org, info, "archived"))
        
    if not all_targets:
        logger.error("No active_hashes.json or archived_hashes.json files found. Run generate_hashes.py first.")
        sys.exit(1)
        
    # Initialize SQLite Database
    init_db()
    
    # Filter out already completed tasks for checkpoint resumption
    completed_tasks = get_completed_tasks()
    remaining_targets = [
        (org, info, status) for org, info, status in all_targets
        if (org, status) not in completed_tasks
    ]
    
    logger.info(f"Total tasks: {len(all_targets)}. Already completed: {len(completed_tasks)}. Remaining tasks to scrape: {len(remaining_targets)}")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5"
    }
    
    # Run Scrapers concurrently using ThreadPoolExecutor
    if remaining_targets:
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = {
                executor.submit(scrape_organisation, org, info, status, headers): (org, status)
                for org, info, status in remaining_targets
            }
            
            for future in as_completed(futures):
                org, status = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    logger.error(f"Organisation '{org}' [{status}] generated an exception: {exc}")
                    # Mark task as failed/incomplete in progress table
                    update_progress(org, status, 0, 0, completed=0)

    logger.info("Scraping operation finished.")

if __name__ == "__main__":
    main()
