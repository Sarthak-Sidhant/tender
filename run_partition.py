#!/usr/bin/env python3
import os
import re
import json
import time
import random
import logging
import sys
import math
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("PartitionScraper")

MAX_THREADS = 5
MAX_RETRIES = 5
BACKOFF_FACTOR = 2
MIN_DELAY = 0.5
MAX_DELAY = 1.5

# Time tracking for preventing job timeouts (6-hour limit in GitHub Actions)
START_TIME = time.time()
MAX_RUN_TIME = 5.2 * 3600  # 5.2 hours in seconds

def clean_filename(name):
    for char in ['/', '\\', ':', '*', '?', '"', '<', '>', '|']:
        name = name.replace(char, '_')
    return name.strip()

def load_hashes_and_weights():
    # Load all hashes
    active_orgs = {}
    archived_orgs = {}
    active_states = {}
    archived_states = {}
    
    if os.path.exists("active_hashes.json"):
        with open("active_hashes.json", "r", encoding="utf-8") as f:
            active_orgs = json.load(f)
    if os.path.exists("archived_hashes.json"):
        with open("archived_hashes.json", "r", encoding="utf-8") as f:
            archived_orgs = json.load(f)
    if os.path.exists("active_state_hashes.json"):
        with open("active_state_hashes.json", "r", encoding="utf-8") as f:
            active_states = json.load(f)
    if os.path.exists("archived_state_hashes.json"):
        with open("archived_state_hashes.json", "r", encoding="utf-8") as f:
            archived_states = json.load(f)

    # Load weights from tenders_summary.json
    weights = {"organizations": {}, "states": {}}
    if os.path.exists("tenders_summary.json"):
        try:
            with open("tenders_summary.json", "r", encoding="utf-8") as f:
                summary = json.load(f)
                weights["organizations"] = summary.get("organizations", {})
                weights["states"] = summary.get("states", {})
        except Exception as e:
            logger.warning(f"Could not load weights from tenders_summary.json: {e}")

    # Build flat list of all tasks
    tasks = []
    
    # 1. Add org tasks
    for org, info in active_orgs.items():
        count = weights["organizations"].get(org, {}).get("active", 0)
        weight = math.ceil(count / 10.0) if count > 0 else 1
        tasks.append({"name": org, "hash": info["hash"], "status": "active", "type": "org", "weight": weight})
        
    for org, info in archived_orgs.items():
        count = weights["organizations"].get(org, {}).get("archived", 0)
        weight = math.ceil(count / 10.0) if count > 0 else 1
        tasks.append({"name": org, "hash": info["hash"], "status": "archived", "type": "org", "weight": weight})

    # 2. Add state tasks
    for state, info in active_states.items():
        count = weights["states"].get(state, {}).get("active", 0)
        weight = math.ceil(count / 10.0) if count > 0 else 1
        tasks.append({"name": state, "hash": info["hash"], "status": "active", "type": "state", "weight": weight})
        
    for state, info in archived_states.items():
        count = weights["states"].get(state, {}).get("archived", 0)
        weight = math.ceil(count / 10.0) if count > 0 else 1
        tasks.append({"name": state, "hash": info["hash"], "status": "archived", "type": "state", "weight": weight})
        
    return tasks

def distribute_tasks_greedy(tasks, total_jobs):
    # Sort tasks descending by weight
    tasks_sorted = sorted(tasks, key=lambda x: x["weight"], reverse=True)
    
    # Initialize bins (total weight, list of tasks)
    bins = [{"total_weight": 0, "tasks": []} for _ in range(total_jobs)]
    
    for task in tasks_sorted:
        # Find the bin with the minimum weight
        min_bin = min(bins, key=lambda x: x["total_weight"])
        min_bin["tasks"].append(task)
        min_bin["total_weight"] += task["weight"]
        
    return bins

def parse_tenders_from_html(html_content):
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
                    
            if detail_url and "/tendersfullview/" in detail_url:
                try:
                    import base64
                    payload = detail_url.split("/tendersfullview/")[-1]
                    url_parts = payload.split("A13h1")
                    if url_parts:
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
    retries = 0
    while retries < MAX_RETRIES:
        try:
            res = session.get(url, headers=headers, timeout=20)
            if res.status_code == 429 or res.status_code >= 500:
                sleep_time = (BACKOFF_FACTOR ** retries) + random.uniform(1, 3)
                time.sleep(sleep_time)
                retries += 1
                continue
            return res
        except Exception:
            sleep_time = (BACKOFF_FACTOR ** retries) + random.uniform(1, 3)
            time.sleep(sleep_time)
            retries += 1
    return None

def scrape_task(task, headers):
    name = task["name"]
    b64_hash = task["hash"]
    status = task["status"]
    ptype = task["type"]
    
    if time.time() - START_TIME > MAX_RUN_TIME:
        logger.warning(f"Approaching 6-hour limit. Skipping execution for '{name}' to save progress cache.")
        return
        
    endpoint = "statedata" if ptype == "state" else "cpppdata"
    
    clean_name_dir = clean_filename(name)
    output_dir = os.path.join("results", ptype, status)
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = os.path.join(output_dir, f"{clean_name_dir}.json")
    
    # If already scraped in a previous run, skip
    if os.path.exists(output_file):
        logger.info(f"[{ptype}][{status}] {name} already exists. Skipping.")
        return
        
    session = requests.Session()
    session.cookies.set("cookieWorked", "yes", domain="eprocure.gov.in", path="/")
    
    all_records = []
    page_num = 0
    consecutive_no_new = 0
    failed_attempts = 0
    
    logger.info(f"Scraping '{name}' [{status}] ({ptype})")
    
    while True:
        if page_num == 1:
            page_num += 1
            continue
            
        url = f"https://eprocure.gov.in/cppp/tendersearch/{endpoint}/{b64_hash}?page={page_num}"
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
        
        res = fetch_page_with_retry(session, url, headers)
        if not res or res.status_code != 200:
            failed_attempts += 1
            logger.warning(f"Failed to fetch page {page_num} for '{name}' (status: {res.status_code if res else 'Timeout'}). skipping page.")
            if failed_attempts >= 5:
                logger.error(f"Too many page failures ({failed_attempts}) for '{name}'. Stopping scrape.")
                break
            page_num += 1
            continue
            
        records = parse_tenders_from_html(res.text)
        if not records:
            logger.info(f"Reached end of records (empty page) for '{name}' at page {page_num}.")
            break
            
        # Avoid duplicate entries on parsing
        new_records = []
        for r in records:
            if r["internal_id"] not in [x["internal_id"] for x in all_records]:
                new_records.append(r)
                
        if not new_records:
            consecutive_no_new += 1
            # Allow up to 12 consecutive pages with duplicate records before concluding we hit the end
            if consecutive_no_new >= 12:
                logger.info(f"Stopping crawl for '{name}': encountered 12 consecutive pages of duplicates.")
                break
        else:
            consecutive_no_new = 0
            all_records.extend(new_records)
            
        if len(records) < 10:
            logger.info(f"Reached last page (less than 10 records: {len(records)}) for '{name}' at page {page_num}.")
            break
            
        page_num += 1

    if all_records and failed_attempts == 0:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(all_records, f, indent=4, ensure_ascii=False)
        logger.info(f"Saved {len(all_records)} records for {name} to {output_file}")
    elif failed_attempts > 0:
        logger.error(f"Failed to scrape all pages successfully for {name} due to {failed_attempts} page failures. Output file NOT saved to ensure data integrity.")
    else:
        logger.info(f"No records found for {name}")

def main():
    parser = argparse.ArgumentParser(description="Load-balanced concurrent CPPP tender scraper partition.")
    parser.add_argument("--job-index", type=int, required=True, help="Index of the GHA runner (0-based)")
    parser.add_argument("--total-jobs", type=int, required=True, help="Total parallel GHA runners")
    args = parser.parse_args()
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://eprocure.gov.in/cppp/"
    }
    
    tasks = load_hashes_and_weights()
    bins = distribute_tasks_greedy(tasks, args.total_jobs)
    
    my_bin = bins[args.job_index]
    my_tasks = my_bin["tasks"]
    my_weight = my_bin["total_weight"]
    
    logger.info("=" * 60)
    logger.info(f"Runner {args.job_index}/{args.total_jobs} starting.")
    logger.info(f"Assigned tasks: {len(my_tasks)} (Estimated pages: {my_weight})")
    logger.info("=" * 60)
    
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(scrape_task, task, headers): task for task in my_tasks}
        for future in as_completed(futures):
            task = futures[future]
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error scraping task {task['name']}: {e}")

    logger.info(f"Runner {args.job_index} completed all assigned tasks.")

if __name__ == "__main__":
    main()
