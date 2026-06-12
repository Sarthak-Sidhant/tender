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

import threading
FAILED_PAGES_LOCK = threading.Lock()
FAILED_PAGES_LIST = []

TEST_MODE = False
IS_RETRY_PHASE = False

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
    global TEST_MODE, IS_RETRY_PHASE
    if TEST_MODE and not IS_RETRY_PHASE and "page=0" in url:
        logger.warning(f"[TEST MODE] Simulating network timeout for: {url}")
        return None
        
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
    import hashlib
    name_hash = hashlib.md5(name.encode("utf-8")).hexdigest()[:8]
    output_dir = os.path.join("results", ptype, status)
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = os.path.join(output_dir, f"{clean_name_dir}_{name_hash}.json")
    
    # Check if a local version exists (e.g. org/Name/status.json or state/Name/status.json)
    local_paths = [
        os.path.join(ptype, name, f"{status}.json"),
        os.path.join(ptype, clean_name_dir, f"{status}.json")
    ]
    for local_path in local_paths:
        if os.path.exists(local_path):
            logger.info(f"Found local data for {name} ({status}) at {local_path}. Copying to results...")
            import shutil
            try:
                shutil.copy2(local_path, output_file)
                return
            except Exception as e:
                logger.error(f"Failed to copy local data from {local_path}: {e}")

    # If already scraped in a previous run, skip
    if os.path.exists(output_file):
        logger.info(f"[{ptype}][{status}] {name} already exists. Skipping.")
        return
        
    session = requests.Session()
    session.cookies.set("cookieWorked", "yes", domain="eprocure.gov.in", path="/")
    
    all_records = []
    seen_internal_ids = set()
    page_num = 0
    consecutive_no_new = 0
    failed_attempts = 0
    
    logger.info(f"Scraping '{name}' [{status}] ({ptype})")
    
    while True:
        if time.time() - START_TIME > MAX_RUN_TIME:
            logger.warning(f"Approaching 6-hour limit. Stopping crawl for '{name}' at page {page_num} to preserve cached progress.")
            with FAILED_PAGES_LOCK:
                FAILED_PAGES_LIST.append({
                    "name": name,
                    "hash": b64_hash,
                    "status": status,
                    "type": ptype,
                    "page": page_num
                })
            break

        if page_num == 1:
            page_num += 1
            continue
            
        url = f"https://eprocure.gov.in/cppp/tendersearch/{endpoint}/{b64_hash}?page={page_num}"
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
        
        res = fetch_page_with_retry(session, url, headers)
        if not res or res.status_code != 200:
            failed_attempts += 1
            logger.warning(f"Failed to fetch page {page_num} for '{name}' (status: {res.status_code if res else 'Timeout'}). logging to retry queue and skipping page.")
            
            with FAILED_PAGES_LOCK:
                FAILED_PAGES_LIST.append({
                    "name": name,
                    "hash": b64_hash,
                    "status": status,
                    "type": ptype,
                    "page": page_num
                })
                
            if failed_attempts >= 5:
                logger.error(f"Too many page failures ({failed_attempts}) for '{name}'. Stopping scrape.")
                break
            page_num += 1
            continue
            
        records = parse_tenders_from_html(res.text)
        if not records:
            logger.info(f"Reached end of records (empty page) for '{name}' at page {page_num}.")
            break
            
        # Keep track of seen IDs only for page-loop termination detection
        has_new = False
        for r in records:
            internal_id = r["internal_id"]
            if internal_id not in seen_internal_ids:
                seen_internal_ids.add(internal_id)
                has_new = True
                
        all_records.extend(records)
                
        if not has_new:
            consecutive_no_new += 1
            # Allow up to 12 consecutive pages with duplicate records before concluding we hit the end
            if consecutive_no_new >= 12:
                logger.info(f"Stopping crawl for '{name}': encountered 12 consecutive pages of duplicates.")
                break
        else:
            consecutive_no_new = 0
            
        if len(records) < 10:
            logger.info(f"Reached last page (less than 10 records: {len(records)}) for '{name}' at page {page_num}.")
            break
            
        page_num += 1

    if all_records:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(all_records, f, indent=4, ensure_ascii=False)
        logger.info(f"Saved {len(all_records)} records for {name} to {output_file}")
        if failed_attempts > 0:
            logger.warning(f"Saved partial records for {name} ({len(all_records)} records). {failed_attempts} pages failed to fetch.")
    else:
        logger.info(f"No records found for {name}")

def retry_failed_pages_in_partition(failed_items, headers):
    global IS_RETRY_PHASE
    IS_RETRY_PHASE = True
    if not failed_items:
        return []
    logger.info(f"Retrying {len(failed_items)} failed pages for this partition...")
    session = requests.Session()
    session.cookies.set("cookieWorked", "yes", domain="eprocure.gov.in", path="/")
    
    still_failed = []
    for item in failed_items:
        name = item["name"]
        b64_hash = item["hash"]
        status = item["status"]
        ptype = item["type"]
        page_num = item["page"]
        
        endpoint = "statedata" if ptype == "state" else "cpppdata"
        url = f"https://eprocure.gov.in/cppp/tendersearch/{endpoint}/{b64_hash}?page={page_num}"
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
        
        logger.info(f"Retrying page: '{name}' [{status}] ({ptype}) Page {page_num}")
        res = fetch_page_with_retry(session, url, headers)
        if res and res.status_code == 200:
            records = parse_tenders_from_html(res.text)
            if records:
                # Merge into the existing JSON file
                clean_name_dir = clean_filename(name)
                import hashlib
                name_hash = hashlib.md5(name.encode("utf-8")).hexdigest()[:8]
                output_dir = os.path.join("results", ptype, status)
                output_file = os.path.join(output_dir, f"{clean_name_dir}_{name_hash}.json")
                
                existing_records = []
                if os.path.exists(output_file):
                    try:
                        with open(output_file, "r", encoding="utf-8") as f:
                            existing_records = json.load(f)
                    except Exception as e:
                        logger.error(f"Failed to load {output_file}: {e}")
                        
                merged_records = {r["internal_id"]: r for r in existing_records}
                for r in records:
                    merged_records[r["internal_id"]] = r
                    
                with open(output_file, "w", encoding="utf-8") as f:
                    json.dump(list(merged_records.values()), f, indent=4, ensure_ascii=False)
                logger.info(f"Successfully recovered and saved page {page_num} for '{name}'")
            else:
                logger.warning(f"Retried page {page_num} for '{name}' but found no records. Removing from retry list.")
        else:
            still_failed.append(item)
            
    return still_failed

def main():
    parser = argparse.ArgumentParser(description="Load-balanced concurrent CPPP tender scraper partition.")
    parser.add_argument("--job-index", type=int, required=True, help="Index of the GHA runner (0-based)")
    parser.add_argument("--total-jobs", type=int, required=True, help="Total parallel GHA runners")
    parser.add_argument("--test", action="store_true", help="Run in test mode with a small task batch and simulated failure")
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
    
    if args.test:
        global TEST_MODE
        TEST_MODE = True
        # Find small tasks with weight == 1 (1 page)
        small_tasks = [t for t in my_tasks if t.get("weight", 1) == 1]
        if not small_tasks:
            small_tasks = my_tasks[:2]
        my_tasks = small_tasks[:2]
        
    my_weight = sum(t["weight"] for t in my_tasks)
    
    # Load previously failed pages that belong to this runner's tasks
    my_failed_pages = []
    if os.path.exists("failed_pages.json"):
        try:
            with open("failed_pages.json", "r", encoding="utf-8") as f:
                all_failed = json.load(f)
                my_task_names = {t["name"] for t in my_tasks}
                my_failed_pages = [item for item in all_failed if item.get("name") in my_task_names]
            logger.info(f"Loaded {len(my_failed_pages)} previously failed pages for this partition.")
        except Exception as e:
            logger.error(f"Failed to load failed_pages.json: {e}")
            
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

    logger.info(f"Runner {args.job_index} completed all assigned tasks. Starting retry phase...")
    
    # Gather failed pages from this run + previously failed pages for this partition
    to_retry = []
    to_retry.extend(my_failed_pages)
    with FAILED_PAGES_LOCK:
        to_retry.extend(FAILED_PAGES_LIST)
        
    # Remove duplicates
    unique_to_retry = []
    seen = set()
    for item in to_retry:
        key = (item["name"], item["status"], item["page"])
        if key not in seen:
            seen.add(key)
            unique_to_retry.append(item)
            
    # Run the retry phase
    still_failed = retry_failed_pages_in_partition(unique_to_retry, headers)
    
    # Save still_failed to results/failed_pages_{job_index}.json
    if still_failed:
        os.makedirs("results", exist_ok=True)
        failed_pages_file = os.path.join("results", f"failed_pages_{args.job_index}.json")
        try:
            with open(failed_pages_file, "w", encoding="utf-8") as f:
                json.dump(still_failed, f, indent=4, ensure_ascii=False)
            logger.info(f"Saved {len(still_failed)} failed page records to {failed_pages_file}")
        except Exception as e:
            logger.error(f"Failed to save failed pages file: {e}")

if __name__ == "__main__":
    main()
