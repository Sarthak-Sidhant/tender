#!/usr/bin/env python3
import os
import re
import json
import time
import random
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("count_tenders.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("CountTenders")

CACHE_FILE = "tender_counts_cache.json"
MAX_THREADS = 12
MAX_RETRIES = 5
BACKOFF_FACTOR = 2

def load_hashes():
    # Load organization hashes
    active_org_hashes = {}
    archived_org_hashes = {}
    if os.path.exists("active_hashes.json"):
        with open("active_hashes.json", "r", encoding="utf-8") as f:
            active_org_hashes = json.load(f)
    if os.path.exists("archived_hashes.json"):
        with open("archived_hashes.json", "r", encoding="utf-8") as f:
            archived_org_hashes = json.load(f)

    # Load state hashes
    active_state_hashes = {}
    archived_state_hashes = {}
    if os.path.exists("active_state_hashes.json"):
        with open("active_state_hashes.json", "r", encoding="utf-8") as f:
            active_state_hashes = json.load(f)
    if os.path.exists("archived_state_hashes.json"):
        with open("archived_state_hashes.json", "r", encoding="utf-8") as f:
            archived_state_hashes = json.load(f)
            
    return active_org_hashes, archived_org_hashes, active_state_hashes, archived_state_hashes

def fetch_total_tenders(name, b64_hash, status, portal_type):
    # Determine the correct URL endpoint base
    # portal_type is either 'org' (cpppdata) or 'state' (statedata)
    endpoint = "statedata" if portal_type == "state" else "cpppdata"
    url = f"https://eprocure.gov.in/cppp/tendersearch/{endpoint}/{b64_hash}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://eprocure.gov.in/cppp/"
    }
    
    session = requests.Session()
    session.cookies.set("cookieWorked", "yes", domain="eprocure.gov.in", path="/")
    
    retries = 0
    while retries < MAX_RETRIES:
        try:
            time.sleep(random.uniform(0.1, 0.4))
            res = session.get(url, headers=headers, timeout=15)
            
            if res.status_code == 429 or res.status_code >= 500:
                sleep_time = (BACKOFF_FACTOR ** retries) + random.uniform(1, 3)
                time.sleep(sleep_time)
                retries += 1
                continue
                
            if res.status_code == 200:
                match = re.search(r"Total\s+Tenders\s*:\s*(\d+)", res.text, re.IGNORECASE)
                if match:
                    count = int(match.group(1))
                    return name, status, portal_type, count
                else:
                    if "No records found" in res.text or "No active tender" in res.text:
                        return name, status, portal_type, 0
                    soup = BeautifulSoup(res.text, "html.parser")
                    text_matches = soup.find_all(string=re.compile("Total Tenders", re.I))
                    for m in text_matches:
                        inner_match = re.search(r"Total\s+Tenders\s*:\s*(\d+)", m, re.IGNORECASE)
                        if inner_match:
                            return name, status, portal_type, int(inner_match.group(1))
                    
                    return name, status, portal_type, 0
            
            return name, status, portal_type, None
            
        except Exception as e:
            sleep_time = (BACKOFF_FACTOR ** retries) + random.uniform(1, 3)
            time.sleep(sleep_time)
            retries += 1
            
    return name, status, portal_type, None

def main():
    active_orgs, archived_orgs, active_states, archived_states = load_hashes()
    
    cache = {}
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                cache = json.load(f)
            logger.info(f"Loaded {len(cache)} cached counts from {CACHE_FILE}")
        except Exception as e:
            logger.error(f"Error reading cache: {e}")

    tasks = []
    
    # 1. Populate organization tasks
    for org, info in active_orgs.items():
        cache_key = f"{org}||active||org"
        if cache_key not in cache or cache[cache_key] is None:
            tasks.append((org, info["hash"], "active", "org"))
            
    for org, info in archived_orgs.items():
        cache_key = f"{org}||archived||org"
        if cache_key not in cache or cache[cache_key] is None:
            tasks.append((org, info["hash"], "archived", "org"))

    # 2. Populate state tasks
    for state, info in active_states.items():
        cache_key = f"{state}||active||state"
        if cache_key not in cache or cache[cache_key] is None:
            tasks.append((state, info["hash"], "active", "state"))
            
    for state, info in archived_states.items():
        cache_key = f"{state}||archived||state"
        if cache_key not in cache or cache[cache_key] is None:
            tasks.append((state, info["hash"], "archived", "state"))
            
    total_tasks = len(tasks)
    logger.info(f"Total tasks to fetch: {total_tasks} (Already cached: {len(cache)})")
    
    if total_tasks > 0:
        completed_count = 0
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            future_to_task = {
                executor.submit(fetch_total_tenders, name, b64_hash, status, ptype): (name, status, ptype)
                for name, b64_hash, status, ptype in tasks
            }
            
            for future in as_completed(future_to_task):
                name, status, ptype = future_to_task[future]
                try:
                    result = future.result()
                    if result is not None:
                        res_name, res_status, res_type, count = result
                        cache_key = f"{res_name}||{res_status}||{res_type}"
                        cache[cache_key] = count
                except Exception as exc:
                    logger.error(f"{name} ({status}/{ptype}) generated an exception: {exc}")
                
                completed_count += 1
                if completed_count % 50 == 0 or completed_count == total_tasks:
                    with open(CACHE_FILE, "w", encoding="utf-8") as f:
                        json.dump(cache, f, indent=4, ensure_ascii=False)
                    logger.info(f"Progress: {completed_count}/{total_tasks} tasks done. Cache updated.")

    # Aggregate results
    org_aggregation = {}
    state_aggregation = {}
    
    total_org_active = 0
    total_org_archived = 0
    total_state_active = 0
    total_state_archived = 0
    
    # Deduplicate cache keys: prefer 3-part keys over 2-part legacy keys
    deduped_cache = {}
    for key, count in cache.items():
        if count is None:
            continue
        parts = key.split("||")
        if len(parts) == 2:
            name, status = parts
            new_key = f"{name}||{status}||org"
            if new_key not in cache:
                deduped_cache[new_key] = count
        else:
            deduped_cache[key] = count

    for key, count in deduped_cache.items():
        parts = key.split("||")
        name, status, ptype = parts
            
        if ptype == "org":
            if name not in org_aggregation:
                org_aggregation[name] = {"active": 0, "archived": 0, "total": 0}
            org_aggregation[name][status] = count
            org_aggregation[name]["total"] += count
            if status == "active":
                total_org_active += count
            else:
                total_org_archived += count
        elif ptype == "state":
            if name not in state_aggregation:
                state_aggregation[name] = {"active": 0, "archived": 0, "total": 0}
            state_aggregation[name][status] = count
            state_aggregation[name]["total"] += count
            if status == "active":
                total_state_active += count
            else:
                total_state_archived += count

    summary = {
        "metadata": {
            "organizations": {
                "total_count": len(org_aggregation),
                "total_active_tenders": total_org_active,
                "total_archived_tenders": total_org_archived,
                "total_tenders": total_org_active + total_org_archived
            },
            "states": {
                "total_count": len(state_aggregation),
                "total_active_tenders": total_state_active,
                "total_archived_tenders": total_state_archived,
                "total_tenders": total_state_active + total_state_archived
            },
            "grand_total_tenders": (total_org_active + total_org_archived + total_state_active + total_state_archived),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        },
        "organizations": org_aggregation,
        "states": state_aggregation
    }
    
    with open("tenders_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=4, ensure_ascii=False)
        
    logger.info("=" * 60)
    logger.info("Aggregation complete!")
    logger.info(f"Organizations: {len(org_aggregation)} (Active: {total_org_active}, Archived: {total_org_archived})")
    logger.info(f"States: {len(state_aggregation)} (Active: {total_state_active}, Archived: {total_state_archived})")
    logger.info(f"Grand Total Tenders (Org + State): {summary['metadata']['grand_total_tenders']}")
    logger.info("Results saved to tenders_summary.json")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
