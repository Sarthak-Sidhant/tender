#!/usr/bin/env python3
import requests
from bs4 import BeautifulSoup
import json
import base64
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("HashGenerator")

STATUS_MAP = {
    "active": "tender",
    "archived": "archivedtenders"
}

def fetch_org_list(session, status_value, form_build_id, form_id, headers):
    ajax_url = "https://eprocure.gov.in/cppp/tendersearch/cpppdata?ajax_form=1"
    ajax_headers = headers.copy()
    ajax_headers["Accept"] = "application/json, text/javascript, */*; q=0.01"
    ajax_headers["X-Requested-With"] = "XMLHttpRequest"
    
    payload = {
        "s_type": status_value,
        "form_build_id": form_build_id,
        "form_id": form_id,
        "_triggering_element_name": "s_type"
    }
    
    try:
        res = session.post(ajax_url, data=payload, headers=ajax_headers, timeout=20)
        if res.status_code == 200:
            data = res.json()
            new_build_id = form_build_id
            for cmd in data:
                if cmd.get("command") == "update_build_id" and cmd.get("new"):
                    new_build_id = cmd.get("new")
            
            for cmd in data:
                if cmd.get("command") == "insert" and cmd.get("data") and "second_field_wrapper1" in cmd.get("data"):
                    soup = BeautifulSoup(cmd.get("data"), "html.parser")
                    options = soup.find_all("option")
                    orgs = [opt.get("value").strip() for opt in options if opt.get("value") and opt.get("value").strip() != "" and opt.get("value") != "select"]
                    return orgs, new_build_id
    except Exception as e:
        logger.error(f"Error fetching organizations for {status_value}: {e}")
    return [], form_build_id

def main():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    session = requests.Session()
    
    logger.info("Accessing portal...")
    res = session.get("https://eprocure.gov.in/cppp/tendersearch/cpppdata", headers=headers)
    soup = BeautifulSoup(res.text, "html.parser")
    
    form_build_id = soup.find("input", {"name": "form_build_id"}).get("value")
    form_id = soup.find("input", {"name": "form_id"}).get("value")
    
    results = {}
    
    for status_name, status_value in STATUS_MAP.items():
        orgs, form_build_id = fetch_org_list(session, status_value, form_build_id, form_id, headers)
        logger.info(f"Retrieved {len(orgs)} organizations for '{status_name}'")
        
        status_hashes = {}
        for org in orgs:
            # Generate the hash by prepending the static 'by' prefix to the standard Base64 representation
            raw_str = f"{status_value}A13h1{org}A13h1selectA13h1nullA13h1null"
            encoded_bytes = base64.b64encode(raw_str.encode('utf-8'))
            b64_hash = "by" + encoded_bytes.decode('utf-8')
            
            status_hashes[org] = {
                "hash": b64_hash,
                "url": f"https://eprocure.gov.in/cppp/tendersearch/cpppdata/{b64_hash}"
            }
        
        results[status_name] = status_hashes
        
        # Save individual JSON files
        filename = f"{status_name}_hashes.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(status_hashes, f, indent=4, ensure_ascii=False)
        logger.info(f"Saved {filename}")

if __name__ == "__main__":
    main()
