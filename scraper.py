import requests
import time
import random
import re
import logging
from bs4 import BeautifulSoup
from datetime import datetime

log = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0"}
TIMEOUT = 10

ENRICH_LIMIT = 100   # ✅ YOU REQUESTED THIS
DAYS_BACK = 10       # ✅ BIG FIX (more useful leads)

# =============================
# 🔹 HELPERS
# =============================

def safe_get(url):
    try:
        return requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    except:
        return None


def extract_phones(text):
    return re.findall(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", text)


def clean_phone(p):
    digits = re.sub(r"\D", "", p)
    return digits if len(digits) == 10 else None


# =============================
# 🔹 LEAD AGE LOGIC (CRITICAL FIX)
# =============================

def is_fresh_lead(lead):
    try:
        reg_date = datetime.strptime(lead["registration_date"], "%Y-%m-%d").date()
        return (datetime.utcnow().date() - reg_date).days <= 3
    except:
        return False


# =============================
# 🔹 YOUR ORIGINAL FETCH FUNCTION
# ⚠️ KEEP YOUR REAL VERSION HERE
# =============================

def fetch_new_registrations():
    log.warning("⚠️ Replace this with your real FMCSA fetch function")
    return []


# =============================
# 🔹 LAYERS
# =============================

def layer_fmcsa(lead):
    # your existing logic
    return lead


def layer_dot_report(lead):
    dot = lead["dot_number"]

    # 🔹 Try /usdot/ first
    url = f"https://dot.report/usdot/{dot}"
    r = safe_get(url)

    # 🔹 fallback to search
    if not r or r.status_code != 200:
        search_url = f"https://dot.report/search/?q={dot}"
        r = safe_get(search_url)
        if not r:
            return lead

        soup = BeautifulSoup(r.text, "lxml")
        link = soup.find("a", href=True)
        if not link:
            return lead

        carrier_url = link["href"]
        if not carrier_url.startswith("http"):
            carrier_url = "https://dot.report" + carrier_url

        r = safe_get(carrier_url)
        if not r:
            return lead

    text = BeautifulSoup(r.text, "lxml").get_text(" ")
    phones = extract_phones(text[:5000])

    for p in phones:
        cleaned = clean_phone(p)
        if cleaned:
            lead["phones_found"].append(cleaned)
            lead["sources_found"] += 1
            break

    time.sleep(random.uniform(1.0, 1.8))
    return lead


def layer_aggregators(lead):
    name = lead["company_name"].replace(" ", "+")
    url = f"https://www.bing.com/search?q={name}+trucking"

    r = safe_get(url)
    if not r:
        return lead

    text = BeautifulSoup(r.text, "lxml").get_text(" ")
    phones = extract_phones(text[:3000])

    for p in phones:
        cleaned = clean_phone(p)
        if cleaned:
            lead["phones_found"].append(cleaned)
            lead["sources_found"] += 1
            break

    time.sleep(random.uniform(1.0, 1.5))
    return lead


# =============================
# 🔹 FINALIZE
# =============================

def finalize(lead):
    phones = lead["phones_found"]

    if not phones:
        lead["phone"] = ""
        lead["phone_confidence"] = "none"
        return lead

    freq = {}
    for p in phones:
        freq[p] = freq.get(p, 0) + 1

    best = max(freq, key=freq.get)
    lead["phone"] = best

    if freq[best] >= 2:
        lead["phone_confidence"] = "high"
    else:
        lead["phone_confidence"] = "medium"

    return lead


# =============================
# 🔹 ENRICH PIPELINE
# =============================

def enrich_lead(raw):
    lead = {
        "dot_number": raw.get("dot_number"),
        "company_name": raw.get("company_name", ""),
        "registration_date": raw.get("registration_date", ""),
        "phones_found": [],
        "sources_found": 0,
        "phone": "",
        "phone_confidence": "none"
    }

    # 🔥 CRITICAL: Fresh leads = limited enrichment
    if is_fresh_lead(lead):
        lead = layer_fmcsa(lead)
        return finalize(lead)

    # ── Layer 1
    lead = layer_fmcsa(lead)

    # ── Layer 2 (DOT.report)
    if lead["sources_found"] < 2:
        lead = layer_dot_report(lead)

    # ── Layer 3 (Aggregators)
    if lead["sources_found"] < 2:
        lead = layer_aggregators(lead)

    return finalize(lead)


# =============================
# 🔹 MAIN ENTRY (DO NOT CHANGE)
# =============================

def run_scraper():
    log.info("🚀 Starting scraper...")

    leads = fetch_new_registrations()

    if not leads:
        log.warning("No leads fetched")
        return []

    # ✅ sort older first (better hit rate)
    leads.sort(key=lambda x: x.get("registration_date", ""))

    leads = leads[:ENRICH_LIMIT]

    results = []

    for i, lead in enumerate(leads):
        log.info(f"[{i+1}/{len(leads)}] DOT {lead.get('dot_number')}")

        enriched = enrich_lead(lead)

        log.info(
            f"→ phone={enriched['phone']} | conf={enriched['phone_confidence']}"
        )

        results.append(enriched)

    return results
