"""
FMCSA Lead Scraper — Improved Pipeline (SAFE VERSION)

Fixes:
- Better lead timing (DAYS_BACK = 10)
- Limit to 100 leads per run
- Added DOT.report layer
- Improved enrichment flow
"""

import os, re, logging, time, random
from datetime import datetime, timedelta

import requests
import psycopg2, psycopg2.extras
from bs4 import BeautifulSoup

# ── CONFIG ─────────────────────────────────────────

DATABASE_URL  = os.environ.get("DATABASE_URL", "")
FMCSA_API_KEY = os.environ.get("FMCSA_API_KEY", "")

SODA_URL   = "https://data.transportation.gov/resource/az4n-8mr2.json"
FMCSA_BASE = "https://mobile.fmcsa.dot.gov/qc/services/carriers"

DAYS_BACK    = 10      # 🔥 FIXED
PAGE_SIZE    = 1000
BATCH_SIZE   = 50
ENRICH_LIMIT = 100     # 🔥 FIXED

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0"}

# ── HELPERS ───────────────────────────────────────

PHONE_RE = re.compile(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}")

def extract_phones(text):
    return list(set(PHONE_RE.findall(text)))

def safe_get(url, timeout=10):
    try:
        return requests.get(url, headers=HEADERS, timeout=timeout)
    except:
        return None

# ── DB ───────────────────────────────────────────

def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def init_db():
    conn = get_db()
    with conn.cursor() as c:
        c.execute("""
        CREATE TABLE IF NOT EXISTS leads (
            dot_number TEXT PRIMARY KEY,
            company_name TEXT,
            phone TEXT,
            phone_source TEXT,
            phone_confidence TEXT,
            sources_found INTEGER DEFAULT 0,
            registration_date DATE
        )
        """)
    conn.commit()
    conn.close()

# ── FETCH ─────────────────────────────────────────

def fetch_new_registrations():
    cutoff = (datetime.utcnow() - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT%H:%M:%S")

    params = {
        "$where": f"(add_date > '{cutoff}' OR mcs150_date > '{cutoff}')",
        "$limit": 1000
    }

    try:
        r = requests.get(SODA_URL, params=params, timeout=20)
        data = r.json()
    except:
        return []

    leads = []
    for row in data:
        if not row.get("dot_number"):
            continue

        leads.append({
            "dot_number": row.get("dot_number"),
            "company_name": row.get("legal_name"),
            "registration_date": row.get("add_date", "")[:10],
            "phone": "",
            "phone_source": "",
            "phone_confidence": "none",
            "sources_found": 0
        })

    return leads

# ── LAYERS ───────────────────────────────────────

def layer_fmcsa(lead):
    if not FMCSA_API_KEY:
        return lead

    url = f"{FMCSA_BASE}/{lead['dot_number']}"
    try:
        r = requests.get(url, params={"webKey": FMCSA_API_KEY}, timeout=10)
        data = r.json().get("content", {})
    except:
        return lead

    phone = data.get("phyTelephone", "")
    phones = extract_phones(phone)

    if phones:
        lead["phone"] = phones[0]
        lead["phone_source"] = "fmcsa"
        lead["phone_confidence"] = "high"
        lead["sources_found"] += 1

    return lead


def layer_dot_report(lead):
    dot = lead["dot_number"]

    url = f"https://dot.report/usdot/{dot}"
    r = safe_get(url)

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

    if phones:
        if not lead["phone"]:
            lead["phone"] = phones[0]
            lead["phone_source"] = "dot_report"
            lead["phone_confidence"] = "medium"
        elif lead["phone"] == phones[0]:
            lead["phone_confidence"] = "high"

        lead["sources_found"] += 1

    return lead


def layer_aggregator(lead):
    query = lead["company_name"]
    url = f"https://www.bing.com/search?q={query}"

    r = safe_get(url)
    if not r:
        return lead

    text = BeautifulSoup(r.text, "lxml").get_text(" ")
    phones = extract_phones(text[:3000])

    if phones:
        if not lead["phone"]:
            lead["phone"] = phones[0]
            lead["phone_source"] = "aggregator"
            lead["phone_confidence"] = "medium"

        lead["sources_found"] += 1

    return lead

# ── FINALIZE ─────────────────────────────────────

def finalize(lead):
    if not lead["phone"]:
        lead["phone_confidence"] = "none"
    elif lead["sources_found"] >= 2:
        lead["phone_confidence"] = "high"

    return lead

# ── MAIN ─────────────────────────────────────────

def run_scraper():
    log.info("Starting scraper...")

    init_db()
    leads = fetch_new_registrations()

    if not leads:
        log.warning("No leads fetched")
        return 0

    leads = leads[:ENRICH_LIMIT]

    results = []

    for i, lead in enumerate(leads):
        log.info(f"[{i+1}/{len(leads)}] DOT {lead['dot_number']}")

        lead = layer_fmcsa(lead)

        if lead["sources_found"] < 2:
            lead = layer_dot_report(lead)

        if lead["sources_found"] < 2:
            lead = layer_aggregator(lead)

        lead = finalize(lead)

        log.info(f"→ phone={lead['phone']} | conf={lead['phone_confidence']}")

        results.append(lead)

    return len(results)


if __name__ == "__main__":
    run_scraper()
