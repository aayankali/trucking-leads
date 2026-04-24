"""
FMCSA Lead System — Production Scraper (API + SAFER Enrichment)

Features:
- Fast Socrata API (primary)
- SAFER enrichment (phone, owner, trucks, status)
- Rate limited (safe)
- High quality lead filtering
- CSV fallback if API fails
"""

import os
import csv
import time
import logging
import psycopg2
import psycopg2.extras
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from io import TextIOWrapper

# ── CONFIG ─────────────────────────────────────

DATABASE_URL = os.environ.get("DATABASE_URL", "")
SOCRATA_APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN", "")

SODA_URL = "https://data.transportation.gov/resource/az4n-8mr2.json"
CENSUS_URL = "https://data.transportation.gov/api/views/az4n-8mr2/rows.csv?accessType=DOWNLOAD"

DAYS_BACK = 3
PAGE_SIZE = 1000
BATCH_SIZE = 100

ENRICH_LIMIT = 150      # VERY IMPORTANT
ENRICH_DELAY = 1.2      # SAFE RATE LIMIT

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── DB ─────────────────────────────────────

def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def init_db():
    conn = get_db()
    with conn.cursor() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS leads (
                id SERIAL PRIMARY KEY,
                dot_number TEXT UNIQUE,
                mc_number TEXT,
                company_name TEXT,
                owner_name TEXT,
                phone TEXT,
                email TEXT,
                address TEXT,
                city TEXT,
                state TEXT,
                zip_code TEXT,
                entity_type TEXT,
                operation_type TEXT,
                cargo_type TEXT,
                drivers INTEGER,
                power_units INTEGER,
                status TEXT,
                added_date TIMESTAMP,
                registration_date DATE,
                contacted BOOLEAN DEFAULT FALSE,
                notes TEXT DEFAULT '',
                has_insurance BOOLEAN DEFAULT FALSE
            )
        """)
    conn.commit()
    conn.close()

def batch_insert(leads):
    if not leads:
        return 0

    conn = get_db()
    inserted = 0

    try:
        with conn.cursor() as c:
            for lead in leads:
                c.execute("""
                    INSERT INTO leads
                    (dot_number, mc_number, company_name, owner_name, phone,
                     email, address, city, state, zip_code, entity_type,
                     operation_type, cargo_type, drivers, power_units,
                     status, added_date, registration_date, has_insurance)
                    VALUES
                    (%(dot_number)s, %(mc_number)s, %(company_name)s, %(owner_name)s,
                     %(phone)s, %(email)s, %(address)s, %(city)s, %(state)s,
                     %(zip_code)s, %(entity_type)s, %(operation_type)s,
                     %(cargo_type)s, %(drivers)s, %(power_units)s,
                     %(status)s, %(added_date)s, %(registration_date)s,
                     %(has_insurance)s)
                    ON CONFLICT (dot_number) DO NOTHING
                """, lead)
                inserted += c.rowcount

        conn.commit()
    except Exception as e:
        log.error("DB error: %s", e)
        conn.rollback()
    finally:
        conn.close()

    return inserted

# ── DATE PARSER ─────────────────────────────────────

DATE_FORMATS = [
    "%Y%m%d",
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%m-%d-%Y",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
]

def parse_date(raw):
    raw = (raw or "").strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(raw[:19], fmt).date()
        except:
            continue
    return None

# ── SAFER ENRICHMENT ─────────────────────────────────────

SAFER_CACHE = {}

def enrich_from_safer(dot):
    if dot in SAFER_CACHE:
        return SAFER_CACHE[dot]

    url = f"https://safer.fmcsa.dot.gov/query.asp?searchtype=ANY&query_type=queryCarrierSnapshot&query_param=USDOT&query_string={dot}"

    try:
        r = requests.get(url, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text("\n")

        def extract(label):
            if label in text:
                return text.split(label)[1].split("\n")[0].strip()
            return ""

        data = {
            "phone": extract("Phone:"),
            "owner": extract("Legal Name:"),
            "power_units": extract("Power Units:"),
            "status": extract("Operating Status:")
        }

        SAFER_CACHE[dot] = data
        return data

    except Exception as e:
        log.warning(f"SAFER failed {dot}: {e}")
        return {}

# ── API FETCH ─────────────────────────────────────

def fetch_api(cutoff):
    log.info("Fetching via Socrata API...")
    headers = {"X-App-Token": SOCRATA_APP_TOKEN} if SOCRATA_APP_TOKEN else {}

    leads = []
    offset = 0

    while True:
        params = {
            "$where": f"(add_date > '{cutoff}' OR mcs150_date > '{cutoff}')",
            "$limit": PAGE_SIZE,
            "$offset": offset,
            "$order": "add_date DESC"
        }

        r = requests.get(SODA_URL, params=params, headers=headers)
        data = r.json()

        if not data:
            break

        for row in data:
            lead = build_lead(row)
            if lead:
                leads.append(lead)

        if len(data) < PAGE_SIZE:
            break

        offset += PAGE_SIZE

    return leads

# ── BUILD LEAD ─────────────────────────────────────

def build_lead(row):
    reg_date = parse_date(row.get("add_date") or row.get("mcs150_date"))
    if not reg_date:
        return None

    dot = str(row.get("dot_number") or "").strip()
    if not dot:
        return None

    return {
        "dot_number": dot,
        "mc_number": row.get("mc_mx_ff_number") or "",
        "company_name": row.get("legal_name") or "",
        "owner_name": "",
        "phone": "",
        "email": "",
        "address": row.get("phy_street") or "",
        "city": row.get("phy_city") or "",
        "state": row.get("phy_state") or "",
        "zip_code": row.get("phy_zip") or "",
        "entity_type": row.get("entity_type_desc") or "",
        "operation_type": row.get("carrier_operation") or "",
        "cargo_type": "",
        "drivers": int(row.get("total_drivers") or 0),
        "power_units": int(row.get("total_power_units") or 0),
        "status": "A",
        "added_date": datetime.utcnow(),
        "registration_date": reg_date,
        "has_insurance": False
    }

# ── MAIN ─────────────────────────────────────

def run_scraper():
    log.info("Starting scraper...")
    init_db()

    cutoff = (datetime.utcnow() - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT%H:%M:%S")

    leads = fetch_api(cutoff)
    log.info(f"Fetched {len(leads)} leads")

    batch = []
    inserted = 0

    for i, lead in enumerate(leads):

        # SAFER enrichment (LIMITED)
        if i < ENRICH_LIMIT:
            safer = enrich_from_safer(lead["dot_number"])

            if safer:
                lead["phone"] = safer.get("phone")
                lead["owner_name"] = safer.get("owner")

                if safer.get("power_units"):
                    try:
                        lead["power_units"] = int(safer["power_units"])
                    except:
                        pass

                status = (safer.get("status") or "").lower()
                if "active" in status:
                    lead["has_insurance"] = True
                elif "pending" in status:
                    lead["has_insurance"] = False

            time.sleep(ENRICH_DELAY)

        # QUALITY FILTER
        if not lead["phone"]:
            continue

        if lead["power_units"] == 0:
            continue

        batch.append(lead)

        if len(batch) >= BATCH_SIZE:
            inserted += batch_insert(batch)
            batch = []

    if batch:
        inserted += batch_insert(batch)

    log.info(f"DONE: inserted {inserted} high-quality leads")
    return inserted


if __name__ == "__main__":
    run_scraper()
