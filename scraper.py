import os
import logging
import psycopg2
import psycopg2.extras
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import time

# ── CONFIG ─────────────────────────

DATABASE_URL = os.environ.get("DATABASE_URL", "")
SODA_URL = "https://data.transportation.gov/resource/az4n-8mr2.json"

DAYS_BACK = 3
PAGE_SIZE = 1000
BATCH_SIZE = 100

ENRICH_LIMIT = 100
DELAY = 1.5

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── DB ─────────────────────────

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

# ── FETCH API ─────────────────────────

def fetch_leads():
    cutoff = (datetime.utcnow() - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT%H:%M:%S")
    log.info("Fetching via Socrata API...")

    leads = []
    offset = 0

    while True:
        params = {
            "$where": f"(add_date > '{cutoff}' OR mcs150_date > '{cutoff}')",
            "$limit": PAGE_SIZE,
            "$offset": offset,
            "$order": "add_date DESC"
        }

        r = requests.get(SODA_URL, params=params)
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

    log.info(f"Fetched {len(leads)} leads")
    return leads

# ── BUILD LEAD ─────────────────────────

def build_lead(row):
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
        "registration_date": datetime.utcnow().date(),
        "has_insurance": False
    }

# ── SAFER ENRICHMENT (FIXED) ─────────────────────────

def enrich_from_safer(dot):
    url = "https://safer.fmcsa.dot.gov/query.asp"

    params = {
        "searchtype": "ANY",
        "query_type": "queryCarrierSnapshot",
        "query_param": "USDOT",
        "query_string": dot
    }

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9"
    }

    try:
        r = requests.get(url, params=params, headers=headers, timeout=15)

        if r.status_code != 200:
            return {}

        soup = BeautifulSoup(r.text, "lxml")
        text = soup.get_text("\n")

        # DEBUG: check if real page loaded
        if "USDOT Number" not in text:
            log.warning(f"SAFER BLOCKED for {dot}")
            return {}

        def extract(label):
            try:
                if label in text:
                    return text.split(label)[1].split("\n")[1].strip()
            except:
                pass
            return ""

        data = {
            "phone": extract("Phone:"),
            "owner": extract("Legal Name:"),
            "power_units": extract("Power Units:"),
            "status": extract("Operating Status:")
        }

        return data

    except Exception as e:
        log.warning(f"SAFER error {dot}: {e}")
        return {}

# ── MAIN ─────────────────────────

def run_scraper():
    log.info("Starting scraper...")
    init_db()

    leads = fetch_leads()

    batch = []
    inserted = 0

    for i, lead in enumerate(leads):

        if i < ENRICH_LIMIT:
            data = enrich_from_safer(lead["dot_number"])

            if i < 5:
                log.info(f"DEBUG {lead['dot_number']} → {data}")

            if data:
                if data.get("phone"):
                    lead["phone"] = data["phone"]

                if data.get("owner"):
                    lead["owner_name"] = data["owner"]

                try:
                    pu = int(data.get("power_units") or 0)
                    if pu > 0:
                        lead["power_units"] = pu
                except:
                    pass

                status = (data.get("status") or "").lower()

                if "active" in status:
                    lead["has_insurance"] = True
                elif "pending" in status:
                    lead["has_insurance"] = False

            time.sleep(DELAY)

        # ❗ DO NOT OVER FILTER
        batch.append(lead)

        if len(batch) >= BATCH_SIZE:
            inserted += batch_insert(batch)
            batch = []

    if batch:
        inserted += batch_insert(batch)

    log.info(f"DONE: inserted {inserted} leads")
    return inserted


if __name__ == "__main__":
    run_scraper()
