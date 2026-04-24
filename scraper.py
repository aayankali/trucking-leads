"""
Optimized FMCSA Trucking Lead Scraper (Production Ready)

- Streams CSV (low RAM)
- Filters high-quality leads
- Improves insurance detection
- Fully compatible with existing DB + dashboard
"""

import os
import csv
import logging
import psycopg2
import psycopg2.extras
import requests
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")

CENSUS_URL = "https://data.transportation.gov/api/views/az4n-8mr2/rows.csv?accessType=DOWNLOAD"


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


def save_lead(lead):
    conn = get_db()
    try:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO leads
                (dot_number, mc_number, company_name, owner_name, phone, email,
                 address, city, state, zip_code, entity_type, operation_type,
                 cargo_type, drivers, power_units, status, added_date,
                 registration_date, has_insurance)
                VALUES
                (%(dot_number)s, %(mc_number)s, %(company_name)s, %(owner_name)s,
                 %(phone)s, %(email)s, %(address)s, %(city)s, %(state)s,
                 %(zip_code)s, %(entity_type)s, %(operation_type)s, %(cargo_type)s,
                 %(drivers)s, %(power_units)s, %(status)s, %(added_date)s,
                 %(registration_date)s, %(has_insurance)s)
                ON CONFLICT (dot_number) DO NOTHING
            """, lead)
        conn.commit()
        return c.rowcount
    except Exception as e:
        log.error("DB error: %s", e)
        conn.rollback()
        return 0
    finally:
        conn.close()


# ── STREAMING CSV (KEY OPTIMIZATION) ─────────────────────────

def stream_rows():
    log.info("Streaming FMCSA census file...")

    r = requests.get(CENSUS_URL, stream=True, timeout=180)
    r.raise_for_status()

    lines = (line.decode("utf-8", errors="ignore") for line in r.iter_lines())
    reader = csv.DictReader(lines)

    for row in reader:
        yield row


# ── PARSE LOGIC ─────────────────────────

def parse_row(row, cutoff):
    try:
        # DATE PARSING
        raw = (row.get("ADD_DATE") or row.get("MCS150_DATE") or "").strip()
        reg_date = None

        for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
            try:
                reg_date = datetime.strptime(raw[:10], fmt).date()
                break
            except:
                continue

        if not reg_date:
            return None

        if datetime(reg_date.year, reg_date.month, reg_date.day) < cutoff:
            return None

        # DOT
        dot = str(row.get("DOT_NUMBER") or "").strip()
        if not dot:
            return None

        # POWER UNIT FILTER (IMPORTANT)
        power_units = safe_int(row.get("TOTAL_POWER_UNITS"))
        if power_units == 0:
            return None

        # MC FILTER (better leads)
        mc = str(row.get("MC_MX_FF_NUMBER") or "").strip()
        if not mc:
            return None

        # 🔥 IMPROVED INSURANCE LOGIC
        authority = (row.get("OPERATING_STATUS") or "").lower()

        if "pending" in authority:
            has_insurance = False
        elif "active" in authority:
            has_insurance = True
        else:
            has_insurance = False

        return {
            "dot_number": dot,
            "mc_number": mc,
            "company_name": row.get("LEGAL_NAME") or row.get("DBA_NAME") or "",
            "owner_name": row.get("PRINCIPAL_NAME") or "",
            "phone": row.get("TELEPHONE") or "",
            "email": row.get("EMAIL_ADDRESS") or "",
            "address": row.get("PHY_STREET") or "",
            "city": row.get("PHY_CITY") or "",
            "state": row.get("PHY_STATE") or "",
            "zip_code": row.get("PHY_ZIP") or "",
            "entity_type": row.get("ENTITY_TYPE_DESC") or "",
            "operation_type": row.get("CARRIER_OPERATION") or "",
            "cargo_type": "",
            "drivers": safe_int(row.get("TOTAL_DRIVERS")),
            "power_units": power_units,
            "status": "A",
            "added_date": datetime.utcnow(),
            "registration_date": reg_date,
            "has_insurance": has_insurance,
        }

    except Exception as e:
        log.error("Parse error: %s", e)
        return None


def safe_int(val):
    try:
        return int(val or 0)
    except:
        return 0


# ── MAIN SCRAPER ─────────────────────────

def run_scraper():
    log.info("=" * 50)
    log.info("Starting optimized scraper")
    log.info("=" * 50)

    init_db()

    cutoff = datetime.utcnow() - timedelta(days=3)
    total = 0
    saved = 0

    for row in stream_rows():
        total += 1

        lead = parse_row(row, cutoff)
        if not lead:
            continue

        saved += save_lead(lead)

        # progress log every 50k rows
        if total % 50000 == 0:
            log.info("Processed %d rows | Saved %d leads", total, saved)

    log.info("DONE: scanned %d rows | saved %d leads", total, saved)
    return saved


if __name__ == "__main__":
    run_scraper()
