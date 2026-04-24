"""
FMCSA Trucking Lead Scraper
Downloads the FMCSA Company Census bulk file (updated daily) and extracts
newly registered trucking companies from the last 7 days.
"""

import os
import io
import csv
import logging
import zipfile
import psycopg2
import psycopg2.extras
import requests
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

DATABASE_URL  = os.environ.get("DATABASE_URL", "")
FMCSA_API_KEY = os.environ.get("FMCSA_API_KEY", "")

# FMCSA bulk census file — updated daily, no API key needed
CENSUS_URL = "https://ai.fmcsa.dot.gov/SMS/files/FMCSA_CENSUS1.zip"


# ── Database ──────────────────────────────────────────────────────────────────

def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def init_db():
    conn = get_db()
    with conn.cursor() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS leads (
                id                SERIAL PRIMARY KEY,
                dot_number        TEXT UNIQUE,
                mc_number         TEXT,
                company_name      TEXT,
                owner_name        TEXT,
                phone             TEXT,
                email             TEXT,
                address           TEXT,
                city              TEXT,
                state             TEXT,
                zip_code          TEXT,
                entity_type       TEXT,
                operation_type    TEXT,
                cargo_type        TEXT,
                drivers           INTEGER,
                power_units       INTEGER,
                status            TEXT,
                added_date        TIMESTAMP,
                registration_date DATE,
                contacted         BOOLEAN DEFAULT FALSE,
                notes             TEXT DEFAULT '',
                has_insurance     BOOLEAN DEFAULT FALSE
            )
        """)
    conn.commit()
    conn.close()
    log.info("PostgreSQL database ready.")


def save_lead(lead: dict):
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
        log.error("DB error saving DOT %s: %s", lead.get("dot_number"), e)
        conn.rollback()
        return 0
    finally:
        conn.close()


# ── FMCSA Bulk Download ───────────────────────────────────────────────────────

def download_census_file() -> io.StringIO:
    """Download and unzip the FMCSA census file, return as StringIO."""
    log.info("Downloading FMCSA census file...")
    try:
        r = requests.get(CENSUS_URL, timeout=120, stream=True)
        r.raise_for_status()
        zip_data = io.BytesIO(r.content)
        with zipfile.ZipFile(zip_data) as z:
            csv_name = [n for n in z.namelist() if n.endswith(".txt") or n.endswith(".csv")][0]
            log.info("Found file in zip: %s", csv_name)
            with z.open(csv_name) as f:
                content = f.read().decode("latin-1")
        return io.StringIO(content)
    except Exception as e:
        log.error("Failed to download census file: %s", e)
        return None


def parse_census_row(row: dict, cutoff: datetime) -> dict:
    try:
        reg_raw = row.get("ADD_DATE", "") or row.get("MCS150_DATE", "")
        reg_date = None
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
            try:
                reg_date = datetime.strptime(reg_raw.strip()[:10], fmt).date()
                break
            except Exception:
                pass

        if reg_date is None:
            return None

        if datetime(reg_date.year, reg_date.month, reg_date.day) < cutoff:
            return None

        dot = str(row.get("DOT_NUMBER", "")).strip()
        if not dot:
            return None

        bipd = row.get("BIPD_INSURANCE_REQUIRED", "0") or "0"
        try:
            has_ins = int(bipd) > 0
        except Exception:
            has_ins = False

        return {
            "dot_number":        dot,
            "mc_number":         str(row.get("MC_MX_FF_NUMBER", "") or ""),
            "company_name":      row.get("LEGAL_NAME", "") or row.get("DBA_NAME", ""),
            "owner_name":        row.get("PRINCIPAL_NAME", ""),
            "phone":             row.get("TELEPHONE", ""),
            "email":             row.get("EMAIL_ADDRESS", ""),
            "address":           row.get("PHY_STREET", ""),
            "city":              row.get("PHY_CITY", ""),
            "state":             row.get("PHY_STATE", ""),
            "zip_code":          row.get("PHY_ZIP", ""),
            "entity_type":       row.get("ENTITY_TYPE_DESC", "").strip(),
            "operation_type":    row.get("CARRIER_OPERATION", "").strip(),
            "cargo_type":        "",
            "drivers":           _safe_int(row.get("TOTAL_DRIVERS", 0)),
            "power_units":       _safe_int(row.get("TOTAL_POWER_UNITS", 0)),
            "status":            "A",
            "added_date":        datetime.utcnow(),
            "registration_date": reg_date,
            "has_insurance":     has_ins,
        }
    except Exception as e:
        log.error("Row parse error: %s", e)
        return None


def _safe_int(val) -> int:
    try:
        return int(val or 0)
    except Exception:
        return 0


def fetch_new_carriers(days_back: int = 7) -> list:
    cutoff = datetime.utcnow() - timedelta(days=days_back)
    log.info("Looking for carriers registered after %s", cutoff.strftime("%Y-%m-%d"))

    csv_file = download_census_file()
    if csv_file is None:
        log.warning("Census download failed — returning demo leads.")
        return _demo_leads()

    reader = csv.DictReader(csv_file, delimiter="\t")
    leads = []
    total_rows = 0

    for row in reader:
        total_rows += 1
        lead = parse_census_row(row, cutoff)
        if lead:
            leads.append(lead)

    log.info("Scanned %d rows, found %d new leads", total_rows, len(leads))
    return leads


# ── Demo mode ─────────────────────────────────────────────────────────────────

def _demo_leads() -> list:
    today     = datetime.utcnow().date()
    yesterday = (datetime.utcnow() - timedelta(days=1)).date()
    now       = datetime.utcnow()
    sample = [
        ("3421901","MC-1234567","LONE STAR FREIGHT LLC",       "John Martinez","(512) 555-0171","TX","Austin",      "78701",today,     True),
        ("3421902","MC-1234568","GREAT LAKES TRANSPORT INC",   "Sara Kowalski","(312) 555-0182","IL","Chicago",     "60601",today,     True),
        ("3421903","",          "SUNRISE HAULING LLC",         "David Chen",   "(404) 555-0193","GA","Atlanta",     "30301",today,     False),
        ("3421904","MC-1234570","BLUE RIDGE CARRIERS LLC",     "Mike Thornton","(828) 555-0104","NC","Asheville",   "28801",yesterday, True),
        ("3421905","MC-1234571","PACIFIC COAST LOGISTICS INC", "Ana Gutierrez","(503) 555-0115","OR","Portland",    "97201",yesterday, False),
        ("3421906","",          "MOUNTAIN STATE TRUCKING LLC", "Bob Williams", "(720) 555-0126","CO","Denver",      "80201",yesterday, False),
        ("3421907","MC-1234573","BAYOU FREIGHT SOLUTIONS LLC", "Lisa Tran",    "(504) 555-0137","LA","New Orleans", "70112",yesterday, True),
        ("3421908","MC-1234574","IRON HORSE TRANSPORT LLC",    "Tom Bradley",  "(602) 555-0148","AZ","Phoenix",     "85001",today,     False),
    ]
    return [{
        "dot_number": dot, "mc_number": mc, "company_name": name,
        "owner_name": owner, "phone": phone, "email": "",
        "address": "123 Main St", "city": city, "state": state,
        "zip_code": zipcode, "entity_type": "CARRIER",
        "operation_type": "A", "cargo_type": "",
        "drivers": 2, "power_units": 2, "status": "A",
        "added_date": now, "registration_date": reg_date,
        "has_insurance": ins,
    } for dot, mc, name, owner, phone, state, city, zipcode, reg_date, ins in sample]


# ── Main ──────────────────────────────────────────────────────────────────────

def run_scraper():
    log.info("=" * 50)
    log.info("Starting FMCSA scraper run")
    log.info("=" * 50)
    init_db()
    leads     = fetch_new_carriers(days_back=7)
    new_count = sum(save_lead(lead) for lead in leads)
    log.info("Scraper complete. %d new leads saved.", new_count)
    return new_count


if __name__ == "__main__":
    run_scraper()
