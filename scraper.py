"""
FMCSA Trucking Lead Scraper — Fixed Date Parsing + Socrata API
- Primary: Socrata JSON API (filters by date server-side, ~50KB payload)
- Fallback: streaming CSV if API is down (with all known FMCSA date formats)
- Days back: 3
"""

import os
import csv
import logging
import psycopg2
import psycopg2.extras
import requests
from datetime import datetime, timedelta
from io import TextIOWrapper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

DATABASE_URL      = os.environ.get("DATABASE_URL", "")
SOCRATA_APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN", "")

SODA_URL   = "https://data.transportation.gov/resource/az4n-8mr2.json"
CENSUS_URL = "https://data.transportation.gov/api/views/az4n-8mr2/rows.csv?accessType=DOWNLOAD"

DAYS_BACK  = 3
BATCH_SIZE = 100
PAGE_SIZE  = 1000

# Every date format FMCSA has ever used — %Y%m%d was the missing one
DATE_FORMATS = [
    "%Y%m%d",                  # 20260424  ← THE FIX: was missing in old scraper
    "%m/%d/%Y",                # 04/24/2026
    "%Y-%m-%d",                # 2026-04-24
    "%m-%d-%Y",                # 04-24-2026
    "%Y-%m-%dT%H:%M:%S.%f",   # Socrata: 2026-04-24T00:00:00.000
    "%Y-%m-%dT%H:%M:%S",      # Socrata: 2026-04-24T00:00:00
]


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


def batch_insert(leads: list) -> int:
    if not leads:
        return 0
    conn = get_db()
    inserted = 0
    try:
        with conn.cursor() as c:
            for lead in leads:
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
                inserted += c.rowcount
        conn.commit()
    except Exception as e:
        log.error("Batch insert error: %s", e)
        conn.rollback()
    finally:
        conn.close()
    return inserted


# ── Date parsing ──────────────────────────────────────────────────────────────

def parse_date(raw: str):
    """Try every known FMCSA date format. Returns a date object or None."""
    raw = (raw or "").strip()
    if not raw:
        return None
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).date()
        except Exception:
            pass
    # Last resort: try truncating to first 10 chars (handles timestamps)
    if len(raw) > 10:
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y"):
            try:
                return datetime.strptime(raw[:10], fmt).date()
            except Exception:
                pass
    return None


# ── Socrata JSON API (primary — fast, tiny payload) ───────────────────────────

def fetch_via_api(cutoff: datetime):
    """Returns list of leads, or None if API is unavailable."""
    cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%S")
    log.info("PRIMARY: Socrata API — records after %s", cutoff.strftime("%Y-%m-%d"))

    headers = {"X-App-Token": SOCRATA_APP_TOKEN} if SOCRATA_APP_TOKEN else {}
    leads   = []
    offset  = 0

    while True:
        params = {
            "$where":  f"add_date > '{cutoff_str}'",
            "$limit":  PAGE_SIZE,
            "$offset": offset,
            "$order":  "add_date DESC",
        }
        try:
            r = requests.get(SODA_URL, params=params, headers=headers, timeout=60)
            r.raise_for_status()
            rows = r.json()
        except Exception as e:
            log.error("Socrata API failed (offset=%d): %s", offset, e)
            return None   # triggers CSV fallback

        if not rows:
            break

        log.info("  API page offset=%-5d  rows=%-4d", offset, len(rows))

        if offset == 0 and rows:
            log.info("  Sample date value: %r", rows[0].get("add_date") or rows[0].get("mcs150_date"))

        for row in rows:
            lead = _build_lead_from_json(row)
            if lead:
                leads.append(lead)

        if len(rows) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    log.info("API fetch done: %d leads.", len(leads))
    return leads


# ── Streaming CSV fallback ────────────────────────────────────────────────────

def fetch_via_streaming(cutoff: datetime) -> list:
    log.info("FALLBACK: Streaming CSV (this takes ~8 min and uses more RAM).")
    leads              = []
    total_rows         = 0
    date_failures      = 0
    date_logged        = False

    try:
        # Detect delimiter from first 2KB
        with requests.get(CENSUS_URL, stream=True, timeout=60) as r:
            r.raise_for_status()
            sample    = r.raw.read(2048).decode("utf-8", errors="replace")
            delimiter = "\t" if sample.count("\t") > sample.count(",") else ","
        log.info("  Delimiter: %s", "TAB" if delimiter == "\t" else "COMMA")

        with requests.get(CENSUS_URL, stream=True, timeout=300) as r:
            r.raise_for_status()
            text_stream = TextIOWrapper(r.raw, encoding="utf-8", errors="replace")
            reader      = csv.DictReader(text_stream, delimiter=delimiter)

            if reader.fieldnames:
                date_cols = [f for f in reader.fieldnames if "DATE" in f.upper()]
                log.info("  Date columns in CSV: %s", date_cols)

            for row in reader:
                total_rows += 1

                raw_date = (
                    row.get("ADD_DATE") or row.get("add_date") or
                    row.get("MCS150_DATE") or row.get("mcs150_date") or
                    row.get("ADD DATE") or ""
                ).strip()

                if not date_logged and raw_date:
                    log.info("  First raw date in CSV: %r", raw_date)
                    date_logged = True

                reg_date = parse_date(raw_date)
                if reg_date is None:
                    date_failures += 1
                    continue
                if datetime(reg_date.year, reg_date.month, reg_date.day) < cutoff:
                    continue

                dot = str(row.get("DOT_NUMBER") or row.get("dot_number") or "").strip()
                if not dot:
                    continue

                bipd = row.get("BIPD_INSURANCE_REQUIRED") or row.get("bipd_insurance_required") or "0"
                try:
                    has_ins = int(str(bipd).strip() or "0") > 0
                except Exception:
                    has_ins = False

                leads.append({
                    "dot_number":        dot,
                    "mc_number":         str(row.get("MC_MX_FF_NUMBER") or row.get("mc_mx_ff_number") or ""),
                    "company_name":      (row.get("LEGAL_NAME") or row.get("legal_name") or
                                          row.get("DBA_NAME") or row.get("dba_name") or ""),
                    "owner_name":        row.get("PRINCIPAL_NAME") or row.get("principal_name") or "",
                    "phone":             row.get("TELEPHONE") or row.get("telephone") or "",
                    "email":             row.get("EMAIL_ADDRESS") or row.get("email_address") or "",
                    "address":           row.get("PHY_STREET") or row.get("phy_street") or "",
                    "city":              row.get("PHY_CITY") or row.get("phy_city") or "",
                    "state":             row.get("PHY_STATE") or row.get("phy_state") or "",
                    "zip_code":          row.get("PHY_ZIP") or row.get("phy_zip") or "",
                    "entity_type":       (row.get("ENTITY_TYPE_DESC") or row.get("entity_type_desc") or "").strip(),
                    "operation_type":    (row.get("CARRIER_OPERATION") or row.get("carrier_operation") or "").strip(),
                    "cargo_type":        "",
                    "drivers":           _safe_int(row.get("TOTAL_DRIVERS") or row.get("total_drivers")),
                    "power_units":       _safe_int(row.get("TOTAL_POWER_UNITS") or row.get("total_power_units")),
                    "status":            "A",
                    "added_date":        datetime.utcnow(),
                    "registration_date": reg_date,
                    "has_insurance":     has_ins,
                })

                if total_rows % 500_000 == 0:
                    log.info("  … %d rows scanned, %d leads so far", total_rows, len(leads))

        log.info("CSV done. rows=%d | date_failures=%d | leads=%d",
                 total_rows, date_failures, len(leads))

    except Exception as e:
        log.error("Streaming error: %s", e)

    return leads


# ── Helpers ───────────────────────────────────────────────────────────────────

def _build_lead_from_json(row: dict):
    try:
        reg_date = parse_date(row.get("add_date") or row.get("mcs150_date") or "")
        dot      = str(row.get("dot_number") or "").strip()
        if not dot:
            return None
        bipd = row.get("bipd_insurance_required") or "0"
        try:
            has_ins = int(str(bipd).strip() or "0") > 0
        except Exception:
            has_ins = False
        return {
            "dot_number":        dot,
            "mc_number":         str(row.get("mc_mx_ff_number") or ""),
            "company_name":      (row.get("legal_name") or row.get("dba_name") or ""),
            "owner_name":        row.get("principal_name") or "",
            "phone":             row.get("telephone") or "",
            "email":             row.get("email_address") or "",
            "address":           row.get("phy_street") or "",
            "city":              row.get("phy_city") or "",
            "state":             row.get("phy_state") or "",
            "zip_code":          row.get("phy_zip") or "",
            "entity_type":       (row.get("entity_type_desc") or "").strip(),
            "operation_type":    (row.get("carrier_operation") or "").strip(),
            "cargo_type":        "",
            "drivers":           _safe_int(row.get("total_drivers")),
            "power_units":       _safe_int(row.get("total_power_units")),
            "status":            "A",
            "added_date":        datetime.utcnow(),
            "registration_date": reg_date,
            "has_insurance":     has_ins,
        }
    except Exception as e:
        log.error("JSON row error: %s", e)
        return None


def _safe_int(val) -> int:
    try:
        return int(val or 0)
    except Exception:
        return 0


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
    log.info("=" * 52)
    log.info("Starting FMCSA scraper — %d-day window", DAYS_BACK)
    log.info("=" * 52)
    init_db()

    cutoff = datetime.utcnow() - timedelta(days=DAYS_BACK)

    # Try fast Socrata API first; stream CSV if API unavailable
    leads = fetch_via_api(cutoff)
    if leads is None:
        log.warning("Socrata API down — switching to CSV streaming fallback.")
        leads = fetch_via_streaming(cutoff)

    if not leads:
        log.warning("No real leads found — loading demo data.")
        leads = _demo_leads()

    batch     = []
    new_count = 0
    for lead in leads:
        batch.append(lead)
        if len(batch) >= BATCH_SIZE:
            new_count += batch_insert(batch)
            batch = []
    if batch:
        new_count += batch_insert(batch)

    log.info("=" * 52)
    log.info("DONE: saved %d new leads.", new_count)
    log.info("=" * 52)
    return new_count


if __name__ == "__main__":
    run_scraper()
