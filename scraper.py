"""
FMCSA Trucking Lead Scraper
Pulls newly registered trucking companies (0-2 days old) from the FMCSA API
and stores them in a PostgreSQL database.
"""

import os
import logging
import psycopg2
import psycopg2.extras
import requests
from datetime import datetime, timedelta
from time import sleep

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

DATABASE_URL  = os.environ.get("DATABASE_URL", "")
FMCSA_API_KEY = os.environ.get("FMCSA_API_KEY", "")
BASE_URL      = "https://mobile.fmcsa.dot.gov/qc/services/carriers"

TRUCKING_OPERATIONS = {"A", "B", "C"}


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


# ── FMCSA API ─────────────────────────────────────────────────────────────────

def fetch_carriers_by_state(state: str, start: int = 0, size: int = 100) -> list:
    if not FMCSA_API_KEY:
        return []
    url = f"{BASE_URL}/docket-number/0"
    params = {"webKey": FMCSA_API_KEY, "start": start, "size": size, "state": state}
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        return r.json().get("content", [])
    except requests.RequestException as e:
        log.error("FMCSA API error for state %s: %s", state, e)
        return []


def fetch_new_carriers(days_back: int = 2) -> list:
    if not FMCSA_API_KEY:
        log.warning("FMCSA_API_KEY not set — returning demo leads.")
        return _demo_leads()

    cutoff = datetime.utcnow() - timedelta(days=days_back)
    leads  = []
    states = [
        "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
        "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
        "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
        "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
        "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
    ]

    for state in states:
        log.info("Scanning state: %s", state)
        start = 0
        while True:
            carriers = fetch_carriers_by_state(state, start=start)
            if not carriers:
                break
            for c in carriers:
                lead = parse_carrier(c)
                if lead and is_recent(lead.get("registration_date", ""), cutoff):
                    leads.append(lead)
            if len(carriers) < 100:
                break
            start += 100
            sleep(0.2)

    log.info("Found %d new trucking leads", len(leads))
    return leads


def parse_carrier(raw: dict):
    try:
        carrier = raw.get("carrier", raw)
        op_type = carrier.get("carrierOperation", {}).get("carrierOperationCode", "")
        if op_type not in TRUCKING_OPERATIONS:
            return None
        dot = str(carrier.get("dotNumber", "")).strip()
        if not dot:
            return None

        ins_flag = carrier.get("insuranceRequiredFlag", "")
        bipd     = int(carrier.get("bipdInsuranceRequired", 0) or 0)
        has_ins  = ins_flag == "Y" or bipd > 0

        reg_raw  = carrier.get("addDate", "")
        reg_date = None
        for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
            try:
                reg_date = datetime.strptime(reg_raw[:10], fmt).date()
                break
            except Exception:
                pass

        return {
            "dot_number":        dot,
            "mc_number":         str(carrier.get("mcNumber", "") or ""),
            "company_name":      carrier.get("legalName") or carrier.get("dbaName") or "",
            "owner_name":        carrier.get("principalName", ""),
            "phone":             carrier.get("telephone", ""),
            "email":             carrier.get("email", ""),
            "address":           carrier.get("phyStreet", ""),
            "city":              carrier.get("phyCity", ""),
            "state":             carrier.get("phyState", ""),
            "zip_code":          carrier.get("phyZipcode", ""),
            "entity_type":       carrier.get("entityType", {}).get("entityTypeDesc", ""),
            "operation_type":    op_type,
            "cargo_type":        carrier.get("cargoCarried", {}).get("cargoCarriedCode", ""),
            "drivers":           int(carrier.get("totalDrivers", 0) or 0),
            "power_units":       int(carrier.get("totalPowerUnits", 0) or 0),
            "status":            carrier.get("statusCode", "A"),
            "added_date":        datetime.utcnow(),
            "registration_date": reg_date,
            "has_insurance":     has_ins,
        }
    except Exception as e:
        log.error("Parse error: %s", e)
        return None


def is_recent(date_val, cutoff: datetime) -> bool:
    if not date_val:
        return False
    if hasattr(date_val, "year"):          # already a date/datetime
        return datetime(date_val.year, date_val.month, date_val.day) >= cutoff
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(str(date_val)[:10], fmt) >= cutoff
        except ValueError:
            pass
    return False


# ── Demo mode ─────────────────────────────────────────────────────────────────

def _demo_leads() -> list:
    today     = datetime.utcnow().date()
    yesterday = (datetime.utcnow() - timedelta(days=1)).date()
    now       = datetime.utcnow()

    sample = [
        ("3421901","MC-1234567","LONE STAR FREIGHT LLC",       "John Martinez", "(512) 555-0171","TX","Austin",      "78701", today,     True),
        ("3421902","MC-1234568","GREAT LAKES TRANSPORT INC",   "Sara Kowalski", "(312) 555-0182","IL","Chicago",     "60601", today,     True),
        ("3421903","",          "SUNRISE HAULING LLC",         "David Chen",    "(404) 555-0193","GA","Atlanta",     "30301", today,     False),
        ("3421904","MC-1234570","BLUE RIDGE CARRIERS LLC",     "Mike Thornton", "(828) 555-0104","NC","Asheville",   "28801", yesterday, True),
        ("3421905","MC-1234571","PACIFIC COAST LOGISTICS INC", "Ana Gutierrez", "(503) 555-0115","OR","Portland",    "97201", yesterday, False),
        ("3421906","",          "MOUNTAIN STATE TRUCKING LLC", "Bob Williams",  "(720) 555-0126","CO","Denver",      "80201", yesterday, False),
        ("3421907","MC-1234573","BAYOU FREIGHT SOLUTIONS LLC", "Lisa Tran",     "(504) 555-0137","LA","New Orleans", "70112", yesterday, True),
        ("3421908","MC-1234574","IRON HORSE TRANSPORT LLC",    "Tom Bradley",   "(602) 555-0148","AZ","Phoenix",     "85001", today,     False),
    ]

    return [{
        "dot_number": dot, "mc_number": mc, "company_name": name,
        "owner_name": owner, "phone": phone, "email": "",
        "address": "123 Main St", "city": city, "state": state,
        "zip_code": zipcode, "entity_type": "CARRIER",
        "operation_type": "A", "cargo_type": "G",
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
    leads     = fetch_new_carriers(days_back=2)
    new_count = sum(save_lead(lead) for lead in leads)
    log.info("Scraper complete. %d new leads saved.", new_count)
    return new_count


if __name__ == "__main__":
    run_scraper()
