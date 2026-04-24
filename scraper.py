# ================== UPDATED SCRAPER.PY ==================

# (imports stay same)
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

DAYS_BACK    = 10
PAGE_SIZE    = 1000
BATCH_SIZE   = 50
ENRICH_LIMIT = 100

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0"}

# ── HELPERS ───────────────────────────────────────

PHONE_RE = re.compile(r'(\+?1[\s.\-]?)?(\(?\d{3}\)?[\s.\-]?)(\d{3}[\s.\-]?)(\d{4})')
EMAIL_RE = re.compile(r'\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b', re.I)

LEAD_DEFAULTS = {
    "dot_number": "",
    "mc_number": "",
    "company_name": "",
    "owner_name": "",
    "phone": "",
    "email": "",
    "website": "",
    "address": "",
    "city": "",
    "state": "",
    "zip_code": "",
    "entity_type": "",
    "operation_type": "",
    "cargo_type": "",
    "drivers": 0,
    "power_units": 0,
    "status": "A",
    "registration_date": None,
    "has_insurance": False,
    "insurance_status": "unknown",
    "phone_source": "",
    "phone_confidence": "none",
    "sources_found": 0,
    "enriched": False,
}

def extract_phones(text):
    phones = []
    for parts in PHONE_RE.findall(text):
        digits = re.sub(r'\D', '', ''.join(parts))
        if len(digits) == 10:
            phones.append(digits)
        elif len(digits) == 11 and digits.startswith("1"):
            phones.append(digits[1:])
    return list(set(phones))

def extract_emails(text):
    return list({m.group(0).lower() for m in EMAIL_RE.finditer(text or "")})

def parse_int(value, default=0):
    try:
        if value is None or value == "":
            return default
        return int(float(str(value).replace(",", "").strip()))
    except (TypeError, ValueError):
        return default

def has_meaningful_value(value):
    if value is None:
        return False
    text = str(value).strip().upper()
    return text not in ("", "0", "0.0", "N", "NO", "FALSE", "NONE", "NULL")

def first_present(*values):
    for value in values:
        if value not in (None, ""):
            return str(value).strip()
    return ""

def normalize_date(value):
    if not value:
        return None
    value = str(value).strip()
    if not value:
        return None
    return value[:10]

def apply_phone(lead, raw_phone, source, confidence="medium"):
    phones = extract_phones(raw_phone or "")
    if not phones:
        return False

    phone = phones[0]
    current = lead.get("phone") or ""
    sources_found = parse_int(lead.get("sources_found"))

    if not current:
        lead["phone"] = phone
        lead["phone_source"] = source
        lead["phone_confidence"] = confidence
        lead["sources_found"] = max(1, sources_found)
        return True

    if current == phone:
        lead["sources_found"] = max(2, sources_found + 1)
        lead["phone_confidence"] = "high"
        return True

    return False

def safe_get(url, timeout=12):
    try:
        return requests.get(url, headers=HEADERS, timeout=timeout)
    except Exception:
        return None

# ── 🔥 NEW FILTER ─────────────────────────────────

def is_good_lead(lead):
    name = (lead.get("company_name") or "").lower()
    state = (lead.get("state") or "").upper()

    if len(state) != 2:
        return False

    bad_words = ["school", "construction", "real estate", "builder", "church"]
    if any(b in name for b in bad_words):
        return False

    trucking_words = ["trucking", "transport", "freight", "logistics", "carrier"]
    if not any(w in name for w in trucking_words):
        return False

    return True

# ── LAYER: DOT REPORT (FIXED) ─────────────────────

def layer_dot_report(lead):
    dot = lead["dot_number"]
    r = safe_get(f"https://dot.report/usdot/{dot}")

    if not r:
        return lead

    soup = BeautifulSoup(r.text, "lxml")

    # 🔥 METHOD 1: TABLE PARSE
    phone = None
    for row in soup.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) >= 2:
            label = cols[0].get_text(strip=True).lower()
            if any(x in label for x in ["phone", "tel"]):
                phone = cols[1].get_text(strip=True)
                break

    # 🔥 METHOD 2: FULL SCAN
    if not phone:
        text = soup.get_text(" ")
        matches = extract_phones(text)
        if matches:
            phone = matches[0]

    # 🔥 SAVE
    if phone:
        apply_phone(lead, phone, "dot_report", "medium")

    text = soup.get_text(" ")
    emails = extract_emails(text)
    if emails and not lead.get("email"):
        lead["email"] = emails[0]

    return lead

# ── MAIN ─────────────────────────────────────────
def fetch_new_registrations():
    cutoff = (datetime.utcnow() - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT%H:%M:%S")
    log.info("Fetching registrations since %s...", cutoff[:10])

    leads = []
    offset = 0

    while True:
        params = {
            "$where":  f"(add_date > '{cutoff}' OR mcs150_date > '{cutoff}')",
            "$limit":  PAGE_SIZE,
            "$offset": offset,
            "$order":  "add_date DESC",
        }

        try:
            r = requests.get(SODA_URL, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.error("Socrata fetch error: %s", e)
            break

        if not data:
            break

        for row in data:
            dot = str(row.get("dot_number") or "").strip()
            if not dot:
                continue

            lead = {
                "dot_number": dot,
                "mc_number": first_present(row.get("docket_number"), row.get("mc_number")),
                "company_name": first_present(row.get("legal_name"), row.get("dba_name")),
                "owner_name": "",
                "phone": "",
                "email": first_present(row.get("email_address"), row.get("email")),
                "website": "",
                "address": first_present(row.get("phy_street"), row.get("physical_address")),
                "city": first_present(row.get("phy_city"), row.get("physical_city")),
                "state": first_present(row.get("phy_state"), row.get("physical_state")).upper(),
                "zip_code": first_present(row.get("phy_zip"), row.get("physical_zip")),
                "entity_type": first_present(row.get("entity_type"), row.get("census_type_id")),
                "operation_type": first_present(row.get("carrier_operation"), row.get("operation_classification")),
                "cargo_type": first_present(row.get("cargo_carried"), row.get("cargo_type")),
                "drivers": parse_int(first_present(row.get("driver_total"), row.get("drivers"))),
                "power_units": parse_int(first_present(row.get("nbr_power_unit"), row.get("power_units"))),
                "status": first_present(row.get("status_code"), row.get("status")) or "A",
                "phone_source": "",
                "phone_confidence": "none",
                "sources_found": 0,
                "has_insurance": False,
                "insurance_status": "unknown",
                "registration_date": normalize_date(first_present(row.get("add_date"), row.get("mcs150_date"))),
                "enriched": False,
            }

            apply_phone(lead, first_present(
                row.get("telephone"),
                row.get("phy_telephone"),
                row.get("mailing_telephone"),
                row.get("phone"),
            ), "socrata", "medium")

            leads.append(lead)

        log.info("%d leads fetched so far...", len(leads))

        if len(data) < PAGE_SIZE:
            break

        offset += PAGE_SIZE

    log.info("Fetched %d base leads from Socrata.", len(leads))
    return leads


def layer_fmcsa(lead):
    if not FMCSA_API_KEY:
        return lead

    url = f"{FMCSA_BASE}/{lead['dot_number']}"

    try:
        r = requests.get(url, params={"webKey": FMCSA_API_KEY}, timeout=10)

        if r.status_code != 200:
            return lead

        content = r.json().get("content", {})
        data = content.get("carrier", content) if isinstance(content, dict) else {}
        if not data:
            return lead

        lead["company_name"] = first_present(
            lead.get("company_name"),
            data.get("legalName"),
            data.get("dbaName"),
        )
        lead["address"] = first_present(lead.get("address"), data.get("phyStreet"))
        lead["city"] = first_present(lead.get("city"), data.get("phyCity"))
        lead["state"] = first_present(lead.get("state"), data.get("phyState")).upper()
        lead["zip_code"] = first_present(lead.get("zip_code"), data.get("phyZipcode"), data.get("phyZip"))
        lead["operation_type"] = first_present(lead.get("operation_type"), data.get("carrierOperation"))
        lead["entity_type"] = first_present(lead.get("entity_type"), data.get("censusTypeId"))
        lead["drivers"] = parse_int(first_present(lead.get("drivers"), data.get("driverTotal")))
        lead["power_units"] = parse_int(first_present(lead.get("power_units"), data.get("nbrPowerUnit"), data.get("powerUnits")))
        lead["status"] = first_present(lead.get("status"), data.get("commonAuthorityStatus"), data.get("statusCode")) or "A"
        lead["email"] = first_present(lead.get("email"), data.get("emailAddress"))
        lead["mc_number"] = first_present(lead.get("mc_number"), data.get("docketNumber"), data.get("mcNumber"))

        phone_raw = first_present(
            data.get("phyTelephone"),
            data.get("mailingTelephone"),
            data.get("telephone"),
            data.get("phone"),
        )
        apply_phone(lead, phone_raw, "fmcsa_api", "high")

        # insurance
        ins_code = data.get("bipdInsuranceOnFile")
        ins_req  = data.get("bipdInsuranceRequired")
        ins_on_file = has_meaningful_value(ins_code)
        ins_required = has_meaningful_value(ins_req)

        if ins_on_file:
            ins_status = "insured"
        elif ins_required:
            ins_status = "none"
        else:
            ins_status = "unknown"

        lead["has_insurance"] = ins_status == "insured"
        lead["insurance_status"] = ins_status

    except Exception as e:
        log.debug(f"FMCSA error {lead['dot_number']}: {e}")

    time.sleep(0.4)
    return lead


def label_values_from_soup(soup):
    fields = {}
    for row in soup.find_all("tr"):
        cells = [c.get_text(" ", strip=True) for c in row.find_all(["td", "th"])]
        cells = [c for c in cells if c]
        if len(cells) < 2:
            continue

        for idx in range(0, len(cells) - 1, 2):
            key = re.sub(r"\s+", " ", cells[idx].replace(":", " ")).strip().lower()
            value = re.sub(r"\s+", " ", cells[idx + 1]).strip()
            if key and value:
                fields[key] = value
    return fields


def field_like(fields, *needles):
    for key, value in fields.items():
        if any(needle in key for needle in needles):
            return value
    return ""


def layer_aggregator(lead):
    """Public fallback layer using FMCSA SAFER pages by DOT number."""
    dot = lead.get("dot_number")
    if not dot:
        return lead

    try:
        r = requests.get(
            "https://safer.fmcsa.dot.gov/query.asp",
            params={
                "searchtype": "ANY",
                "query_type": "queryCarrierSnapshot",
                "query_param": "USDOT",
                "query_string": dot,
            },
            headers=HEADERS,
            timeout=12,
        )
        if r.status_code != 200:
            return lead
    except Exception as e:
        log.debug("SAFER error %s: %s", dot, e)
        return lead

    soup = BeautifulSoup(r.text, "lxml")
    fields = label_values_from_soup(soup)

    apply_phone(lead, field_like(fields, "phone", "telephone"), "fmcsa_safer", "medium")

    company = first_present(
        field_like(fields, "legal name"),
        field_like(fields, "dba name"),
    )
    if company and not lead.get("company_name"):
        lead["company_name"] = company

    address = field_like(fields, "physical address")
    if address and not lead.get("address"):
        lead["address"] = address

    status = field_like(fields, "operating status")
    if status and not lead.get("status"):
        lead["status"] = status

    entity_type = field_like(fields, "entity type")
    if entity_type and not lead.get("entity_type"):
        lead["entity_type"] = entity_type

    emails = extract_emails(soup.get_text(" "))
    if emails and not lead.get("email"):
        lead["email"] = emails[0]

    time.sleep(0.2)
    return lead


def parse_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in ("1", "true", "t", "yes", "y")


def finalize(lead):
    clean = LEAD_DEFAULTS.copy()
    clean.update(lead or {})

    clean["dot_number"] = str(clean.get("dot_number") or "").strip()
    clean["mc_number"] = str(clean.get("mc_number") or "").strip()
    clean["company_name"] = str(clean.get("company_name") or "").strip()
    clean["owner_name"] = str(clean.get("owner_name") or "").strip()
    clean["phone"] = str(clean.get("phone") or "").strip()
    clean["email"] = str(clean.get("email") or "").strip().lower()
    clean["website"] = str(clean.get("website") or "").strip()
    clean["address"] = str(clean.get("address") or "").strip()
    clean["city"] = str(clean.get("city") or "").strip()
    clean["state"] = str(clean.get("state") or "").strip().upper()[:2]
    clean["zip_code"] = str(clean.get("zip_code") or "").strip()
    clean["drivers"] = parse_int(clean.get("drivers"))
    clean["power_units"] = parse_int(clean.get("power_units"))
    clean["sources_found"] = parse_int(clean.get("sources_found"))
    clean["registration_date"] = normalize_date(clean.get("registration_date"))
    clean["has_insurance"] = parse_bool(clean.get("has_insurance"))

    if clean["insurance_status"] not in ("none", "unknown", "partial", "insured"):
        clean["insurance_status"] = "unknown"

    if clean["phone"]:
        if clean["sources_found"] >= 2:
            clean["phone_confidence"] = "high"
        elif clean["phone_confidence"] not in ("medium", "high"):
            clean["phone_confidence"] = "medium"
    else:
        clean["phone_source"] = ""
        clean["phone_confidence"] = "none"
        clean["sources_found"] = 0

    clean["enriched"] = True
    return clean


def get_db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def ensure_db():
    conn = get_db()
    with conn.cursor() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS leads (
                id SERIAL PRIMARY KEY,
                dot_number TEXT UNIQUE,
                mc_number TEXT,
                company_name TEXT,
                owner_name TEXT,
                phone TEXT DEFAULT '',
                email TEXT DEFAULT '',
                address TEXT,
                city TEXT,
                state TEXT,
                zip_code TEXT,
                entity_type TEXT,
                operation_type TEXT,
                cargo_type TEXT,
                drivers INTEGER DEFAULT 0,
                power_units INTEGER DEFAULT 0,
                status TEXT DEFAULT 'A',
                added_date TIMESTAMP,
                registration_date DATE,
                contacted BOOLEAN DEFAULT FALSE,
                notes TEXT DEFAULT '',
                has_insurance BOOLEAN DEFAULT FALSE
            )
        """)
    conn.commit()

    columns = [
        ("insurance_status", "TEXT", "'unknown'"),
        ("phone_source", "TEXT", "''"),
        ("phone_confidence", "TEXT", "'none'"),
        ("sources_found", "INTEGER", "0"),
        ("enriched", "BOOLEAN", "FALSE"),
        ("website", "TEXT", "''"),
        ("email", "TEXT", "''"),
        ("owner_name", "TEXT", "''"),
        ("address", "TEXT", "''"),
        ("city", "TEXT", "''"),
        ("state", "TEXT", "''"),
        ("zip_code", "TEXT", "''"),
        ("entity_type", "TEXT", "''"),
        ("operation_type", "TEXT", "''"),
        ("cargo_type", "TEXT", "''"),
        ("drivers", "INTEGER", "0"),
        ("power_units", "INTEGER", "0"),
        ("status", "TEXT", "'A'"),
        ("has_insurance", "BOOLEAN", "FALSE"),
    ]
    for col, col_type, default in columns:
        try:
            with conn.cursor() as c:
                c.execute(f"ALTER TABLE leads ADD COLUMN {col} {col_type} DEFAULT {default}")
            conn.commit()
        except psycopg2.errors.DuplicateColumn:
            conn.rollback()
    conn.close()


def batch_insert(leads):
    if not leads:
        return 0

    ensure_db()

    columns = [
        "dot_number", "mc_number", "company_name", "owner_name",
        "phone", "email", "website", "address", "city", "state", "zip_code",
        "entity_type", "operation_type", "cargo_type", "drivers", "power_units",
        "status", "registration_date", "added_date", "has_insurance",
        "insurance_status", "phone_source", "phone_confidence", "sources_found",
        "enriched",
    ]

    now = datetime.utcnow()
    rows = []
    for lead in leads:
        lead = finalize(lead)
        row = []
        for col in columns:
            row.append(now if col == "added_date" else lead.get(col))
        rows.append(tuple(row))

    sql = f"""
        INSERT INTO leads ({", ".join(columns)})
        VALUES %s
        ON CONFLICT (dot_number) DO UPDATE SET
            mc_number = COALESCE(NULLIF(EXCLUDED.mc_number, ''), leads.mc_number),
            company_name = COALESCE(NULLIF(EXCLUDED.company_name, ''), leads.company_name),
            owner_name = COALESCE(NULLIF(EXCLUDED.owner_name, ''), leads.owner_name),
            phone = COALESCE(NULLIF(EXCLUDED.phone, ''), leads.phone),
            email = COALESCE(NULLIF(EXCLUDED.email, ''), leads.email),
            website = COALESCE(NULLIF(EXCLUDED.website, ''), leads.website),
            address = COALESCE(NULLIF(EXCLUDED.address, ''), leads.address),
            city = COALESCE(NULLIF(EXCLUDED.city, ''), leads.city),
            state = COALESCE(NULLIF(EXCLUDED.state, ''), leads.state),
            zip_code = COALESCE(NULLIF(EXCLUDED.zip_code, ''), leads.zip_code),
            entity_type = COALESCE(NULLIF(EXCLUDED.entity_type, ''), leads.entity_type),
            operation_type = COALESCE(NULLIF(EXCLUDED.operation_type, ''), leads.operation_type),
            cargo_type = COALESCE(NULLIF(EXCLUDED.cargo_type, ''), leads.cargo_type),
            drivers = GREATEST(EXCLUDED.drivers, leads.drivers),
            power_units = GREATEST(EXCLUDED.power_units, leads.power_units),
            status = COALESCE(NULLIF(EXCLUDED.status, ''), leads.status),
            registration_date = COALESCE(EXCLUDED.registration_date, leads.registration_date),
            has_insurance = EXCLUDED.has_insurance,
            insurance_status = EXCLUDED.insurance_status,
            phone_source = COALESCE(NULLIF(EXCLUDED.phone_source, ''), leads.phone_source),
            phone_confidence = EXCLUDED.phone_confidence,
            sources_found = GREATEST(EXCLUDED.sources_found, leads.sources_found),
            enriched = EXCLUDED.enriched
        RETURNING id
    """

    conn = get_db()
    try:
        with conn.cursor() as c:
            saved = psycopg2.extras.execute_values(c, sql, rows, page_size=100, fetch=True)
        conn.commit()
        return len(saved)
    finally:
        conn.close()




def run_scraper():
    log.info("Starting scraper...")

    if not DATABASE_URL:
        log.error("DATABASE_URL is not set; cannot save leads.")
        return 0

    leads = fetch_new_registrations()

    if not leads:
        log.warning("No leads fetched")
        return 0

    # 🔥 FILTER BEFORE
    leads = [l for l in leads if is_good_lead(l)]
    log.info("After trucking filter: %d leads", len(leads))

    leads.sort(key=lambda x: str(x.get("registration_date") or ""), reverse=True)
    leads = leads[:ENRICH_LIMIT]

    batch = []
    inserted = 0

    stats = {"kept": 0, "skip_no_phone": 0, "skip_ins": 0}

    for i, lead in enumerate(leads):
        log.info("[%d/%d] DOT %s", i+1, len(leads), lead["dot_number"])

        lead = layer_fmcsa(lead)
        lead = layer_dot_report(lead)
        lead = layer_aggregator(lead)
        lead = finalize(lead)

        # 🔥 HARD FILTER
        if not lead.get("phone"):
            stats["skip_no_phone"] += 1
            continue

        if lead.get("has_insurance") or lead.get("insurance_status") == "insured":
            stats["skip_ins"] += 1
            continue

        stats["kept"] += 1

        batch.append(lead)

        if len(batch) >= BATCH_SIZE:
            inserted += batch_insert(batch)
            batch = []

    if batch:
        inserted += batch_insert(batch)

    log.info("Saved: %d", inserted)
    log.info("Kept: %d | Skip no phone: %d | Skip insurance: %d",
             stats["kept"], stats["skip_no_phone"], stats["skip_ins"])

    return inserted
