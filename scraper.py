"""
FMCSA Lead Scraper — Fixed Pipeline
Fixes:
  1. DB schema matches dashboard.py exactly (all columns present)
  2. run_scraper() actually inserts leads into DB (was missing batch_insert call)
  3. Uses ON CONFLICT DO UPDATE so re-runs enrich existing rows instead of silently skipping
"""

import os, re, logging, time, random
from datetime import datetime, timedelta

import requests
import psycopg2, psycopg2.extras
from bs4 import BeautifulSoup

# ── CONFIG ────────────────────────────────────────────────────────────────────

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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}

# ── DATABASE ──────────────────────────────────────────────────────────────────

def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def init_db():
    conn = get_db()
    with conn.cursor() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS leads (
                id                SERIAL PRIMARY KEY,
                dot_number        TEXT UNIQUE,
                mc_number         TEXT        DEFAULT '',
                company_name      TEXT        DEFAULT '',
                owner_name        TEXT        DEFAULT '',
                phone             TEXT        DEFAULT '',
                email             TEXT        DEFAULT '',
                website           TEXT        DEFAULT '',
                address           TEXT        DEFAULT '',
                city              TEXT        DEFAULT '',
                state             TEXT        DEFAULT '',
                zip_code          TEXT        DEFAULT '',
                entity_type       TEXT        DEFAULT '',
                operation_type    TEXT        DEFAULT '',
                cargo_type        TEXT        DEFAULT '',
                drivers           INTEGER     DEFAULT 0,
                power_units       INTEGER     DEFAULT 0,
                status            TEXT        DEFAULT 'A',
                added_date        TIMESTAMP   DEFAULT NOW(),
                registration_date DATE,
                contacted         BOOLEAN     DEFAULT FALSE,
                notes             TEXT        DEFAULT '',
                has_insurance     BOOLEAN     DEFAULT FALSE,
                insurance_status  TEXT        DEFAULT 'unknown',
                phone_source      TEXT        DEFAULT '',
                phone_confidence  TEXT        DEFAULT 'none',
                sources_found     INTEGER     DEFAULT 0,
                enriched          BOOLEAN     DEFAULT FALSE
            )
        """)
        conn.commit()

    # Safely add any columns missing on pre-existing tables
    new_cols = [
        ("mc_number",        "TEXT",      "''"),
        ("owner_name",       "TEXT",      "''"),
        ("email",            "TEXT",      "''"),
        ("website",          "TEXT",      "''"),
        ("address",          "TEXT",      "''"),
        ("city",             "TEXT",      "''"),
        ("state",            "TEXT",      "''"),
        ("zip_code",         "TEXT",      "''"),
        ("entity_type",      "TEXT",      "''"),
        ("operation_type",   "TEXT",      "''"),
        ("cargo_type",       "TEXT",      "''"),
        ("drivers",          "INTEGER",   "0"),
        ("power_units",      "INTEGER",   "0"),
        ("status",           "TEXT",      "'A'"),
        ("added_date",       "TIMESTAMP", "NOW()"),
        ("contacted",        "BOOLEAN",   "FALSE"),
        ("notes",            "TEXT",      "''"),
        ("has_insurance",    "BOOLEAN",   "FALSE"),
        ("insurance_status", "TEXT",      "'unknown'"),
        ("phone_source",     "TEXT",      "''"),
        ("phone_confidence", "TEXT",      "'none'"),
        ("sources_found",    "INTEGER",   "0"),
        ("enriched",         "BOOLEAN",   "FALSE"),
    ]
    for col, col_type, default in new_cols:
        try:
            with conn.cursor() as c:
                c.execute(f"ALTER TABLE leads ADD COLUMN {col} {col_type} DEFAULT {default}")
            conn.commit()
        except psycopg2.errors.DuplicateColumn:
            conn.rollback()
        except Exception:
            conn.rollback()

    conn.close()
    log.info("Database ready.")


def batch_insert(leads: list) -> int:
    if not leads:
        return 0
    conn = get_db()
    affected = 0
    try:
        with conn.cursor() as c:
            for lead in leads:
                c.execute("""
                    INSERT INTO leads (
                        dot_number, mc_number, company_name, owner_name,
                        phone, email, website,
                        address, city, state, zip_code,
                        entity_type, operation_type, cargo_type,
                        drivers, power_units, status,
                        added_date, registration_date,
                        has_insurance, insurance_status,
                        phone_source, phone_confidence,
                        sources_found, enriched
                    ) VALUES (
                        %(dot_number)s, %(mc_number)s, %(company_name)s, %(owner_name)s,
                        %(phone)s, %(email)s, %(website)s,
                        %(address)s, %(city)s, %(state)s, %(zip_code)s,
                        %(entity_type)s, %(operation_type)s, %(cargo_type)s,
                        %(drivers)s, %(power_units)s, %(status)s,
                        %(added_date)s, %(registration_date)s,
                        %(has_insurance)s, %(insurance_status)s,
                        %(phone_source)s, %(phone_confidence)s,
                        %(sources_found)s, %(enriched)s
                    )
                    ON CONFLICT (dot_number) DO UPDATE SET
                        phone            = CASE WHEN EXCLUDED.phone != '' THEN EXCLUDED.phone ELSE leads.phone END,
                        owner_name       = CASE WHEN EXCLUDED.owner_name != '' THEN EXCLUDED.owner_name ELSE leads.owner_name END,
                        email            = CASE WHEN EXCLUDED.email != '' THEN EXCLUDED.email ELSE leads.email END,
                        website          = CASE WHEN EXCLUDED.website != '' THEN EXCLUDED.website ELSE leads.website END,
                        phone_source     = CASE WHEN EXCLUDED.phone != '' THEN EXCLUDED.phone_source ELSE leads.phone_source END,
                        phone_confidence = CASE WHEN EXCLUDED.phone != '' THEN EXCLUDED.phone_confidence ELSE leads.phone_confidence END,
                        insurance_status = EXCLUDED.insurance_status,
                        has_insurance    = EXCLUDED.has_insurance,
                        sources_found    = EXCLUDED.sources_found,
                        enriched         = EXCLUDED.enriched,
                        power_units      = CASE WHEN EXCLUDED.power_units > 0 THEN EXCLUDED.power_units ELSE leads.power_units END
                """, lead)
                affected += c.rowcount
        conn.commit()
    except Exception as e:
        log.error("DB insert error: %s", e)
        conn.rollback()
    finally:
        conn.close()
    return affected

# ── HELPERS ───────────────────────────────────────────────────────────────────

PHONE_RE = re.compile(
    r'(\+?1[\s.\-]?)?(\(?\d{3}\)?[\s.\-]?)(\d{3}[\s.\-]?)(\d{4})'
)

def extract_phones(text):
    phones = []
    for parts in PHONE_RE.findall(text):
        digits = re.sub(r'\D', '', ''.join(parts))
        if len(digits) == 10:
            phones.append(f"({digits[:3]}) {digits[3:6]}-{digits[6:]}")
        elif len(digits) == 11 and digits[0] == '1':
            phones.append(f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}")
    return list(dict.fromkeys(phones))

def safe_get(url, timeout=12):
    for _ in range(2):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            if r.status_code == 200:
                return r
            if r.status_code == 429:
                log.warning("Rate limited — sleeping 30s")
                time.sleep(30)
        except Exception as e:
            log.debug("Request error %s: %s", url, e)
        time.sleep(random.uniform(1, 2))
    return None

def _safe_int(val):
    try:
        return int(val or 0)
    except Exception:
        return 0

# ── STEP 1: FETCH FROM SOCRATA ────────────────────────────────────────────────

def fetch_new_registrations():
    cutoff = (datetime.utcnow() - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT%H:%M:%S")
    log.info("Fetching registrations since %s...", cutoff[:10])
    leads, offset = [], 0

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

            reg_date = None
            for field in ("add_date", "mcs150_date"):
                raw = (row.get(field) or "")[:10]
                if raw:
                    try:
                        reg_date = datetime.fromisoformat(raw).date()
                        break
                    except Exception:
                        pass

            leads.append({
                "dot_number":        dot,
                "mc_number":         row.get("mc_mx_ff_number") or "",
                "company_name":      row.get("legal_name") or row.get("dba_name") or "",
                "owner_name":        "",
                "phone":             "",
                "email":             "",
                "website":           "",
                "address":           row.get("phy_street") or "",
                "city":              row.get("phy_city") or "",
                "state":             row.get("phy_state") or "",
                "zip_code":          row.get("phy_zip") or "",
                "entity_type":       row.get("entity_type_desc") or "",
                "operation_type":    row.get("carrier_operation") or "",
                "cargo_type":        "",
                "drivers":           _safe_int(row.get("total_drivers")),
                "power_units":       _safe_int(row.get("total_power_units")),
                "status":            "A",
                "added_date":        datetime.utcnow(),
                "registration_date": reg_date or datetime.utcnow().date(),
                "has_insurance":     False,
                "insurance_status":  "unknown",
                "phone_source":      "",
                "phone_confidence":  "none",
                "sources_found":     0,
                "enriched":          False,
            })

        log.info("  %d leads fetched so far...", len(leads))
        if len(data) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    log.info("Fetched %d base leads from Socrata.", len(leads))
    return leads

# ── LAYER 1: FMCSA API ────────────────────────────────────────────────────────

def layer_fmcsa(lead):
    if not FMCSA_API_KEY:
        return lead

    url = f"{FMCSA_BASE}/{lead['dot_number']}"
    try:
        r = requests.get(url, params={"webKey": FMCSA_API_KEY},
                         headers=HEADERS, timeout=10)
        if r.status_code == 429:
            log.warning("FMCSA rate limit — sleeping 15s")
            time.sleep(15)
            return lead
        if r.status_code != 200:
            return lead

        carrier = r.json().get("content", {})
        if not carrier:
            return lead

        phone_raw = (carrier.get("phyTelephone") or
                     carrier.get("mailingTelephone") or "").strip()
        phones = extract_phones(phone_raw) if phone_raw else []

        if phones:
            lead["phone"]            = phones[0]
            lead["phone_source"]     = "fmcsa_api"
            lead["phone_confidence"] = "high"
            lead["sources_found"]   += 1

        owner = (carrier.get("dbaName") or carrier.get("legalName") or "").strip()
        if owner and not lead["owner_name"]:
            lead["owner_name"] = owner

        email = (carrier.get("email") or "").strip()
        if email and not lead["email"]:
            lead["email"] = email

        pu = _safe_int(carrier.get("totalPowerUnits"))
        if pu > 0:
            lead["power_units"] = pu

        allowed  = (carrier.get("allowedToOperate") or "").upper()
        ins_code = carrier.get("bipdInsuranceOnFile")
        ins_req  = carrier.get("bipdInsuranceRequired")
        if ins_code and ins_req:
            ins_status = "insured"
        elif ins_req and not ins_code:
            ins_status = "none"
        else:
            ins_status = "unknown"

        lead["has_insurance"]    = allowed == "Y" or ins_status == "insured"
        lead["insurance_status"] = ins_status

    except Exception as e:
        log.debug("FMCSA API error DOT %s: %s", lead["dot_number"], e)

    time.sleep(0.4)
    return lead

# ── LAYER 2: DOT.REPORT ───────────────────────────────────────────────────────

def layer_dot_report(lead):
    dot = lead["dot_number"]
    r   = safe_get(f"https://dot.report/usdot/{dot}")

    if not r:
        search_r = safe_get(f"https://dot.report/search/?q={dot}")
        if not search_r:
            return lead
        soup = BeautifulSoup(search_r.text, "lxml")
        link = soup.find("a", href=re.compile(r"/usdot/\d+"))
        if not link:
            return lead
        href = link["href"]
        if not href.startswith("http"):
            href = "https://dot.report" + href
        r = safe_get(href)
        if not r:
            return lead

    text   = BeautifulSoup(r.text, "lxml").get_text(" ")
    phones = extract_phones(text[:6000])

    if phones:
        if not lead["phone"]:
            lead["phone"]            = phones[0]
            lead["phone_source"]     = "dot_report"
            lead["phone_confidence"] = "medium"
        elif lead["phone"] == phones[0]:
            lead["phone_confidence"] = "high"
        lead["sources_found"] += 1

    time.sleep(random.uniform(1.0, 1.8))
    return lead

# ── LAYER 3: BING SEARCH ─────────────────────────────────────────────────────

def layer_aggregator(lead):
    company = lead.get("company_name") or ""
    state   = lead.get("state") or ""
    if not company:
        return lead

    query = f"{company} {state} trucking phone"
    r     = safe_get(f"https://www.bing.com/search?q={requests.utils.quote(query)}")
    if not r:
        return lead

    text   = BeautifulSoup(r.text, "lxml").get_text(" ")
    phones = extract_phones(text[:4000])

    if phones:
        if not lead["phone"]:
            lead["phone"]            = phones[0]
            lead["phone_source"]     = "aggregator"
            lead["phone_confidence"] = "medium"
        elif lead["phone"] == phones[0]:
            lead["phone_confidence"] = "high"
        lead["sources_found"] += 1

    time.sleep(random.uniform(1.0, 1.5))
    return lead

# ── FINALIZE ──────────────────────────────────────────────────────────────────

def finalize(lead):
    if not lead["phone"]:
        lead["phone_confidence"] = "none"
    elif lead["sources_found"] >= 2 and lead["phone_confidence"] != "high":
        lead["phone_confidence"] = "high"
    lead["enriched"] = bool(
        lead.get("phone") or lead.get("email") or lead.get("website")
    )
    return lead

# ── MAIN ──────────────────────────────────────────────────────────────────────

def run_scraper():
    log.info("=" * 60)
    log.info("FMCSA Scraper — %d-day window, limit %d leads", DAYS_BACK, ENRICH_LIMIT)
    log.info("FMCSA API key: %s", "YES" if FMCSA_API_KEY else "NO (DOT.report + Bing)")
    log.info("=" * 60)

    init_db()
    leads = fetch_new_registrations()

    if not leads:
        log.warning("No leads fetched from Socrata.")
        return 0

    leads = [l for l in leads if l.get("company_name") and l.get("state")]
    log.info("After pre-filter: %d leads", len(leads))

    # Enrich oldest first (higher hit rate on DOT.report)
    leads.sort(key=lambda x: str(x.get("registration_date") or ""))
    to_enrich = leads[:ENRICH_LIMIT]

    batch    = []
    inserted = 0
    stats    = {"fmcsa": 0, "dot_report": 0, "aggregator": 0,
                "high": 0, "medium": 0, "none": 0}

    for i, lead in enumerate(to_enrich):
        log.info("[%d/%d] DOT %s — %s (%s)",
                 i + 1, len(to_enrich),
                 lead["dot_number"], lead["company_name"], lead["state"])

        lead = layer_fmcsa(lead)
        if lead["phone_source"] == "fmcsa_api":
            stats["fmcsa"] += 1

        if not lead["phone"] or lead["phone_confidence"] != "high":
            lead = layer_dot_report(lead)
            if lead["phone_source"] == "dot_report":
                stats["dot_report"] += 1

        if not lead["phone"] or lead["phone_confidence"] == "none":
            lead = layer_aggregator(lead)
            if lead["phone_source"] == "aggregator":
                stats["aggregator"] += 1

        lead = finalize(lead)

        conf = lead["phone_confidence"]
        if conf in stats:
            stats[conf] += 1

        log.info("  → phone=%-18s | conf=%-6s | source=%s",
                 lead["phone"] or "(none)",
                 lead["phone_confidence"],
                 lead["phone_source"] or "—")

        batch.append(lead)

        # ✅ THE KEY FIX: save to DB every BATCH_SIZE leads
        if len(batch) >= BATCH_SIZE:
            inserted += batch_insert(batch)
            batch = []

    # Save final partial batch
    if batch:
        inserted += batch_insert(batch)

    log.info("=" * 60)
    log.info("DONE — processed=%d | saved=%d", len(to_enrich), inserted)
    log.info("Sources: FMCSA=%d DOT.report=%d Bing=%d",
             stats["fmcsa"], stats["dot_report"], stats["aggregator"])
    log.info("Confidence: high=%d medium=%d none=%d",
             stats["high"], stats["medium"], stats["none"])
    log.info("=" * 60)
    return inserted


if __name__ == "__main__":
    run_scraper()
