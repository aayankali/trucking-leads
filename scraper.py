"""
Trucking Lead Scraper — uses FMCSA REST API for enrichment (phone, owner, insurance).
Replaces the broken SAFER HTML scraper which was being blocked.

Env vars required:
  DATABASE_URL   — PostgreSQL connection string (Railway provides this)
  FMCSA_API_KEY  — Your key from https://ask.fmcsa.dot.gov/app/answers/detail/a_id/48
"""

import os
import logging
import psycopg2
import psycopg2.extras
import requests
from datetime import datetime, timedelta
import time

# ── CONFIG ─────────────────────────────────────────────────────────────────────

DATABASE_URL  = os.environ.get("DATABASE_URL", "")
FMCSA_API_KEY = os.environ.get("FMCSA_API_KEY", "")

SODA_URL      = "https://data.transportation.gov/resource/az4n-8mr2.json"
FMCSA_BASE    = "https://mobile.fmcsa.dot.gov/qc/services/carriers"

DAYS_BACK   = 3      # how far back to look for new registrations
PAGE_SIZE   = 1000   # Socrata page size
BATCH_SIZE  = 100    # DB insert batch size
ENRICH_LIMIT = 300   # max leads to enrich per run (API rate-limit safety)
DELAY        = 0.4   # seconds between FMCSA API calls (~150 req/min safe)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── DATABASE ───────────────────────────────────────────────────────────────────

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


def batch_insert(leads):
    """Insert leads, UPDATE existing rows with any newly enriched fields."""
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
                    ON CONFLICT (dot_number) DO UPDATE SET
                        phone         = CASE WHEN EXCLUDED.phone != '' THEN EXCLUDED.phone ELSE leads.phone END,
                        owner_name    = CASE WHEN EXCLUDED.owner_name != '' THEN EXCLUDED.owner_name ELSE leads.owner_name END,
                        power_units   = CASE WHEN EXCLUDED.power_units > 0 THEN EXCLUDED.power_units ELSE leads.power_units END,
                        has_insurance = EXCLUDED.has_insurance,
                        status        = EXCLUDED.status
                """, lead)
                inserted += c.rowcount

        conn.commit()
    except Exception as e:
        log.error("DB insert error: %s", e)
        conn.rollback()
    finally:
        conn.close()

    return inserted


# ── SOCRATA FETCH ──────────────────────────────────────────────────────────────

def fetch_new_registrations():
    """Pull recently registered/updated carriers from the FMCSA public dataset."""
    cutoff = (datetime.utcnow() - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT%H:%M:%S")
    log.info("Fetching new registrations from Socrata (last %d days)...", DAYS_BACK)

    leads  = []
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
            log.error("Socrata fetch error at offset %d: %s", offset, e)
            break

        if not data:
            break

        for row in data:
            lead = _build_lead_from_socrata(row)
            if lead:
                leads.append(lead)

        log.info("  fetched %d so far (offset %d)...", len(leads), offset)

        if len(data) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    log.info("Total fetched from Socrata: %d", len(leads))
    return leads


def _build_lead_from_socrata(row):
    dot = str(row.get("dot_number") or "").strip()
    if not dot:
        return None

    # Parse registration date from add_date field
    reg_date = None
    raw_date = row.get("add_date") or row.get("mcs150_date") or ""
    if raw_date:
        try:
            reg_date = datetime.fromisoformat(raw_date[:10]).date()
        except Exception:
            reg_date = datetime.utcnow().date()

    return {
        "dot_number":       dot,
        "mc_number":        row.get("mc_mx_ff_number") or "",
        "company_name":     row.get("legal_name") or "",
        "owner_name":       "",          # filled by FMCSA API enrichment
        "phone":            "",          # filled by FMCSA API enrichment
        "email":            "",
        "address":          row.get("phy_street") or "",
        "city":             row.get("phy_city") or "",
        "state":            row.get("phy_state") or "",
        "zip_code":         row.get("phy_zip") or "",
        "entity_type":      row.get("entity_type_desc") or "",
        "operation_type":   row.get("carrier_operation") or "",
        "cargo_type":       "",
        "drivers":          _safe_int(row.get("total_drivers")),
        "power_units":      _safe_int(row.get("total_power_units")),
        "status":           "A",
        "added_date":       datetime.utcnow(),
        "registration_date": reg_date or datetime.utcnow().date(),
        "has_insurance":    False,
    }


def _safe_int(val):
    try:
        return int(val or 0)
    except (ValueError, TypeError):
        return 0


# ── FMCSA REST API ENRICHMENT ──────────────────────────────────────────────────

def enrich_from_fmcsa_api(dot):
    """
    Call the official FMCSA REST API to get phone, owner, insurance status.
    API docs: https://ai.fmcsa.dot.gov/SMS/Carrier/{dot}/
    Returns a dict with keys: phone, owner_name, power_units, has_insurance
    """
    if not FMCSA_API_KEY:
        return {}

    url = f"{FMCSA_BASE}/{dot}"
    params = {"webKey": FMCSA_API_KEY}

    try:
        r = requests.get(url, params=params, timeout=15)

        if r.status_code == 404:
            return {}          # carrier not found — brand new, not yet indexed
        if r.status_code == 429:
            log.warning("FMCSA API rate limited — sleeping 10s")
            time.sleep(10)
            return {}
        if r.status_code != 200:
            log.warning("FMCSA API %s → HTTP %d", dot, r.status_code)
            return {}

        data = r.json()
        carrier = data.get("content", {})

        if not carrier:
            return {}

        # Extract phone — API returns it as phyTelephone
        phone = (
            carrier.get("phyTelephone")
            or carrier.get("mailingTelephone")
            or ""
        )

        # Clean phone: keep only digits + formatting
        if phone:
            phone = phone.strip()

        # Owner / DBA name
        owner = (
            carrier.get("dbaName")
            or carrier.get("legalName")
            or ""
        ).strip()

        # Power units
        power_units = _safe_int(carrier.get("totalPowerUnits"))

        # Insurance — FMCSA marks active/authorized carriers
        operating_status = (carrier.get("allowedToOperate") or "").upper()
        has_insurance = operating_status == "Y"

        return {
            "phone":        phone,
            "owner_name":   owner,
            "power_units":  power_units,
            "has_insurance": has_insurance,
        }

    except Exception as e:
        log.warning("FMCSA API error for DOT %s: %s", dot, e)
        return {}


# ── MAIN SCRAPER ───────────────────────────────────────────────────────────────

def run_scraper():
    log.info("=" * 60)
    log.info("Starting FMCSA scraper run")
    log.info("FMCSA API key present: %s", bool(FMCSA_API_KEY))
    log.info("=" * 60)

    init_db()

    leads = fetch_new_registrations()

    if not leads:
        log.info("No new leads found. Exiting.")
        return 0

    enriched_count = 0
    batch    = []
    inserted = 0

    for i, lead in enumerate(leads):

        # Enrich via FMCSA API (up to ENRICH_LIMIT per run)
        if FMCSA_API_KEY and i < ENRICH_LIMIT:
            api_data = enrich_from_fmcsa_api(lead["dot_number"])

            if api_data:
                if api_data.get("phone"):
                    lead["phone"] = api_data["phone"]
                if api_data.get("owner_name"):
                    lead["owner_name"] = api_data["owner_name"]
                if api_data.get("power_units", 0) > 0:
                    lead["power_units"] = api_data["power_units"]
                lead["has_insurance"] = api_data.get("has_insurance", False)
                enriched_count += 1

            # Log first 5 for debugging
            if i < 5:
                log.info(
                    "ENRICHED DOT %s → phone=%s owner=%s insured=%s",
                    lead["dot_number"],
                    lead.get("phone") or "(none)",
                    lead.get("owner_name") or "(none)",
                    lead.get("has_insurance"),
                )

            time.sleep(DELAY)

        batch.append(lead)

        if len(batch) >= BATCH_SIZE:
            inserted += batch_insert(batch)
            batch = []

    if batch:
        inserted += batch_insert(batch)

    log.info("=" * 60)
    log.info("DONE — leads processed: %d | enriched: %d | DB rows affected: %d",
             len(leads), enriched_count, inserted)
    log.info("=" * 60)
    return inserted


if __name__ == "__main__":
    run_scraper()
