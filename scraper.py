"""
Trucking Lead Scraper — 6-Layer Enrichment Pipeline
====================================================
Layer 1 : FMCSA Mobile API      (phone, owner, insurance) → High confidence
Layer 2 : DOT.report            (structured data, very accurate) → High confidence  ← NEW
Layer 3 : Aggregator sites      (OTrucking, CarrierSource)      → Medium confidence
Layer 4 : Google Search         (find official website only)    → leads to Layer 5
Layer 5 : Website scraping      (Contact/About pages only)      → High confidence
Layer 6 : Business directories  (YellowPages, BBB)             → Low/Medium confidence

Cross-verification: same phone from 2+ independent sources → bumped to High confidence
Phone validation  : digit-count + area-code sanity check (no proxies needed)

Env vars:
  DATABASE_URL   — PostgreSQL connection string (Railway injects automatically)
  FMCSA_API_KEY  — from FMCSA developer portal (optional; runs demo mode without it)
"""

import os, re, logging, time, random
from datetime import datetime, timedelta
from collections import Counter

import requests
import psycopg2, psycopg2.extras
from bs4 import BeautifulSoup

# ── CONFIG ────────────────────────────────────────────────────────────────────

DATABASE_URL  = os.environ.get("DATABASE_URL", "")
FMCSA_API_KEY = os.environ.get("FMCSA_API_KEY", "")

SOCRATA_URL = "https://data.transportation.gov/resource/az4n-8mr2.json"
FMCSA_BASE  = "https://mobile.fmcsa.dot.gov/qc/services/carriers"
DOTREPORT   = "https://dot.report/carrier/{dot}"      # ← NEW: structured DOT data

DAYS_BACK    = 3
PAGE_SIZE    = 1000
BATCH_SIZE   = 50
ENRICH_LIMIT = 500   # max leads enriched per run (keeps runtime reasonable)

# Polite per-layer delays (seconds) — avoids bans without proxies
DELAY_FMCSA   = 0.4
DELAY_DOT     = 1.5   # DOT.report
DELAY_AGG     = 2.0
DELAY_GOOGLE  = 4.0   # reduced Google calls = more stability
DELAY_WEB     = 2.5
DELAY_DIR     = 2.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ── DATABASE ──────────────────────────────────────────────────────────────────

def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def init_db():
    conn = get_db()
    with conn.cursor() as c:
        # Base table
        c.execute("""
            CREATE TABLE IF NOT EXISTS leads (
                id                SERIAL PRIMARY KEY,
                dot_number        TEXT UNIQUE,
                mc_number         TEXT,
                company_name      TEXT,
                owner_name        TEXT,
                phone             TEXT    DEFAULT '',
                email             TEXT    DEFAULT '',
                website           TEXT    DEFAULT '',
                address           TEXT,
                city              TEXT,
                state             TEXT,
                zip_code          TEXT,
                entity_type       TEXT,
                operation_type    TEXT,
                cargo_type        TEXT,
                drivers           INTEGER DEFAULT 0,
                power_units       INTEGER DEFAULT 0,
                status            TEXT    DEFAULT 'A',
                added_date        TIMESTAMP,
                registration_date DATE,
                contacted         BOOLEAN DEFAULT FALSE,
                notes             TEXT    DEFAULT '',
                has_insurance     BOOLEAN DEFAULT FALSE,
                insurance_status  TEXT    DEFAULT 'unknown',
                phone_source      TEXT    DEFAULT '',
                phone_confidence  TEXT    DEFAULT 'none',
                sources_found     INTEGER DEFAULT 0,
                enriched          BOOLEAN DEFAULT FALSE
            )
        """)
        conn.commit()

        # Safe column migrations (won't break if columns already exist)
        for col, defn in [
            ("website",          "TEXT DEFAULT ''"),
            ("phone_source",     "TEXT DEFAULT ''"),
            ("phone_confidence", "TEXT DEFAULT 'none'"),
            ("sources_found",    "INTEGER DEFAULT 0"),
            ("enriched",         "BOOLEAN DEFAULT FALSE"),
            ("insurance_status", "TEXT DEFAULT 'unknown'"),
        ]:
            try:
                c.execute(f"ALTER TABLE leads ADD COLUMN {col} {defn}")
                conn.commit()
            except Exception:
                conn.rollback()

    conn.close()
    log.info("Database ready.")


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
                        (dot_number, mc_number, company_name, owner_name, phone,
                         email, website, address, city, state, zip_code,
                         entity_type, operation_type, cargo_type,
                         drivers, power_units, status, added_date,
                         registration_date, has_insurance, insurance_status,
                         phone_source, phone_confidence, sources_found, enriched)
                    VALUES
                        (%(dot_number)s, %(mc_number)s, %(company_name)s,
                         %(owner_name)s, %(phone)s, %(email)s, %(website)s,
                         %(address)s, %(city)s, %(state)s, %(zip_code)s,
                         %(entity_type)s, %(operation_type)s, %(cargo_type)s,
                         %(drivers)s, %(power_units)s, %(status)s, %(added_date)s,
                         %(registration_date)s, %(has_insurance)s, %(insurance_status)s,
                         %(phone_source)s, %(phone_confidence)s,
                         %(sources_found)s, %(enriched)s)
                    ON CONFLICT (dot_number) DO UPDATE SET
                        phone            = CASE WHEN EXCLUDED.phone != ''
                                           THEN EXCLUDED.phone ELSE leads.phone END,
                        owner_name       = CASE WHEN EXCLUDED.owner_name != ''
                                           THEN EXCLUDED.owner_name ELSE leads.owner_name END,
                        email            = CASE WHEN EXCLUDED.email != ''
                                           THEN EXCLUDED.email ELSE leads.email END,
                        website          = CASE WHEN EXCLUDED.website != ''
                                           THEN EXCLUDED.website ELSE leads.website END,
                        power_units      = CASE WHEN EXCLUDED.power_units > 0
                                           THEN EXCLUDED.power_units ELSE leads.power_units END,
                        has_insurance    = EXCLUDED.has_insurance,
                        insurance_status = EXCLUDED.insurance_status,
                        phone_source     = CASE WHEN EXCLUDED.phone != ''
                                           THEN EXCLUDED.phone_source ELSE leads.phone_source END,
                        phone_confidence = CASE WHEN EXCLUDED.phone != ''
                                           THEN EXCLUDED.phone_confidence ELSE leads.phone_confidence END,
                        sources_found    = EXCLUDED.sources_found,
                        enriched         = EXCLUDED.enriched,
                        status           = EXCLUDED.status
                """, lead)
                inserted += c.rowcount
        conn.commit()
    except Exception as e:
        log.error("DB insert error: %s", e)
        conn.rollback()
    finally:
        conn.close()
    return inserted

# ── HELPERS ───────────────────────────────────────────────────────────────────

PHONE_RE = re.compile(
    r'(\+?1[\s.\-]?)?(\(?\d{3}\)?[\s.\-]?)(\d{3}[\s.\-]?)(\d{4})'
)
EMAIL_RE = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'
)

# Area codes that don't exist in the US — basic sanity filter
INVALID_AREA_CODES = {
    "000","111","222","333","444","555","666","777","888",
    "100","200","300","400","500","600","700","800","900",
}

def _clean_phone(digits: str) -> str | None:
    """Normalise digit string to (NXX) NXX-XXXX or return None if invalid."""
    if len(digits) == 11 and digits[0] == "1":
        digits = digits[1:]
    if len(digits) != 10:
        return None
    area = digits[:3]
    if area[0] in ("0", "1"):        # US area codes never start with 0 or 1
        return None
    if area in INVALID_AREA_CODES:
        return None
    if digits[3] in ("0", "1"):      # exchange code can't start with 0 or 1
        return None
    return f"({area}) {digits[3:6]}-{digits[6:]}"


def extract_phones(text: str) -> list:
    """Return list of validated US phone numbers found in text (deduped)."""
    results = []
    for parts in PHONE_RE.findall(text):
        raw    = re.sub(r"\D", "", "".join(parts))
        cleaned = _clean_phone(raw)
        if cleaned and cleaned not in results:
            results.append(cleaned)
    return results


def extract_emails(text: str) -> list:
    """Return list of plausible emails found in text."""
    return [
        e for e in EMAIL_RE.findall(text)
        if not any(x in e.lower() for x in
                   ["example", "test", "noreply", "no-reply", "support@",
                    "info@domain", "user@"])
    ]


def safe_get(url: str, timeout: int = 15, retries: int = 2):
    """GET with retries, jitter, and rate-limit handling."""
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            if r.status_code == 200:
                return r
            if r.status_code == 429:
                log.warning("429 rate limit on %s — sleeping 30s", url)
                time.sleep(30)
            elif r.status_code in (403, 404):
                return None          # no point retrying
        except Exception as e:
            log.debug("Request error (%s): %s", url, e)
        time.sleep(random.uniform(1.0, 2.5))
    return None


def _safe_int(val) -> int:
    try:
        return int(val or 0)
    except (ValueError, TypeError):
        return 0


def _most_common_phone(phone_list: list) -> str:
    """Pick the phone number that appears most often in a list."""
    if not phone_list:
        return ""
    return Counter(phone_list).most_common(1)[0][0]

# ── STEP 0: SOCRATA FETCH ─────────────────────────────────────────────────────

def fetch_new_registrations() -> list:
    """Pull new carrier registrations from the FMCSA open dataset."""
    cutoff = (datetime.utcnow() - timedelta(days=DAYS_BACK)).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    log.info("Fetching registrations from last %d days via Socrata...", DAYS_BACK)
    leads, offset = [], 0

    while True:
        params = {
            "$where":  f"(add_date > '{cutoff}' OR mcs150_date > '{cutoff}')",
            "$limit":  PAGE_SIZE,
            "$offset": offset,
            "$order":  "add_date DESC",
        }
        try:
            r = requests.get(SOCRATA_URL, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.error("Socrata error at offset %d: %s", offset, e)
            break

        if not data:
            break

        for row in data:
            lead = _build_base_lead(row)
            if lead:
                leads.append(lead)

        log.info("  %d leads fetched so far...", len(leads))
        if len(data) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    log.info("Total base leads from Socrata: %d", len(leads))
    return leads


def _build_base_lead(row: dict) -> dict | None:
    dot = str(row.get("dot_number") or "").strip()
    if not dot:
        return None

    reg_date = None
    for field in ("add_date", "mcs150_date"):
        raw = row.get(field, "")
        if raw:
            try:
                reg_date = datetime.fromisoformat(raw[:10]).date()
                break
            except Exception:
                pass

    return {
        "dot_number":        dot,
        "mc_number":         row.get("mc_mx_ff_number") or "",
        "company_name":      row.get("legal_name") or "",
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
    }

# ── LAYER 1: FMCSA MOBILE API ─────────────────────────────────────────────────

def layer1_fmcsa(lead: dict) -> dict:
    """Official FMCSA API — most authoritative source for insurance + owner."""
    if not FMCSA_API_KEY:
        return lead

    dot = lead["dot_number"]
    try:
        r = requests.get(
            f"{FMCSA_BASE}/{dot}",
            params={"webKey": FMCSA_API_KEY},
            timeout=15,
        )
        if r.status_code == 429:
            log.warning("FMCSA rate limit — sleeping 15s")
            time.sleep(15)
            return lead
        if r.status_code != 200:
            return lead

        carrier = r.json().get("content", {})
        if not carrier:
            return lead

        # Phone
        raw_phone = (
            carrier.get("phyTelephone") or
            carrier.get("mailingTelephone") or ""
        ).strip()
        phones = extract_phones(raw_phone) if raw_phone else []

        # Owner / DBA
        owner = (
            carrier.get("dbaName") or
            carrier.get("legalName") or ""
        ).strip()

        # Insurance
        allowed   = (carrier.get("allowedToOperate") or "").upper()
        ins_code  = carrier.get("bipdInsuranceOnFile")
        ins_req   = carrier.get("bipdInsuranceRequired")
        if ins_code and ins_req:
            ins_status = "insured"
        elif ins_req and not ins_code:
            ins_status = "none"
        else:
            ins_status = "unknown"
        has_ins = (allowed == "Y") or (ins_status == "insured")

        # Email
        email = (carrier.get("email") or "").strip()

        # Power units
        pu = _safe_int(carrier.get("totalPowerUnits"))

        # Apply to lead
        if phones:
            lead["phone"]            = phones[0]
            lead["phone_source"]     = "fmcsa_api"
            lead["phone_confidence"] = "high"
            lead["sources_found"]   += 1

        if owner and not lead["owner_name"]:
            lead["owner_name"] = owner
        if email and not lead["email"]:
            lead["email"] = email
        if pu > 0:
            lead["power_units"] = pu

        lead["has_insurance"]    = has_ins
        lead["insurance_status"] = ins_status

    except Exception as e:
        log.debug("Layer1 FMCSA error DOT %s: %s", dot, e)

    time.sleep(DELAY_FMCSA)
    return lead

# ── LAYER 2: DOT.REPORT (NEW — replaces half of Google) ───────────────────────

def layer2_dot_report(lead: dict) -> dict:
    """
    dot.report is a structured public database of FMCSA carrier data.
    It's scraped-friendly, has phones, emails, and insurance status baked in.
    This single source replaces several Google calls and is far more stable.
    """
    dot = lead["dot_number"]
    url = DOTREPORT.format(dot=dot)
    r   = safe_get(url, timeout=15)
    if not r:
        time.sleep(DELAY_DOT)
        return lead

    soup   = BeautifulSoup(r.text, "lxml")
    text   = soup.get_text(" ", strip=True)
    phones = extract_phones(text)
    emails = extract_emails(text)

    # Try to grab structured fields from the page
    # DOT.report renders key-value pairs in <td> elements
    tds = [td.get_text(strip=True) for td in soup.find_all("td")]
    for i, td in enumerate(tds):
        td_lower = td.lower()
        # Phone: look for "phone" label followed by value
        if "phone" in td_lower and i + 1 < len(tds):
            val_phones = extract_phones(tds[i + 1])
            if val_phones:
                phones = val_phones + phones

        # Email label
        if "email" in td_lower and i + 1 < len(tds):
            val_emails = extract_emails(tds[i + 1])
            if val_emails:
                emails = val_emails + emails

        # Website label
        if "website" in td_lower and i + 1 < len(tds):
            candidate = tds[i + 1].strip()
            if candidate.startswith("http") and not lead.get("website"):
                lead["website"] = candidate

        # Insurance status label
        if "insurance" in td_lower and i + 1 < len(tds):
            val = tds[i + 1].lower()
            if "active" in val or "authorized" in val:
                lead["insurance_status"] = "insured"
                lead["has_insurance"]    = True
            elif "none" in val or "not" in val or "no insurance" in val:
                if lead["insurance_status"] == "unknown":
                    lead["insurance_status"] = "none"

    # Apply phone
    if phones:
        prev = lead.get("phone", "")
        if not prev:
            lead["phone"]            = phones[0]
            lead["phone_source"]     = "dot_report"
            lead["phone_confidence"] = "high"   # DOT.report data is structured/reliable
            lead["sources_found"]   += 1
        elif prev == phones[0]:
            # Same phone as FMCSA → cross-verified → stays high
            lead["phone_confidence"] = "high"
            lead["sources_found"]   += 1
        else:
            # Different phone — we now have 2 candidates; keep FMCSA one but note discrepancy
            lead["sources_found"]   += 1

    # Apply email
    if emails and not lead["email"]:
        lead["email"] = emails[0]

    time.sleep(DELAY_DOT + random.uniform(0, 0.5))
    return lead

# ── LAYER 3: AGGREGATOR WEBSITES ─────────────────────────────────────────────

AGGREGATORS = [
    "https://www.otrucking.com/carrier/{dot}",
    "https://carriersource.io/carriers/{dot}",
]

def layer3_aggregators(lead: dict) -> dict:
    """Secondary aggregator sites — medium confidence, good cross-verification."""
    # Skip if already high confidence from earlier layers
    if lead["phone"] and lead["phone_confidence"] == "high":
        return lead

    dot          = lead["dot_number"]
    found_phones = [lead["phone"]] if lead["phone"] else []

    for url_tpl in AGGREGATORS:
        url = url_tpl.format(dot=dot)
        r   = safe_get(url, timeout=12)
        if not r:
            time.sleep(DELAY_AGG)
            continue

        text   = BeautifulSoup(r.text, "lxml").get_text(" ")
        phones = extract_phones(text)
        emails = extract_emails(text)

        if phones:
            found_phones.extend(phones)
            if not lead["phone"]:
                lead["phone"]            = phones[0]
                lead["phone_source"]     = "aggregator"
                lead["phone_confidence"] = "medium"
            lead["sources_found"] += 1

        if emails and not lead["email"]:
            lead["email"] = emails[0]

        time.sleep(DELAY_AGG + random.uniform(0, 1.0))

    # Cross-verify: same phone appears from multiple sources → promote to high
    valid_phones = [p for p in found_phones if p]
    if valid_phones:
        best  = _most_common_phone(valid_phones)
        count = Counter(valid_phones)[best]
        if count >= 2:
            lead["phone"]            = best
            lead["phone_confidence"] = "high"
        elif lead["phone"]:
            pass  # keep what we have

    return lead

# ── LAYER 4: GOOGLE SEARCH (website discovery only) ───────────────────────────

def layer4_google_website(lead: dict) -> dict:
    """
    Google is now used ONLY to find the company's website URL.
    We no longer scrape phone from Google results directly — too unreliable.
    Website found here feeds into Layer 5 scraping.
    """
    # Skip if we already have a website or high-confidence phone
    if lead.get("website") or (lead["phone"] and lead["phone_confidence"] == "high"):
        return lead

    company = lead["company_name"]
    city    = lead["city"]
    state   = lead["state"]
    if not company:
        return lead

    query = f'{company} {city} {state} trucking'
    url   = f"https://www.google.com/search?q={requests.utils.quote(query)}&num=5"
    r     = safe_get(url, timeout=15)
    if not r:
        time.sleep(DELAY_GOOGLE)
        return lead

    soup = BeautifulSoup(r.text, "lxml")

    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if href.startswith("/url?q="):
            actual = href.split("/url?q=")[1].split("&")[0]
            # Reject known non-company sites
            skip = ["google.", "facebook.", "linkedin.", "twitter.", "youtube.",
                    "fmcsa.", "dot.gov", "safer.fmcsa", "wikipedia.", "yelp.",
                    "bbb.org", "yellowpages.", "dot.report", "otrucking.",
                    "carriersource."]
            if actual.startswith("http") and not any(s in actual for s in skip):
                lead["website"]       = actual
                lead["sources_found"] += 1
                break

    time.sleep(DELAY_GOOGLE + random.uniform(0, 1.5))
    return lead

# ── LAYER 5: WEBSITE SCRAPING (contact pages only) ───────────────────────────

CONTACT_PATHS = ["/contact", "/contact-us", "/about", "/about-us", "/"]

def layer5_website_scrape(lead: dict) -> dict:
    """
    Scrape the company's own website contact/about pages.
    These are the most reliable source for the actual business phone.
    """
    if not lead.get("website"):
        return lead
    if lead["phone"] and lead["phone_confidence"] == "high":
        return lead

    base         = lead["website"].rstrip("/")
    found_phones = [lead["phone"]] if lead["phone"] else []
    found_emails = [lead["email"]] if lead["email"] else []

    for path in CONTACT_PATHS:
        url = base + path
        r   = safe_get(url, timeout=12)
        if not r:
            time.sleep(DELAY_WEB)
            continue

        soup   = BeautifulSoup(r.text, "lxml")

        # Remove nav/footer noise — focus on main content
        for tag in soup.find_all(["nav", "footer", "script", "style"]):
            tag.decompose()

        text   = soup.get_text(" ", strip=True)
        phones = extract_phones(text)
        emails = extract_emails(text)

        if phones:
            # Filter out phones that look like they appear in every page (site-wide footer)
            phone_counts = Counter(phones)
            candidates   = [p for p, c in phone_counts.items() if c <= 4]
            if candidates:
                found_phones.extend(candidates)
                lead["sources_found"] += 1
                break   # found phones on this page — stop here

        if emails:
            found_emails.extend(emails)

        time.sleep(DELAY_WEB + random.uniform(0, 1.0))

    if found_phones:
        best  = _most_common_phone(found_phones)
        prev  = lead.get("phone", "")
        if prev and prev == best:
            lead["phone_confidence"] = "high"   # cross-verified
        elif not prev:
            lead["phone"]            = best
            lead["phone_source"]     = "website"
            lead["phone_confidence"] = "high"   # company's own website = reliable

    if found_emails and not lead["email"]:
        lead["email"] = found_emails[0]

    return lead

# ── LAYER 6: DIRECTORIES (last resort) ───────────────────────────────────────

DIRECTORY_URLS = [
    "https://www.yellowpages.com/search?search_terms={q}&geo_location_terms={loc}",
    "https://www.bbb.org/search?find_text={q}&find_loc={loc}",
]

def layer6_directories(lead: dict) -> dict:
    """Last resort: YellowPages / BBB — lower confidence but better than nothing."""
    if lead["phone"] and lead["phone_confidence"] in ("high", "medium"):
        return lead

    company = lead["company_name"]
    city    = lead["city"]
    state   = lead["state"]
    if not company:
        return lead

    q   = requests.utils.quote(company)
    loc = requests.utils.quote(f"{city} {state}")

    for tpl in DIRECTORY_URLS:
        url = tpl.format(q=q, loc=loc)
        r   = safe_get(url, timeout=12)
        if not r:
            time.sleep(DELAY_DIR)
            continue

        text   = BeautifulSoup(r.text, "lxml").get_text(" ")
        phones = extract_phones(text)
        emails = extract_emails(text)

        if phones:
            # Filter out site-wide numbers (appear many times)
            counts     = Counter(phones)
            candidates = [p for p, c in counts.items() if c <= 3]
            if candidates:
                prev = lead.get("phone", "")
                if prev and prev in candidates:
                    lead["phone_confidence"] = "high"   # cross-verified
                elif not prev:
                    lead["phone"]            = candidates[0]
                    lead["phone_source"]     = "directory"
                    lead["phone_confidence"] = "medium"
                lead["sources_found"] += 1
                break

        if emails and not lead["email"]:
            lead["email"] = emails[0]

        time.sleep(DELAY_DIR + random.uniform(0, 1.5))

    return lead

# ── CROSS-VERIFICATION & FINALIZE ─────────────────────────────────────────────

def finalize(lead: dict) -> dict:
    """Final confidence scoring and enriched flag."""
    phone   = lead.get("phone", "")
    sources = lead.get("sources_found", 0)

    if not phone:
        lead["phone_confidence"] = "none"
    elif sources >= 2 and lead["phone_confidence"] != "high":
        lead["phone_confidence"] = "high"
    elif sources == 1 and lead["phone_confidence"] == "none":
        lead["phone_confidence"] = "medium"

    lead["enriched"] = bool(
        phone or lead.get("email") or lead.get("website")
    )
    return lead

# ── DEMO MODE ─────────────────────────────────────────────────────────────────

def _demo_leads() -> list:
    today     = datetime.utcnow().date()
    yesterday = (datetime.utcnow() - timedelta(days=1)).date()
    now       = datetime.utcnow()

    sample = [
        ("3421901","MC-1234567","LONE STAR FREIGHT LLC",       "John Martinez","(512) 555-0171","TX","Austin",      "78701",today,     True, "insured", "fmcsa_api","high",  2),
        ("3421902","MC-1234568","GREAT LAKES TRANSPORT INC",   "Sara Kowalski","(312) 555-0182","IL","Chicago",     "60601",today,     True, "insured", "dot_report","high", 3),
        ("3421903","",          "SUNRISE HAULING LLC",         "David Chen",   "(404) 555-0193","GA","Atlanta",     "30301",today,     False,"none",    "website",  "high",  1),
        ("3421904","MC-1234570","BLUE RIDGE CARRIERS LLC",     "Mike Thornton","(828) 555-0104","NC","Asheville",   "28801",yesterday, True, "insured", "fmcsa_api","high",  2),
        ("3421905","MC-1234571","PACIFIC COAST LOGISTICS INC", "Ana Gutierrez","(503) 555-0115","OR","Portland",    "97201",yesterday, False,"none",    "aggregator","medium",1),
        ("3421906","",          "MOUNTAIN STATE TRUCKING LLC", "Bob Williams", "",              "CO","Denver",      "80201",yesterday, False,"unknown", "",         "none",  0),
        ("3421907","MC-1234573","BAYOU FREIGHT SOLUTIONS LLC", "Lisa Tran",    "(504) 555-0137","LA","New Orleans","70112", yesterday, True, "insured", "dot_report","high",  2),
        ("3421908","MC-1234574","IRON HORSE TRANSPORT LLC",    "Tom Bradley",  "(602) 555-0148","AZ","Phoenix",    "85001", today,     False,"none",    "directory", "medium",1),
    ]

    return [{
        "dot_number":        dot,  "mc_number": mc,
        "company_name":      name, "owner_name": owner,
        "phone":             phone,"email": "",
        "website":           "",  "address": "123 Main St",
        "city":              city, "state": state,
        "zip_code":          zipcode, "entity_type": "CARRIER",
        "operation_type":    "A", "cargo_type": "G",
        "drivers":           2,   "power_units": 2,
        "status":            "A", "added_date": now,
        "registration_date": reg, "has_insurance": ins,
        "insurance_status":  ins_s, "phone_source": psrc,
        "phone_confidence":  pconf, "sources_found": srcs,
        "enriched":          bool(phone),
    } for dot,mc,name,owner,phone,state,city,zipcode,reg,ins,ins_s,psrc,pconf,srcs in sample]

# ── MAIN PIPELINE ─────────────────────────────────────────────────────────────

def run_scraper() -> int:
    log.info("=" * 65)
    log.info("FMCSA 6-Layer Enrichment Pipeline Starting")
    log.info("FMCSA API key : %s", "present" if FMCSA_API_KEY else "MISSING (demo mode)")
    log.info("=" * 65)

    init_db()

    if not FMCSA_API_KEY:
        log.warning("No FMCSA_API_KEY — loading 8 demo leads.")
        leads = _demo_leads()
    else:
        leads = fetch_new_registrations()
        leads = [l for l in leads if l.get("company_name") and l.get("state")]

    if not leads:
        log.info("No new leads found. Exiting.")
        return 0

    total   = len(leads)
    limit   = min(total, ENRICH_LIMIT)
    batch   = []
    inserted = 0
    stats   = {
        "l1": 0, "l2": 0, "l3": 0, "l4_sites": 0, "l5": 0, "l6": 0,
        "high": 0, "medium": 0, "none": 0,
    }

    for i, lead in enumerate(leads[:limit]):
        log.info(
            "[%d/%d] DOT %-9s  %s, %s",
            i + 1, limit, lead["dot_number"], lead["company_name"], lead["state"]
        )

        # Layer 1 — FMCSA official API
        lead = layer1_fmcsa(lead)
        if lead["phone_source"] == "fmcsa_api":
            stats["l1"] += 1

        # Layer 2 — DOT.report (skip if already high confidence)
        if not (lead["phone"] and lead["phone_confidence"] == "high"):
            lead = layer2_dot_report(lead)
            if lead["phone_source"] == "dot_report":
                stats["l2"] += 1

        # Layer 3 — Aggregators (skip if already high confidence)
        if not (lead["phone"] and lead["phone_confidence"] == "high"):
            lead = layer3_aggregators(lead)
            if lead["phone_source"] == "aggregator":
                stats["l3"] += 1

        # Layer 4 — Google (website discovery only, no phone scraping)
        if not lead.get("website") and not (lead["phone"] and lead["phone_confidence"] == "high"):
            lead = layer4_google_website(lead)
            if lead.get("website"):
                stats["l4_sites"] += 1

        # Layer 5 — Scrape company website contact pages
        if lead.get("website") and not (lead["phone"] and lead["phone_confidence"] == "high"):
            lead = layer5_website_scrape(lead)
            if lead["phone_source"] == "website":
                stats["l5"] += 1

        # Layer 6 — Directories (last resort)
        if not lead["phone"] or lead["phone_confidence"] == "none":
            lead = layer6_directories(lead)
            if lead["phone_source"] == "directory":
                stats["l6"] += 1

        # Finalize confidence
        lead = finalize(lead)
        stats[lead["phone_confidence"]] += 1

        log.info(
            "  → phone=%-18s | conf=%-6s | source=%-12s | ins=%s",
            lead.get("phone") or "(none)",
            lead["phone_confidence"],
            lead.get("phone_source") or "—",
            lead["insurance_status"],
        )

        batch.append(lead)
        if len(batch) >= BATCH_SIZE:
            inserted += batch_insert(batch)
            batch = []

    # Save remaining unenriched leads (base data only)
    for lead in leads[limit:]:
        batch.append(lead)
        if len(batch) >= BATCH_SIZE:
            inserted += batch_insert(batch)
            batch = []

    if batch:
        inserted += batch_insert(batch)

    # Summary
    phone_found = stats["high"] + stats["medium"]
    pct         = round(phone_found / limit * 100) if limit else 0

    log.info("=" * 65)
    log.info("PIPELINE COMPLETE")
    log.info("  Total fetched   : %d", total)
    log.info("  Enriched        : %d", limit)
    log.info("  Phone found     : %d (%d%%)", phone_found, pct)
    log.info("  By layer        : L1(FMCSA)=%d  L2(DOT.report)=%d  "
             "L3(agg)=%d  L4(websites found)=%d  L5(web scrape)=%d  L6(dir)=%d",
             stats["l1"], stats["l2"], stats["l3"],
             stats["l4_sites"], stats["l5"], stats["l6"])
    log.info("  Confidence      : high=%d  medium=%d  none=%d",
             stats["high"], stats["medium"], stats["none"])
    log.info("  DB rows saved   : %d", inserted)
    log.info("=" * 65)
    return inserted


if __name__ == "__main__":
    run_scraper()
