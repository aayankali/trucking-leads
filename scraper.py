"""
Trucking Lead Scraper — 5-Layer Enrichment Pipeline
====================================================
Layer 1: FMCSA Mobile API        (phone, owner, insurance) → High confidence
Layer 2: Aggregator sites        (OTrucking, CarrierSource) → Medium confidence  
Layer 3: Google Search           (find official website)    → leads to Layer 4
Layer 4: Website scraping        (Contact/About pages)      → High confidence
Layer 5: Business directories    (Google Maps, Yelp, BBB)   → Low/Medium confidence

Cross-verification: same phone from 2+ sources = High confidence

Env vars:
  DATABASE_URL   — PostgreSQL (Railway provides)
  FMCSA_API_KEY  — from FMCSA developer portal
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

DAYS_BACK    = 3
PAGE_SIZE    = 1000
BATCH_SIZE   = 50
ENRICH_LIMIT = 500   # leads to enrich per run

# Per-layer delays (seconds) — polite, no proxies needed
DELAY_FMCSA  = 0.4
DELAY_AGG    = 2.0
DELAY_GOOGLE = 3.5
DELAY_WEB    = 2.5
DELAY_DIR    = 2.0

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
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
                mc_number         TEXT,
                company_name      TEXT,
                owner_name        TEXT,
                phone             TEXT,
                email             TEXT,
                website           TEXT,
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
                has_insurance     BOOLEAN DEFAULT FALSE,
                insurance_status  TEXT DEFAULT 'unknown',
                phone_source      TEXT DEFAULT '',
                phone_confidence  TEXT DEFAULT 'none',
                sources_found     INTEGER DEFAULT 0,
                enriched          BOOLEAN DEFAULT FALSE
            )
        """)
        # Migrate existing tables
        existing = [r[0] for r in c.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='leads'"
        ).fetchall()] if False else []
        try:
            c.execute("SELECT website FROM leads LIMIT 1")
        except Exception:
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
                except Exception:
                    pass
    conn.commit()
    conn.close()
    log.info("Database ready.")

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
                         email, website, address, city, state, zip_code, entity_type,
                         operation_type, cargo_type, drivers, power_units, status,
                         added_date, registration_date, has_insurance, insurance_status,
                         phone_source, phone_confidence, sources_found, enriched)
                    VALUES
                        (%(dot_number)s, %(mc_number)s, %(company_name)s, %(owner_name)s,
                         %(phone)s, %(email)s, %(website)s, %(address)s, %(city)s,
                         %(state)s, %(zip_code)s, %(entity_type)s, %(operation_type)s,
                         %(cargo_type)s, %(drivers)s, %(power_units)s, %(status)s,
                         %(added_date)s, %(registration_date)s, %(has_insurance)s,
                         %(insurance_status)s, %(phone_source)s, %(phone_confidence)s,
                         %(sources_found)s, %(enriched)s)
                    ON CONFLICT (dot_number) DO UPDATE SET
                        phone            = CASE WHEN EXCLUDED.phone != '' THEN EXCLUDED.phone ELSE leads.phone END,
                        owner_name       = CASE WHEN EXCLUDED.owner_name != '' THEN EXCLUDED.owner_name ELSE leads.owner_name END,
                        email            = CASE WHEN EXCLUDED.email != '' THEN EXCLUDED.email ELSE leads.email END,
                        website          = CASE WHEN EXCLUDED.website != '' THEN EXCLUDED.website ELSE leads.website END,
                        power_units      = CASE WHEN EXCLUDED.power_units > 0 THEN EXCLUDED.power_units ELSE leads.power_units END,
                        has_insurance    = EXCLUDED.has_insurance,
                        insurance_status = EXCLUDED.insurance_status,
                        phone_source     = CASE WHEN EXCLUDED.phone != '' THEN EXCLUDED.phone_source ELSE leads.phone_source END,
                        phone_confidence = CASE WHEN EXCLUDED.phone != '' THEN EXCLUDED.phone_confidence ELSE leads.phone_confidence END,
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
    r'(\+?1[\s.\-]?)?'
    r'(\(?\d{3}\)?[\s.\-]?)'
    r'(\d{3}[\s.\-]?)'
    r'(\d{4})'
)
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')

def extract_phones(text):
    """Return list of cleaned US phone numbers found in text."""
    raw = PHONE_RE.findall(text)
    phones = []
    for parts in raw:
        digits = re.sub(r'\D', '', ''.join(parts))
        if len(digits) == 10:
            phones.append(f"({digits[:3]}) {digits[3:6]}-{digits[6:]}")
        elif len(digits) == 11 and digits[0] == '1':
            phones.append(f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}")
    return list(dict.fromkeys(phones))  # dedupe preserving order

def extract_emails(text):
    return [e for e in EMAIL_RE.findall(text)
            if not any(x in e.lower() for x in ['example','test','noreply','no-reply'])]

def safe_get(url, timeout=15, retries=2):
    """GET with retries and jitter. Returns response or None."""
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            if r.status_code == 200:
                return r
            if r.status_code == 429:
                log.warning("Rate limited on %s — sleeping 30s", url)
                time.sleep(30)
        except Exception as e:
            log.debug("Request failed (%s): %s", url, e)
        time.sleep(random.uniform(1, 2.5))
    return None

def _safe_int(val):
    try:
        return int(val or 0)
    except (ValueError, TypeError):
        return 0

# ── STEP 1: SOCRATA FETCH ─────────────────────────────────────────────────────

def fetch_new_registrations():
    cutoff = (datetime.utcnow() - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT%H:%M:%S")
    log.info("Fetching new registrations (last %d days)...", DAYS_BACK)
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
            log.error("Socrata error at offset %d: %s", offset, e)
            break
        if not data:
            break
        for row in data:
            lead = _build_base_lead(row)
            if lead:
                leads.append(lead)
        log.info("  %d leads so far...", len(leads))
        if len(data) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    log.info("Fetched %d base leads from Socrata.", len(leads))
    return leads

def _build_base_lead(row):
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

def layer1_fmcsa(lead):
    """Official FMCSA API — most authoritative source."""
    dot = lead["dot_number"]
    if not FMCSA_API_KEY:
        return lead

    url = f"{FMCSA_BASE}/{dot}"
    try:
        r = requests.get(url, params={"webKey": FMCSA_API_KEY}, timeout=15)
        if r.status_code == 404:
            return lead
        if r.status_code == 429:
            log.warning("FMCSA rate limit — sleeping 15s")
            time.sleep(15)
            return lead
        if r.status_code != 200:
            return lead

        data    = r.json()
        carrier = data.get("content", {})
        if not carrier:
            return lead

        # Phone
        phone = (carrier.get("phyTelephone") or carrier.get("mailingTelephone") or "").strip()
        phones = extract_phones(phone) if phone else []

        # Owner name
        owner = (carrier.get("dbaName") or carrier.get("legalName") or "").strip()

        # Power units
        pu = _safe_int(carrier.get("totalPowerUnits"))

        # Insurance / operating status
        allowed   = (carrier.get("allowedToOperate") or "").upper()
        ins_code  = carrier.get("bipdInsuranceOnFile")
        ins_req   = carrier.get("bipdInsuranceRequired")
        if ins_code and ins_req:
            ins_status = "insured"
        elif ins_req and not ins_code:
            ins_status = "none"
        else:
            ins_status = "unknown"
        has_ins = allowed == "Y" or ins_status == "insured"

        # Apply
        if phones:
            lead["phone"]            = phones[0]
            lead["phone_source"]     = "fmcsa_api"
            lead["phone_confidence"] = "high"
            lead["sources_found"]   += 1

        if owner and not lead["owner_name"]:
            lead["owner_name"] = owner
        if pu > 0:
            lead["power_units"] = pu

        lead["has_insurance"]   = has_ins
        lead["insurance_status"] = ins_status

        # Email from FMCSA
        email = (carrier.get("email") or "").strip()
        if email and not lead["email"]:
            lead["email"] = email

    except Exception as e:
        log.debug("Layer1 FMCSA error DOT %s: %s", dot, e)

    time.sleep(DELAY_FMCSA)
    return lead

# ── LAYER 2: AGGREGATOR WEBSITES ─────────────────────────────────────────────

AGGREGATORS = [
    "https://www.otrucking.com/carrier/{dot}",
    "https://carriersource.io/carriers/{dot}",
    "https://www.truckinginfo.com/carriers/{dot}",
]

def layer2_aggregators(lead):
    """Scrape trucking aggregator sites for phone/contact data."""
    if lead["phone"] and lead["phone_confidence"] == "high":
        return lead  # already have high confidence phone

    dot = lead["dot_number"]
    found_phones = [lead["phone"]] if lead["phone"] else []

    for url_tpl in AGGREGATORS:
        url = url_tpl.format(dot=dot)
        r   = safe_get(url, timeout=12)
        if not r:
            time.sleep(DELAY_AGG)
            continue

        soup   = BeautifulSoup(r.text, "lxml")
        text   = soup.get_text(" ")
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

        time.sleep(DELAY_AGG + random.uniform(0, 1))

    # Cross-verify: if same phone from 2+ sources → high confidence
    if len(set(found_phones)) == 1 and len(found_phones) >= 2:
        lead["phone_confidence"] = "high"
    elif len(found_phones) > 1:
        # Pick most common
        from collections import Counter
        most_common = Counter(found_phones).most_common(1)[0][0]
        lead["phone"] = most_common
        lead["phone_confidence"] = "high" if Counter(found_phones)[most_common] >= 2 else "medium"

    return lead

# ── LAYER 3: GOOGLE SEARCH (find website) ─────────────────────────────────────

def layer3_google_search(lead):
    """Search Google for the company's official website."""
    if lead.get("website"):
        return lead  # already have website

    company = lead["company_name"]
    state   = lead["state"]
    city    = lead["city"]

    if not company:
        return lead

    query = f'{company} {city} {state} trucking official site'
    url   = f"https://www.google.com/search?q={requests.utils.quote(query)}&num=5"

    r = safe_get(url, timeout=15)
    if not r:
        time.sleep(DELAY_GOOGLE)
        return lead

    soup  = BeautifulSoup(r.text, "lxml")
    links = []

    # Extract organic result links
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if href.startswith("/url?q="):
            actual = href.split("/url?q=")[1].split("&")[0]
            if actual.startswith("http") and not any(x in actual for x in [
                "google.", "facebook.", "linkedin.", "twitter.", "youtube.",
                "fmcsa.", "dot.gov", "safer.fmcsa", "wikipedia."
            ]):
                links.append(actual)

    if links:
        lead["website"] = links[0]
        lead["sources_found"] += 1

    time.sleep(DELAY_GOOGLE + random.uniform(0, 1.5))
    return lead

# ── LAYER 4: WEBSITE SCRAPING ─────────────────────────────────────────────────

CONTACT_PATHS = ["/contact", "/contact-us", "/about", "/about-us", "/", "/home"]

def layer4_website_scrape(lead):
    """Visit the company's website and scrape contact info."""
    if not lead.get("website"):
        return lead
    if lead["phone"] and lead["phone_confidence"] == "high":
        return lead

    base = lead["website"].rstrip("/")
    found_phones = [lead["phone"]] if lead["phone"] else []
    found_emails = [lead["email"]] if lead["email"] else []

    for path in CONTACT_PATHS:
        url = base + path
        r   = safe_get(url, timeout=12)
        if not r:
            time.sleep(DELAY_WEB)
            continue

        soup   = BeautifulSoup(r.text, "lxml")
        text   = soup.get_text(" ")
        phones = extract_phones(text)
        emails = extract_emails(text)

        if phones:
            found_phones.extend(phones)
            lead["sources_found"] += 1
        if emails:
            found_emails.extend(emails)

        # Stop at first page that has phone
        if phones:
            break

        time.sleep(DELAY_WEB + random.uniform(0, 1))

    if found_phones:
        from collections import Counter
        best = Counter(found_phones).most_common(1)[0][0]
        prev = lead.get("phone", "")
        if prev and prev == best:
            lead["phone_confidence"] = "high"  # confirmed by 2 sources
        elif not prev:
            lead["phone"]            = best
            lead["phone_source"]     = "website"
            lead["phone_confidence"] = "high"
        elif prev != best:
            # Cross-verify: different phone, keep original with medium
            pass

    if found_emails and not lead["email"]:
        lead["email"] = found_emails[0]

    return lead

# ── LAYER 5: BUSINESS DIRECTORIES ─────────────────────────────────────────────

def layer5_directories(lead):
    """Query Google Maps / Yelp / BBB for business phone."""
    if lead["phone"] and lead["phone_confidence"] in ("high", "medium"):
        return lead  # good enough already

    company = lead["company_name"]
    city    = lead["city"]
    state   = lead["state"]

    if not company:
        return lead

    sources = [
        # Google Maps (unofficial search endpoint)
        f"https://www.google.com/maps/search/{requests.utils.quote(company + ' ' + city + ' ' + state)}",
        # Yelp
        f"https://www.yelp.com/search?find_desc={requests.utils.quote(company)}&find_loc={requests.utils.quote(city + ' ' + state)}",
        # BBB
        f"https://www.bbb.org/search?find_text={requests.utils.quote(company)}&find_loc={requests.utils.quote(city + ' ' + state)}",
        # YellowPages
        f"https://www.yellowpages.com/search?search_terms={requests.utils.quote(company)}&geo_location_terms={requests.utils.quote(city + ' ' + state)}",
    ]

    for url in sources:
        r = safe_get(url, timeout=12)
        if not r:
            time.sleep(DELAY_DIR)
            continue

        text   = BeautifulSoup(r.text, "lxml").get_text(" ")
        phones = extract_phones(text)
        emails = extract_emails(text)

        if phones:
            # Filter: don't grab phones that appear >3 times (likely site footer)
            from collections import Counter
            counts = Counter(phones)
            candidates = [p for p, c in counts.items() if c <= 3]
            if candidates:
                prev = lead.get("phone", "")
                if prev and prev in candidates:
                    lead["phone_confidence"] = "high"  # cross-verified
                elif not prev:
                    lead["phone"]            = candidates[0]
                    lead["phone_source"]     = "directory"
                    lead["phone_confidence"] = "medium"
                lead["sources_found"] += 1
                break  # one directory success is enough

        if emails and not lead["email"]:
            lead["email"] = emails[0]

        time.sleep(DELAY_DIR + random.uniform(0, 1.5))

    return lead

# ── CROSS-VERIFICATION ────────────────────────────────────────────────────────

def cross_verify(lead):
    """Final confidence scoring based on sources found."""
    phone = lead.get("phone", "")
    sources = lead.get("sources_found", 0)

    if not phone:
        lead["phone_confidence"] = "none"
    elif sources >= 2 and lead["phone_confidence"] != "high":
        lead["phone_confidence"] = "high"
    elif sources == 1 and lead["phone_confidence"] == "none":
        lead["phone_confidence"] = "medium"

    lead["enriched"] = bool(phone or lead.get("email") or lead.get("website"))
    return lead

# ── MAIN SCRAPER ──────────────────────────────────────────────────────────────

def run_scraper():
    log.info("=" * 65)
    log.info("FMCSA 5-Layer Enrichment Pipeline Starting")
    log.info("API key present: %s", bool(FMCSA_API_KEY))
    log.info("=" * 65)

    init_db()
    leads = fetch_new_registrations()

    if not leads:
        log.info("No new leads. Exiting.")
        return 0

    # Pre-filter: skip leads with no company name or state
    leads = [l for l in leads if l.get("company_name") and l.get("state")]
    log.info("After pre-filter: %d leads to enrich", len(leads))

    total      = len(leads)
    limit      = min(total, ENRICH_LIMIT)
    batch      = []
    inserted   = 0
    stats = {"l1":0,"l2":0,"l3":0,"l4":0,"l5":0,"high":0,"medium":0,"none":0}

    for i, lead in enumerate(leads[:limit]):
        log.info("[%d/%d] Enriching DOT %s — %s, %s",
                 i+1, limit, lead["dot_number"], lead["company_name"], lead["state"])

        # ── Layer 1: FMCSA API ──
        lead = layer1_fmcsa(lead)
        if lead["phone"]: stats["l1"] += 1

        # ── Layer 2: Aggregators (skip if already high confidence) ──
        if not lead["phone"] or lead["phone_confidence"] != "high":
            lead = layer2_aggregators(lead)
            if lead["phone_source"] == "aggregator": stats["l2"] += 1

        # ── Layer 3: Google → find website ──
        if not lead["phone"] or lead["phone_confidence"] != "high":
            lead = layer3_google_search(lead)
            if lead.get("website"): stats["l3"] += 1

        # ── Layer 4: Scrape website ──
        if lead.get("website") and (not lead["phone"] or lead["phone_confidence"] != "high"):
            lead = layer4_website_scrape(lead)
            if lead["phone_source"] == "website": stats["l4"] += 1

        # ── Layer 5: Directories (last resort) ──
        if not lead["phone"] or lead["phone_confidence"] == "none":
            lead = layer5_directories(lead)
            if lead["phone_source"] == "directory": stats["l5"] += 1

        # ── Cross-verify & finalize ──
        lead = cross_verify(lead)
        conf = lead["phone_confidence"]
        if conf in stats: stats[conf] += 1

        log.info("  → phone=%s | conf=%s | source=%s | insured=%s",
                 lead.get("phone") or "(none)",
                 lead["phone_confidence"],
                 lead.get("phone_source") or "—",
                 lead["insurance_status"])

        batch.append(lead)

        if len(batch) >= BATCH_SIZE:
            inserted += batch_insert(batch)
            batch = []

    # Remaining leads beyond ENRICH_LIMIT (save base data only)
    for lead in leads[limit:]:
        batch.append(lead)
        if len(batch) >= BATCH_SIZE:
            inserted += batch_insert(batch)
            batch = []

    if batch:
        inserted += batch_insert(batch)

    # Final report
    phone_found = stats["l1"] + stats["l2"] + stats["l4"] + stats["l5"]
    pct = round(phone_found / limit * 100) if limit else 0

    log.info("=" * 65)
    log.info("PIPELINE COMPLETE")
    log.info("  Total processed : %d", total)
    log.info("  Enriched        : %d", limit)
    log.info("  Phone found     : %d (%d%%)", phone_found, pct)
    log.info("  By layer        : L1=%d L2=%d L3(web)=%d L4=%d L5=%d",
             stats["l1"], stats["l2"], stats["l3"], stats["l4"], stats["l5"])
    log.info("  Confidence      : high=%d medium=%d none=%d",
             stats["high"], stats["medium"], stats["none"])
    log.info("  DB rows affected: %d", inserted)
    log.info("=" * 65)
    return inserted

if __name__ == "__main__":
    run_scraper()
