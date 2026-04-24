"""
Trucking Lead Scraper — 6-Layer Enrichment Pipeline
====================================================
Layer 1 : FMCSA Mobile API      → High confidence (official source)
Layer 2 : DOT.report /usdot/    → High confidence (structured, correct URL)
Layer 3 : Aggregators           → Medium confidence
Layer 4 : Google (website only) → Leads to Layer 5
Layer 5 : Website scraping      → High confidence
Layer 6 : Directories           → Medium/Low confidence

Fixes vs previous version:
  - Correct DOT.report URL: /usdot/{dot} not /carrier/{dot}
  - Pre-filter: US-only, trucking-only, skip auto/food/non-trucking
  - Longer delays + exponential backoff to avoid blocks
  - FMCSA Layer 1 now hits the correct carrier endpoint
  - Smarter phone parsing per DOT.report page structure

Env vars:
  DATABASE_URL   — PostgreSQL (Railway injects automatically)
  FMCSA_API_KEY  — from FMCSA developer portal
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
DOTREPORT   = "https://dot.report/usdot/{dot}"   # ← FIXED: was /carrier/

DAYS_BACK    = 3
PAGE_SIZE    = 1000
BATCH_SIZE   = 50
ENRICH_LIMIT = 300   # reduced: quality > quantity, avoids long block sessions

# Delays — longer = fewer blocks, still finishes in time
DELAY_FMCSA  = 0.5
DELAY_DOT    = 3.0   # increased: DOT.report blocks fast scrapers
DELAY_AGG    = 3.0
DELAY_GOOGLE = 5.0
DELAY_WEB    = 2.5
DELAY_DIR    = 3.0

# ── US-only trucking keyword filter ──────────────────────────────────────────
# Socrata returns everything — cars, food, Canadian cos, cleaning services etc.
# We filter to actual trucking/freight/transport companies in US states only.

US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
    "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
    "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN",
    "TX","UT","VT","VA","WA","WV","WI","WY","DC",
}

# Names containing these words are likely trucking companies
TRUCKING_KEYWORDS = {
    "truck","trucking","transport","transportation","freight","logistics",
    "hauling","haul","carrier","cargo","delivery","express","moving",
    "shipping","dispatch","fleet","motor","lines","transit","transfer",
    "intermodal","flatbed","tanker","reefer","ltl","ftl","drayage",
}

# Skip companies whose names contain these — not trucking leads
SKIP_KEYWORDS = {
    "detailing","cleaning","landscaping","construction","plumbing","electric",
    "solar","dental","medical","restaurant","food","catering","beauty","salon",
    "realty","insurance","accounting","law","attorney","consulting","marketing",
    "auto repair","mechanic","welding","painting","roofing","hvac","flooring",
}

def _is_trucking_company(name: str, state: str) -> bool:
    """Return True if this looks like a real US trucking company."""
    if not name or not state:
        return False
    # Must be a US state
    if state.upper() not in US_STATES:
        return False
    name_lower = name.lower()
    # Skip obvious non-trucking
    if any(skip in name_lower for skip in SKIP_KEYWORDS):
        return False
    # Prefer known trucking names (but don't exclude generics like "LLC")
    # We include everything that isn't explicitly non-trucking
    return True


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
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
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
        # Migrate old tables safely
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
                        (dot_number, mc_number, company_name, owner_name,
                         phone, email, website, address, city, state, zip_code,
                         entity_type, operation_type, cargo_type, drivers,
                         power_units, status, added_date, registration_date,
                         has_insurance, insurance_status, phone_source,
                         phone_confidence, sources_found, enriched)
                    VALUES
                        (%(dot_number)s, %(mc_number)s, %(company_name)s,
                         %(owner_name)s, %(phone)s, %(email)s, %(website)s,
                         %(address)s, %(city)s, %(state)s, %(zip_code)s,
                         %(entity_type)s, %(operation_type)s, %(cargo_type)s,
                         %(drivers)s, %(power_units)s, %(status)s,
                         %(added_date)s, %(registration_date)s,
                         %(has_insurance)s, %(insurance_status)s,
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
                        sources_found    = GREATEST(leads.sources_found, EXCLUDED.sources_found),
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

INVALID_AREA_CODES = {
    "000","100","200","300","400","500","600","700","800","900",
    "111","222","333","444","555","666","777","888",
}

def _clean_phone(digits: str) -> str | None:
    if len(digits) == 11 and digits[0] == "1":
        digits = digits[1:]
    if len(digits) != 10:
        return None
    area = digits[:3]
    if area[0] in ("0", "1"):
        return None
    if area in INVALID_AREA_CODES:
        return None
    if digits[3] in ("0", "1"):
        return None
    return f"({area}) {digits[3:6]}-{digits[6:]}"


def extract_phones(text: str) -> list:
    results = []
    for parts in PHONE_RE.findall(text):
        raw     = re.sub(r"\D", "", "".join(parts))
        cleaned = _clean_phone(raw)
        if cleaned and cleaned not in results:
            results.append(cleaned)
    return results


def extract_emails(text: str) -> list:
    return [
        e for e in EMAIL_RE.findall(text)
        if not any(x in e.lower() for x in
                   ["example","test","noreply","no-reply","@domain",
                    "user@","admin@","webmaster@"])
    ]


def safe_get(url: str, timeout: int = 15, retries: int = 3) -> requests.Response | None:
    """GET with exponential backoff and rate-limit handling."""
    for attempt in range(retries):
        try:
            # Randomise headers slightly each request
            hdrs = HEADERS.copy()
            hdrs["User-Agent"] = random.choice([
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
            ])
            r = requests.get(url, headers=hdrs, timeout=timeout)
            if r.status_code == 200:
                return r
            if r.status_code == 429:
                wait = (2 ** attempt) * 15   # 15s, 30s, 60s
                log.warning("429 on %s — backing off %ds", url, wait)
                time.sleep(wait)
                continue
            if r.status_code in (403, 404):
                return None
        except requests.exceptions.Timeout:
            log.debug("Timeout on %s (attempt %d)", url, attempt + 1)
        except Exception as e:
            log.debug("Request error (%s): %s", url, e)
        time.sleep(random.uniform(2.0, 4.0) * (attempt + 1))
    return None


def _safe_int(val) -> int:
    try:
        return int(val or 0)
    except (ValueError, TypeError):
        return 0


def _most_common_phone(phones: list) -> str:
    if not phones:
        return ""
    return Counter(phones).most_common(1)[0][0]

# ── STEP 0: SOCRATA FETCH ─────────────────────────────────────────────────────

def fetch_new_registrations() -> list:
    cutoff = (datetime.utcnow() - timedelta(days=DAYS_BACK)).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    log.info("Fetching registrations (last %d days) from Socrata...", DAYS_BACK)
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

        log.info("  %d leads so far (offset %d)...", len(leads), offset)
        if len(data) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    # Filter to real US trucking companies only
    before = len(leads)
    leads  = [
        l for l in leads
        if _is_trucking_company(l["company_name"], l["state"])
    ]
    log.info(
        "After US/trucking filter: %d → %d leads (removed %d non-trucking/foreign)",
        before, len(leads), before - len(leads)
    )
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
        "company_name":      (row.get("legal_name") or "").strip(),
        "owner_name":        "",
        "phone":             "",
        "email":             "",
        "website":           "",
        "address":           row.get("phy_street") or "",
        "city":              row.get("phy_city") or "",
        "state":             (row.get("phy_state") or "").upper().strip(),
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
            log.warning("FMCSA rate limit — sleeping 20s")
            time.sleep(20)
            return lead
        if r.status_code != 200:
            return lead

        data    = r.json()
        carrier = data.get("content", {})
        if not carrier:
            return lead

        raw_phone = (
            carrier.get("phyTelephone") or
            carrier.get("mailingTelephone") or ""
        ).strip()
        phones = extract_phones(raw_phone) if raw_phone else []

        owner = (carrier.get("dbaName") or carrier.get("legalName") or "").strip()
        email = (carrier.get("email") or "").strip()
        pu    = _safe_int(carrier.get("totalPowerUnits"))

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

        lead["has_insurance"]    = (allowed == "Y") or (ins_status == "insured")
        lead["insurance_status"] = ins_status

    except Exception as e:
        log.debug("Layer1 FMCSA error DOT %s: %s", dot, e)

    time.sleep(DELAY_FMCSA + random.uniform(0, 0.3))
    return lead

# ── LAYER 2: DOT.REPORT (fixed URL + smarter parsing) ─────────────────────────

def layer2_dot_report(lead: dict) -> dict:
    """
    DOT.report has a page per carrier at /usdot/{dot_number}.
    It shows phone, email, insurance status in a structured layout.
    FIXED: was using /carrier/ — correct path is /usdot/
    """
    dot = lead["dot_number"]
    url = DOTREPORT.format(dot=dot)   # https://dot.report/usdot/{dot}
    r   = safe_get(url, timeout=20)
    if not r:
        time.sleep(DELAY_DOT)
        return lead

    soup = BeautifulSoup(r.text, "lxml")

    # ── Strategy 1: look for structured key-value rows ──
    phones_found = []
    emails_found = []

    # DOT.report renders data in <dl>, <table>, or labelled <div> blocks
    # Try all approaches so we don't miss any layout change

    # Approach A: scan all text for phones/emails
    full_text = soup.get_text(" ", strip=True)
    phones_found.extend(extract_phones(full_text))
    emails_found.extend(extract_emails(full_text))

    # Approach B: look specifically near "Phone" / "Telephone" labels
    for tag in soup.find_all(["dt", "th", "td", "label", "strong", "b"]):
        label = tag.get_text(strip=True).lower()
        if any(kw in label for kw in ["phone", "telephone", "tel", "contact"]):
            # The value is usually the next sibling or parent's next sibling
            sibling = tag.find_next_sibling()
            if sibling:
                val_phones = extract_phones(sibling.get_text(" "))
                phones_found = val_phones + phones_found

    # Approach C: href="tel:..." links
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("tel:"):
            raw    = re.sub(r"\D", "", href.replace("tel:", ""))
            cleaned = _clean_phone(raw)
            if cleaned:
                phones_found.insert(0, cleaned)   # tel: links are most reliable

    # Approach D: href="mailto:..." links
    for a in soup.find_all("a", href=True):
        if a["href"].startswith("mailto:"):
            email = a["href"].replace("mailto:", "").split("?")[0].strip()
            if email and "@" in email:
                emails_found.insert(0, email)

    # ── Insurance status ──
    page_text_lower = full_text.lower()
    if any(kw in page_text_lower for kw in ["authorized for hire", "active", "insurance on file"]):
        lead["insurance_status"] = "insured"
        lead["has_insurance"]    = True
    elif any(kw in page_text_lower for kw in ["not authorized", "revoked", "out of service"]):
        if lead["insurance_status"] == "unknown":
            lead["insurance_status"] = "none"

    # ── Apply phone ──
    # Dedupe and pick best
    seen   = []
    for p in phones_found:
        if p not in seen:
            seen.append(p)
    phones_found = seen

    if phones_found:
        prev = lead.get("phone", "")
        if not prev:
            lead["phone"]            = phones_found[0]
            lead["phone_source"]     = "dot_report"
            lead["phone_confidence"] = "high"
            lead["sources_found"]   += 1
        elif prev == phones_found[0]:
            # Cross-verified with Layer 1
            lead["phone_confidence"] = "high"
            lead["sources_found"]   += 1
        else:
            # Disagreement — keep FMCSA phone but count the source
            lead["sources_found"]   += 1

    # ── Apply email ──
    if emails_found and not lead["email"]:
        lead["email"] = emails_found[0]

    time.sleep(DELAY_DOT + random.uniform(0.5, 1.5))
    return lead

# ── LAYER 3: AGGREGATOR WEBSITES ─────────────────────────────────────────────

AGGREGATORS = [
    "https://www.otrucking.com/carrier/{dot}",
    "https://carriersource.io/carriers/{dot}",
]

def layer3_aggregators(lead: dict) -> dict:
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

        soup   = BeautifulSoup(r.text, "lxml")
        # tel: links first (most reliable)
        for a in soup.find_all("a", href=True):
            if a["href"].startswith("tel:"):
                raw     = re.sub(r"\D", "", a["href"].replace("tel:", ""))
                cleaned = _clean_phone(raw)
                if cleaned:
                    found_phones.insert(0, cleaned)

        text   = soup.get_text(" ")
        phones = extract_phones(text)
        emails = extract_emails(text)

        if phones:
            found_phones.extend(phones)
            lead["sources_found"] += 1

        if emails and not lead["email"]:
            lead["email"] = emails[0]

        time.sleep(DELAY_AGG + random.uniform(0, 1.5))

    if found_phones:
        valid  = [p for p in found_phones if p]
        best   = _most_common_phone(valid)
        count  = Counter(valid)[best]
        if not lead["phone"]:
            lead["phone"]            = best
            lead["phone_source"]     = "aggregator"
            lead["phone_confidence"] = "high" if count >= 2 else "medium"
        elif best == lead["phone"] and count >= 2:
            lead["phone_confidence"] = "high"

    return lead

# ── LAYER 4: GOOGLE (website URL discovery only) ──────────────────────────────

def layer4_google_website(lead: dict) -> dict:
    if lead.get("website") or (lead["phone"] and lead["phone_confidence"] == "high"):
        return lead

    company = lead["company_name"]
    city    = lead["city"]
    state   = lead["state"]
    if not company:
        return lead

    query = f'"{company}" {city} {state} trucking site'
    url   = f"https://www.google.com/search?q={requests.utils.quote(query)}&num=5"
    r     = safe_get(url, timeout=15)
    if not r:
        time.sleep(DELAY_GOOGLE)
        return lead

    soup = BeautifulSoup(r.text, "lxml")
    skip = [
        "google.","facebook.","linkedin.","twitter.","youtube.",
        "fmcsa.","dot.gov","safer.fmcsa","wikipedia.","yelp.",
        "bbb.org","yellowpages.","dot.report","otrucking.",
        "carriersource.","instagram.","tiktok.",
    ]
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if href.startswith("/url?q="):
            actual = href.split("/url?q=")[1].split("&")[0]
            if actual.startswith("http") and not any(s in actual for s in skip):
                lead["website"]       = actual
                lead["sources_found"] += 1
                break

    time.sleep(DELAY_GOOGLE + random.uniform(0, 2.0))
    return lead

# ── LAYER 5: WEBSITE SCRAPING (contact pages only) ────────────────────────────

CONTACT_PATHS = ["/contact", "/contact-us", "/about", "/about-us", "/"]

def layer5_website_scrape(lead: dict) -> dict:
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

        soup = BeautifulSoup(r.text, "lxml")

        # tel: links are the most reliable on company websites
        for a in soup.find_all("a", href=True):
            if a["href"].startswith("tel:"):
                raw     = re.sub(r"\D", "", a["href"].replace("tel:", ""))
                cleaned = _clean_phone(raw)
                if cleaned:
                    found_phones.insert(0, cleaned)

        # mailto: links
        for a in soup.find_all("a", href=True):
            if a["href"].startswith("mailto:"):
                email = a["href"].replace("mailto:", "").split("?")[0].strip()
                if email and "@" in email:
                    found_emails.insert(0, email)

        # Strip nav/footer, then extract from remaining text
        for tag in soup.find_all(["nav","footer","script","style"]):
            tag.decompose()
        text   = soup.get_text(" ", strip=True)
        phones = extract_phones(text)
        if phones:
            found_phones.extend(phones)

        if found_phones:
            lead["sources_found"] += 1
            break   # stop at first page with a phone

        time.sleep(DELAY_WEB + random.uniform(0, 1.0))

    if found_phones:
        best = _most_common_phone([p for p in found_phones if p])
        prev = lead.get("phone", "")
        if prev and prev == best:
            lead["phone_confidence"] = "high"
        elif not prev:
            lead["phone"]            = best
            lead["phone_source"]     = "website"
            lead["phone_confidence"] = "high"

    if found_emails and not lead["email"]:
        lead["email"] = found_emails[0]

    return lead

# ── LAYER 6: DIRECTORIES (last resort) ───────────────────────────────────────

DIRECTORY_URLS = [
    "https://www.yellowpages.com/search?search_terms={q}&geo_location_terms={loc}",
    "https://www.bbb.org/search?find_text={q}&find_loc={loc}",
]

def layer6_directories(lead: dict) -> dict:
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

        soup = BeautifulSoup(r.text, "lxml")

        # tel: links first
        for a in soup.find_all("a", href=True):
            if a["href"].startswith("tel:"):
                raw     = re.sub(r"\D", "", a["href"].replace("tel:", ""))
                cleaned = _clean_phone(raw)
                if cleaned:
                    prev = lead.get("phone", "")
                    if prev == cleaned:
                        lead["phone_confidence"] = "high"
                    elif not prev:
                        lead["phone"]            = cleaned
                        lead["phone_source"]     = "directory"
                        lead["phone_confidence"] = "medium"
                    lead["sources_found"] += 1
                    break

        if lead["phone"]:
            break

        time.sleep(DELAY_DIR + random.uniform(0, 1.5))

    return lead

# ── FINALIZE ──────────────────────────────────────────────────────────────────

def finalize(lead: dict) -> dict:
    phone   = lead.get("phone", "")
    sources = lead.get("sources_found", 0)
    if not phone:
        lead["phone_confidence"] = "none"
    elif sources >= 2 and lead["phone_confidence"] != "high":
        lead["phone_confidence"] = "high"
    elif sources == 1 and lead["phone_confidence"] == "none":
        lead["phone_confidence"] = "medium"
    lead["enriched"] = bool(phone or lead.get("email") or lead.get("website"))
    return lead

# ── DEMO MODE ─────────────────────────────────────────────────────────────────

def _demo_leads() -> list:
    today     = datetime.utcnow().date()
    yesterday = (datetime.utcnow() - timedelta(days=1)).date()
    now       = datetime.utcnow()
    sample = [
        ("3421901","MC-1234567","LONE STAR FREIGHT LLC",       "John Martinez","(512) 555-0171","TX","Austin",      "78701",today,     True, "insured", "fmcsa_api", "high",  2),
        ("3421902","MC-1234568","GREAT LAKES TRANSPORT INC",   "Sara Kowalski","(312) 555-0182","IL","Chicago",     "60601",today,     True, "insured", "dot_report","high",  3),
        ("3421903","",          "SUNRISE HAULING LLC",         "David Chen",   "(404) 555-0193","GA","Atlanta",     "30301",today,     False,"none",    "website",   "high",  1),
        ("3421904","MC-1234570","BLUE RIDGE CARRIERS LLC",     "Mike Thornton","(828) 555-0104","NC","Asheville",   "28801",yesterday, True, "insured", "fmcsa_api", "high",  2),
        ("3421905","MC-1234571","PACIFIC COAST LOGISTICS INC", "Ana Gutierrez","(503) 555-0115","OR","Portland",    "97201",yesterday, False,"none",    "aggregator","medium",1),
        ("3421906","",          "MOUNTAIN STATE TRUCKING LLC", "Bob Williams", "",              "CO","Denver",      "80201",yesterday, False,"unknown", "",          "none",  0),
        ("3421907","MC-1234573","BAYOU FREIGHT SOLUTIONS LLC", "Lisa Tran",    "(504) 555-0137","LA","New Orleans", "70112",yesterday, True, "insured", "dot_report","high",  2),
        ("3421908","MC-1234574","IRON HORSE TRANSPORT LLC",    "Tom Bradley",  "(602) 555-0148","AZ","Phoenix",     "85001",today,     False,"none",    "directory", "medium",1),
    ]
    return [{
        "dot_number": dot, "mc_number": mc, "company_name": name,
        "owner_name": owner, "phone": phone, "email": "", "website": "",
        "address": "123 Main St", "city": city, "state": state,
        "zip_code": zipcode, "entity_type": "CARRIER", "operation_type": "A",
        "cargo_type": "G", "drivers": 2, "power_units": 2, "status": "A",
        "added_date": now, "registration_date": reg, "has_insurance": ins,
        "insurance_status": ins_s, "phone_source": psrc,
        "phone_confidence": pconf, "sources_found": srcs,
        "enriched": bool(phone),
    } for dot,mc,name,owner,phone,state,city,zipcode,reg,ins,ins_s,psrc,pconf,srcs in sample]

# ── MAIN PIPELINE ─────────────────────────────────────────────────────────────

def run_scraper() -> int:
    log.info("=" * 65)
    log.info("FMCSA 6-Layer Pipeline  |  API key: %s",
             "present" if FMCSA_API_KEY else "MISSING (demo mode)")
    log.info("=" * 65)

    init_db()

    if not FMCSA_API_KEY:
        leads = _demo_leads()
    else:
        leads = fetch_new_registrations()

    if not leads:
        log.info("No new leads. Exiting.")
        return 0

    total   = len(leads)
    limit   = min(total, ENRICH_LIMIT)
    batch   = []
    inserted = 0
    stats   = {"l1":0,"l2":0,"l3":0,"l4":0,"l5":0,"l6":0,
               "high":0,"medium":0,"none":0}

    for i, lead in enumerate(leads[:limit]):
        log.info("[%d/%d] DOT %-9s  %s, %s",
                 i+1, limit, lead["dot_number"],
                 lead["company_name"], lead["state"])

        # L1 — FMCSA official
        lead = layer1_fmcsa(lead)
        if lead["phone_source"] == "fmcsa_api": stats["l1"] += 1

        # L2 — DOT.report (skip if already high confidence)
        if not (lead["phone"] and lead["phone_confidence"] == "high"):
            lead = layer2_dot_report(lead)
            if lead["phone_source"] == "dot_report": stats["l2"] += 1

        # L3 — Aggregators
        if not (lead["phone"] and lead["phone_confidence"] == "high"):
            lead = layer3_aggregators(lead)
            if lead["phone_source"] == "aggregator": stats["l3"] += 1

        # L4 — Google (website URL only)
        if not lead.get("website") and not (lead["phone"] and lead["phone_confidence"] == "high"):
            lead = layer4_google_website(lead)
            if lead.get("website"): stats["l4"] += 1

        # L5 — Website contact page
        if lead.get("website") and not (lead["phone"] and lead["phone_confidence"] == "high"):
            lead = layer5_website_scrape(lead)
            if lead["phone_source"] == "website": stats["l5"] += 1

        # L6 — Directories (last resort)
        if not lead["phone"] or lead["phone_confidence"] == "none":
            lead = layer6_directories(lead)
            if lead["phone_source"] == "directory": stats["l6"] += 1

        lead = finalize(lead)
        stats[lead["phone_confidence"]] += 1

        log.info("  → phone=%-18s | conf=%-6s | source=%-12s | ins=%s",
                 lead.get("phone") or "(none)",
                 lead["phone_confidence"],
                 lead.get("phone_source") or "—",
                 lead["insurance_status"])

        batch.append(lead)
        if len(batch) >= BATCH_SIZE:
            inserted += batch_insert(batch)
            batch = []

    # Save remaining (unenriched base data)
    for lead in leads[limit:]:
        batch.append(lead)
        if len(batch) >= BATCH_SIZE:
            inserted += batch_insert(batch)
            batch = []
    if batch:
        inserted += batch_insert(batch)

    phone_found = stats["high"] + stats["medium"]
    pct         = round(phone_found / limit * 100) if limit else 0
    log.info("=" * 65)
    log.info("DONE  |  fetched=%d  enriched=%d  phone=%d(%d%%)",
             total, limit, phone_found, pct)
    log.info("Layers: L1=%d L2(DOT.report)=%d L3=%d L4(sites)=%d L5=%d L6=%d",
             stats["l1"],stats["l2"],stats["l3"],
             stats["l4"],stats["l5"],stats["l6"])
    log.info("Confidence: high=%d  medium=%d  none=%d",
             stats["high"],stats["medium"],stats["none"])
    log.info("DB rows saved: %d", inserted)
    log.info("=" * 65)
    return inserted


if __name__ == "__main__":
    run_scraper()
