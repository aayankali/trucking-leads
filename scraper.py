# ================== UPDATED SCRAPER.PY ==================

# (imports stay same)
import os, re, logging, time, random
from datetime import datetime, timedelta
from urllib.parse import urlparse

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
UPDATE_ENRICH_LIMIT = 30
TARGET_CALL_READY = 40
CANDIDATE_POOL_LIMIT = 1000
RETRY_CANDIDATE_LIMIT = 300
RETRY_STALE_HOURS = 72
SAFER_MAX_CALLS = 20
SAFER_DELAY_SECONDS = 3.0

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
    "phone_sources": "",
    "email_source": "",
    "website_source": "",
    "sources_found": 0,
    "enriched": False,
    "is_trucking": False,
    "lead_status": "new",
    "skip_reason": "",
    "enrichment_errors": "",
    "last_enriched_at": None,
    "lead_score": 0,
    "detail_score": 0,
}

TRUCKING_WORDS = [
    "trucking", "truck", "transport", "transportation", "freight", "logistics",
    "carrier", "hauling", "haulage", "cartage", "express", "delivery",
    "dispatch", "cargo", "moving", "movers", "hotshot", "hot shot",
    "linehaul", "drayage", "distribution", "courier", "ltl", "fleet",
]
BAD_WORDS = [
    "school", "church", "real estate", "builder", "landscaping", "lawn care",
    "restaurant", "cleaning", "plumbing", "roofing", "hvac",
]
GENERIC_EMAIL_DOMAINS = {
    "gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "aol.com",
    "icloud.com", "live.com", "msn.com", "proton.me", "protonmail.com",
}
PHONE_SOURCE_PRIORITY = {
    "fmcsa_api": 4,
    "socrata": 3,
    "fmcsa_safer": 3,
    "dot_report": 2,
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

def add_unique_csv(lead, field, value):
    value = str(value or "").strip()
    if not value:
        return
    existing = [x.strip() for x in str(lead.get(field) or "").split(",") if x.strip()]
    if value not in existing:
        existing.append(value)
        lead[field] = ", ".join(existing)

def add_error(lead, value):
    add_unique_csv(lead, "enrichment_errors", value)

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
    add_unique_csv(lead, "phone_sources", f"{source}:{phone}")

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

    old_priority = PHONE_SOURCE_PRIORITY.get(lead.get("phone_source"), 0)
    new_priority = PHONE_SOURCE_PRIORITY.get(source, 0)
    if new_priority > old_priority:
        lead["phone"] = phone
        lead["phone_source"] = source
        lead["phone_confidence"] = confidence
        lead["sources_found"] = max(1, sources_found)

    add_error(lead, f"phone_conflict_{source}")
    return False

def apply_email(lead, raw_email, source):
    emails = extract_emails(raw_email or "")
    if not emails:
        return False
    if not lead.get("email"):
        lead["email"] = emails[0]
        lead["email_source"] = source
    return True

def normalize_website(url):
    url = str(url or "").strip()
    if not url or "@" in url:
        return ""
    if url.startswith("//"):
        url = "https:" + url
    if not re.match(r"^https?://", url, re.I):
        url = "https://" + url
    parsed = urlparse(url)
    if "." not in (parsed.netloc or ""):
        return ""
    return url.split("#", 1)[0].rstrip("/")

def apply_website(lead, raw_url, source):
    website = normalize_website(raw_url)
    if not website:
        return False
    host = urlparse(website).netloc.lower()
    blocked = ("fmcsa.dot.gov", "safer.fmcsa.dot.gov", "dot.report", "transportation.gov")
    if any(blocked_host in host for blocked_host in blocked):
        return False
    if not lead.get("website"):
        lead["website"] = website
        lead["website_source"] = source
    return True

def apply_website_from_email(lead):
    if lead.get("website") or not lead.get("email"):
        return False
    domain = lead["email"].split("@")[-1].lower()
    if domain in GENERIC_EMAIL_DOMAINS:
        return False
    return apply_website(lead, domain, "email_domain")

def safe_get(url, timeout=12, params=None, retries=2, source="http"):
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
            if r.status_code < 400:
                return r
            if r.status_code not in (429, 500, 502, 503, 504):
                log.debug("%s returned HTTP %s for %s", source, r.status_code, url)
                return r
            log.debug("%s retryable HTTP %s for %s", source, r.status_code, url)
        except Exception as e:
            log.debug("%s request failed for %s: %s", source, url, e)
        if attempt < retries:
            time.sleep(0.75 * (attempt + 1))
    return None

# ── 🔥 NEW FILTER ─────────────────────────────────

def is_good_lead(lead):
    return classify_trucking_lead(lead)[0]

def classify_trucking_lead(lead):
    name = (lead.get("company_name") or "").lower()
    state = (lead.get("state") or "").upper()
    operation = (lead.get("operation_type") or "").lower()
    cargo = (lead.get("cargo_type") or "").lower()
    entity = (lead.get("entity_type") or "").lower()
    text = " ".join([name, operation, cargo, entity])

    if len(state) != 2:
        return False, "invalid_state"

    has_trucking_word = any(w in text for w in TRUCKING_WORDS)
    has_units = parse_int(lead.get("power_units")) > 0 or parse_int(lead.get("drivers")) > 0
    carrier_entity = any(w in text for w in ["carrier", "interstate", "intrastate", "motor carrier"])

    if any(b in name for b in BAD_WORDS) and not (has_trucking_word or carrier_entity):
        return False, "non_trucking_business_name"

    if has_trucking_word or has_units or carrier_entity:
        return True, ""

    return False, "weak_trucking_match"

def compute_detail_score(lead):
    score = 0
    if lead.get("phone"):
        score += 35
    if lead.get("company_name"):
        score += 10
    if lead.get("address"):
        score += 10
    if lead.get("city") and lead.get("state"):
        score += 10
    if lead.get("email"):
        score += 10
    if lead.get("website"):
        score += 10
    if parse_int(lead.get("power_units")) > 0:
        score += 8
    if parse_int(lead.get("drivers")) > 0:
        score += 5
    if lead.get("mc_number"):
        score += 4
    if lead.get("insurance_status") in ("none", "insured"):
        score += 8
    return min(score, 100)

def score_candidate(lead):
    score = 0
    is_trucking, reason = classify_trucking_lead(lead)
    name = (lead.get("company_name") or "").lower()
    text = " ".join([
        name,
        (lead.get("operation_type") or "").lower(),
        (lead.get("cargo_type") or "").lower(),
        (lead.get("entity_type") or "").lower(),
    ])

    if len((lead.get("state") or "").strip()) == 2:
        score += 20
    else:
        score -= 40
    if is_trucking:
        score += 45
    elif reason == "weak_trucking_match":
        score += 5
    else:
        score -= 25
    if any(w in text for w in TRUCKING_WORDS):
        score += 25
    if parse_int(lead.get("power_units")) > 0:
        score += 20
    if parse_int(lead.get("drivers")) > 0:
        score += 15
    if lead.get("phone"):
        score += 50
    if lead.get("email"):
        score += 8
    if lead.get("website"):
        score += 8
    if lead.get("address") or lead.get("city"):
        score += 8
    if any(b in name for b in BAD_WORDS) and not any(w in text for w in TRUCKING_WORDS):
        score -= 35
    if lead.get("registration_date"):
        score += 10

    lead["lead_score"] = score
    lead["detail_score"] = compute_detail_score(lead)
    return score

def select_scored_candidates(leads, limit=CANDIDATE_POOL_LIMIT):
    by_dot = {}
    for lead in leads:
        dot = str(lead.get("dot_number") or "").strip()
        if not dot:
            continue
        lead["dot_number"] = dot
        score_candidate(lead)
        existing = by_dot.get(dot)
        if not existing or parse_int(lead.get("lead_score")) > parse_int(existing.get("lead_score")):
            by_dot[dot] = lead
    candidates = list(by_dot.values())
    candidates.sort(
        key=lambda x: (
            parse_int(x.get("lead_score")),
            str(x.get("registration_date") or ""),
            parse_int(x.get("power_units")),
            parse_int(x.get("drivers")),
        ),
        reverse=True,
    )
    return candidates[:limit]

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
    if emails:
        apply_email(lead, emails[0], "dot_report")

    for a in soup.find_all("a", href=True):
        if apply_website(lead, a["href"], "dot_report"):
            break

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
            r = safe_get(SODA_URL, params=params, timeout=30, retries=2, source="socrata")
            if r is None:
                raise RuntimeError("empty response")
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
                "email": "",
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
            apply_email(lead, first_present(row.get("email_address"), row.get("email")), "socrata")
            apply_website(lead, first_present(row.get("website"), row.get("url")), "socrata")

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
        r = safe_get(url, params={"webKey": FMCSA_API_KEY}, timeout=10, retries=2, source="fmcsa_api")

        if not r or r.status_code != 200:
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
        apply_email(lead, data.get("emailAddress"), "fmcsa_api")
        apply_website(lead, first_present(data.get("website"), data.get("url"), data.get("webUrl")), "fmcsa_api")
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


def new_safer_state():
    return {"calls": 0, "blocked": False, "last_call": 0.0}

def should_use_safer(lead, safer_state):
    if safer_state.get("blocked"):
        return False
    if safer_state.get("calls", 0) >= SAFER_MAX_CALLS:
        return False
    return not lead.get("phone") or not lead.get("address") or not lead.get("city")

def layer_aggregator(lead, safer_state=None):
    """Public fallback layer using FMCSA SAFER pages by DOT number."""
    dot = lead.get("dot_number")
    if not dot:
        return lead
    safer_state = safer_state or new_safer_state()

    if not should_use_safer(lead, safer_state):
        if safer_state.get("blocked"):
            add_error(lead, "safer_skipped_blocked")
        elif safer_state.get("calls", 0) >= SAFER_MAX_CALLS:
            add_error(lead, "safer_skipped_budget")
        return lead

    wait_for = SAFER_DELAY_SECONDS - (time.monotonic() - safer_state.get("last_call", 0.0))
    if wait_for > 0:
        time.sleep(wait_for + random.uniform(0.1, 0.7))
    safer_state["last_call"] = time.monotonic()
    safer_state["calls"] = safer_state.get("calls", 0) + 1

    try:
        r = safe_get(
            "https://safer.fmcsa.dot.gov/query.asp",
            params={
                "searchtype": "ANY",
                "query_type": "queryCarrierSnapshot",
                "query_param": "USDOT",
                "query_string": dot,
            },
            timeout=12,
            retries=2,
            source="fmcsa_safer",
        )
        if not r:
            add_error(lead, "safer_failed")
            return lead
        if r.status_code in (403, 429):
            safer_state["blocked"] = True
            add_error(lead, f"safer_blocked_{r.status_code}")
            return lead
        if r.status_code != 200:
            add_error(lead, f"safer_http_{r.status_code}")
            return lead
    except Exception as e:
        log.debug("SAFER error %s: %s", dot, e)
        add_error(lead, "safer_exception")
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
    if emails:
        apply_email(lead, emails[0], "fmcsa_safer")

    return lead


def layer_contact_inference(lead):
    """Final lightweight enrichment from data already discovered by trusted sources."""
    apply_website_from_email(lead)
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
    clean["phone_sources"] = str(clean.get("phone_sources") or "").strip()
    clean["email_source"] = str(clean.get("email_source") or "").strip()
    clean["website_source"] = str(clean.get("website_source") or "").strip()
    clean["enrichment_errors"] = str(clean.get("enrichment_errors") or "").strip()
    clean["lead_score"] = parse_int(clean.get("lead_score"))

    if clean["insurance_status"] not in ("none", "unknown", "partial", "insured"):
        clean["insurance_status"] = "unknown"

    apply_website_from_email(clean)

    if clean["phone"]:
        phone_source_names = {
            entry.split(":", 1)[0]
            for entry in clean["phone_sources"].split(",")
            if entry.strip().endswith(clean["phone"])
        }
        if phone_source_names:
            clean["sources_found"] = max(clean["sources_found"], len(phone_source_names))
        if clean["sources_found"] >= 2:
            clean["phone_confidence"] = "high"
        elif clean["phone_confidence"] not in ("medium", "high"):
            clean["phone_confidence"] = "medium"
    else:
        clean["phone_source"] = ""
        clean["phone_confidence"] = "none"
        clean["sources_found"] = 0

    is_trucking, reason = classify_trucking_lead(clean)
    clean["is_trucking"] = is_trucking
    if not clean["phone"]:
        clean["lead_status"] = "needs_phone"
        clean["skip_reason"] = reason or "no_phone_found"
    elif clean["has_insurance"] or clean["insurance_status"] == "insured":
        clean["lead_status"] = "insured"
        clean["skip_reason"] = "already_insured"
    else:
        clean["lead_status"] = "call_ready"
        clean["skip_reason"] = "" if is_trucking else reason

    clean["detail_score"] = compute_detail_score(clean)
    clean["lead_score"] = max(clean["lead_score"], score_candidate(clean))
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
                has_insurance BOOLEAN DEFAULT FALSE,
                lead_status TEXT DEFAULT 'new',
                skip_reason TEXT DEFAULT '',
                is_trucking BOOLEAN DEFAULT FALSE,
                phone_sources TEXT DEFAULT '',
                email_source TEXT DEFAULT '',
                website_source TEXT DEFAULT '',
                enrichment_errors TEXT DEFAULT '',
                last_enriched_at TIMESTAMP,
                lead_score INTEGER DEFAULT 0,
                detail_score INTEGER DEFAULT 0
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
        ("lead_status", "TEXT", "'new'"),
        ("skip_reason", "TEXT", "''"),
        ("is_trucking", "BOOLEAN", "FALSE"),
        ("phone_sources", "TEXT", "''"),
        ("email_source", "TEXT", "''"),
        ("website_source", "TEXT", "''"),
        ("enrichment_errors", "TEXT", "''"),
        ("last_enriched_at", "TIMESTAMP", "NULL"),
        ("lead_score", "INTEGER", "0"),
        ("detail_score", "INTEGER", "0"),
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
        "enriched", "is_trucking", "lead_status", "skip_reason", "phone_sources",
        "email_source", "website_source", "enrichment_errors", "last_enriched_at",
        "lead_score", "detail_score",
    ]

    now = datetime.utcnow()
    rows = []
    for lead in leads:
        lead = finalize(lead)
        row = []
        for col in columns:
            row.append(now if col in ("added_date", "last_enriched_at") else lead.get(col))
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
            enriched = EXCLUDED.enriched,
            is_trucking = EXCLUDED.is_trucking,
            lead_status = EXCLUDED.lead_status,
            skip_reason = EXCLUDED.skip_reason,
            phone_sources = COALESCE(NULLIF(EXCLUDED.phone_sources, ''), leads.phone_sources),
            email_source = COALESCE(NULLIF(EXCLUDED.email_source, ''), leads.email_source),
            website_source = COALESCE(NULLIF(EXCLUDED.website_source, ''), leads.website_source),
            enrichment_errors = COALESCE(NULLIF(EXCLUDED.enrichment_errors, ''), leads.enrichment_errors),
            last_enriched_at = EXCLUDED.last_enriched_at,
            lead_score = EXCLUDED.lead_score,
            detail_score = EXCLUDED.detail_score
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


def fetch_existing_dot_map():
    try:
        ensure_db()
        conn = get_db()
        try:
            with conn.cursor() as c:
                c.execute("SELECT dot_number, lead_status, last_enriched_at FROM leads")
                rows = c.fetchall()
        finally:
            conn.close()
        return {str(r["dot_number"]): dict(r) for r in rows if r.get("dot_number")}
    except Exception as e:
        log.debug("Existing DOT lookup skipped: %s", e)
        return {}


def fetch_retry_candidates(limit=RETRY_CANDIDATE_LIMIT):
    try:
        ensure_db()
        conn = get_db()
        try:
            with conn.cursor() as c:
                c.execute("""
                    SELECT dot_number, mc_number, company_name, owner_name, phone, email, website,
                           address, city, state, zip_code, entity_type, operation_type, cargo_type,
                           drivers, power_units, status, registration_date, has_insurance,
                           insurance_status, phone_source, phone_confidence, phone_sources,
                           email_source, website_source, sources_found, is_trucking, lead_status,
                           skip_reason, enrichment_errors, lead_score, detail_score
                    FROM leads
                    WHERE lead_status IN ('needs_phone', 'new')
                      AND (
                        last_enriched_at IS NULL
                        OR last_enriched_at < (NOW() AT TIME ZONE 'UTC') - (%s * INTERVAL '1 hour')
                      )
                    ORDER BY lead_score DESC, COALESCE(last_enriched_at, added_date) ASC NULLS FIRST
                    LIMIT %s
                """, (RETRY_STALE_HOURS, limit))
                rows = c.fetchall()
        finally:
            conn.close()
        retry_leads = [dict(r) for r in rows]
        for lead in retry_leads:
            lead["_candidate_type"] = "update"
        return retry_leads
    except Exception as e:
        log.debug("Retry candidate fetch skipped: %s", e)
        return []


def enrich_candidate(lead, safer_state):
    lead = layer_fmcsa(lead)

    if not lead.get("phone") or not lead.get("email") or not lead.get("website"):
        lead = layer_dot_report(lead)

    if should_use_safer(lead, safer_state):
        lead = layer_aggregator(lead, safer_state)

    lead = layer_contact_inference(lead)
    return finalize(lead)




def run_scraper():
    log.info("Starting scraper...")

    if not DATABASE_URL:
        log.error("DATABASE_URL is not set; cannot save leads.")
        return 0

    leads = fetch_new_registrations()

    if not leads:
        log.warning("No leads fetched")
        return 0

    existing_dots = fetch_existing_dot_map()
    scored_new_pool = select_scored_candidates(leads)
    fresh_candidates = [
        lead for lead in scored_new_pool
        if str(lead.get("dot_number") or "") not in existing_dots
    ]
    for lead in fresh_candidates:
        lead["_candidate_type"] = "new"

    retry_candidates = select_scored_candidates(fetch_retry_candidates(), limit=RETRY_CANDIDATE_LIMIT)
    for lead in retry_candidates:
        lead["_candidate_type"] = "update"

    update_budget = min(UPDATE_ENRICH_LIMIT, len(retry_candidates))
    new_budget = ENRICH_LIMIT - update_budget

    selected_fresh = fresh_candidates[:new_budget]
    selected_update = retry_candidates[:update_budget]

    if len(selected_fresh) < new_budget:
        extra_needed = new_budget - len(selected_fresh)
        selected_update += retry_candidates[update_budget:update_budget + extra_needed]

    if len(selected_update) < update_budget:
        extra_needed = update_budget - len(selected_update)
        selected_fresh += fresh_candidates[new_budget:new_budget + extra_needed]

    selected = (selected_fresh + selected_update)[:ENRICH_LIMIT]

    log.info(
        "Scored %d raw candidates. Existing DOTs: %d | selected new: %d | selected updates: %d | total attempts: %d",
        len(scored_new_pool), len(existing_dots), len(selected_fresh), len(selected_update), len(selected)
    )

    batch = []
    inserted = 0
    safer_state = new_safer_state()
    call_ready = 0

    stats = {"call_ready": 0, "needs_phone": 0, "insured": 0, "not_trucking": 0}
    kind_stats = {"new": 0, "update": 0}

    for i, lead in enumerate(selected):
        kind = lead.get("_candidate_type", "new")
        log.info(
            "[%d/%d] %s ready=%d/%d score=%s DOT %s",
            i+1, len(selected), kind, call_ready, TARGET_CALL_READY,
            lead.get("lead_score", 0), lead["dot_number"]
        )

        lead = enrich_candidate(lead, safer_state)

        status = lead.get("lead_status") or "new"
        stats[status] = stats.get(status, 0) + 1
        kind_stats[kind] = kind_stats.get(kind, 0) + 1
        if status == "call_ready":
            call_ready += 1

        batch.append(lead)

        if len(batch) >= BATCH_SIZE:
            inserted += batch_insert(batch)
            batch = []

    if batch:
        inserted += batch_insert(batch)

    log.info("Saved: %d", inserted)
    log.info("Processed new: %d | Processed updates: %d | SAFER calls: %d/%d | SAFER blocked: %s",
             kind_stats.get("new", 0), kind_stats.get("update", 0),
             safer_state.get("calls", 0), SAFER_MAX_CALLS, safer_state.get("blocked", False))
    log.info("Call ready: %d | Need phone: %d | Insured: %d | Not trucking: %d",
             stats.get("call_ready", 0), stats.get("needs_phone", 0),
             stats.get("insured", 0), stats.get("not_trucking", 0))

    return inserted
