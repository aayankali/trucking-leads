# ================== FINAL WORKING SCRAPER.PY ==================

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

def extract_phones(text):
    phones = []
    for parts in PHONE_RE.findall(text):
        digits = re.sub(r'\D', '', ''.join(parts))
        if len(digits) == 10:
            phones.append(digits)
        elif len(digits) == 11 and digits.startswith("1"):
            phones.append(digits[1:])
    return list(set(phones))

def safe_get(url, timeout=12):
    try:
        return requests.get(url, headers=HEADERS, timeout=timeout)
    except:
        return None

# ── FILTER ───────────────────────────────────────

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

# ── FETCH DATA ───────────────────────────────────

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

            leads.append({
                "dot_number": dot,
                "company_name": row.get("legal_name") or "",
                "phone": "",
                "phone_source": "",
                "phone_confidence": "none",
                "sources_found": 0,
                "state": row.get("phy_state") or "",
                "has_insurance": False,
                "insurance_status": "unknown",
                "registration_date": row.get("add_date", "")[:10],
            })

        log.info("%d leads fetched so far...", len(leads))

        if len(data) < PAGE_SIZE:
            break

        offset += PAGE_SIZE

    log.info("Fetched %d base leads from Socrata.", len(leads))
    return leads

# ── LAYERS ───────────────────────────────────────

def layer_fmcsa(lead):
    if not FMCSA_API_KEY:
        return lead

    url = f"{FMCSA_BASE}/{lead['dot_number']}"

    try:
        r = requests.get(url, params={"webKey": FMCSA_API_KEY}, timeout=10)
        if r.status_code != 200:
            return lead

        data = r.json().get("content", {})

        phone_raw = data.get("phyTelephone") or data.get("mailingTelephone") or ""
        phones = extract_phones(phone_raw)

        if phones:
            lead["phone"] = phones[0]
            lead["phone_source"] = "fmcsa_api"
            lead["phone_confidence"] = "high"
            lead["sources_found"] += 1

        allowed = (data.get("allowedToOperate") or "").upper()
        ins_code = data.get("bipdInsuranceOnFile")
        ins_req  = data.get("bipdInsuranceRequired")

        if ins_code and ins_req:
            ins_status = "insured"
        elif ins_req and not ins_code:
            ins_status = "none"
        else:
            ins_status = "unknown"

        lead["has_insurance"] = allowed == "Y" or ins_status == "insured"
        lead["insurance_status"] = ins_status

    except Exception:
        pass

    time.sleep(0.4)
    return lead


def layer_dot_report(lead):
    dot = lead["dot_number"]
    r = safe_get(f"https://dot.report/usdot/{dot}")

    if not r:
        return lead

    soup = BeautifulSoup(r.text, "lxml")

    phone = None
    for row in soup.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) >= 2:
            label = cols[0].get_text(strip=True).lower()
            if "phone" in label:
                phone = cols[1].get_text(strip=True)
                break

    if not phone:
        matches = extract_phones(soup.get_text(" "))
        if matches:
            phone = matches[0]

    if phone:
        cleaned = re.sub(r"\D", "", phone)
        if len(cleaned) == 11 and cleaned.startswith("1"):
            cleaned = cleaned[1:]

        if len(cleaned) == 10:
            if not lead["phone"]:
                lead["phone"] = cleaned
                lead["phone_source"] = "dot_report"
                lead["phone_confidence"] = "medium"
            elif lead["phone"] == cleaned:
                lead["phone_confidence"] = "high"

            lead["sources_found"] += 1

    return lead


def layer_aggregator(lead):
    query = f"{lead['company_name']} {lead['state']} trucking phone"
    url = f"https://www.bing.com/search?q={requests.utils.quote(query)}"

    r = safe_get(url)
    if not r:
        return lead

    text = BeautifulSoup(r.text, "lxml").get_text(" ")
    phones = extract_phones(text)

    if phones:
        if not lead["phone"]:
            lead["phone"] = phones[0]
            lead["phone_source"] = "aggregator"
            lead["phone_confidence"] = "medium"
        elif lead["phone"] == phones[0]:
            lead["phone_confidence"] = "high"

        lead["sources_found"] += 1

    time.sleep(random.uniform(1.0, 1.5))
    return lead

# ── FINALIZE ─────────────────────────────────────

def finalize(lead):
    if lead.get("phone"):
        phone = re.sub(r"\D", "", lead["phone"])
        if len(phone) == 11 and phone.startswith("1"):
            phone = phone[1:]
        if len(phone) == 10:
            lead["phone"] = phone
        else:
            lead["phone"] = ""
    return lead

# ── DB INSERT ────────────────────────────────────

def batch_insert(leads):
    if not leads:
        return 0

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    query = """
    INSERT INTO leads (
        dot_number, company_name, phone, phone_source,
        phone_confidence, sources_found, state,
        has_insurance, insurance_status, registration_date
    )
    VALUES %s
    ON CONFLICT (dot_number) DO NOTHING
    """

    values = [(
        l["dot_number"], l["company_name"], l["phone"],
        l["phone_source"], l["phone_confidence"],
        l["sources_found"], l["state"],
        l["has_insurance"], l["insurance_status"],
        l["registration_date"]
    ) for l in leads]

    psycopg2.extras.execute_values(cur, query, values)

    inserted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()

    return inserted

# ── MAIN ─────────────────────────────────────────

def run_scraper():
    log.info("Starting scraper...")

    leads = fetch_new_registrations()

    if not leads:
        log.warning("No leads fetched")
        return 0

    leads = [l for l in leads if is_good_lead(l)]
    log.info("After trucking filter: %d leads", len(leads))

    leads.sort(key=lambda x: str(x.get("registration_date") or ""))
    leads = leads[:ENRICH_LIMIT]

    batch = []
    inserted = 0

    stats = {"kept": 0, "skip_phone": 0, "skip_ins": 0}

    for i, lead in enumerate(leads):
        log.info("[%d/%d] DOT %s", i+1, len(leads), lead["dot_number"])

        lead = layer_fmcsa(lead)
        lead = layer_dot_report(lead)
        lead = layer_aggregator(lead)
        lead = finalize(lead)

        if lead.get("phone"):
            stats["skip_phone"] += 1
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
    log.info("Kept: %d | Skip phone: %d | Skip insurance: %d",
             stats["kept"], stats["skip_phone"], stats["skip_ins"])

    return inserted
