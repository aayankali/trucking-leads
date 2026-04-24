import requests
import time
import random
import re
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

DELAY_MIN = 1.2
DELAY_MAX = 2.5
TIMEOUT = 15

# =============================
# 🔹 FETCH LEADS (FROM YOUR DB / API)
# =============================
def fetch_leads():
    """
    Replace this with YOUR existing lead fetching logic.
    This is a fallback so scheduler doesn't break.
    """
    try:
        from database import get_recent_leads  # <-- adjust if needed
        return get_recent_leads()
    except:
        print("[WARN] Could not fetch leads from DB")
        return []

# =============================
# 🔹 SOURCES
# =============================
def fetch_dot_report(dot):
    try:
        url = f"https://dot.report/{dot}"
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)

        if r.status_code != 200:
            return None

        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text(" ", strip=True)

        match = re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", text)
        return match.group(0) if match else None

    except:
        return None


def fetch_safer(dot):
    try:
        url = f"https://safer.fmcsa.dot.gov/query.asp?query_type=queryCarrierSnapshot&query_param=USDOT&query_string={dot}"
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)

        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text(" ", strip=True)

        match = re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{3}[-.\s]?\d{4}", text)
        return match.group(0) if match else None

    except:
        return None


def fetch_directory(company):
    try:
        query = company.replace(" ", "+")
        url = f"https://www.bing.com/search?q={query}+trucking+phone"
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)

        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text(" ", strip=True)

        match = re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", text)
        return match.group(0) if match else None

    except:
        return None

# =============================
# 🔹 ENRICHMENT
# =============================
def enrich_lead(lead):
    dot = lead.get("dot_number")
    company = lead.get("company_name", "")

    phones = []
    sources = []

    def add(phone, source):
        if phone:
            phones.append(phone)
            sources.append(source)

    # ⭐ Layer 1 — DOT.report
    phone = fetch_dot_report(dot)
    add(phone, "dot_report")

    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    # ⭐ Layer 2 — SAFER
    if len(phones) < 2:
        phone = fetch_safer(dot)
        add(phone, "safer")

        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    # ⭐ Layer 3 — Directory
    if len(phones) < 2:
        phone = fetch_directory(company)
        add(phone, "directory")

    # =============================
    # 🔹 CONFIDENCE
    # =============================
    final_phone = ""
    confidence = "none"

    if phones:
        freq = {}
        for p in phones:
            freq[p] = freq.get(p, 0) + 1

        final_phone = max(freq, key=freq.get)

        if freq[final_phone] >= 2:
            confidence = "high"
        else:
            confidence = "medium"

    lead["phone"] = final_phone
    lead["phone_confidence"] = confidence
    lead["sources_found"] = ",".join(sources)

    return lead

# =============================
# 🔹 MAIN ENTRY (FIXED)
# =============================
def run_scraper(leads=None):
    # 🔥 FIX: supports both modes
    if leads is None:
        leads = fetch_leads()

    print(f"[INFO] Running scraper on {len(leads)} leads")

    results = []

    for i, lead in enumerate(leads):
        print(f"[{i+1}/{len(leads)}] Enriching DOT {lead.get('dot_number')}")

        enriched = enrich_lead(lead)

        print(
            f"→ phone={enriched['phone']} | conf={enriched['phone_confidence']} | source={enriched['sources_found']}"
        )

        results.append(enriched)

    return results
