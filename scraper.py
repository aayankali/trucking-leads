import requests
import time
import random
import re
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# 🔥 CONFIG
DELAY_MIN = 1.2
DELAY_MAX = 2.5
TIMEOUT = 15
RETRIES = 2

# ⭐ STRONG SOURCES
def fetch_dot_report(dot):
    try:
        url = f"https://dot.report/{dot}"
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code != 200:
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        text = soup.get_text(" ", strip=True)

        phone_match = re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", text)
        if phone_match:
            return phone_match.group(0)

    except:
        return None

    return None


def fetch_safer(dot):
    try:
        url = f"https://safer.fmcsa.dot.gov/query.asp?query_type=queryCarrierSnapshot&query_param=USDOT&query_string={dot}"
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)

        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text(" ", strip=True)

        phone_match = re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", text)
        if phone_match:
            return phone_match.group(0)

    except:
        return None

    return None


def fetch_directory(company):
    try:
        query = company.replace(" ", "+")
        url = f"https://www.bing.com/search?q={query}+trucking+phone"
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)

        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text(" ", strip=True)

        phone_match = re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", text)
        if phone_match:
            return phone_match.group(0)

    except:
        return None

    return None


# 🧠 CORE ENRICHMENT ENGINE
def enrich_lead(lead):
    dot = lead["dot_number"]
    company = lead["company_name"]

    phones = []
    sources = []

    def add_phone(phone, source):
        if phone:
            phones.append(phone)
            sources.append(source)

    # 🔥 Layer 1 — DOT.report (MAIN)
    phone = fetch_dot_report(dot)
    add_phone(phone, "dot_report")

    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    # 🔥 Layer 2 — SAFER fallback
    if len(phones) < 2:
        phone = fetch_safer(dot)
        add_phone(phone, "safer")

        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    # 🔥 Layer 3 — Directory (Bing)
    if len(phones) < 2:
        phone = fetch_directory(company)
        add_phone(phone, "directory")

    # 🧠 CONFIDENCE LOGIC
    final_phone = None
    confidence = "none"

    if phones:
        # Count occurrences
        freq = {}
        for p in phones:
            freq[p] = freq.get(p, 0) + 1

        final_phone = max(freq, key=freq.get)

        if freq[final_phone] >= 2:
            confidence = "high"
        else:
            confidence = "medium"

    lead["phone"] = final_phone or ""
    lead["phone_confidence"] = confidence
    lead["sources_found"] = ",".join(sources)

    return lead


# 🚀 MAIN LOOP
def run_scraper(leads):
    results = []

    for i, lead in enumerate(leads):
        print(f"[{i+1}/{len(leads)}] Enriching DOT {lead['dot_number']}")

        enriched = enrich_lead(lead)

        print(
            f"→ phone={enriched['phone']} | conf={enriched['phone_confidence']} | source={enriched['sources_found']}"
        )

        results.append(enriched)

    return results
