def layer_dot_report(lead):
    """DOT.report scraping — HIGH VALUE source"""
    if lead["phone"] and lead["phone_confidence"] == "high":
        return lead

    dot = lead["dot_number"]
    url = f"https://dot.report/{dot}"

    r = safe_get(url, timeout=10)
    if not r:
        return lead

    text = BeautifulSoup(r.text, "lxml").get_text(" ")
    phones = extract_phones(text)

    if phones:
        prev = lead.get("phone", "")
        if prev and prev == phones[0]:
            lead["phone_confidence"] = "high"
        elif not prev:
            lead["phone"] = phones[0]
            lead["phone_source"] = "dot_report"
            lead["phone_confidence"] = "high"
        else:
            # different phone found → keep both in mind
            pass

        lead["sources_found"] += 1

    time.sleep(1.2)
    return lead
