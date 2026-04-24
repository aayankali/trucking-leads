"""
Trucking Lead Generation Dashboard
Flask web app backed by PostgreSQL.
"""

import os
import csv
import io
import psycopg2
import psycopg2.extras
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_file, redirect, url_for

app = Flask(__name__, template_folder=".")
DATABASE_URL = os.environ.get("DATABASE_URL", "")


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


def query(sql, params=()):
    conn = get_db()
    with conn.cursor() as c:
        c.execute(sql, params)
        rows = c.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def execute(sql, params=()):
    conn = get_db()
    with conn.cursor() as c:
        c.execute(sql, params)
    conn.commit()
    conn.close()


with app.app_context():
    init_db()


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    total        = query("SELECT COUNT(*) AS n FROM leads")[0]["n"]
    today_str    = datetime.utcnow().strftime("%Y-%m-%d")
    today        = query("SELECT COUNT(*) AS n FROM leads WHERE added_date::date = %s", (today_str,))[0]["n"]
    contacted    = query("SELECT COUNT(*) AS n FROM leads WHERE contacted = TRUE")[0]["n"]
    fresh        = query("SELECT COUNT(*) AS n FROM leads WHERE contacted = FALSE")[0]["n"]
    insured      = query("SELECT COUNT(*) AS n FROM leads WHERE has_insurance = TRUE")[0]["n"]
    no_insurance = query("SELECT COUNT(*) AS n FROM leads WHERE has_insurance = FALSE")[0]["n"]

    search           = request.args.get("search", "").strip()
    state            = request.args.get("state", "")
    date_from        = request.args.get("date_from", "")
    date_to          = request.args.get("date_to", "")
    contacted_filter = request.args.get("contacted", "")
    insurance_filter = request.args.get("insurance", "")
    page             = max(1, int(request.args.get("page", 1)))
    per_page         = 25

    sql    = "SELECT * FROM leads WHERE 1=1"
    params = []

    if search:
        sql += " AND (company_name ILIKE %s OR owner_name ILIKE %s OR dot_number ILIKE %s OR phone ILIKE %s OR city ILIKE %s)"
        s = f"%{search}%"
        params += [s, s, s, s, s]
    if state:
        sql += " AND state = %s"; params.append(state)
    if date_from:
        sql += " AND registration_date >= %s"; params.append(date_from)
    if date_to:
        sql += " AND registration_date <= %s"; params.append(date_to)
    if contacted_filter == "0":
        sql += " AND contacted = FALSE"
    elif contacted_filter == "1":
        sql += " AND contacted = TRUE"
    if insurance_filter == "1":
        sql += " AND has_insurance = TRUE"
    elif insurance_filter == "0":
        sql += " AND has_insurance = FALSE"

    count_sql      = sql.replace("SELECT *", "SELECT COUNT(*) AS n")
    total_filtered = query(count_sql, params)[0]["n"]

    sql += " ORDER BY added_date DESC LIMIT %s OFFSET %s"
    params += [per_page, (page - 1) * per_page]
    leads = query(sql, params)

    states      = [r["state"] for r in query("SELECT DISTINCT state FROM leads WHERE state != '' ORDER BY state")]
    total_pages = max(1, (total_filtered + per_page - 1) // per_page)

    return render_template("index.html",
        leads=leads,
        stats={"total": total, "today": today, "contacted": contacted,
               "fresh": fresh, "insured": insured, "no_insurance": no_insurance},
        states=states,
        filters={"search": search, "state": state, "date_from": date_from,
                 "date_to": date_to, "contacted": contacted_filter,
                 "insurance": insurance_filter},
        page=page,
        total_pages=total_pages,
        total_filtered=total_filtered,
    )


@app.route("/mark-contacted/<int:lead_id>", methods=["POST"])
def mark_contacted(lead_id):
    val = request.form.get("value", "1") == "1"
    execute("UPDATE leads SET contacted = %s WHERE id = %s", (val, lead_id))
    return redirect(request.referrer or url_for("index"))


@app.route("/save-note/<int:lead_id>", methods=["POST"])
def save_note(lead_id):
    note = request.form.get("note", "")
    execute("UPDATE leads SET notes = %s WHERE id = %s", (note, lead_id))
    return jsonify({"ok": True})


@app.route("/export")
def export():
    search           = request.args.get("search", "").strip()
    state            = request.args.get("state", "")
    date_from        = request.args.get("date_from", "")
    date_to          = request.args.get("date_to", "")
    insurance_filter = request.args.get("insurance", "")

    sql    = "SELECT * FROM leads WHERE 1=1"
    params = []

    if search:
        sql += " AND (company_name ILIKE %s OR owner_name ILIKE %s OR dot_number ILIKE %s)"
        s = f"%{search}%"; params += [s, s, s]
    if state:
        sql += " AND state = %s"; params.append(state)
    if date_from:
        sql += " AND registration_date >= %s"; params.append(date_from)
    if date_to:
        sql += " AND registration_date <= %s"; params.append(date_to)
    if insurance_filter == "1":
        sql += " AND has_insurance = TRUE"
    elif insurance_filter == "0":
        sql += " AND has_insurance = FALSE"

    sql += " ORDER BY added_date DESC"
    leads = query(sql, params)

    fields = ["dot_number","mc_number","company_name","owner_name","phone","email",
              "address","city","state","zip_code","entity_type","drivers","power_units",
              "registration_date","added_date","contacted","notes","has_insurance"]
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    for lead in leads:
        writer.writerow({k: lead.get(k, "") for k in fields})

    output.seek(0)
    filename = f"trucking_leads_{datetime.utcnow().strftime('%Y%m%d')}.csv"
    return send_file(
        io.BytesIO(output.getvalue().encode("utf-8")),
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename,
    )


@app.route("/api/stats")
def api_stats():
    total     = query("SELECT COUNT(*) AS n FROM leads")[0]["n"]
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    today     = query("SELECT COUNT(*) AS n FROM leads WHERE added_date::date = %s", (today_str,))[0]["n"]
    contacted = query("SELECT COUNT(*) AS n FROM leads WHERE contacted = TRUE")[0]["n"]
    insured   = query("SELECT COUNT(*) AS n FROM leads WHERE has_insurance = TRUE")[0]["n"]
    by_state  = query("SELECT state, COUNT(*) AS n FROM leads GROUP BY state ORDER BY n DESC LIMIT 10")
    return jsonify({"total": total, "today": today, "contacted": contacted,
                    "insured": insured, "by_state": by_state})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
