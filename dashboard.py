"""
Trucking Lead Generation Dashboard — Flask + PostgreSQL
Supports 5-layer enrichment fields: phone confidence, website, email, insurance status
"""

import os, csv, io
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_file, redirect, url_for
import psycopg2, psycopg2.extras

app = Flask(__name__, template_folder=".")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

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

def init_db():
    conn = get_db()
    # Create base table if not exists
    with conn.cursor() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS leads (
                id SERIAL PRIMARY KEY, dot_number TEXT UNIQUE,
                mc_number TEXT, company_name TEXT, owner_name TEXT,
                phone TEXT DEFAULT '', email TEXT DEFAULT '',
                address TEXT, city TEXT, state TEXT, zip_code TEXT,
                entity_type TEXT, operation_type TEXT, cargo_type TEXT,
                drivers INTEGER DEFAULT 0, power_units INTEGER DEFAULT 0,
                status TEXT DEFAULT 'A',
                added_date TIMESTAMP, registration_date DATE,
                contacted BOOLEAN DEFAULT FALSE, notes TEXT DEFAULT '',
                has_insurance BOOLEAN DEFAULT FALSE
            )
        """)
    conn.commit()

    # Migrate: add each new column in its own transaction
    new_columns = [
        ("insurance_status",  "TEXT",    "'unknown'"),
        ("phone_source",      "TEXT",    "''"),
        ("phone_confidence",  "TEXT",    "'none'"),
        ("sources_found",     "INTEGER", "0"),
        ("enriched",          "BOOLEAN", "FALSE"),
        ("website",           "TEXT",    "''"),
    ]
    for col, col_type, default in new_columns:
        try:
            with conn.cursor() as c:
                c.execute(f"ALTER TABLE leads ADD COLUMN {col} {col_type} DEFAULT {default}")
            conn.commit()
        except psycopg2.errors.DuplicateColumn:
            conn.rollback()
        except Exception as e:
            conn.rollback()
    conn.close()


with app.app_context():
    init_db()

@app.route("/")
def index():
    total     = query("SELECT COUNT(*) AS n FROM leads")[0]["n"]
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    today     = query("SELECT COUNT(*) AS n FROM leads WHERE added_date::date=%s",(today_str,))[0]["n"]
    contacted = query("SELECT COUNT(*) AS n FROM leads WHERE contacted=TRUE")[0]["n"]
    no_ins    = query("SELECT COUNT(*) AS n FROM leads WHERE insurance_status IN ('none','unknown')")[0]["n"]
    high_conf = query("SELECT COUNT(*) AS n FROM leads WHERE phone_confidence='high'")[0]["n"]
    with_phone= query("SELECT COUNT(*) AS n FROM leads WHERE phone!='' AND phone IS NOT NULL")[0]["n"]

    search       = request.args.get("search","").strip()
    state        = request.args.get("state","")
    date_from    = request.args.get("date_from","")
    date_to      = request.args.get("date_to","")
    contacted_f  = request.args.get("contacted","")
    insurance_f  = request.args.get("insurance","")
    confidence_f = request.args.get("confidence","")
    page         = max(1,int(request.args.get("page",1)))
    per_page     = 25

    sql    = "SELECT * FROM leads WHERE 1=1"
    params = []

    if search:
        sql += " AND (company_name ILIKE %s OR owner_name ILIKE %s OR dot_number ILIKE %s OR phone ILIKE %s OR city ILIKE %s)"
        s = f"%{search}%"; params += [s,s,s,s,s]
    if state:
        sql += " AND state=%s"; params.append(state)
    if date_from:
        sql += " AND registration_date>=%s"; params.append(date_from)
    if date_to:
        sql += " AND registration_date<=%s"; params.append(date_to)
    if contacted_f == "0":
        sql += " AND contacted=FALSE"
    elif contacted_f == "1":
        sql += " AND contacted=TRUE"
    if insurance_f == "hot":
        sql += " AND insurance_status IN ('none','unknown')"
    elif insurance_f:
        sql += " AND insurance_status=%s"; params.append(insurance_f)
    if confidence_f:
        sql += " AND phone_confidence=%s"; params.append(confidence_f)

    count_sql      = sql.replace("SELECT *","SELECT COUNT(*) AS n")
    total_filtered = query(count_sql, params)[0]["n"]

    sql += """ ORDER BY
        CASE phone_confidence WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
        CASE insurance_status WHEN 'none' THEN 1 WHEN 'unknown' THEN 2 ELSE 3 END,
        added_date DESC LIMIT %s OFFSET %s"""
    params += [per_page,(page-1)*per_page]
    leads = query(sql, params)

    states      = [r["state"] for r in query("SELECT DISTINCT state FROM leads WHERE state!='' ORDER BY state")]
    total_pages = max(1,(total_filtered+per_page-1)//per_page)

    return render_template("index.html",
        leads=leads,
        stats={"total":total,"today":today,"contacted":contacted,
               "no_insurance":no_ins,"high_conf":high_conf,"with_phone":with_phone},
        states=states,
        filters={"search":search,"state":state,"date_from":date_from,"date_to":date_to,
                 "contacted":contacted_f,"insurance":insurance_f,"confidence":confidence_f},
        page=page, total_pages=total_pages, total_filtered=total_filtered,
    )

@app.route("/mark-contacted/<int:lead_id>", methods=["POST"])
def mark_contacted(lead_id):
    val = request.form.get("value","1") == "1"
    execute("UPDATE leads SET contacted=%s WHERE id=%s",(val,lead_id))
    return redirect(request.referrer or url_for("index"))

@app.route("/save-note/<int:lead_id>", methods=["POST"])
def save_note(lead_id):
    execute("UPDATE leads SET notes=%s WHERE id=%s",(request.form.get("note",""),lead_id))
    return jsonify({"ok":True})

@app.route("/export")
def export():
    search      = request.args.get("search","").strip()
    state       = request.args.get("state","")
    insurance_f = request.args.get("insurance","")
    confidence_f= request.args.get("confidence","")

    sql = "SELECT * FROM leads WHERE 1=1"; params=[]
    if search:
        s=f"%{search}%"; sql+=" AND (company_name ILIKE %s OR dot_number ILIKE %s)"; params+=[s,s]
    if state:
        sql+=" AND state=%s"; params.append(state)
    if insurance_f=="hot":
        sql+=" AND insurance_status IN ('none','unknown')"
    elif insurance_f:
        sql+=" AND insurance_status=%s"; params.append(insurance_f)
    if confidence_f:
        sql+=" AND phone_confidence=%s"; params.append(confidence_f)
    sql+=(" ORDER BY CASE phone_confidence WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,"
          "CASE insurance_status WHEN 'none' THEN 1 ELSE 2 END, added_date DESC")

    leads = query(sql, params)
    fields = ["dot_number","mc_number","company_name","owner_name","phone","email","website",
              "address","city","state","zip_code","entity_type","drivers","power_units",
              "insurance_status","phone_confidence","phone_source","sources_found",
              "registration_date","added_date","contacted","notes"]
    out = io.StringIO()
    w   = csv.DictWriter(out, fieldnames=fields)
    w.writeheader()
    for lead in leads:
        w.writerow({k: lead.get(k,"") for k in fields})
    out.seek(0)
    fname = f"trucking_leads_{datetime.utcnow().strftime('%Y%m%d')}.csv"
    return send_file(io.BytesIO(out.getvalue().encode("utf-8")),
                     mimetype="text/csv", as_attachment=True, download_name=fname)

@app.route("/api/stats")
def api_stats():
    total     = query("SELECT COUNT(*) AS n FROM leads")[0]["n"]
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    today     = query("SELECT COUNT(*) AS n FROM leads WHERE added_date::date=%s",(today_str,))[0]["n"]
    by_conf   = query("SELECT phone_confidence, COUNT(*) AS n FROM leads GROUP BY phone_confidence")
    by_ins    = query("SELECT insurance_status, COUNT(*) AS n FROM leads GROUP BY insurance_status")
    by_state  = query("SELECT state, COUNT(*) AS n FROM leads GROUP BY state ORDER BY n DESC LIMIT 10")
    return jsonify({"total":total,"today":today,"by_confidence":by_conf,
                    "by_insurance":by_ins,"by_state":by_state})

if __name__ == "__main__":
    port = int(os.environ.get("PORT",5000))
    app.run(host="0.0.0.0", port=port, debug=False)
