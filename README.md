# 🚛 Trucking Lead Generation System

Automatically pulls newly registered trucking companies from the FMCSA database
every 24 hours. Hosted free on Railway with a permanent PostgreSQL database.

---

## Files in this repo

| File | What it does |
|------|-------------|
| scraper.py | Pulls new trucking leads from FMCSA API |
| scheduler.py | Keeps scraper running every 24h automatically |
| dashboard.py | The web dashboard (Flask) |
| index.html | Dashboard UI |
| railway.json | Railway deployment config |
| Procfile | Tells Railway how to start each service |
| requirements.txt | Python packages needed |

---

## SETUP INSTRUCTIONS (step by step)

### STEP 1 — Create a GitHub account (if you don't have one)
1. Go to https://github.com and click Sign Up
2. Create your free account

### STEP 2 — Upload these files to GitHub
1. On GitHub, click the "+" button (top right) → "New repository"
2. Name it: trucking-leads
3. Make it Public
4. Click "Create repository"
5. On the next screen, click "uploading an existing file"
6. Drag ALL files from this zip directly into the page (not in any subfolder)
7. Click "Commit changes"

### STEP 3 — Create a Railway account
1. Go to https://railway.app
2. Click "Login" → "Login with GitHub"
3. Authorize Railway to access your GitHub

### STEP 4 — Create a new Railway project
1. On Railway dashboard, click "New Project"
2. Click "Deploy from GitHub repo"
3. Select your "trucking-leads" repo
4. Railway will start setting it up — wait for it to finish

### STEP 5 — Add a PostgreSQL database (this stores your leads permanently)
1. Inside your Railway project, click "+ New"
2. Click "Database"
3. Click "Add PostgreSQL"
4. Railway creates the database automatically — wait ~30 seconds
5. Click on the PostgreSQL service that appeared
6. Click the "Variables" tab
7. Find "DATABASE_URL" — copy it (you'll need it in Step 7)

### STEP 6 — Add a second service for the scraper (auto 24h updates)
1. Inside your Railway project, click "+ New"
2. Click "GitHub Repo" → select "trucking-leads" again
3. When it asks what to run, set the start command to:
   python scheduler.py
4. This is your background worker that fetches new leads every 24 hours

### STEP 7 — Connect both services to the database
Do this for BOTH the dashboard service AND the scraper service:
1. Click on the service
2. Click "Variables" tab
3. Click "+ New Variable"
4. Name: DATABASE_URL
5. Value: paste the DATABASE_URL you copied in Step 5
   (Railway also has a shortcut: click "Add Reference" → select your
    PostgreSQL service → select DATABASE_URL — this links them automatically)

### STEP 8 — Add your FMCSA API key
Do this for BOTH services (same steps as above, add a second variable):
1. Name: FMCSA_API_KEY
2. Value: your API key from FMCSA

Don't have a key yet? The system runs in DEMO MODE with 8 sample leads
until your key arrives. Get your free key at:
https://ask.fmcsa.dot.gov/app/answers/detail/a_id/48

### STEP 9 — Open your dashboard
1. Click on the dashboard service (the one running gunicorn)
2. Click "Settings" tab
3. Under "Networking", click "Generate Domain"
4. Railway gives you a URL like: https://trucking-leads-production.up.railway.app
5. Open that URL — your dashboard is live!

### STEP 10 — You're done!
- The scraper runs automatically every 24 hours and adds new leads
- Your database persists forever — leads are never lost
- Use the dashboard to search, filter by state/insurance, mark contacted, export CSV

---

## FAQ

Q: How do I know the scraper is running?
A: Click the scraper service in Railway → click "Logs" tab. You'll see
   "Starting FMCSA scraper run" every 24 hours.

Q: The dashboard shows 0 leads / demo leads — is something wrong?
A: Either your FMCSA API key isn't set yet (demo mode is normal), or the
   scraper hasn't run yet. Click the scraper service → "Deploy" → "Restart"
   to trigger it manually.

Q: Will Railway always be free?
A: Railway's free Hobby plan gives $5 of credit per month. This system uses
   roughly $0.50–2/month depending on traffic. If you exceed $5 free credit,
   it's $5/month to upgrade. Still very cheap.

---
Built with Python, Flask, PostgreSQL, FMCSA Public API. Hosted on Railway.
