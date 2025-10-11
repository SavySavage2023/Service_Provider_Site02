# Local Services Site (Flask)

A simple, professional website with an admin panel to:
- Add/remove services
- Define allowed ZIP codes (local-only gating)
  - Each ZIP can have a radius (20/40/60/80 miles); clients within that distance of any listed ZIP are accepted
- Collect and export contact form leads

No external databases or APIs required. Data stores in `instance/site.db` (SQLite).

## Quickstart (Windows PowerShell)

1) Create and activate a virtual environment
```powershell
cd Websites
py -3 -m venv .venv
. .\.venv\Scripts\Activate.ps1
```

2) Install dependencies
```powershell
pip install -r requirements.txt
```

3) Set environment variables (change the passwords and secret before production)
```powershell
# Use a strong random secret key
$env:SECRET_KEY = "replace-with-a-secure-random-hex"
# Admin password (used for /admin)
$env:ADMIN_PASSWORD = "changeme"  # set your own
# Optional: provider password (used for /provider). If not set, provider uses the admin password
$env:PROVIDER_PASSWORD = "changeme"  # set your own or omit to reuse admin password
```

4) Run the app
```powershell
python .\app.py
# then open http://127.0.0.1:5000 in your browser
```

## Using the Admin
## Using the Provider area

- Go to http://127.0.0.1:5000/provider/login
- Login with the password set in `PROVIDER_PASSWORD` (or the admin password if not set)
- Manage profile at `/provider/profile`, view leads at `/provider/leads`


- Go to http://127.0.0.1:5000/admin
- Login with the password set in `ADMIN_PASSWORD`
- Add ZIP codes for your service area (must be 5 digits)
  - Choose a radius (20/40/60/80 mi) for each ZIP; proximity is computed automatically
- Add services (title, description, optional price, active/inactive)
- Leads appear under “Leads”; you can export CSV

## Notes

- Security: This uses a single admin password and basic session auth.
  - For production hardening, consider:
    - Setting `SECRET_KEY` to a secure random string.
    - Running behind HTTPS.
    - Using a strong admin password; prefer `ADMIN_PASSWORD_HASH` instead of plaintext.
    - Adding CSRF protection (e.g., Flask-WTF).
- Local-only gating:
  - Enforced server-side by checking the contact form ZIP against your allowed list.
  - You can pre-load ZIPs by adding them in the admin UI.
- Backups:
  - The SQLite database is at `instance/site.db`. Back it up periodically.
- Deploy:
  - Can be deployed to services like Render/Railway/Azure App Service. Make sure to store env vars securely.

## Deploy

### Option A: Render (recommended, free tier)
1) Push this folder to a Git repo (GitHub/GitLab).
2) Create a new Web Service on https://render.com and select your repo.
3) Set:
  - Build Command: `pip install -r requirements.txt`
  - Start Command: `gunicorn -w 2 -b 0.0.0.0:$PORT app:app`
4) Set environment variables:
  - SECRET_KEY: a long random string
  - ADMIN_PASSWORD: strong password for /admin
  - PROVIDER_PASSWORD: strong password for /provider (optional; defaults to admin password)
5) Deploy. You can also use `render.yaml` in this repo (New → From YAML).

### Option B: Docker
Build and run locally or in any container host:
```bash
docker build -t local-services .
docker run -p 8000:8000 -e SECRET_KEY=... -e ADMIN_PASSWORD=... local-services
```
Then open http://localhost:8000.

### Option C: Any VM or host
Install Python 3.11+, set env vars, then run:
```bash
pip install -r requirements.txt
gunicorn -w 2 -b 0.0.0.0:8000 app:app
```
Reverse-proxy with Nginx/Apache and add TLS via Let’s Encrypt.
