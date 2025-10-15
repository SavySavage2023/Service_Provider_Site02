# Copilot Instructions for Local Services Site (Flask)

## Project Overview
- This is a Flask web app for managing local services, with admin and provider panels.
- Data is stored in a local SQLite database at `instance/site.db`.
- No external APIs or databases are required; all data is local.
- The app is structured for easy deployment on cloud hosts (Render, Railway, Azure) or local packaging (Docker, PyInstaller EXE).

## Key Components
- `app.py`: Main Flask app entry point; routes and app setup.
- `Modules/`: Contains Jupyter notebooks for modular logic (admin, auth, db, products, services, etc.).
- `templates/`: Jinja2 HTML templates for all UI views (admin, provider, products, etc.).
- `static/`: Static assets (CSS, uploads).
- `instance/`: SQLite database and instance-specific files.

## Developer Workflows
- **Local Development**:
  - Create and activate a Python venv: `py -3 -m venv .venv; . .\.venv\Scripts\Activate.ps1`
  - Install dependencies: `pip install -r requirements.txt`
  - Set environment variables in PowerShell:
    - `$env:SECRET_KEY = "..."`
    - `$env:ADMIN_PASSWORD = "..."`
    - `$env:PROVIDER_PASSWORD = "..."` (optional)
  - Run: `python .\app.py` (dev server)
- **Production/Cloud**:
  - Use `gunicorn -w 2 -b 0.0.0.0:$PORT app:app` for deployment.
  - Use `render.yaml` or `Procfile` for cloud hosts.
- **Packaging**:
  - Docker: See README for build/run commands.
  - PyInstaller: `pyinstaller --onefile --add-data "templates;templates" --add-data "static;static" app.py`

## Project-Specific Patterns
- **ZIP Code Gating**: Admin can define allowed ZIP codes and radius; contact form checks ZIP proximity server-side.
- **Single Password Auth**: Admin and provider areas use environment variable passwords; session-based auth.
- **No ORM**: Direct SQLite usage; no SQLAlchemy or external DBs.
- **Jupyter Notebooks**: Business logic and utilities are modularized in notebooks under `Modules/`.
- **CSV Export**: Leads can be exported from the admin panel.

## Conventions & Integration Points
- All environment variables must be set before running (see README).
- Database file is always at `instance/site.db`.
- Templates and static files must be included for packaging (see PyInstaller example).
- Cloud deploys require proper env vars and use `gunicorn`.
- Ignore `Procfile` and `render.yaml` for local dev; they're for cloud hosts.

## Examples
- To add a new service, update logic in `Modules/services.ipynb` and corresponding template in `templates/admin_services.html`.
- To change ZIP code logic, see `Modules/admin.ipynb` and `app.py` for routing.
- For new provider features, update `Modules/provider.ipynb` and related templates.

## References
- See `README.md` for full setup, packaging, and deployment instructions.
- Key files: `app.py`, `requirements.txt`, `Modules/`, `templates/`, `static/`, `instance/site.db`.

---

**If any section is unclear or missing, please provide feedback to improve these instructions.**
