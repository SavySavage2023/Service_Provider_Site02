import os
import re
import sqlite3
import datetime
from functools import wraps
from contextlib import closing
from flask import (
    Flask, render_template, request, redirect,
    url_for, session, flash, abort
)
from werkzeug.security import generate_password_hash, check_password_hash
from io import StringIO
import csv
try:
    import pgeocode  # optional; proximity disabled if missing
except Exception:
    pgeocode = None

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    # dotenv is optional; ignore if not installed
    pass

APP_DIR = os.path.abspath(os.path.dirname(__file__))
INSTANCE_DIR = os.path.join(APP_DIR, "instance")
os.makedirs(INSTANCE_DIR, exist_ok=True)
DB_PATH = os.path.join(INSTANCE_DIR, "site.db")


def get_env(name, default=None):
    val = os.environ.get(name)
    return val if val is not None else default


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    db = get_db()
    try:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS services (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              title TEXT NOT NULL,
              description TEXT,
              price TEXT,
                            posted_by TEXT,
              active INTEGER NOT NULL DEFAULT 1,
              created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS zips (
              zip TEXT PRIMARY KEY,
              radius_miles INTEGER NOT NULL DEFAULT 20
            );

            CREATE TABLE IF NOT EXISTS leads (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT NOT NULL,
              email TEXT,
              phone TEXT,
              zip TEXT,
              message TEXT,
              created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS profile (
              id INTEGER PRIMARY KEY CHECK (id = 1),
                            first_name TEXT,
              business_name TEXT,
              contact_email TEXT,
              phone TEXT,
              base_zip TEXT,
              address TEXT,
              about TEXT
            );

            CREATE TABLE IF NOT EXISTS blocked_addresses (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              address TEXT NOT NULL,
              zip TEXT,
              reason TEXT,
              created_at TEXT NOT NULL
            );

                        -- Password storage (DB becomes source of truth so passwords can be changed in-app)
                        CREATE TABLE IF NOT EXISTS auth (
                            id INTEGER PRIMARY KEY CHECK (id = 1),
                            admin_password_hash TEXT,
                            provider_password_hash TEXT
                        );

                        INSERT OR IGNORE INTO auth (id) VALUES (1);

            INSERT OR IGNORE INTO profile (id, business_name)
            VALUES (1, 'Your Mom''s Services');
            """
        )
        db.commit()
    finally:
        db.close()

    # Try to enable FTS5 if available
    db = get_db()
    try:
        cur = db.cursor()
        cur.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS services_fts USING fts5(
              title,
              description,
              content='services',
              content_rowid='id',
              tokenize = 'porter'
            );
            """
        )
        # Triggers to keep FTS in sync
        cur.executescript(
            """
            CREATE TRIGGER IF NOT EXISTS services_ai AFTER INSERT ON services BEGIN
              INSERT INTO services_fts(rowid, title, description)
              VALUES (new.id, new.title, new.description);
            END;
            CREATE TRIGGER IF NOT EXISTS services_ad AFTER DELETE ON services BEGIN
              DELETE FROM services_fts WHERE rowid = old.id;
            END;
            CREATE TRIGGER IF NOT EXISTS services_au AFTER UPDATE ON services BEGIN
              DELETE FROM services_fts WHERE rowid = old.id;
              INSERT INTO services_fts(rowid, title, description)
              VALUES (new.id, new.title, new.description);
            END;
            """
        )
        db.commit()
        app_has_fts = True
    except sqlite3.OperationalError:
        # SQLite build without FTS5; continue without it
        app_has_fts = False
    finally:
        db.close()

    # Ensure radius_miles column exists for older DBs
    db = get_db()
    try:
        cur = db.cursor()
        cur.execute("PRAGMA table_info(zips)")
        cols = [row[1] for row in cur.fetchall()]
        if "radius_miles" not in cols:
            cur.execute("ALTER TABLE zips ADD COLUMN radius_miles INTEGER NOT NULL DEFAULT 20")
            db.commit()
    finally:
        db.close()

    # Ensure posted_by exists on services (migration for older DBs)
    db = get_db()
    try:
        cur = db.cursor()
        cur.execute("PRAGMA table_info(services)")
        scols = [row[1] for row in cur.fetchall()]
        if "posted_by" not in scols:
            cur.execute("ALTER TABLE services ADD COLUMN posted_by TEXT")
            db.commit()
    finally:
        db.close()

    # Backfill posted_by with provider first_name or business_name where missing
    db = get_db()
    try:
        cur = db.cursor()
        cur.execute("SELECT first_name, business_name FROM profile WHERE id = 1")
        row = cur.fetchone()
        first = row["first_name"] if row and row["first_name"] else None
        biz = row["business_name"] if row and row["business_name"] else None
        fallback = first or biz or "Provider"
        cur.execute(
            "UPDATE services SET posted_by = ? WHERE posted_by IS NULL OR posted_by = ''",
            (fallback,),
        )
        db.commit()
    finally:
        db.close()

    # Ensure first_name exists on profile (migration for older DBs)
    db = get_db()
    try:
        cur = db.cursor()
        cur.execute("PRAGMA table_info(profile)")
        pcols = [row[1] for row in cur.fetchall()]
        if "first_name" not in pcols:
            cur.execute("ALTER TABLE profile ADD COLUMN first_name TEXT")
            db.commit()
        if "profile_photo" not in pcols:
            cur.execute("ALTER TABLE profile ADD COLUMN profile_photo TEXT")
            db.commit()
    finally:
        db.close()

    # Ensure address exists on leads (migration for older DBs)
    db = get_db()
    try:
        cur = db.cursor()
        cur.execute("PRAGMA table_info(leads)")
        lcols = [row[1] for row in cur.fetchall()]
        if "address" not in lcols:
            cur.execute("ALTER TABLE leads ADD COLUMN address TEXT")
            db.commit()
        if "status" not in lcols:
            cur.execute("ALTER TABLE leads ADD COLUMN status TEXT DEFAULT 'new'")
            db.commit()
        if "follow_up_date" not in lcols:
            cur.execute("ALTER TABLE leads ADD COLUMN follow_up_date TEXT")
            db.commit()
        if "recurring" not in lcols:
            cur.execute("ALTER TABLE leads ADD COLUMN recurring INTEGER DEFAULT 0")
            db.commit()
    finally:
        db.close()

    # Rebuild FTS index to ensure in sync with existing rows (if available)
    if 'app_has_fts' in locals() and app_has_fts:
        db = get_db()
        try:
            cur = db.cursor()
            cur.execute("INSERT INTO services_fts(services_fts) VALUES ('rebuild')")
            db.commit()
        except sqlite3.OperationalError:
            pass
        finally:
            db.close()


def create_app():
    app = Flask(__name__, instance_path=INSTANCE_DIR, instance_relative_config=True)

    # Secret key for sessions
    secret_key = get_env("SECRET_KEY")
    if not secret_key:
        # Ephemeral for dev if not provided
        secret_key = os.urandom(32)
        print("WARNING: SECRET_KEY not set. Using a temporary key. Set SECRET_KEY for production.")
    app.secret_key = secret_key

    # Init DB on startup
    init_db()

    # Password config from DB (fallback to env on first run)
    with closing(get_db()) as db:
        cur = db.cursor()
        cur.execute("SELECT admin_password_hash, provider_password_hash FROM auth WHERE id = 1")
        row = cur.fetchone()

        # Resolve admin hash
        admin_hash_db = row["admin_password_hash"] if row else None
        if not admin_hash_db:
            admin_password_hash = get_env("ADMIN_PASSWORD_HASH")
            admin_password_plain = get_env("ADMIN_PASSWORD")
            if admin_password_plain and not admin_password_hash:
                admin_password_hash = generate_password_hash(admin_password_plain)
            if not admin_password_hash:
                print("WARNING: ADMIN_PASSWORD/ADMIN_PASSWORD_HASH not set. Defaulting to 'changeme'.")
                admin_password_hash = generate_password_hash("changeme")
            cur.execute("UPDATE auth SET admin_password_hash = ? WHERE id = 1", (admin_password_hash,))
            db.commit()
            admin_hash_final = admin_password_hash
        else:
            admin_hash_final = admin_hash_db

        # Resolve provider hash (defaults to admin if missing)
        provider_hash_db = row["provider_password_hash"] if row else None
        if not provider_hash_db:
            provider_password_hash = get_env("PROVIDER_PASSWORD_HASH")
            provider_password_plain = get_env("PROVIDER_PASSWORD")
            if provider_password_plain and not provider_password_hash:
                provider_password_hash = generate_password_hash(provider_password_plain)
            if not provider_password_hash:
                provider_password_hash = admin_hash_final
                print("INFO: PROVIDER_PASSWORD not set; provider login uses the admin password.")
            cur.execute("UPDATE auth SET provider_password_hash = ? WHERE id = 1", (provider_password_hash,))
            db.commit()
            provider_hash_final = provider_password_hash
        else:
            provider_hash_final = provider_hash_db

    app.config["ADMIN_PASSWORD_HASH"] = admin_hash_final
    app.config["PROVIDER_PASSWORD_HASH"] = provider_hash_final

    # Template globals
    @app.context_processor
    def inject_globals():
        # Load profile once per request
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("SELECT first_name, business_name, contact_email, phone, base_zip, address, about, profile_photo FROM profile WHERE id = 1")
            row = cur.fetchone()
        profile = {
            "first_name": (row["first_name"] if row and row["first_name"] else ""),
            "business_name": (row["business_name"] if row else "Your Mom's Services"),
            "contact_email": (row["contact_email"] if row else ""),
            "phone": (row["phone"] if row else ""),
            "base_zip": (row["base_zip"] if row else ""),
            "address": (row["address"] if row else ""),
            "about": (row["about"] if row else ""),
            "profile_photo": (row["profile_photo"] if row else "")
        }
        return {
            "current_year": datetime.datetime.now().year,
            "profile": profile,
        }

    def login_required(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if not session.get("admin"):
                return redirect(url_for("admin_login", next=request.path))
            return f(*args, **kwargs)
        return wrapper

    def provider_required(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if not session.get("provider"):
                return redirect(url_for("provider_login", next=request.path))
            return f(*args, **kwargs)
        return wrapper

    # Distance calculator (km by default; we'll convert to miles)
    geo_dist = pgeocode.GeoDistance("US") if pgeocode else None

    def is_zip_allowed(zip_code: str) -> bool:
        if not zip_code or not re.fullmatch(r"\d{5}", zip_code):
            return False
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("SELECT zip, radius_miles FROM zips")
            rows = cur.fetchall()
        # Quick exact match first
        if any(r["zip"] == zip_code for r in rows):
            return True
        # Proximity check (if pgeocode available)
        if geo_dist is not None:
            for r in rows:
                try:
                    km = geo_dist.query_postal_code(r["zip"], zip_code)
                except Exception:
                    km = None
                if km is None or (isinstance(km, float) and (km != km)):
                    # NaN or missing distance
                    continue
                miles = float(km) * 0.621371
                if miles <= float(r["radius_miles"]):
                    return True
        return False

    @app.route("/")
    def home():
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute(
                "SELECT id, title, description, price, posted_by FROM services WHERE active = 1 ORDER BY created_at DESC LIMIT 6"
            )
            services = cur.fetchall()
        return render_template("index.html", services=services, title="Home")

    @app.route("/services")
    def services():
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute(
                "SELECT id, title, description, price, posted_by FROM services WHERE active = 1 ORDER BY created_at DESC"
            )
            services = cur.fetchall()
        return render_template("services.html", services=services, title="Services")

    @app.get("/search")
    def search():
        q = (request.args.get("q", "") or "").strip()
        results = []
        count = 0
        if q:
            # Build tokens and simple synonym expansion
            tokens = re.findall(r"\w+", q.lower())
            synonyms = {
                "lawn": ["yard", "mow", "mowing", "landscape", "landscaping"],
                "clean": ["cleaning", "housekeeping", "maid"],
                "repair": ["fix", "handyman"],
            }
            expanded = set(tokens)
            for t in tokens:
                for syn in synonyms.get(t, []):
                    expanded.add(syn)

            # Prefer FTS with stemming + prefix, fallback to LIKE OR terms
            match_expr = " OR ".join(f"{t}*" for t in expanded)
            with closing(get_db()) as db:
                cur = db.cursor()
                try:
                    cur.execute(
                        """
                        SELECT s.id, s.title, s.description, s.price, s.posted_by
                        FROM services s
                        JOIN services_fts f ON f.rowid = s.id
                        WHERE s.active = 1 AND f MATCH ?
                        ORDER BY bm25(services_fts) ASC
                        """,
                        (match_expr or None,),
                    )
                    results = cur.fetchall()
                except sqlite3.OperationalError:
                    # Fallback for environments without FTS5
                    ors = []
                    params = []
                    for t in expanded:
                        like = f"%{t}%"
                        ors.append("title LIKE ? OR description LIKE ?")
                        params.extend([like, like])
                    where = "(" + ") OR (".join(ors) + ")" if ors else "1=1"
                    cur.execute(
                        f"""
                        SELECT id, title, description, price, posted_by
                        FROM services
                        WHERE active = 1 AND {where}
                        ORDER BY created_at DESC
                        """,
                        params,
                    )
                    results = cur.fetchall()
                count = len(results)
        return render_template("search.html", q=q, results=results, count=count, title="Search")

    @app.route("/contact", methods=["GET", "POST"])
    def contact():
        if request.method == "POST":
            name = request.form.get("name", "").strip()
            email = request.form.get("email", "").strip()
            phone = request.form.get("phone", "").strip()
            zip_code = request.form.get("zip", "").strip()
            address = request.form.get("address", "").strip()
            message = request.form.get("message", "").strip()
            svc = request.form.get("service", "").strip()

            if not name:
                flash("Please enter your name.", "error")
                return render_template("contact.html", form=request.form, title="Contact")
            if not re.fullmatch(r"\d{5}", zip_code or ""):
                flash("Please enter a valid 5-digit ZIP code.", "error")
                return render_template("contact.html", form=request.form, title="Contact")

            if not is_zip_allowed(zip_code):
                flash(
                    "Sorry, we currently serve local clients only. Your ZIP code is outside our service area.",
                    "error",
                )
                return render_template("contact.html", form=request.form, title="Contact")

            # If a service was passed, prefix it into the message for context
            if svc:
                prefix = f"[Service: {svc}] "
                if not message or not message.startswith(prefix):
                    message = prefix + (message or "")

            with closing(get_db()) as db:
                cur = db.cursor()
                cur.execute(
                    """
                    INSERT INTO leads (name, email, phone, zip, address, message, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        name,
                        email,
                        phone,
                        zip_code,
                        address,
                        message,
                        datetime.datetime.utcnow().isoformat(),
                    ),
                )
                db.commit()

            flash("Thanks! Your message has been sent. We'll get back to you soon.", "success")
            return redirect(url_for("services"))

        # GET: Optional deep-link parameters for placeholders
        service_name = (request.args.get("service", "") or "").strip()
        poster = (request.args.get("poster", "") or "").strip()
        if service_name or poster:
            hi = f"Hi {poster}," if poster else "Hi," 
            example = f"{hi} I'm interested in {service_name}. Could you help me?"
        else:
            example = "Tell us briefly what you need."
        form_defaults = {"service": service_name}
        return render_template("contact.html", form=form_defaults, message_placeholder=example, title="Contact")

    # Admin auth
    @app.route("/admin/login", methods=["GET", "POST"])
    def admin_login():
        if request.method == "POST":
            password = request.form.get("password", "")
            if check_password_hash(app.config["ADMIN_PASSWORD_HASH"], password):
                session["admin"] = True
                flash("Welcome back!", "success")
                return redirect(request.args.get("next") or url_for("admin_dashboard"))
            else:
                flash("Invalid password.", "error")
        return render_template("admin_login.html", title="Admin Login")

    @app.post("/admin/logout")
    @login_required
    def admin_logout():
        session.pop("admin", None)
        flash("Logged out.", "info")
        return redirect(url_for("admin_login"))

    # Change admin password
    @app.route("/admin/password", methods=["GET", "POST"])
    @login_required
    def admin_change_password():
        if request.method == "POST":
            current = request.form.get("current", "")
            new = request.form.get("new", "")
            confirm = request.form.get("confirm", "")
            if not check_password_hash(app.config["ADMIN_PASSWORD_HASH"], current):
                flash("Current password is incorrect.", "error")
            elif not new or len(new) < 6:
                flash("New password must be at least 6 characters.", "error")
            elif new != confirm:
                flash("Passwords do not match.", "error")
            else:
                new_hash = generate_password_hash(new)
                with closing(get_db()) as db:
                    cur = db.cursor()
                    cur.execute("UPDATE auth SET admin_password_hash = ? WHERE id = 1", (new_hash,))
                    db.commit()
                app.config["ADMIN_PASSWORD_HASH"] = new_hash
                flash("Admin password updated.", "success")
                return redirect(url_for("admin_dashboard"))
        return render_template("admin_change_password.html", title="Change Admin Password")

    # Provider auth
    @app.route("/provider/login", methods=["GET", "POST"])
    def provider_login():
        if request.method == "POST":
            password = request.form.get("password", "")
            if check_password_hash(app.config["PROVIDER_PASSWORD_HASH"], password):
                session["provider"] = True
                flash("Welcome, Provider!", "success")
                return redirect(request.args.get("next") or url_for("provider_dashboard"))
            else:
                flash("Invalid password.", "error")
        return render_template("provider_login.html", title="Provider Login")

    @app.post("/provider/logout")
    @provider_required
    def provider_logout():
        session.pop("provider", None)
        flash("Logged out.", "info")
        return redirect(url_for("provider_login"))

    # Change provider password
    @app.route("/provider/password", methods=["GET", "POST"])
    @provider_required
    def provider_change_password():
        if request.method == "POST":
            current = request.form.get("current", "")
            new = request.form.get("new", "")
            confirm = request.form.get("confirm", "")
            if not check_password_hash(app.config["PROVIDER_PASSWORD_HASH"], current):
                flash("Current password is incorrect.", "error")
            elif not new or len(new) < 6:
                flash("New password must be at least 6 characters.", "error")
            elif new != confirm:
                flash("Passwords do not match.", "error")
            else:
                new_hash = generate_password_hash(new)
                with closing(get_db()) as db:
                    cur = db.cursor()
                    cur.execute("UPDATE auth SET provider_password_hash = ? WHERE id = 1", (new_hash,))
                    db.commit()
                app.config["PROVIDER_PASSWORD_HASH"] = new_hash
                flash("Provider password updated.", "success")
                return redirect(url_for("provider_dashboard"))
        return render_template("provider_change_password.html", title="Change Provider Password")

    @app.route("/admin")
    @login_required
    def admin_dashboard():
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("SELECT COUNT(*) FROM services")
            services_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM zips")
            zips_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM leads")
            leads_count = cur.fetchone()[0]
        return render_template(
            "admin_dashboard.html",
            services_count=services_count,
            zips_count=zips_count,
            leads_count=leads_count,
            title="Admin",
        )

    # Provider dashboard (minimal)
    @app.route("/provider")
    @provider_required
    def provider_dashboard():
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("SELECT COUNT(*) FROM leads")
            leads_count = cur.fetchone()[0]
        return render_template("provider_dashboard.html", leads_count=leads_count, title="Provider")

    # Admin provider (formerly profile)
    @app.route("/provider/profile", methods=["GET", "POST"])
    @provider_required
    def provider_profile():
        if request.method == "POST":
            first_name = request.form.get("first_name", "").strip()
            business_name = request.form.get("business_name", "").strip()
            contact_email = request.form.get("contact_email", "").strip()
            phone = request.form.get("phone", "").strip()
            base_zip = request.form.get("base_zip", "").strip()
            address = request.form.get("address", "").strip()
            about = request.form.get("about", "").strip()
            profile_photo = request.form.get("profile_photo", "").strip()

            if base_zip and not re.fullmatch(r"\d{5}", base_zip):
                flash("Base ZIP must be 5 digits.", "error")
            else:
                with closing(get_db()) as db:
                    cur = db.cursor()
                    cur.execute(
                        """
                        UPDATE profile
                        SET first_name=?, business_name=?, contact_email=?, phone=?, base_zip=?, address=?, about=?, profile_photo=?
                        WHERE id = 1
                        """,
                        (first_name, business_name, contact_email, phone, base_zip, address, about, profile_photo),
                    )
                    db.commit()
                flash("Provider profile saved.", "success")
                return redirect(url_for("provider_profile"))

        # Load current values
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("SELECT first_name, business_name, contact_email, phone, base_zip, address, about, profile_photo FROM profile WHERE id = 1")
            row = cur.fetchone()
        # Count leads for display
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("SELECT COUNT(*) FROM leads")
            leads_count = cur.fetchone()[0]

        form = {
            "first_name": row["first_name"] if row else "",
            "business_name": row["business_name"] if row else "",
            "contact_email": row["contact_email"] if row else "",
            "phone": row["phone"] if row else "",
            "base_zip": row["base_zip"] if row else "",
            "address": row["address"] if row else "",
            "about": row["about"] if row else "",
            "profile_photo": row["profile_photo"] if row else "",
        }
        return render_template("provider_profile.html", form=form, title="Provider Profile", leads_count=leads_count)

    # Back-compat redirect
    # Back-compat redirects from admin to provider
    @app.route("/admin/profile")
    @login_required
    def admin_profile():
        return redirect(url_for("provider_profile"))

    @app.route("/admin/provider")
    @login_required
    def admin_provider():
        return redirect(url_for("provider_profile"))

    # Services CRUD
    @app.route("/admin/services")
    @login_required
    def admin_services():
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute(
                "SELECT id, title, description, price, active, created_at FROM services ORDER BY created_at DESC"
            )
            rows = cur.fetchall()
        return render_template("admin_services.html", services=rows, title="Manage Services")

    @app.route("/admin/services/new", methods=["GET", "POST"])
    @login_required
    def admin_service_new():
        if request.method == "POST":
            title = request.form.get("title", "").strip()
            description = request.form.get("description", "").strip()
            price = request.form.get("price", "").strip()
            posted_by = request.form.get("posted_by", "").strip()
            active = 1 if request.form.get("active") == "on" else 0

            if not title:
                flash("Title is required.", "error")
                return render_template("admin_service_form.html", form=request.form, mode="new", title="Add Service")

            with closing(get_db()) as db:
                cur = db.cursor()
                # Default poster to provider first_name, then business_name
                if not posted_by:
                    cur.execute("SELECT first_name, business_name FROM profile WHERE id=1")
                    r = cur.fetchone()
                    posted_by = (
                        (r["first_name"] if r and r["first_name"] else None)
                        or (r["business_name"] if r and r["business_name"] else None)
                        or "Provider"
                    )
                cur.execute(
                    """
                    INSERT INTO services (title, description, price, posted_by, active, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (title, description, price, posted_by, active, datetime.datetime.utcnow().isoformat()),
                )
                db.commit()
            flash("Service added.", "success")
            return redirect(url_for("admin_services"))

        return render_template("admin_service_form.html", form={}, mode="new", title="Add Service")

    @app.route("/admin/services/<int:service_id>/edit", methods=["GET", "POST"])
    @login_required
    def admin_service_edit(service_id):
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("SELECT id, title, description, price, posted_by, active FROM services WHERE id = ?", (service_id,))
            service = cur.fetchone()
            if not service:
                abort(404)

        if request.method == "POST":
            title = request.form.get("title", "").strip()
            description = request.form.get("description", "").strip()
            price = request.form.get("price", "").strip()
            posted_by = request.form.get("posted_by", "").strip()
            active = 1 if request.form.get("active") == "on" else 0

            if not title:
                flash("Title is required.", "error")
                return render_template(
                    "admin_service_form.html", form=request.form, mode="edit", service=service, title="Edit Service"
                )

            with closing(get_db()) as db:
                cur = db.cursor()
                # Default poster to provider first_name, then business_name
                if not posted_by:
                    cur.execute("SELECT first_name, business_name FROM profile WHERE id=1")
                    r = cur.fetchone()
                    posted_by = (
                        (r["first_name"] if r and r["first_name"] else None)
                        or (r["business_name"] if r and r["business_name"] else None)
                        or "Provider"
                    )
                cur.execute(
                    """
                    UPDATE services
                    SET title = ?, description = ?, price = ?, posted_by = ?, active = ?
                    WHERE id = ?
                """,
                    (title, description, price, posted_by, active, service_id),
                )
                db.commit()
            flash("Service updated.", "success")
            return redirect(url_for("admin_services"))

        return render_template(
            "admin_service_form.html",
            form=dict(
                title=service["title"],
                description=service["description"],
                price=service["price"],
                posted_by=service["posted_by"],
                active=bool(service["active"]),
            ),
            mode="edit",
            service=service,
            title="Edit Service",
        )

    @app.post("/admin/services/<int:service_id>/delete")
    @login_required
    def admin_service_delete(service_id):
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("DELETE FROM services WHERE id = ?", (service_id,))
            db.commit()
        flash("Service deleted.", "info")
        return redirect(url_for("admin_services"))

    # ZIP management
    @app.route("/admin/zips", methods=["GET", "POST"])
    @login_required
    def admin_zips():
        if request.method == "POST":
            zip_code = request.form.get("zip", "").strip()
            radius = request.form.get("radius", "20").strip()
            try:
                radius_val = int(radius)
            except ValueError:
                radius_val = 20
            if radius_val not in (20, 40, 60, 80):
                radius_val = 20
            if not re.fullmatch(r"\d{5}", zip_code):
                flash("Enter a valid 5-digit ZIP.", "error")
            else:
                try:
                    with closing(get_db()) as db:
                        cur = db.cursor()
                        # Insert if new and set radius
                        cur.execute("INSERT OR IGNORE INTO zips (zip, radius_miles) VALUES (?, ?)", (zip_code, radius_val))
                        # Ensure radius updated
                        cur.execute("UPDATE zips SET radius_miles = ? WHERE zip = ?", (radius_val, zip_code))
                        db.commit()
                    flash(f"ZIP {zip_code} added/updated with {radius_val} miles.", "success")
                except Exception as e:
                    flash(f"Error adding ZIP: {e}", "error")

        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("SELECT zip, radius_miles FROM zips ORDER BY zip")
            zips = [(row["zip"], row["radius_miles"]) for row in cur.fetchall()]
        return render_template("admin_zips.html", zips=zips, title="Manage ZIPs")

    @app.post("/admin/zips/<zip_code>/delete")
    @login_required
    def admin_zip_delete(zip_code):
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("DELETE FROM zips WHERE zip = ?", (zip_code,))
            db.commit()
        flash(f"ZIP {zip_code} removed.", "info")
        return redirect(url_for("admin_zips"))

    @app.post("/admin/zips/<zip_code>/update")
    @login_required
    def admin_zip_update(zip_code):
        radius = request.form.get("radius", "20").strip()
        try:
            radius_val = int(radius)
        except ValueError:
            radius_val = 20
        if radius_val not in (20, 40, 60, 80):
            radius_val = 20
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("UPDATE zips SET radius_miles = ? WHERE zip = ?", (radius_val, zip_code))
            db.commit()
        flash(f"ZIP {zip_code} radius updated to {radius_val} miles.", "success")
        return redirect(url_for("admin_zips"))

    # Leads
    # Provider leads
    @app.route("/provider/leads")
    @provider_required
    def provider_leads():
        filter_status = request.args.get('filter')
        
        with closing(get_db()) as db:
            cur = db.cursor()
            
            # Build query based on filter
            if filter_status == 'completed':
                query = "SELECT id, name, email, phone, zip, address, message, created_at, status, recurring FROM leads WHERE status = 'completed' ORDER BY created_at DESC"
                page_title = "Completed Leads"
            elif filter_status == 'rejected':
                query = "SELECT id, name, email, phone, zip, address, message, created_at, status, recurring FROM leads WHERE status = 'rejected' ORDER BY created_at DESC"
                page_title = "Rejected Leads"
            elif filter_status == 'subscribers':
                query = "SELECT id, name, email, phone, zip, address, message, created_at, status, recurring FROM leads WHERE recurring = 1 ORDER BY created_at DESC"
                page_title = "Weekly Subscribers"
            else:
                # Default: Get active/pending leads only (not completed or rejected)
                query = "SELECT id, name, email, phone, zip, address, message, created_at, status, recurring FROM leads WHERE status IS NULL OR status NOT IN ('completed', 'rejected') ORDER BY created_at DESC"
                page_title = "Active Leads"
            
            cur.execute(query)
            leads = cur.fetchall()
            
            # Calculate statistics
            cur.execute("SELECT COUNT(*) FROM leads WHERE status = 'completed'")
            completed_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM leads WHERE status = 'rejected'")
            rejected_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM leads WHERE recurring = 1")
            subscribers_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM blocked_addresses")
            blocked_count = cur.fetchone()[0]
            
        stats = {
            'completed': completed_count,
            'rejected': rejected_count,
            'subscribers': subscribers_count,
            'blocked': blocked_count
        }
        
        return render_template("provider_leads.html", leads=leads, stats=stats, title="Leads", 
                             filter_status=filter_status, page_title=page_title)

    @app.post("/provider/leads/<int:lead_id>/action")
    @provider_required
    def provider_lead_action(lead_id):
        action = request.form.get("action")
        
        with closing(get_db()) as db:
            cur = db.cursor()
            
            if action == "complete":
                # Check if this is a recurring client
                cur.execute("SELECT recurring, name, email, phone, zip, address, message FROM leads WHERE id = ?", (lead_id,))
                lead_info = cur.fetchone()
                
                cur.execute("UPDATE leads SET status = 'completed' WHERE id = ?", (lead_id,))
                
                # If recurring, automatically schedule next week
                if lead_info and lead_info["recurring"]:
                    next_week = (datetime.datetime.now() + datetime.timedelta(days=7)).isoformat()
                    cur.execute(
                        """
                        INSERT INTO leads (name, email, phone, zip, address, message, created_at, status, recurring)
                        VALUES (?, ?, ?, ?, ?, ?, ?, 'scheduled', 1)
                        """,
                        (
                            lead_info["name"],
                            lead_info["email"],
                            lead_info["phone"],
                            lead_info["zip"],
                            lead_info["address"],
                            f"[RECURRING WEEKLY] {lead_info['message'] or ''}".strip(),
                            next_week,
                        )
                    )
                    flash(f"Lead completed and next week scheduled for {lead_info['name']}.", "success")
                else:
                    flash("Lead marked as completed.", "success")
                    
            elif action == "reject":
                cur.execute("UPDATE leads SET status = 'rejected' WHERE id = ?", (lead_id,))
                flash("Lead rejected.", "info")
            elif action == "tomorrow":
                tomorrow = (datetime.datetime.now() + datetime.timedelta(days=1)).date().isoformat()
                cur.execute("UPDATE leads SET status = 'scheduled', follow_up_date = ? WHERE id = ?", (tomorrow, lead_id))
                flash("Lead scheduled for tomorrow.", "success")
            elif action == "weekly":
                # Toggle weekly status
                cur.execute("SELECT recurring FROM leads WHERE id = ?", (lead_id,))
                current = cur.fetchone()
                new_recurring = 0 if current and current["recurring"] else 1
                cur.execute("UPDATE leads SET recurring = ? WHERE id = ?", (new_recurring, lead_id))
                flash(f"Lead {'added to' if new_recurring else 'removed from'} weekly schedule.", "success")
            
            db.commit()
        
        return redirect(url_for("provider_leads"))

    @app.route("/provider/blocked-addresses", methods=["GET", "POST"])
    @provider_required
    def provider_blocked_addresses():
        if request.method == "POST":
            address = request.form.get("address", "").strip()
            zip_code = request.form.get("zip", "").strip()
            reason = request.form.get("reason", "").strip()
            
            if address:
                with closing(get_db()) as db:
                    cur = db.cursor()
                    cur.execute(
                        "INSERT INTO blocked_addresses (address, zip, reason, created_at) VALUES (?, ?, ?, ?)",
                        (address, zip_code, reason, datetime.datetime.utcnow().isoformat())
                    )
                    db.commit()
                flash("Address blocked successfully.", "success")
                return redirect(url_for("provider_blocked_addresses"))
            else:
                flash("Address is required.", "error")
        
        # Get blocked addresses
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("SELECT id, address, zip, reason, created_at FROM blocked_addresses ORDER BY created_at DESC")
            blocked = cur.fetchall()
        
        return render_template("provider_blocked_addresses.html", blocked=blocked, title="Blocked Addresses")

    @app.post("/provider/blocked-addresses/<int:block_id>/delete")
    @provider_required
    def provider_unblock_address(block_id):
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("DELETE FROM blocked_addresses WHERE id = ?", (block_id,))
            db.commit()
        flash("Address unblocked.", "info")
        return redirect(url_for("provider_blocked_addresses"))

    @app.route("/provider/sorted-leads")
    @provider_required
    def provider_sorted_leads():
        with closing(get_db()) as db:
            cur = db.cursor()
            
            # Get sorted (completed/rejected) leads
            cur.execute(
                """
                SELECT id, name, email, phone, zip, address, message, created_at, status, recurring, follow_up_date
                FROM leads 
                WHERE status IN ('completed', 'rejected')
                ORDER BY created_at DESC
            """
            )
            sorted_leads = cur.fetchall()
            
            # Get pending/new leads count for reference
            cur.execute("SELECT COUNT(*) FROM leads WHERE status IS NULL OR status = 'new'")
            pending_count = cur.fetchone()[0]
            
            # Calculate statistics for sorted leads
            cur.execute("SELECT COUNT(*) FROM leads WHERE status = 'completed'")
            completed_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM leads WHERE status = 'rejected'")
            rejected_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM leads WHERE recurring = 1 AND status = 'completed'")
            recurring_completed = cur.fetchone()[0]
        
        stats = {
            'pending': pending_count,
            'completed': completed_count,
            'rejected': rejected_count,
            'recurring_completed': recurring_completed
        }
        
        return render_template("provider_sorted_leads.html", leads=sorted_leads, stats=stats, title="Sorted Leads")

    @app.post("/provider/leads/<int:lead_id>/schedule-next")
    @provider_required
    def provider_schedule_next_week(lead_id):
        with closing(get_db()) as db:
            cur = db.cursor()
            
            # Get the original lead info
            cur.execute("SELECT name, email, phone, zip, address, message FROM leads WHERE id = ?", (lead_id,))
            original = cur.fetchone()
            
            if original:
                # Create next week's lead entry
                next_week = (datetime.datetime.now() + datetime.timedelta(days=7)).isoformat()
                cur.execute(
                    """
                    INSERT INTO leads (name, email, phone, zip, address, message, created_at, status, recurring)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'scheduled', 1)
                    """,
                    (
                        original["name"],
                        original["email"], 
                        original["phone"],
                        original["zip"],
                        original["address"],
                        f"[RECURRING WEEKLY] {original['message'] or ''}".strip(),
                        next_week,
                    )
                )
                db.commit()
                flash(f"Next week's appointment scheduled for {original['name']}.", "success")
            else:
                flash("Lead not found.", "error")
                
        return redirect(url_for("provider_sorted_leads"))

    @app.post("/provider/leads/<int:lead_id>/block-address")
    @provider_required
    def provider_block_lead_address(lead_id):
        with closing(get_db()) as db:
            cur = db.cursor()
            
            # Get the lead's address info
            cur.execute("SELECT name, address, zip FROM leads WHERE id = ?", (lead_id,))
            lead = cur.fetchone()
            
            if lead and lead["address"] and lead["address"].strip():
                # Add to blocked addresses
                cur.execute(
                    "INSERT INTO blocked_addresses (address, zip, reason, created_at) VALUES (?, ?, ?, ?)",
                    (
                        lead["address"],
                        lead["zip"],
                        f"Blocked from lead: {lead['name']}",
                        datetime.datetime.utcnow().isoformat()
                    )
                )
                db.commit()
                flash(f"Address '{lead['address']}' has been blocked.", "success")
            else:
                flash("No address found for this lead to block.", "error")
                
        return redirect(url_for("provider_leads"))

    @app.get("/provider/leads/export.csv")
    @provider_required
    def provider_leads_export_csv():
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute(
                "SELECT name, email, phone, zip, address, message, created_at FROM leads ORDER BY created_at DESC"
            )
            rows = cur.fetchall()

        si = StringIO()
        writer = csv.writer(si)
        writer.writerow(["name", "email", "phone", "zip", "address", "message", "created_at"])
        for r in rows:
            writer.writerow([r["name"], r["email"], r["phone"], r["zip"], r["address"], r["message"], r["created_at"]])
        data = si.getvalue()
        return app.response_class(
            data,
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=leads.csv"},
        )

    return app


app = create_app()


if __name__ == "__main__":
    # Convenient dev run: python app.py
    app.run(host="127.0.0.1", port=5000, debug=True)
