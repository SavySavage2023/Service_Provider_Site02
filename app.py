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
              provider_id INTEGER NOT NULL DEFAULT 0,
              active INTEGER NOT NULL DEFAULT 1,
              is_certified INTEGER DEFAULT 0,
              certification_proof TEXT,
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
              provider_id INTEGER NOT NULL DEFAULT 0,
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
              about TEXT,
              profile_photo TEXT
            );

            CREATE TABLE IF NOT EXISTS blocked_addresses (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              address TEXT NOT NULL,
              zip TEXT,
              reason TEXT,
              created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS providers (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              email TEXT UNIQUE NOT NULL,
              password_hash TEXT NOT NULL,
              first_name TEXT,
              business_name TEXT,
              phone TEXT,
              base_zip TEXT,
              address TEXT,
              about TEXT,
              profile_photo TEXT,
              active INTEGER NOT NULL DEFAULT 1,
              created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS products (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              title TEXT NOT NULL,
              description TEXT,
              price TEXT,
              provider_id INTEGER NOT NULL DEFAULT 0,
              active INTEGER NOT NULL DEFAULT 1,
              created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS provider_zips (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              provider_id INTEGER NOT NULL,
              zip_code TEXT NOT NULL,
              radius_miles INTEGER DEFAULT 10,
              created_at TEXT NOT NULL,
              UNIQUE(provider_id, zip_code)
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
        if "provider_id" not in scols:
            cur.execute("ALTER TABLE services ADD COLUMN provider_id INTEGER DEFAULT 0")
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
        if "provider_id" not in lcols:
            cur.execute("ALTER TABLE leads ADD COLUMN provider_id INTEGER DEFAULT 0")
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
        # Only inject current year, profile is loaded per-route as needed
        return {
            "current_year": datetime.datetime.now().year,
        }

    def login_required(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            # Temporarily disable login requirement - allow all access
            # if not session.get("admin"):
            #     return redirect(url_for("admin_login", next=request.path))
            return f(*args, **kwargs)
        return wrapper

    def provider_required(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            # Temporarily disable provider requirement - allow all access
            # if not session.get("provider"):
            #     return redirect(url_for("provider_login", next=request.path))
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

    @app.context_processor
    def inject_global_context():
        """Make profile and other common data available to all templates"""
        try:
            with closing(get_db()) as db:
                cur = db.cursor()
                cur.execute("SELECT business_name, first_name FROM profile WHERE id = 1")
                profile_row = cur.fetchone()
                if profile_row:
                    global_profile = {
                        "business_name": profile_row["business_name"] or "Your Service Provider",
                        "first_name": profile_row["first_name"] or ""
                    }
                else:
                    global_profile = {"business_name": "Your Service Provider", "first_name": ""}
        except:
            global_profile = {"business_name": "Your Service Provider", "first_name": ""}
        
        return {
            'profile': global_profile,
            'current_year': datetime.datetime.now().year
        }

    @app.route("/")
    def home():
        # Determine which provider profile to show
        # If a provider is logged in, show their profile; otherwise show carousel for visitors
        if session.get('provider'):
            # A provider is logged in
            featured_provider_id = session.get('provider_id', 0)
            show_carousel = False
        else:
            # No one logged in - show carousel for visitors
            featured_provider_id = 0  # Default fallback
            show_carousel = True
        
        with closing(get_db()) as db:
            cur = db.cursor()
            
            # Initialize variables
            services = []
            products = []
            
            # Get all providers for carousel if visitor, or specific provider if logged in
            all_providers = []
            all_provider_services = {}
            all_provider_products = {}
            if show_carousel:
                # Get admin/mom's profile first - only show if there's meaningful data or services
                cur.execute("SELECT first_name, business_name, contact_email, phone, base_zip, address, about, profile_photo FROM profile WHERE id = 1")
                admin_row = cur.fetchone()
                
                # Check if admin has any active services
                cur.execute("SELECT COUNT(*) FROM services WHERE provider_id = 0 AND active = 1")
                admin_service_count = cur.fetchone()[0]
                
                # Only add admin profile if there's meaningful profile data or active services
                if (admin_row and admin_service_count > 0) or (admin_row and (admin_row["business_name"] or admin_row["first_name"] or admin_row["about"])):
                    admin_provider = {
                        "id": 0,
                        "first_name": admin_row["first_name"] or "",
                        "business_name": admin_row["business_name"] or "Your Service Provider",
                        "contact_email": admin_row["contact_email"] or "",
                        "phone": admin_row["phone"] or "",
                        "base_zip": admin_row["base_zip"] or "",
                        "address": admin_row["address"] or "",
                        "about": admin_row["about"] or "",
                        "profile_photo": admin_row["profile_photo"] or ""
                    }
                    all_providers.append(admin_provider)
                    
                    # Get admin's services
                    cur.execute("SELECT id, title, description, price, posted_by FROM services WHERE provider_id = 0 AND active = 1 ORDER BY created_at DESC LIMIT 6")
                    admin_services = cur.fetchall()
                    # Convert Row objects to dictionaries
                    all_provider_services[0] = [dict(row) for row in admin_services]
                    
                    # Get admin's products
                    cur.execute("SELECT id, title, description, price FROM products WHERE provider_id = 0 AND active = 1 ORDER BY created_at DESC LIMIT 6")
                    admin_products = cur.fetchall()
                    # Convert Row objects to dictionaries
                    all_provider_products[0] = [dict(row) for row in admin_products]
                
                # Get all active registered providers
                cur.execute("SELECT id, first_name, business_name, phone, base_zip, address, about, profile_photo FROM providers WHERE active = 1 ORDER BY business_name")
                provider_rows = cur.fetchall()
                for provider_row in provider_rows:
                    first_name = provider_row["first_name"] or ""
                    business_name = provider_row["business_name"] or ""
                    provider_id = provider_row["id"]
                    
                    # Handle business name for providers
                    if business_name in ["", "Individual", "Service Provider"] or not business_name.strip():
                        display_business_name = f"{first_name.capitalize()}'s Services" if first_name else "Service Provider"
                    else:
                        display_business_name = business_name
                    
                    provider_data = {
                        "id": provider_id,
                        "first_name": first_name.capitalize() if first_name else "",
                        "business_name": display_business_name,
                        "contact_email": "",  # Providers don't expose email publicly
                        "phone": provider_row["phone"] or "",
                        "base_zip": provider_row["base_zip"] or "",
                        "address": provider_row["address"] or "",
                        "about": provider_row["about"] or "",
                        "profile_photo": provider_row["profile_photo"] or ""
                    }
                    all_providers.append(provider_data)
                    
                    # Get this provider's services
                    cur.execute("SELECT id, title, description, price, posted_by FROM services WHERE provider_id = ? AND active = 1 ORDER BY created_at DESC LIMIT 6", (provider_id,))
                    provider_services = cur.fetchall()
                    # Convert Row objects to dictionaries
                    all_provider_services[provider_id] = [dict(row) for row in provider_services]
                    
                    # Get this provider's products
                    cur.execute("SELECT id, title, description, price FROM products WHERE provider_id = ? AND active = 1 ORDER BY created_at DESC LIMIT 6", (provider_id,))
                    provider_products = cur.fetchall()
                    # Convert Row objects to dictionaries
                    all_provider_products[provider_id] = [dict(row) for row in provider_products]
                
                # Use first provider as default profile for template compatibility
                profile = all_providers[0] if all_providers else None
                # Use first provider's services as default
                services = all_provider_services.get(0, []) if all_provider_services else []
                # Use first provider's products as default
                products = all_provider_products.get(0, []) if all_provider_products else []
            else:
                # Single provider view (logged in user)
                if featured_provider_id == 0:
                    # Use the main profile table for admin/mom
                    cur.execute("SELECT first_name, business_name, contact_email, phone, base_zip, address, about, profile_photo FROM profile WHERE id = 1")
                    profile_row = cur.fetchone()
                    profile = {
                        "first_name": profile_row["first_name"] if profile_row else "",
                        "business_name": profile_row["business_name"] if profile_row else "Your Service Provider",
                        "contact_email": profile_row["contact_email"] if profile_row else "",
                        "phone": profile_row["phone"] if profile_row else "",
                        "base_zip": profile_row["base_zip"] if profile_row else "",
                        "address": profile_row["address"] if profile_row else "",
                        "about": profile_row["about"] if profile_row else "",
                        "profile_photo": profile_row["profile_photo"] if profile_row else ""
                    }
                else:
                    # Use the providers table for registered providers
                    cur.execute("SELECT first_name, business_name, phone, base_zip, address, about, profile_photo FROM providers WHERE id = ? AND active = 1", (featured_provider_id,))
                    provider_row = cur.fetchone()
                    if provider_row:
                        # Handle generic business names
                        business_name = provider_row["business_name"] or ""
                        first_name = provider_row["first_name"] or ""
                        
                        if business_name in ["", "Individual", "Service Provider"] or not business_name.strip():
                            display_name = first_name.capitalize() if first_name else "Provider"
                        else:
                            display_name = business_name
                            
                        profile = {
                            "first_name": first_name.capitalize() if first_name else "",
                            "business_name": display_name,
                            "contact_email": "",  # Providers don't expose email publicly
                            "phone": provider_row["phone"] or "",
                            "base_zip": provider_row["base_zip"] or "",
                            "address": provider_row["address"] or "",
                            "about": provider_row["about"] or "",
                            "profile_photo": provider_row["profile_photo"] or ""
                        }
                    else:
                        # Fallback to admin profile
                        profile = {"business_name": "Service Provider", "first_name": "", "contact_email": "", "phone": "", "base_zip": "", "address": "", "about": "", "profile_photo": ""}
            
            # Get featured services from the featured provider (for single provider view)
            if not show_carousel:
                cur.execute(
                    "SELECT id, title, description, price, posted_by FROM services WHERE provider_id = ? AND active = 1 ORDER BY created_at DESC LIMIT 6",
                    (featured_provider_id,)
                )
                services_raw = cur.fetchall()
                services = [dict(row) for row in services_raw]
                
                # Get featured products from the featured provider (for single provider view)
                cur.execute(
                    "SELECT id, title, description, price FROM products WHERE provider_id = ? AND active = 1 ORDER BY created_at DESC LIMIT 6",
                    (featured_provider_id,)
                )
                products_raw = cur.fetchall()
                products = [dict(row) for row in products_raw]
        
        return render_template("index.html", 
                             services=services, 
                             products=products, 
                             profile=profile, 
                             featured_provider_id=featured_provider_id, 
                             show_carousel=show_carousel, 
                             all_providers=all_providers, 
                             all_provider_services=all_provider_services, 
                             all_provider_products=all_provider_products, 
                             title="Home")

    @app.route("/services")
    def services():
        # Get filter parameters
        search_query = request.args.get('q', '').strip()
        filter_type = request.args.get('filter', 'all')  # all, certified, best_price
        zip_param = (request.args.get('zip', '') or '').strip()
        
        with closing(get_db()) as db:
            cur = db.cursor()
            
            # Build the SQL query based on filters
            base_query = (
                "SELECT s.id, s.title, s.description, s.price, s.posted_by, s.provider_id, s.is_certified, "
                "s.certification_proof, p.business_name as provider_business_name, "
                "p.base_zip as provider_base_zip "
                "FROM services s "
                "LEFT JOIN providers p ON s.provider_id = p.id "
                "WHERE s.active = 1"
            )
            params = []
            
            # Add search query filter
            if search_query:
                base_query += " AND (LOWER(s.title) LIKE LOWER(?) OR LOWER(s.description) LIKE LOWER(?) OR LOWER(s.posted_by) LIKE LOWER(?) OR LOWER(p.business_name) LIKE LOWER(?) OR LOWER(p.first_name) LIKE LOWER(?))"
                search_param = f"%{search_query}%"
                params.extend([search_param, search_param, search_param, search_param, search_param])
            
            # Add filter type
            if filter_type == 'certified':
                # Filter by service-level certification
                base_query += " AND s.is_certified = 1"
            elif filter_type == 'best_price':
                # Order by lowest price (assuming numeric price ranges)
                base_query += " ORDER BY CASE WHEN s.price LIKE '$%' THEN CAST(SUBSTR(s.price, 2, INSTR(s.price || '-', '-') - 2) AS INTEGER) ELSE 999999 END ASC, s.created_at DESC"
            elif filter_type == 'by_provider':
                # Group by provider - order by provider business name, then service title
                base_query += " ORDER BY COALESCE(p.business_name, 'Independent Contributor'), s.title ASC"
            else:
                base_query += " ORDER BY s.created_at DESC"
            
            # Execute query
            cur.execute(base_query, params)
            services_raw = cur.fetchall()

            # If a ZIP filter is provided, compute allowed providers and filter services
            allowed_provider_ids = None
            applied_zip = None
            if re.fullmatch(r"\d{5}", zip_param or ""):
                applied_zip = zip_param
                allowed_provider_ids = set()

                # Include admin provider (id=0) if profile base_zip matches/near
                cur.execute("SELECT base_zip FROM profile WHERE id = 1")
                profile_row = cur.fetchone()
                admin_base_zip = (profile_row["base_zip"] if profile_row else None) or ""

                def within_radius(z1: str, z2: str, miles: float) -> bool:
                    if not z1 or not z2 or not re.fullmatch(r"\d{5}", z1) or not re.fullmatch(r"\d{5}", z2):
                        return False
                    if geo_dist is None:
                        return z1 == z2
                    try:
                        km = geo_dist.query_postal_code(z1, z2)
                        if km is None or (isinstance(km, float) and (km != km)):
                            return False
                        return (float(km) * 0.621371) <= miles
                    except Exception:
                        return False

                # Preload provider service areas
                cur.execute("SELECT provider_id, zip_code, radius_miles FROM provider_zips")
                provider_area_rows = cur.fetchall()
                areas_by_provider = {}
                for r in provider_area_rows:
                    areas_by_provider.setdefault(r["provider_id"], []).append((r["zip_code"], r["radius_miles"]))

                # Compute allowed providers from providers table
                cur.execute("SELECT id, base_zip FROM providers WHERE active = 1")
                prov_rows = cur.fetchall()

                # Admin provider id=0
                if admin_base_zip and (admin_base_zip == applied_zip or within_radius(admin_base_zip, applied_zip, 20)):
                    allowed_provider_ids.add(0)

                for pr in prov_rows:
                    pid = pr["id"]
                    base_zip = (pr["base_zip"] or "").strip()
                    match = False
                    # Base ZIP check (20 miles default radius)
                    if base_zip and (base_zip == applied_zip or within_radius(base_zip, applied_zip, 20)):
                        match = True
                    # Service area ZIPs
                    if not match:
                        for (z, rm) in areas_by_provider.get(pid, []) or []:
                            if z == applied_zip or within_radius(z, applied_zip, float(rm or 10)):
                                match = True
                                break
                    if match:
                        allowed_provider_ids.add(pid)

            # Convert Row objects to dictionaries for JSON serialization
            services = []
            for service in services_raw:
                # Apply ZIP filtering if present
                if allowed_provider_ids is not None:
                    # service['provider_id'] may be 0 for admin
                    if service["provider_id"] not in allowed_provider_ids:
                        continue
                services.append({
                    'id': service['id'],
                    'title': service['title'],
                    'description': service['description'],
                    'price': service['price'],
                    'posted_by': service['posted_by'],
                    'provider_id': service['provider_id'],
                    'is_certified': service['is_certified'],
                    'certification_proof': service['certification_proof'],
                    'provider_business_name': service['provider_business_name']
                })
            
            # Get profile info for the template (needed for fallback poster names)
            cur.execute("SELECT first_name, business_name FROM profile WHERE id = 1")
            profile_row = cur.fetchone()
            profile = {
                "first_name": profile_row["first_name"] if profile_row else "",
                "business_name": profile_row["business_name"] if profile_row else ""
            }
            
        return render_template(
            "services.html",
            services=services,
            profile=profile,
            search_query=search_query,
            filter_type=filter_type,
            applied_zip=(applied_zip if 'applied_zip' in locals() else (zip_param if zip_param else None)),
            title="All Services",
        )

    @app.route("/provider/<int:provider_id>/services")
    def provider_services(provider_id):
        with closing(get_db()) as db:
            cur = db.cursor()
            
            # Get services for this specific provider
            cur.execute(
                "SELECT id, title, description, price, posted_by FROM services WHERE provider_id = ? AND active = 1 ORDER BY created_at DESC",
                (provider_id,)
            )
            services_raw = cur.fetchall()
            # Convert Row objects to dictionaries
            services = [dict(row) for row in services_raw]
            
            # Get provider info
            if provider_id == 0:
                cur.execute("SELECT first_name, business_name FROM profile WHERE id = 1")
                profile_row = cur.fetchone()
                provider_name = profile_row["business_name"] if profile_row else "Services"
            else:
                cur.execute("SELECT first_name, business_name FROM providers WHERE id = ? AND active = 1", (provider_id,))
                provider_row = cur.fetchone()
                provider_name = provider_row["business_name"] if provider_row else "Provider Services"
            
            profile = {
                "first_name": profile_row["first_name"] if 'profile_row' in locals() and profile_row else (provider_row["first_name"] if 'provider_row' in locals() and provider_row else ""),
                "business_name": provider_name
            }
            
        return render_template("services.html", services=services, profile=profile, title=f"{provider_name} - Services")

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
            
            # Determine which provider should get this lead
            provider_id = 0  # Default to admin/mom's profile
            if svc:
                # Try to find provider who offers this service
                with closing(get_db()) as db_temp:
                    cur_temp = db_temp.cursor()
                    cur_temp.execute("SELECT posted_by FROM services WHERE title = ? AND active = 1", (svc,))
                    service_row = cur_temp.fetchone()
                    if service_row and service_row["posted_by"]:
                        # Find provider by business name or first name
                        cur_temp.execute(
                            "SELECT id FROM providers WHERE business_name = ? OR first_name = ? AND active = 1", 
                            (service_row["posted_by"], service_row["posted_by"])
                        )
                        provider_row = cur_temp.fetchone()
                        if provider_row:
                            provider_id = provider_row["id"]

            with closing(get_db()) as db:
                cur = db.cursor()
                cur.execute(
                    """
                    INSERT INTO leads (name, email, phone, zip, address, message, created_at, provider_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        name,
                        email,
                        phone,
                        zip_code,
                        address,
                        message,
                        datetime.datetime.utcnow().isoformat(),
                        provider_id,
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
            email = request.form.get("email", "").strip().lower()
            password = request.form.get("password", "")
            
            if email and password:
                # Check database providers first
                with closing(get_db()) as db:
                    cur = db.cursor()
                    cur.execute("SELECT id, password_hash, first_name, business_name FROM providers WHERE email = ? AND active = 1", (email,))
                    provider = cur.fetchone()
                    
                    if provider and check_password_hash(provider["password_hash"], password):
                        session["provider"] = True
                        session["provider_id"] = provider["id"]
                        name = provider["first_name"] or provider["business_name"]
                        flash(f"Welcome back, {name}!", "success")
                        return redirect(request.args.get("next") or url_for("provider_dashboard"))
            
            # Fallback to original admin provider login
            if check_password_hash(app.config["PROVIDER_PASSWORD_HASH"], password):
                session["provider"] = True
                session["provider_id"] = 0  # Admin provider
                flash("Welcome, Provider!", "success")
                return redirect(request.args.get("next") or url_for("provider_dashboard"))
            
            flash("Invalid email or password.", "error")
        return render_template("provider_login.html", title="Provider Login")

    @app.route("/provider/register", methods=["GET", "POST"])
    def provider_register():
        if request.method == "POST":
            email = request.form.get("email", "").strip().lower()
            password = request.form.get("password", "").strip()
            confirm_password = request.form.get("confirm_password", "").strip()
            first_name = request.form.get("first_name", "").strip()
            business_name = request.form.get("business_name", "").strip()
            
            # Validation
            if not email or "@" not in email:
                flash("Please enter a valid email address.", "error")
                return render_template("provider_register.html", form=request.form, title="Join as Provider")
            
            if not password or len(password) < 6:
                flash("Password must be at least 6 characters.", "error")
                return render_template("provider_register.html", form=request.form, title="Join as Provider")
            
            if password != confirm_password:
                flash("Passwords do not match.", "error")
                return render_template("provider_register.html", form=request.form, title="Join as Provider")
            
            # Set default business name if not provided
            if not business_name:
                business_name = "Independent Contributor"
            
            # Check if email already exists
            with closing(get_db()) as db:
                cur = db.cursor()
                cur.execute("SELECT id FROM providers WHERE email = ?", (email,))
                if cur.fetchone():
                    flash("An account with this email already exists.", "error")
                    return render_template("provider_register.html", form=request.form, title="Join as Provider")
                
                # Create new provider
                password_hash = generate_password_hash(password)
                cur.execute(
                    """
                    INSERT INTO providers (email, password_hash, first_name, business_name, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (email, password_hash, first_name, business_name, datetime.datetime.utcnow().isoformat())
                )
                db.commit()
                provider_id = cur.lastrowid
            
            # Auto-login after registration
            session["provider"] = True
            session["provider_id"] = provider_id
            flash(f"Welcome {first_name or business_name}! Your provider account has been created.", "success")
            return redirect(url_for("provider_dashboard"))
        
        return render_template("provider_register.html", form={}, title="Join as Provider")

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

    @app.route("/admin/settings", methods=["GET", "POST"])
    @login_required
    def admin_settings():
        if request.method == "POST":
            featured_provider_id = request.form.get("featured_provider_id", "0")
            flash(f"Homepage will now feature the selected provider.", "success")
            return redirect(url_for("admin_settings"))
        
        # Get all active providers for selection
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("SELECT id, business_name, first_name FROM providers WHERE active = 1 ORDER BY business_name")
            providers = cur.fetchall()
        
        return render_template("admin_settings.html", providers=providers, title="Admin Settings")

    @app.route("/admin/analytics")
    @login_required
    def admin_analytics():
        with closing(get_db()) as db:
            cur = db.cursor()
            
            # Get service statistics with lead counts
            cur.execute("""
                SELECT 
                    s.id,
                    s.title,
                    s.price,
                    s.posted_by,
                    s.provider_id,
                    COUNT(l.id) as total_leads,
                    COUNT(CASE WHEN l.created_at >= date('now', '-7 days') THEN 1 END) as leads_this_week,
                    COUNT(CASE WHEN l.created_at >= date('now', '-30 days') THEN 1 END) as leads_this_month
                FROM services s
                LEFT JOIN leads l ON l.message LIKE '%' || s.title || '%'
                WHERE s.active = 1
                GROUP BY s.id, s.title, s.price, s.posted_by, s.provider_id
                ORDER BY total_leads DESC, s.title
            """)
            service_stats = cur.fetchall()
            
            # Get overall platform statistics
            cur.execute("SELECT COUNT(*) FROM leads WHERE created_at >= date('now', '-7 days')")
            total_leads_week = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM leads WHERE created_at >= date('now', '-30 days')")
            total_leads_month = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM leads")
            total_leads_all = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM services WHERE active = 1")
            total_services = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM providers WHERE active = 1")
            total_providers = cur.fetchone()[0]
            
            # Get provider performance
            cur.execute("""
                SELECT 
                    p.business_name,
                    p.first_name,
                    COUNT(s.id) as service_count,
                    COUNT(l.id) as total_leads
                FROM providers p
                LEFT JOIN services s ON s.provider_id = p.id AND s.active = 1
                LEFT JOIN leads l ON l.message LIKE '%' || s.title || '%'
                WHERE p.active = 1
                GROUP BY p.id, p.business_name, p.first_name
                ORDER BY total_leads DESC
            """)
            provider_stats = cur.fetchall()
            
        return render_template("admin_analytics.html", 
                             service_stats=service_stats,
                             total_leads_week=total_leads_week,
                             total_leads_month=total_leads_month,
                             total_leads_all=total_leads_all,
                             total_services=total_services,
                             total_providers=total_providers,
                             provider_stats=provider_stats,
                             title="Analytics Dashboard")

    # Provider dashboard (minimal)
    @app.route("/provider")
    @provider_required
    def provider_dashboard():
        current_provider_id = session.get('provider_id', 0)
        
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("SELECT COUNT(*) FROM leads WHERE provider_id = ?", (current_provider_id,))
            leads_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM services WHERE provider_id = ?", (current_provider_id,))
            services_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM products WHERE provider_id = ?", (current_provider_id,))
            products_count = cur.fetchone()[0]
            
            # Calculate total views (placeholder for now)
            total_views = (services_count + products_count) * 15  # Mock data
            
        return render_template("provider_dashboard.html", 
                             leads_count=leads_count, 
                             services_count=services_count,
                             products_count=products_count,
                             total_views=total_views,
                             title="Provider Dashboard")

    @app.route("/provider/manage-services")
    @provider_required
    def provider_manage_services():
        current_provider_id = session.get('provider_id', 0)
        
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute(
                "SELECT id, title, description, price, active, is_certified, certification_proof, created_at FROM services WHERE provider_id = ? ORDER BY created_at DESC",
                (current_provider_id,)
            )
            services = cur.fetchall()
        
        return render_template("provider_services.html", services=services, title="Manage My Services")

    @app.route("/provider/services/new", methods=["GET", "POST"])
    @provider_required
    def provider_service_new():
        if request.method == "POST":
            title = request.form.get("title", "").strip()
            description = request.form.get("description", "").strip()
            price = request.form.get("price", "").strip()
            active = 1 if request.form.get("active") == "on" else 0
            is_certified = 1 if request.form.get("is_certified") == "on" else 0
            certification_proof = request.form.get("certification_proof", "").strip()
            current_provider_id = session.get('provider_id', 0)

            if not title:
                flash("Title is required.", "error")
                return render_template("provider_service_form.html", form=request.form, mode="new", title="Add Service")
            
            # Check if provider already has a service with this title
            with closing(get_db()) as db:
                cur = db.cursor()
                cur.execute("SELECT id FROM services WHERE provider_id = ? AND LOWER(title) = LOWER(?) AND active = 1", 
                           (current_provider_id, title))
                existing_service = cur.fetchone()
                
                if existing_service:
                    flash(f"You already have a service called '{title}'. Please update your existing service instead of creating a duplicate.", "error")
                    return render_template("provider_service_form.html", form=request.form, mode="new", title="Add Service")
            
            # Validate certification proof if claiming certified
            if is_certified and not certification_proof:
                flash("Certification proof/link is required for certified services.", "error")
                return render_template("provider_service_form.html", form=request.form, mode="new", title="Add Service")

            with closing(get_db()) as db:
                cur = db.cursor()
                
                # Get provider info for posted_by field
                if current_provider_id > 0:
                    cur.execute("SELECT first_name, business_name FROM providers WHERE id = ?", (current_provider_id,))
                    provider_row = cur.fetchone()
                    posted_by = provider_row["first_name"] or provider_row["business_name"] if provider_row else "Provider"
                else:
                    # Admin provider - use profile table
                    cur.execute("SELECT first_name, business_name FROM profile WHERE id = 1")
                    profile_row = cur.fetchone()
                    posted_by = profile_row["first_name"] or profile_row["business_name"] if profile_row else "Provider"
                
                cur.execute(
                    """
                    INSERT INTO services (title, description, price, posted_by, provider_id, active, is_certified, certification_proof, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (title, description, price, posted_by, current_provider_id, active, is_certified, certification_proof, datetime.datetime.utcnow().isoformat()),
                )
                db.commit()
            flash("Service added successfully.", "success")
            return redirect(url_for("provider_manage_services"))

        return render_template("provider_service_form.html", form={}, mode="new", title="Add Service")

    @app.route("/provider/services/<int:service_id>/edit", methods=["GET", "POST"])
    @provider_required
    def provider_service_edit(service_id):
        current_provider_id = session.get('provider_id', 0)
        
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute(
                "SELECT id, title, description, price, active, is_certified, certification_proof FROM services WHERE id = ? AND provider_id = ?", 
                (service_id, current_provider_id)
            )
            service = cur.fetchone()
            if not service:
                flash("Service not found or access denied.", "error")
                return redirect(url_for("provider_manage_services"))

        if request.method == "POST":
            title = request.form.get("title", "").strip()
            description = request.form.get("description", "").strip()
            price = request.form.get("price", "").strip()
            active = 1 if request.form.get("active") == "on" else 0
            is_certified = 1 if request.form.get("is_certified") == "on" else 0
            certification_proof = request.form.get("certification_proof", "").strip()

            if not title:
                flash("Title is required.", "error")
                return render_template(
                    "provider_service_form.html", form=request.form, mode="edit", service=service, title="Edit Service"
                )
            
            # Check if provider already has a different service with this title
            with closing(get_db()) as db:
                cur = db.cursor()
                cur.execute("SELECT id FROM services WHERE provider_id = ? AND LOWER(title) = LOWER(?) AND id != ? AND active = 1", 
                           (current_provider_id, title, service_id))
                existing_service = cur.fetchone()
                
                if existing_service:
                    flash(f"You already have another service called '{title}'. Please choose a different title or update that existing service.", "error")
                    return render_template(
                        "provider_service_form.html", form=request.form, mode="edit", service=service, title="Edit Service"
                    )
            
            # Validate certification proof if claiming certified
            if is_certified and not certification_proof:
                flash("Certification proof/link is required for certified services.", "error")
                return render_template(
                    "provider_service_form.html", form=request.form, mode="edit", service=service, title="Edit Service"
                )

            with closing(get_db()) as db:
                cur = db.cursor()
                cur.execute(
                    """
                    UPDATE services
                    SET title = ?, description = ?, price = ?, active = ?, is_certified = ?, certification_proof = ?
                    WHERE id = ? AND provider_id = ?
                """,
                    (title, description, price, active, is_certified, certification_proof, service_id, current_provider_id),
                )
                db.commit()
                flash("Service updated successfully.", "success")
                return redirect(url_for("provider_manage_services"))
        
        return render_template(
            "provider_service_form.html",
            form=dict(
                title=service["title"],
                description=service["description"],
                price=service["price"],
                active=bool(service["active"]),
                is_certified=service["is_certified"],
                certification_proof=service["certification_proof"],
            ),
            mode="edit",
            service=service,
            title="Edit Service",
        )

    @app.post("/provider/services/<int:service_id>/delete")
    @provider_required
    def provider_service_delete(service_id):
        current_provider_id = session.get('provider_id', 0)
        
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("DELETE FROM services WHERE id = ? AND provider_id = ?", (service_id, current_provider_id))
            db.commit()
        flash("Service deleted.", "info")
        return redirect(url_for("provider_manage_services"))

    # Provider Products Management
    @app.route("/provider/products")
    @provider_required
    def provider_products():
        current_provider_id = session.get('provider_id', 0)
        
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute(
                "SELECT id, title, description, price, active, created_at FROM products WHERE provider_id = ? ORDER BY created_at DESC",
                (current_provider_id,)
            )
            products = cur.fetchall()
        
        return render_template("provider_products.html", products=products, title="My Products")

    @app.route("/provider/products/new", methods=["GET", "POST"])
    @provider_required
    def provider_product_new():
        current_provider_id = session.get('provider_id', 0)
        
        if request.method == "POST":
            title = request.form.get("title", "").strip()
            description = request.form.get("description", "").strip()
            price = request.form.get("price", "").strip()
            active = 1 if request.form.get("active") == "on" else 0

            if not title:
                flash("Title is required.", "error")
                return render_template("provider_product_form.html", form=request.form, mode="new", title="Add Product")

            with closing(get_db()) as db:
                cur = db.cursor()
                cur.execute(
                    """
                    INSERT INTO products (title, description, price, provider_id, active, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (title, description, price, current_provider_id, active, datetime.datetime.utcnow().isoformat())
                )
                db.commit()
                flash("Product added successfully.", "success")
                return redirect(url_for("provider_products"))

        return render_template("provider_product_form.html", mode="new", title="Add Product")

    @app.route("/provider/products/<int:product_id>/edit", methods=["GET", "POST"])
    @provider_required
    def provider_product_edit(product_id):
        current_provider_id = session.get('provider_id', 0)
        
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("SELECT * FROM products WHERE id = ? AND provider_id = ?", (product_id, current_provider_id))
            product = cur.fetchone()
            if not product:
                flash("Product not found or access denied.", "error")
                return redirect(url_for("provider_products"))

        if request.method == "POST":
            title = request.form.get("title", "").strip()
            description = request.form.get("description", "").strip()
            price = request.form.get("price", "").strip()
            active = 1 if request.form.get("active") == "on" else 0

            if not title:
                flash("Title is required.", "error")
                return render_template("provider_product_form.html", form=request.form, mode="edit", product=product, title="Edit Product")

            with closing(get_db()) as db:
                cur = db.cursor()
                cur.execute(
                    """
                    UPDATE products
                    SET title = ?, description = ?, price = ?, active = ?
                    WHERE id = ? AND provider_id = ?
                    """,
                    (title, description, price, active, product_id, current_provider_id),
                )
                db.commit()
                flash("Product updated successfully.", "success")
                return redirect(url_for("provider_products"))
        
        return render_template("provider_product_form.html", product=product, mode="edit", title="Edit Product")

    @app.route("/provider/products/<int:product_id>/delete", methods=["POST"])
    @provider_required
    def provider_product_delete(product_id):
        current_provider_id = session.get('provider_id', 0)
        
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("DELETE FROM products WHERE id = ? AND provider_id = ?", (product_id, current_provider_id))
            db.commit()
        flash("Product deleted.", "info")
        return redirect(url_for("provider_products"))

    # Provider Analytics
    @app.route("/provider/analytics")
    @provider_required
    def provider_analytics():
        current_provider_id = session.get('provider_id', 0)
        
        with closing(get_db()) as db:
            cur = db.cursor()
            
            # Get basic counts
            cur.execute("SELECT COUNT(*) FROM services WHERE provider_id = ? AND active = 1", (current_provider_id,))
            services_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM products WHERE provider_id = ? AND active = 1", (current_provider_id,))
            products_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM leads WHERE provider_id = ?", (current_provider_id,))
            total_leads = cur.fetchone()[0]
            
            # Get recent leads for trend analysis
            cur.execute(
                "SELECT DATE(created_at) as date, COUNT(*) as count FROM leads WHERE provider_id = ? AND created_at >= date('now', '-30 days') GROUP BY DATE(created_at) ORDER BY date",
                (current_provider_id,)
            )
            daily_leads = cur.fetchall()
            
            # Get top services by leads generated
            cur.execute(
                """
                SELECT s.title, s.price, COUNT(l.id) as lead_count
                FROM services s
                LEFT JOIN leads l ON l.message LIKE '%' || s.title || '%' AND l.provider_id = s.provider_id
                WHERE s.provider_id = ? AND s.active = 1
                GROUP BY s.id, s.title, s.price
                ORDER BY lead_count DESC
                LIMIT 5
                """,
                (current_provider_id,)
            )
            top_services = cur.fetchall()
            
            # Get top products by leads generated
            cur.execute(
                """
                SELECT p.title, p.price, COUNT(l.id) as lead_count
                FROM products p
                LEFT JOIN leads l ON l.message LIKE '%' || p.title || '%' AND l.provider_id = p.provider_id
                WHERE p.provider_id = ? AND p.active = 1
                GROUP BY p.id, p.title, p.price
                ORDER BY lead_count DESC
                LIMIT 5
                """,
                (current_provider_id,)
            )
            top_products = cur.fetchall()
            
        # Mock data for views and traffic sources
        total_views = (services_count + products_count) * 15
        traffic_sources = [
            {"source": "Direct Traffic", "visits": total_views * 0.4, "percentage": 40},
            {"source": "Google Search", "visits": total_views * 0.3, "percentage": 30},
            {"source": "Social Media", "visits": total_views * 0.2, "percentage": 20},
            {"source": "Referrals", "visits": total_views * 0.1, "percentage": 10}
        ]
        
        return render_template("provider_analytics.html", 
                             services_count=services_count,
                             products_count=products_count,
                             total_leads=total_leads,
                             total_views=total_views,
                             daily_leads=daily_leads,
                             top_services=top_services,
                             top_products=top_products,
                             traffic_sources=traffic_sources,
                             title="My Analytics")

    # Provider Service Areas (ZIP Code Management)
    @app.route("/provider/service-areas", methods=["GET", "POST"])
    @provider_required
    def provider_service_areas():
        current_provider_id = session.get('provider_id', 0)
        
        if request.method == "POST":
            zip_code = request.form.get("zip_code", "").strip()
            radius_miles = int(request.form.get("radius_miles", 10))
            
            if not zip_code or len(zip_code) != 5 or not zip_code.isdigit():
                flash("Please enter a valid 5-digit ZIP code.", "error")
                return redirect(url_for("provider_service_areas"))
            
            with closing(get_db()) as db:
                cur = db.cursor()
                
                # Check current ZIP count (max 5 for services)
                cur.execute("SELECT COUNT(*) FROM provider_zips WHERE provider_id = ?", (current_provider_id,))
                current_count = cur.fetchone()[0]
                
                if current_count >= 5:
                    flash("Maximum of 5 service areas allowed. Remove an existing area to add a new one.", "error")
                    return redirect(url_for("provider_service_areas"))
                
                # Check if ZIP already exists for this provider
                cur.execute("SELECT id FROM provider_zips WHERE provider_id = ? AND zip_code = ?", (current_provider_id, zip_code))
                if cur.fetchone():
                    flash("You already serve this ZIP code.", "error")
                    return redirect(url_for("provider_service_areas"))
                
                # Validate proximity if other ZIPs exist (simple validation for now)
                if current_count > 0:
                    cur.execute("SELECT zip_code FROM provider_zips WHERE provider_id = ?", (current_provider_id,))
                    existing_zips = [row[0] for row in cur.fetchall()]
                    
                    # Basic proximity check - first 3 digits should be similar for nearby areas
                    zip_prefix = zip_code[:3]
                    existing_prefixes = [z[:3] for z in existing_zips]
                    
                    # Allow if any existing ZIP has similar prefix (within same general region)
                    if not any(abs(int(zip_prefix) - int(prefix)) <= 20 for prefix in existing_prefixes):
                        flash(f"New service area {zip_code} should be near your existing areas for efficient service delivery.", "warning")
                
                # Add the ZIP code
                cur.execute(
                    "INSERT INTO provider_zips (provider_id, zip_code, radius_miles, created_at) VALUES (?, ?, ?, ?)",
                    (current_provider_id, zip_code, radius_miles, datetime.datetime.utcnow().isoformat())
                )
                db.commit()
                flash(f"Service area {zip_code} added successfully.", "success")
                return redirect(url_for("provider_service_areas"))
        
        # Get current service areas
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute(
                "SELECT id, zip_code, radius_miles, created_at FROM provider_zips WHERE provider_id = ? ORDER BY created_at DESC",
                (current_provider_id,)
            )
            service_areas = cur.fetchall()
        
        return render_template("provider_service_areas.html", service_areas=service_areas, title="My Service Areas")

    @app.route("/provider/service-areas/<int:area_id>/delete", methods=["POST"])
    @provider_required
    def provider_service_area_delete(area_id):
        current_provider_id = session.get('provider_id', 0)
        
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("DELETE FROM provider_zips WHERE id = ? AND provider_id = ?", (area_id, current_provider_id))
            db.commit()
        
        flash("Service area removed.", "info")
        return redirect(url_for("provider_service_areas"))

    # Admin provider (formerly profile)
    @app.route("/provider/profile", methods=["GET", "POST"])
    @provider_required
    def provider_profile():
        current_provider_id = session.get('provider_id', 0)
        
        if request.method == "POST":
            first_name = request.form.get("first_name", "").strip()
            business_name = request.form.get("business_name", "").strip()
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
                    
                    if current_provider_id == 0:
                        # Admin provider - update main profile table (remove address for safety)
                        cur.execute(
                            """
                            UPDATE profile
                            SET first_name=?, business_name=?, phone=?, base_zip=?, about=?, profile_photo=?
                            WHERE id = 1
                            """,
                            (first_name, business_name, phone, base_zip, about, profile_photo),
                        )
                    else:
                        # Regular provider - update providers table (no address for safety)
                        cur.execute(
                            """
                            UPDATE providers
                            SET first_name=?, business_name=?, phone=?, base_zip=?, about=?, profile_photo=?
                            WHERE id = ?
                            """,
                            (first_name, business_name, phone, base_zip, about, profile_photo, current_provider_id),
                        )
                    db.commit()
                flash("Provider profile saved.", "success")
                return redirect(url_for("provider_profile"))

        # Load current values
        with closing(get_db()) as db:
            cur = db.cursor()
            
            if current_provider_id == 0:
                # Admin provider - use main profile table
                cur.execute("SELECT first_name, business_name, phone, base_zip, address, about, profile_photo FROM profile WHERE id = 1")
                row = cur.fetchone()
            else:
                # Regular provider - use providers table
                cur.execute("SELECT first_name, business_name, phone, base_zip, address, about, profile_photo FROM providers WHERE id = ?", (current_provider_id,))
                row = cur.fetchone()
            
            # Count leads for display
            cur.execute("SELECT COUNT(*) FROM leads WHERE provider_id = ?", (current_provider_id,))
            leads_count = cur.fetchone()[0]

        form = {
            "first_name": row["first_name"] if row else "",
            "business_name": row["business_name"] if row else "",
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
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("""
                SELECT p.id, p.email, p.first_name, p.business_name, p.phone, p.base_zip, 
                       p.active, p.created_at, COUNT(s.id) as service_count
                FROM providers p
                LEFT JOIN services s ON s.provider_id = p.id AND s.active = 1
                GROUP BY p.id
                ORDER BY p.created_at DESC
            """)
            providers = cur.fetchall()
        return render_template("admin_providers.html", providers=providers, title="Manage Providers")

    @app.route("/admin/providers/<int:provider_id>/toggle", methods=["POST"])
    @login_required
    def admin_provider_toggle(provider_id):
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("SELECT active, first_name, business_name FROM providers WHERE id = ?", (provider_id,))
            provider = cur.fetchone()
            
            if not provider:
                flash("Provider not found.", "error")
                return redirect(url_for("admin_provider"))
            
            new_status = 0 if provider["active"] else 1
            cur.execute("UPDATE providers SET active = ? WHERE id = ?", (new_status, provider_id))
            db.commit()
            
            provider_name = provider["first_name"] or provider["business_name"] or f"Provider {provider_id}"
            status_text = "activated" if new_status else "deactivated"
            flash(f"Provider '{provider_name}' has been {status_text}.", "success")
            
        return redirect(url_for("admin_provider"))

    @app.route("/admin/providers/<int:provider_id>/delete", methods=["POST"])
    @login_required
    def admin_provider_delete(provider_id):
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("SELECT first_name, business_name FROM providers WHERE id = ?", (provider_id,))
            provider = cur.fetchone()
            
            if not provider:
                flash("Provider not found.", "error")
                return redirect(url_for("admin_provider"))
            
            provider_name = provider["first_name"] or provider["business_name"] or f"Provider {provider_id}"
            
            # Delete provider's services first
            cur.execute("DELETE FROM services WHERE provider_id = ?", (provider_id,))
            # Delete the provider
            cur.execute("DELETE FROM providers WHERE id = ?", (provider_id,))
            db.commit()
            
            flash(f"Provider '{provider_name}' and all their services have been permanently deleted.", "success")
            
        return redirect(url_for("admin_provider"))

    # Services CRUD
    @app.route("/admin/services")
    @login_required
    def admin_services():
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute(
                "SELECT id, title, description, price, active, is_certified, certification_proof, created_at FROM services ORDER BY created_at DESC"
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
            is_certified = 1 if request.form.get("is_certified") == "on" else 0
            certification_proof = request.form.get("certification_proof", "").strip()

            if not title:
                flash("Title is required.", "error")
                return render_template("admin_service_form.html", form=request.form, mode="new", title="Add Service")
            
            # Validate certification proof if claiming certified
            if is_certified and not certification_proof:
                flash("Certification proof/link is required for certified services.", "error")
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
                    INSERT INTO services (title, description, price, posted_by, active, is_certified, certification_proof, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (title, description, price, posted_by, active, is_certified, certification_proof, datetime.datetime.utcnow().isoformat()),
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
            cur.execute("SELECT id, title, description, price, posted_by, active, is_certified, certification_proof FROM services WHERE id = ?", (service_id,))
            service = cur.fetchone()
            if not service:
                abort(404)

        if request.method == "POST":
            title = request.form.get("title", "").strip()
            description = request.form.get("description", "").strip()
            price = request.form.get("price", "").strip()
            posted_by = request.form.get("posted_by", "").strip()
            active = 1 if request.form.get("active") == "on" else 0
            is_certified = 1 if request.form.get("is_certified") == "on" else 0
            certification_proof = request.form.get("certification_proof", "").strip()

            if not title:
                flash("Title is required.", "error")
                return render_template(
                    "admin_service_form.html", form=request.form, mode="edit", service=service, title="Edit Service"
                )
            
            # Validate certification proof if claiming certified
            if is_certified and not certification_proof:
                flash("Certification proof/link is required for certified services.", "error")
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
                    SET title = ?, description = ?, price = ?, posted_by = ?, active = ?, is_certified = ?, certification_proof = ?
                    WHERE id = ?
                """,
                    (title, description, price, posted_by, active, is_certified, certification_proof, service_id),
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
                is_certified=service["is_certified"],
                certification_proof=service["certification_proof"],
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
        current_provider_id = session.get('provider_id', 0)
        
        with closing(get_db()) as db:
            cur = db.cursor()
            
            # Build query based on filter, filtered by current provider
            base_where = "WHERE provider_id = ?"
            params = [current_provider_id]
            
            if filter_status == 'completed':
                query = f"SELECT id, name, email, phone, zip, address, message, created_at, status, recurring FROM leads {base_where} AND status = 'completed' ORDER BY created_at DESC"
                page_title = "Completed Leads"
            elif filter_status == 'rejected':
                query = f"SELECT id, name, email, phone, zip, address, message, created_at, status, recurring FROM leads {base_where} AND status = 'rejected' ORDER BY created_at DESC"
                page_title = "Rejected Leads"
            elif filter_status == 'subscribers':
                query = f"SELECT id, name, email, phone, zip, address, message, created_at, status, recurring FROM leads {base_where} AND recurring = 1 ORDER BY created_at DESC"
                page_title = "Weekly Subscribers"
            else:
                # Default: Get active/pending leads only (not completed or rejected)
                query = f"SELECT id, name, email, phone, zip, address, message, created_at, status, recurring FROM leads {base_where} AND (status IS NULL OR status NOT IN ('completed', 'rejected')) ORDER BY created_at DESC"
                page_title = "Active Leads"
            
            cur.execute(query, params)
            leads = cur.fetchall()
            
            # Calculate statistics for current provider
            cur.execute("SELECT COUNT(*) FROM leads WHERE provider_id = ? AND status = 'completed'", (current_provider_id,))
            completed_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM leads WHERE provider_id = ? AND status = 'rejected'", (current_provider_id,))
            rejected_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM leads WHERE provider_id = ? AND recurring = 1", (current_provider_id,))
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
