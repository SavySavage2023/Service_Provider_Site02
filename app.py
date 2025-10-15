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
from werkzeug.security import check_password_hash, generate_password_hash
import csv

# Import nbimporter for loading Jupyter notebook modules
import sys
import importlib.util
_app_dir = os.path.abspath(os.path.dirname(__file__))
_modules_dir = os.path.join(_app_dir, 'Modules')
sys.path.insert(0, _modules_dir)

# --- SETUP DATABASE FUNCTION EARLY ---
APP_DIR = os.path.abspath(os.path.dirname(__file__))
INSTANCE_DIR = os.path.join(APP_DIR, "instance")
os.makedirs(INSTANCE_DIR, exist_ok=True)
DB_PATH = os.path.join(INSTANCE_DIR, "site.db")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# Create a fake 'db' module with get_db so notebooks can import it
import types
db_module = types.ModuleType('db')
db_module.get_db = get_db
sys.modules['db'] = db_module

# Track which modules loaded successfully for fault tolerance
MODULE_STATUS = {
    'events': False,
    'services': False,
    'products': False,
    'contact': False,
    'admin': False,
    'profile': False
}

# Import nbimporter
try:
    import nbimporter
    nbimporter.options['only_defs'] = False
except ImportError:
    print("ERROR: nbimporter not installed. Install with: pip install nbimporter")
    sys.exit(1)

# Helper function to safely import a notebook module
def safe_import_notebook(module_name, function_names, use_spec=False):
    """
    Safely import a notebook module with proper error handling.
    Returns a dict of {function_name: function or None}
    """
    result = {name: None for name in function_names}
    
    try:
        notebook_path = os.path.join(_modules_dir, f"{module_name}.ipynb")
        if not os.path.exists(notebook_path):
            raise ImportError(f"Notebook file not found: {notebook_path}")
        
        # Use nbimporter's NotebookLoader directly
        from nbimporter import NotebookLoader
        loader = NotebookLoader(path=[_modules_dir])
        
        # Load the module using the loader
        module = loader.load_module(module_name)
        
        if module:
            # Store in sys.modules for future imports
            sys.modules[module_name] = module
            
            # Get the functions from the module
            for func_name in function_names:
                if hasattr(module, func_name):
                    result[func_name] = getattr(module, func_name)
                    print(f"[OK] Loaded {module_name}.{func_name}")
                else:
                    print(f"[FAIL] Function {func_name} not found in {module_name}")
                    print(f"   Available: {[attr for attr in dir(module) if not attr.startswith('_')]}")
            
            MODULE_STATUS[module_name] = True
        else:
            raise ImportError(f"Failed to load module {module_name}")
        
    except Exception as e:
        print(f"[FAIL] Failed to load {module_name}: {e}")
        if os.environ.get('DEBUG'):
            import traceback
            traceback.print_exc()
        MODULE_STATUS[module_name] = False
    
    return result

# Import each module independently with fault tolerance
print("\nLoading notebook modules...")
print("=" * 60)

events_funcs = safe_import_notebook('events', ['get_events_page'])
get_events_page = events_funcs['get_events_page']

# On Linux (Render), filenames are case-sensitive. The notebook is 'services.ipynb'.
services_funcs = safe_import_notebook('services', ['get_services_page'])
get_services_page = services_funcs['get_services_page']

products_funcs = safe_import_notebook('products', ['get_products_page'])
get_products_page = products_funcs['get_products_page']

contact_funcs = safe_import_notebook('contact', ['get_contact_page'])
get_contact_page = contact_funcs['get_contact_page']

admin_funcs = safe_import_notebook('admin', ['get_admin_dashboard', 'get_admin_analytics', 'get_admin_leads', 'get_admin_assign_leads', 'get_admin_events'])
get_admin_dashboard = admin_funcs['get_admin_dashboard']
get_admin_analytics = admin_funcs['get_admin_analytics']
get_admin_leads = admin_funcs['get_admin_leads']
get_admin_assign_leads = admin_funcs['get_admin_assign_leads']
get_admin_events = admin_funcs['get_admin_events']

profile_funcs = safe_import_notebook('profile', ['get_public_provider_profile'])
get_public_provider_profile = profile_funcs['get_public_provider_profile']

print("=" * 60)
print(f"Module Status: {sum(MODULE_STATUS.values())}/{len(MODULE_STATUS)} modules loaded\n")

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

# --- FLASK APP INIT ---
app = Flask(__name__, instance_path=INSTANCE_DIR, instance_relative_config=True)

APP_DIR = os.path.abspath(os.path.dirname(__file__))
INSTANCE_DIR = os.path.join(APP_DIR, "instance")
os.makedirs(INSTANCE_DIR, exist_ok=True)
DB_PATH = os.path.join(INSTANCE_DIR, "site.db")


def get_env(name, default=None):
    val = os.environ.get(name)
    return val if val is not None else default


# get_db() already defined earlier for module imports

def init_db():
    # Ensure image columns exist in products table for existing databases
    db = get_db()
    try:
        cur = db.cursor()
        cur.execute("PRAGMA table_info(products)")
        pcols = [row[1] for row in cur.fetchall()]
        for col in ["image1", "image2", "image3", "image4", "image5"]:
            if col not in pcols:
                cur.execute(f"ALTER TABLE products ADD COLUMN {col} TEXT")
        db.commit()
    finally:
        db.close()
    # Ensure custom_url column exists before upsert
    db = get_db()
    try:
        cur = db.cursor()
        cur.execute("PRAGMA table_info(providers)")
        pcols = [row[1] for row in cur.fetchall()]
        if "custom_url" not in pcols:
            cur.execute("ALTER TABLE providers ADD COLUMN custom_url TEXT")
            db.commit()
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_providers_custom_url ON providers(custom_url)")
            db.commit()
    finally:
        db.close()
    # Upsert demo provider with all social media pins for demo - COMMENTED OUT FOR CLEAN START
    # db = get_db()
    # try:
    #     cur = db.cursor()
    #     cur.execute("SELECT id FROM providers WHERE custom_url = 'taime-demo'")
    #     row = cur.fetchone()
    #     if row:
    #         cur.execute("""
    #             UPDATE providers SET
    #                 linkedin_url = 'https://linkedin.com/in/taime',
    #                 facebook_url = 'https://facebook.com/taime',
    #                 instagram_url = 'https://instagram.com/taime',
    #                 twitter_url = 'https://twitter.com/taime',
    #                 website_url = 'https://taime.com',
    #                 youtube_url = 'https://youtube.com/taime'
    #             WHERE custom_url = 'taime-demo'
    #         """)
    #     else:
    #         cur.execute(
    #             "INSERT INTO providers (email, password_hash, first_name, business_name, phone, base_zip, address, about, profile_photo, linkedin_url, facebook_url, instagram_url, twitter_url, website_url, youtube_url, custom_url, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    #             (
    #                 'demo@provider.com', 'demo', 'Taime', 'Taime Designs', '555-123-4567', '90210', '123 Main St', 'Sample provider with all social pins.', '',
    #                 'https://linkedin.com/in/taime',
    #                 'https://facebook.com/taime',
    #                 'https://instagram.com/taime',
    #                 'https://twitter.com/taime',
    #                 'https://taime.com',
    #                 'https://youtube.com/taime',
    #                 'taime-demo', 1, datetime.datetime.now().isoformat()
    #             )
    #         )
    #     db.commit()
    # finally:
    #     db.close()
    db = get_db()
    try:
        db.executescript(
            """
CREATE TABLE IF NOT EXISTS profile (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    business_name TEXT,
    first_name TEXT,
    contact_email TEXT,
    phone TEXT,
    base_zip TEXT,
    address TEXT,
    about TEXT,
    profile_photo TEXT
);

INSERT OR IGNORE INTO profile (id, business_name) VALUES (1, 'Your Mom''s Services');

CREATE TABLE IF NOT EXISTS leads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    phone TEXT,
    zip TEXT,
    address TEXT,
    service TEXT,
    message TEXT,
    status TEXT DEFAULT 'new',
    follow_up_date TEXT,
    recurring INTEGER DEFAULT 0,
    provider_id INTEGER DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS zips (
    zip TEXT PRIMARY KEY,
    radius_miles INTEGER NOT NULL DEFAULT 20
);

CREATE TABLE IF NOT EXISTS services (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    description TEXT,
    price TEXT,
    posted_by TEXT,
    provider_id INTEGER NOT NULL DEFAULT 0,
    active INTEGER NOT NULL DEFAULT 1,
        image1 TEXT,
        image2 TEXT,
        image3 TEXT,
        image4 TEXT,
        image5 TEXT,
    is_certified INTEGER DEFAULT 0,
    certification_proof TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS blocked_addresses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT NOT NULL,
    zip TEXT,
    message TEXT,
    provider_id INTEGER NOT NULL DEFAULT 0
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
    linkedin_url TEXT,
    facebook_url TEXT,
    instagram_url TEXT,
    twitter_url TEXT,
    website_url TEXT,
    youtube_url TEXT,
    custom_url TEXT UNIQUE,
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

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    description TEXT,
    date TEXT NOT NULL,
    location TEXT,
    zip TEXT,
    provider_id INTEGER NOT NULL,
    price REAL DEFAULT 0,
    hours REAL DEFAULT 0,
    status TEXT DEFAULT 'pending',
    blocked INTEGER DEFAULT 0,
    created_at TEXT NOT NULL
);

-- Password storage (DB becomes source of truth so passwords can be changed in-app)
CREATE TABLE IF NOT EXISTS auth (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    admin_password_hash TEXT,
    provider_password_hash TEXT
);

INSERT OR IGNORE INTO auth (id) VALUES (1);
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

    db = get_db()
    try:
        cur = db.cursor()
        cur.execute("PRAGMA table_info(leads)")
        lcols = [row[1] for row in cur.fetchall()]
        if "service" not in lcols:
            cur.execute("ALTER TABLE leads ADD COLUMN service TEXT")
            db.commit()
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

    # Add new columns to events table for job tracking
    db = get_db()
    try:
        cur = db.cursor()
        cur.execute("PRAGMA table_info(events)")
        ecols = [row[1] for row in cur.fetchall()]
        if "price" not in ecols:
            cur.execute("ALTER TABLE events ADD COLUMN price REAL DEFAULT 0")
            db.commit()
        if "hours" not in ecols:
            cur.execute("ALTER TABLE events ADD COLUMN hours REAL DEFAULT 0")
            db.commit()
        if "status" not in ecols:
            cur.execute("ALTER TABLE events ADD COLUMN status TEXT DEFAULT 'pending'")
            db.commit()
        if "blocked" not in ecols:
            cur.execute("ALTER TABLE events ADD COLUMN blocked INTEGER DEFAULT 0")
            db.commit()
    finally:
        db.close()

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

    @app.route('/product/<int:product_id>')
    def product_detail(product_id):
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("""
                SELECT p.*, pr.first_name, pr.business_name
                FROM products p
                LEFT JOIN providers pr ON p.provider_id = pr.id
                WHERE p.id = ? AND p.active = 1
            """, (product_id,))
            product = cur.fetchone()
            if not product:
                abort(404)
            product = dict(product)
            provider = {
                'first_name': product.get('first_name', ''),
                'business_name': product.get('business_name', '')
            }
        back_url = request.referrer or url_for('public_provider_profile', provider_id=product['provider_id'])
        return render_template('product_detail.html', product=product, provider=provider, back_url=back_url)

    # Secret key for sessions (must be set for production)
    secret_key = get_env("SECRET_KEY")
    if not secret_key:
        raise RuntimeError("SECRET_KEY environment variable must be set for production hosting.")
    app.secret_key = secret_key

    # Secure session cookies
    app.config["SESSION_COOKIE_SECURE"] = True
    app.config["SESSION_COOKIE_HTTPONLY"] = True

    # CSRF protection (Flask-WTF)
    csrf = None
    try:
        from flask_wtf import CSRFProtect
        csrf = CSRFProtect(app)
    except ImportError:
        print("WARNING: Flask-WTF not installed. CSRF protection is disabled.")

    # Init DB on startup
    init_db()
    
    # Add social media columns if they don't exist
    with closing(get_db()) as db:
        try:
            db.execute("ALTER TABLE providers ADD COLUMN linkedin_url TEXT")
            db.execute("ALTER TABLE providers ADD COLUMN facebook_url TEXT") 
            db.execute("ALTER TABLE providers ADD COLUMN instagram_url TEXT")
            db.execute("ALTER TABLE providers ADD COLUMN twitter_url TEXT")
            db.execute("ALTER TABLE providers ADD COLUMN website_url TEXT")
            db.execute("ALTER TABLE providers ADD COLUMN youtube_url TEXT")
            db.commit()
            print("Added social media columns to providers table")
        except sqlite3.OperationalError:
            # Columns already exist
            pass

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
        except Exception:
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
                # Load active registered providers first
                cur.execute(
                    "SELECT id, first_name, business_name, phone, base_zip, address, about, profile_photo FROM providers WHERE active = 1 ORDER BY business_name"
                )
                provider_rows = cur.fetchall()

                if provider_rows and len(provider_rows) > 0:
                    # Show ONLY real providers when any exist
                    for provider_row in provider_rows:
                        first_name = provider_row["first_name"] or ""
                        business_name = provider_row["business_name"] or ""
                        provider_id = provider_row["id"]
                        if business_name in ["", "Individual", "Service Provider"] or not business_name.strip():
                            display_business_name = f"{first_name.capitalize()}'s Services" if first_name else "Service Provider"
                        else:
                            display_business_name = business_name
                        provider_data = {
                            "id": provider_id,
                            "first_name": first_name.capitalize() if first_name else "",
                            "business_name": display_business_name,
                            "contact_email": "",
                            "phone": provider_row["phone"] or "",
                            "base_zip": provider_row["base_zip"] or "",
                            "address": provider_row["address"] or "",
                            "about": provider_row["about"] or "",
                            "profile_photo": provider_row["profile_photo"] or ""
                        }
                        all_providers.append(provider_data)
                        # Load this provider's services/products
                        cur.execute(
                            "SELECT id, title, description, price, posted_by FROM services WHERE provider_id = ? AND active = 1 ORDER BY created_at DESC LIMIT 6",
                            (provider_id,)
                        )
                        provider_services = cur.fetchall()
                        all_provider_services[provider_id] = [dict(row) for row in provider_services]
                        cur.execute(
                            "SELECT id, title, description, price FROM products WHERE provider_id = ? AND active = 1 ORDER BY created_at DESC LIMIT 6",
                            (provider_id,)
                        )
                        provider_products = cur.fetchall()
                        all_provider_products[provider_id] = [dict(row) for row in provider_products]

                    profile = all_providers[0] if all_providers else None
                else:
                    # No active providers yet — optionally include the admin/company profile if it has content or services
                    cur.execute(
                        "SELECT first_name, business_name, contact_email, phone, base_zip, address, about, profile_photo FROM profile WHERE id = 1"
                    )
                    admin_row = cur.fetchone()
                    cur.execute("SELECT COUNT(*) FROM services WHERE provider_id = 0 AND active = 1")
                    admin_service_count = cur.fetchone()[0]

                    if admin_row and (
                        admin_service_count > 0
                        or (admin_row["business_name"] or admin_row["first_name"] or admin_row["about"]) 
                    ):
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

                        # Admin/company services
                        cur.execute(
                            "SELECT id, title, description, price, posted_by FROM services WHERE provider_id = 0 AND active = 1 ORDER BY created_at DESC LIMIT 6"
                        )
                        admin_services = cur.fetchall()
                        all_provider_services[0] = [dict(row) for row in admin_services]

                        # Admin/company products
                        cur.execute(
                            "SELECT id, title, description, price FROM products WHERE provider_id = 0 AND active = 1 ORDER BY created_at DESC LIMIT 6"
                        )
                        admin_products = cur.fetchall()
                        all_provider_products[0] = [dict(row) for row in admin_products]

                    profile = all_providers[0] if all_providers else None
                    services = all_provider_services.get(0, []) if all_provider_services else []
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

    # Helper function for maintenance page
    def maintenance_page(module_name):
        """Return a maintenance page when a module is unavailable"""
        return render_template("maintenance.html", 
                             module_name=module_name, 
                             title=f"{module_name.title()} - Under Maintenance"), 503

    # Module functions are imported at the top of the file
    
    @app.route("/profile/<int:provider_id>")
    def public_provider_profile(provider_id):
        if get_public_provider_profile is None:
            return maintenance_page("Profile")
        try:
            return get_public_provider_profile(request, provider_id)
        except Exception as e:
            print(f"Error in profile module: {e}")
            return maintenance_page("Profile")

    
    @app.route("/events", methods=["GET", "POST"])
    def events():
        if get_events_page is None:
            return maintenance_page("Events")
        try:
            return get_events_page(request)
        except Exception as e:
            print(f"Error in events module: {e}")
            return maintenance_page("Events")
    
    @app.route("/services")
    def services():
        if get_services_page is None:
            return maintenance_page("Services")
        try:
            return get_services_page(request)
        except Exception as e:
            print(f"Error in services module: {e}")
            return maintenance_page("Services")

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

    
    @app.route("/products")
    def products():
        if get_products_page is None:
            return maintenance_page("Products")
        try:
            return get_products_page(request)
        except Exception as e:
            print(f"Error in products module: {e}")
            import traceback
            traceback.print_exc()
            return maintenance_page("Products")

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
        if get_contact_page is None:
            return maintenance_page("Contact")
        try:
            return get_contact_page(request)
        except Exception as e:
            print(f"Error in contact module: {e}")
            return maintenance_page("Contact")

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

    @app.route("/admin/logout", methods=["GET", "POST"])
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
            business_name = request.form.get("business_name", "").strip() or "Independent"
            
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
                business_name = "Independent"
            
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

    @app.route("/provider/logout", methods=["GET", "POST"])
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
        if get_admin_dashboard is None:
            return maintenance_page("Admin Dashboard")
        try:
            return get_admin_dashboard(request)
        except Exception as e:
            print(f"Error in admin dashboard module: {e}")
            return maintenance_page("Admin Dashboard")

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
        if get_admin_analytics is None:
            return maintenance_page("Admin Analytics")
        try:
            return get_admin_analytics(request)
        except Exception as e:
            print(f"Error in admin analytics module: {e}")
            return maintenance_page("Admin Analytics")

    @app.route("/admin/leads")
    @login_required
    def admin_leads():
        if get_admin_leads is None:
            return maintenance_page("Admin Leads")
        try:
            return get_admin_leads(request)
        except Exception as e:
            print(f"Error in admin leads module: {e}")
            return maintenance_page("Admin Leads")

    @app.route("/admin/assign-leads")
    @login_required
    def admin_assign_leads():
        if get_admin_assign_leads is None:
            return maintenance_page("Admin Assign Leads")
        try:
            return get_admin_assign_leads(request)
        except Exception as e:
            print(f"Error in admin assign leads module: {e}")
            return maintenance_page("Admin Assign Leads")

    @app.route("/admin/events")
    @login_required
    def admin_events():
        if get_admin_events is None:
            return maintenance_page("Admin Events")
        try:
            return get_admin_events(request)
        except Exception as e:
            print(f"Error in admin events module: {e}")
            return maintenance_page("Admin Events")

    @app.post("/admin/events/<int:event_id>/delete")
    @login_required
    def admin_event_delete(event_id):
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("DELETE FROM events WHERE id = ?", (event_id,))
            db.commit()
        flash("Event deleted.", "info")
        return redirect(url_for("admin_events"))

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

            # Handle up to 5 image uploads from a single input
            image_paths = []
            upload_folder = os.path.join(APP_DIR, "static", "uploads", "products")
            os.makedirs(upload_folder, exist_ok=True)
            files = request.files.getlist("images")
            for idx, file in enumerate(files[:5]):
                if file and file.filename:
                    ext = os.path.splitext(file.filename)[1].lower()
                    if ext not in [".jpg", ".jpeg", ".png", ".gif", ".webp"]:
                        flash(f"Image {idx+1} must be a valid image file.", "error")
                        return render_template("provider_product_form.html", form=request.form, mode="new", title="Add Product")
                    filename = f"{current_provider_id}_{int(datetime.datetime.utcnow().timestamp())}_{idx+1}{ext}"
                    save_path = os.path.join(upload_folder, filename)
                    file.save(save_path)
                    image_paths.append(f"/static/uploads/products/{filename}")
                else:
                    image_paths.append("")
            # Pad to 5 images
            while len(image_paths) < 5:
                image_paths.append("")

            if not title:
                flash("Title is required.", "error")
                return render_template("provider_product_form.html", form=request.form, mode="new", title="Add Product")

            with closing(get_db()) as db:
                cur = db.cursor()
                cur.execute(
                    """
                    INSERT INTO products (title, description, price, provider_id, active, created_at, image1, image2, image3, image4, image5)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (title, description, price, current_provider_id, active, datetime.datetime.utcnow().isoformat(),
                     image_paths[0], image_paths[1], image_paths[2], image_paths[3], image_paths[4])
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

            # Handle up to 5 image uploads and removals
            image_paths = []
            upload_folder = os.path.join(APP_DIR, "static", "uploads", "products")
            os.makedirs(upload_folder, exist_ok=True)
            for i in range(1, 6):
                remove = request.form.get(f"remove_image{i}")
                file = request.files.get(f"image{i}")
                current_img = product[f"image{i}"] if product else ""
                if remove:
                    image_paths.append("")
                elif file and file.filename:
                    ext = os.path.splitext(file.filename)[1].lower()
                    if ext not in [".jpg", ".jpeg", ".png", ".gif", ".webp"]:
                        flash(f"Image {i} must be a valid image file.", "error")
                        return render_template("provider_product_form.html", form=request.form, mode="edit", product=product, title="Edit Product")
                    filename = f"{current_provider_id}_{int(datetime.datetime.utcnow().timestamp())}_{i}{ext}"
                    save_path = os.path.join(upload_folder, filename)
                    file.save(save_path)
                    image_paths.append(f"/static/uploads/products/{filename}")
                else:
                    image_paths.append(current_img)

            if not title:
                flash("Title is required.", "error")
                return render_template("provider_product_form.html", form=request.form, mode="edit", product=product, title="Edit Product")

            with closing(get_db()) as db:
                cur = db.cursor()
                cur.execute(
                    """
                    UPDATE products
                    SET title = ?, description = ?, price = ?, active = ?,
                        image1 = ?, image2 = ?, image3 = ?, image4 = ?, image5 = ?
                    WHERE id = ? AND provider_id = ?
                    """,
                    (title, description, price, active,
                     image_paths[0], image_paths[1], image_paths[2], image_paths[3], image_paths[4],
                     product_id, current_provider_id),
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
    # Provider Events
    @app.route("/provider/events")
    @provider_required
    def provider_events():
        current_provider_id = session.get('provider_id', 0)
        
        with closing(get_db()) as db:
            db.row_factory = sqlite3.Row
            cur = db.cursor()
            cur.execute(
                "SELECT id, title, description, date, location, zip, price, hours, status, blocked, created_at FROM events WHERE provider_id = ? ORDER BY date ASC",
                (current_provider_id,)
            )
            rows = cur.fetchall()
            events = [dict(row) for row in rows]
            
            # Calculate stats
            completed = sum(1 for e in events if e.get('status') == 'completed')
            rejected = sum(1 for e in events if e.get('status') == 'rejected')
            subscribers = sum(1 for e in events if e.get('status') == 'pending')
            blocked = sum(1 for e in events if e.get('blocked') == 1)
            total_value = sum(e.get('price', 0) or 0 for e in events if e.get('status') == 'completed')
            total_hours = sum(e.get('hours', 0) or 0 for e in events if e.get('status') == 'completed')
            value_per_hour = (total_value / total_hours) if total_hours > 0 else 0
        
        return render_template("provider_events.html", events=events, title="Provider Schedule",
                             completed=completed, rejected=rejected, subscribers=subscribers, 
                             blocked=blocked, total_value=total_value, total_hours=total_hours,
                             value_per_hour=value_per_hour)

    @app.route("/provider/events/<int:event_id>/edit", methods=["GET", "POST"])
    @provider_required
    def provider_event_edit(event_id):
        current_provider_id = session.get('provider_id', 0)
        
        with closing(get_db()) as db:
            cur = db.cursor()
            
            # Get the event
            cur.execute(
                "SELECT id, title, description, date, location, zip, price, hours, status, blocked FROM events WHERE id = ? AND provider_id = ?",
                (event_id, current_provider_id)
            )
            event = cur.fetchone()
            
            if not event:
                flash("Event not found", "error")
                return redirect(url_for('provider_events'))
            
            if request.method == "POST":
                title = request.form.get("title", "").strip()
                description = request.form.get("description", "").strip()
                date = request.form.get("date", "").strip()
                location = request.form.get("location", "").strip()
                zip_code = request.form.get("zip", "").strip()
                price = float(request.form.get("price", 0) or 0)
                hours = float(request.form.get("hours", 0) or 0)
                status = request.form.get("status", "pending")
                blocked = 1 if request.form.get("blocked") else 0
                
                if not title or not date:
                    flash("Title and date are required", "error")
                else:
                    cur.execute(
                        "UPDATE events SET title = ?, description = ?, date = ?, location = ?, zip = ?, price = ?, hours = ?, status = ?, blocked = ? WHERE id = ? AND provider_id = ?",
                        (title, description, date, location, zip_code, price, hours, status, blocked, event_id, current_provider_id)
                    )
                    db.commit()
                    flash("Event updated successfully", "success")
                    return redirect(url_for('provider_events'))
        
        return render_template("provider_event_form.html", event=event, title="Edit Event")

    @app.route("/provider/events/new", methods=["GET", "POST"])
    @provider_required
    def provider_event_new():
        current_provider_id = session.get('provider_id', 0)
        
        if request.method == "POST":
            title = request.form.get("title", "").strip()
            description = request.form.get("description", "").strip()
            date = request.form.get("date", "").strip()
            location = request.form.get("location", "").strip()
            zip_code = request.form.get("zip", "").strip()
            price = float(request.form.get("price", 0) or 0)
            hours = float(request.form.get("hours", 0) or 0)
            status = request.form.get("status", "pending")
            
            if not title or not date:
                flash("Title and date are required", "error")
            else:
                with closing(get_db()) as db:
                    cur = db.cursor()
                    now = datetime.datetime.now().isoformat()
                    cur.execute(
                        "INSERT INTO events (provider_id, title, description, date, location, zip, price, hours, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (current_provider_id, title, description, date, location, zip_code, price, hours, status, now)
                    )
                    db.commit()
                flash("Event created successfully", "success")
                return redirect(url_for('provider_events'))
        
        return render_template("provider_event_form.html", event=None, title="Add New Event")

    # Provider Analytics
    # Ensure visits table exists for UTM/profit tracking
    with closing(get_db()) as db:
        cur = db.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS visits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider_id INTEGER,
                utm_source TEXT,
                utm_medium TEXT,
                utm_campaign TEXT,
                profit REAL DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        db.commit()

    # Add sample data to visits table if empty (demo only)
    with closing(get_db()) as db:
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM visits")
        if cur.fetchone()[0] == 0:
            sample_visits = [
                (1, 'Google Ads', 'cpc', 'summer_sale', 120.0),
                (1, 'YouTube', 'video', 'how_to_video', 200.0),
                (1, 'Facebook', 'social', 'fb_campaign', 80.0),
                (1, 'Google Ads', 'cpc', 'fall_sale', 150.0),
                (1, 'YouTube', 'video', 'review_video', 220.0),
                (1, 'Instagram', 'social', 'insta_promo', 60.0),
                (1, 'Direct', 'none', 'direct', 50.0),
                (1, 'Facebook', 'social', 'fb_campaign2', 90.0),
                (1, 'Google Ads', 'cpc', 'holiday_sale', 180.0),
                (1, 'YouTube', 'video', 'ad_video', 210.0)
            ]
            for provider_id, utm_source, utm_medium, utm_campaign, profit in sample_visits:
                cur.execute("INSERT INTO visits (provider_id, utm_source, utm_medium, utm_campaign, profit) VALUES (?, ?, ?, ?, ?)",
                            (provider_id, utm_source, utm_medium, utm_campaign, profit))
            db.commit()

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
            # Aggregate profit by UTM source for traffic analytics
            cur.execute("""
                SELECT utm_source, SUM(profit) as total_profit, COUNT(*) as visit_count
                FROM visits
                WHERE provider_id = ?
                GROUP BY utm_source
                ORDER BY total_profit DESC
            """, (current_provider_id,))
            traffic_sources = cur.fetchall()

            # Prepare data for bar chart (max profit normalization)
            max_profit = max([row[1] for row in traffic_sources], default=1)
            traffic_chart = [
                {
                    "source": row[0],
                    "profit": row[1],
                    "visits": row[2],
                    "bar_width": int((row[1] / max_profit) * 100) if max_profit else 0
                }
                for row in traffic_sources
            ]

        return render_template("provider_analytics.html", 
                                 services_count=services_count,
                                 products_count=products_count,
                                 total_leads=total_leads,
                                 total_views=(services_count + products_count) * 15,
                                 daily_leads=daily_leads,
                                 top_services=top_services,
                                 top_products=top_products,
                                 traffic_chart=traffic_chart,
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
            custom_url = request.form.get("custom_url", "").strip()

            # Social media fields
            linkedin_url = request.form.get("linkedin_url", "").strip()
            facebook_url = request.form.get("facebook_url", "").strip()
            instagram_url = request.form.get("instagram_url", "").strip()
            twitter_url = request.form.get("twitter_url", "").strip()
            website_url = request.form.get("website_url", "").strip()
            youtube_url = request.form.get("youtube_url", "").strip()

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
                        # Regular provider - update providers table including social media and custom url
                        cur.execute(
                            """
                            UPDATE providers
                            SET first_name=?, business_name=?, phone=?, base_zip=?, about=?, profile_photo=?,
                                linkedin_url=?, facebook_url=?, instagram_url=?, twitter_url=?, website_url=?, youtube_url=?, custom_url=?
                            WHERE id = ?
                            """,
                            (first_name, business_name, phone, base_zip, about, profile_photo,
                             linkedin_url, facebook_url, instagram_url, twitter_url, website_url, youtube_url, custom_url,
                             current_provider_id),
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
                social_media = {
                    "linkedin_url": "",
                    "facebook_url": "",
                    "instagram_url": "",
                    "twitter_url": "",
                    "website_url": "",
                    "youtube_url": "",
                    "custom_url": "",
                }
            else:
                # Regular provider - use providers table with social media and custom url
                cur.execute("""
                    SELECT first_name, business_name, phone, base_zip, address, about, profile_photo,
                           linkedin_url, facebook_url, instagram_url, twitter_url, website_url, youtube_url, custom_url
                    FROM providers WHERE id = ?
                """, (current_provider_id,))
                row = cur.fetchone()
                social_media = {
                    "linkedin_url": row["linkedin_url"] if row else "",
                    "facebook_url": row["facebook_url"] if row else "",
                    "instagram_url": row["instagram_url"] if row else "",
                    "twitter_url": row["twitter_url"] if row else "",
                    "website_url": row["website_url"] if row else "",
                    "youtube_url": row["youtube_url"] if row else "",
                    "custom_url": row["custom_url"] if row else "",
                }
            
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
            **social_media
        }
        return render_template("provider_profile.html", form=form, title="Provider Profile", leads_count=leads_count)
    
    # Exempt provider_profile from CSRF
    if csrf:
        csrf.exempt(provider_profile)

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

    # Products CRUD
    @app.route("/admin/products")
    @login_required
    def admin_products():
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute(
                "SELECT p.id, p.title, p.description, p.price, p.active, p.created_at, p.provider_id, pr.business_name, pr.first_name FROM products p LEFT JOIN providers pr ON p.provider_id = pr.id ORDER BY p.created_at DESC"
            )
            rows = cur.fetchall()
        return render_template("admin_products.html", products=rows, title="Manage Products")

    @app.route("/admin/products/new", methods=["GET", "POST"])
    @login_required
    def admin_product_new():
        if request.method == "POST":
            title = request.form.get("title", "").strip()
            description = request.form.get("description", "").strip()
            price = request.form.get("price", "").strip()
            provider_id = int(request.form.get("provider_id", 0))
            active = 1 if request.form.get("active") == "on" else 0

            if not title:
                flash("Title is required.", "error")
                return render_template("admin_product_form.html", form=request.form, mode="new", title="Add Product")

            with closing(get_db()) as db:
                cur = db.cursor()
                cur.execute(
                    """
                    INSERT INTO products (title, description, price, provider_id, active, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (title, description, price, provider_id, active, datetime.datetime.utcnow().isoformat()),
                )
                db.commit()
                flash("Product added.", "success")
                return redirect(url_for("admin_products"))

        # Get providers for dropdown
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("SELECT id, first_name, business_name FROM providers WHERE active = 1 ORDER BY business_name")
            providers = cur.fetchall()
        
        return render_template("admin_product_form.html", form={}, mode="new", providers=providers, title="Add Product")

    @app.route("/admin/products/<int:product_id>/edit", methods=["GET", "POST"])
    @login_required
    def admin_product_edit(product_id):
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("SELECT id, title, description, price, provider_id, active FROM products WHERE id = ?", (product_id,))
            product = cur.fetchone()
            if not product:
                abort(404)

        if request.method == "POST":
            title = request.form.get("title", "").strip()
            description = request.form.get("description", "").strip()
            price = request.form.get("price", "").strip()
            provider_id = int(request.form.get("provider_id", 0))
            active = 1 if request.form.get("active") == "on" else 0

            if not title:
                flash("Title is required.", "error")
                return render_template(
                    "admin_product_form.html", form=request.form, mode="edit", product=product, title="Edit Product"
                )

            with closing(get_db()) as db:
                cur = db.cursor()
                cur.execute(
                    """
                    UPDATE products
                    SET title = ?, description = ?, price = ?, provider_id = ?, active = ?
                    WHERE id = ?
                """,
                    (title, description, price, provider_id, active, product_id),
                )
                db.commit()
            flash("Product updated.", "success")
            return redirect(url_for("admin_products"))

        # Get providers for dropdown
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("SELECT id, first_name, business_name FROM providers WHERE active = 1 ORDER BY business_name")
            providers = cur.fetchall()

        return render_template(
            "admin_product_form.html",
            form=dict(
                title=product["title"],
                description=product["description"],
                price=product["price"],
                provider_id=product["provider_id"],
                active=bool(product["active"]),
            ),
            mode="edit",
            product=product,
            providers=providers,
            title="Edit Product",
        )

    @app.post("/admin/products/<int:product_id>/delete")
    @login_required
    def admin_product_delete(product_id):
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("DELETE FROM products WHERE id = ?", (product_id,))
            db.commit()
        flash("Product deleted.", "info")
        return redirect(url_for("admin_products"))

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


    @app.route("/u/<custom_url>")
    def provider_custom_profile(custom_url):
        with closing(get_db()) as db:
            cur = db.cursor()
            cur.execute("""
                SELECT id FROM providers WHERE custom_url = ? AND active = 1
            """, (custom_url,))
            row = cur.fetchone()
            if not row:
                abort(404)
            provider_id = row["id"]
        return redirect(url_for("public_provider_profile", provider_id=provider_id))

    # End of all route definitions
    print('DEBUG: Returning app from create_app()')
    return app

app = create_app()


if __name__ == "__main__":
    # Convenient dev run: python app.py
    app.run(host="127.0.0.1", port=5000, debug=False, use_reloader=False)
