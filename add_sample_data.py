#!/usr/bin/env python3
"""
Sample Data Generator for Service Provider Platform
Adds realistic fake data for testing and preview purposes
"""

import sqlite3
import datetime
import random
from werkzeug.security import generate_password_hash

# Database path
DB_PATH = "instance/site.db"

# Sample data
SAMPLE_PROVIDERS = [
    {
        "email": "sarah@cleanpro.com",
        "first_name": "Sarah",
        "business_name": "CleanPro Services LLC",
        "phone": "(555) 123-4567",
        "base_zip": "85379",
        "about": "Licensed and insured professional cleaning services with 10+ years experience. EPA certified eco-friendly products."
    },
    {
        "email": "mike@handyfix.com", 
        "first_name": "Mike",
        "business_name": "HandyFix Solutions Inc",
        "phone": "(555) 234-5678",
        "base_zip": "85374",
        "about": "Licensed contractor #CR-12345. Bonded and insured home repair specialist with same-day service available."
    },
    {
        "email": "lisa@gardencare.com",
        "first_name": "Lisa", 
        "business_name": "Garden Care Plus LLC",
        "phone": "(555) 345-6789",
        "base_zip": "85375",
        "about": "Certified landscaping professional. Licensed irrigation specialist. Transform your outdoor space with expertise."
    },
    {
        "email": "carlos@techhelp.com",
        "first_name": "Carlos",
        "business_name": "Independent Contributor",
        "phone": "(555) 456-7890", 
        "base_zip": "85376",
        "about": "Computer repair and tech support. Fast, reliable solutions for all your technology needs."
    },
    {
        "email": "jennifer@propainting.com",
        "first_name": "Jennifer",
        "business_name": "Pro Painting Services LLC",
        "phone": "(555) 567-8901",
        "base_zip": "85377",
        "about": "Licensed painting contractor. Fully insured residential and commercial painting with warranty guarantee."
    },
    {
        "email": "david@eliteelectric.com",
        "first_name": "David",
        "business_name": "Elite Electrical Corp",
        "phone": "(555) 678-9012",
        "base_zip": "85378",
        "about": "Master electrician license #E-98765. 24/7 emergency electrical services. Residential and commercial."
    }
]

SAMPLE_SERVICES = [
    # Sarah's services
    {"title": "Deep House Cleaning", "description": "Complete deep cleaning service including bathrooms, kitchen, bedrooms, and living areas", "price": "$150-200"},
    {"title": "Weekly Maintenance Cleaning", "description": "Regular weekly cleaning to keep your home spotless", "price": "$80-120"},
    {"title": "Move-in/Move-out Cleaning", "description": "Thorough cleaning for property transitions", "price": "$200-300"},
    {"title": "Office Cleaning", "description": "Professional office and commercial space cleaning", "price": "$100-180"},
    
    # Mike's services  
    {"title": "Bathroom Remodeling", "description": "Complete bathroom renovation and repair services", "price": "$2000-5000"},
    {"title": "Kitchen Cabinet Repair", "description": "Fix and restore kitchen cabinets to like-new condition", "price": "$300-800"},
    {"title": "Drywall Repair", "description": "Professional drywall patching and painting", "price": "$150-400"},
    {"title": "Plumbing Repairs", "description": "Licensed plumbing repairs and installations", "price": "$100-500"},
    
    # Lisa's services
    {"title": "Lawn Mowing Service", "description": "Regular lawn mowing and edging for beautiful yards", "price": "$40-80"},
    {"title": "Garden Design", "description": "Custom garden design and landscaping consultation", "price": "$200-600"},
    {"title": "Tree Trimming", "description": "Professional tree trimming and pruning services", "price": "$150-400"},
    {"title": "Sprinkler Installation", "description": "Install and repair irrigation systems", "price": "$800-2000"},
    
    # Carlos's services
    {"title": "Computer Repair", "description": "Hardware and software troubleshooting and repair", "price": "$75-200"},
    {"title": "Virus Removal", "description": "Remove malware and optimize computer performance", "price": "$100-150"},
    {"title": "Data Recovery", "description": "Recover lost files and data from damaged drives", "price": "$200-500"},
    {"title": "Network Setup", "description": "Home and office network installation and configuration", "price": "$150-350"}
]

SAMPLE_PRODUCTS = [
    # Sarah's products
    {"title": "Eco-Friendly Cleaning Kit", "description": "Professional-grade eco-friendly cleaning supplies bundle", "price": "$45"},
    {"title": "Microfiber Cloth Set", "description": "Premium microfiber cloths for streak-free cleaning", "price": "$25"},
    {"title": "All-Purpose Cleaner", "description": "Concentrated all-purpose cleaner, makes 32 bottles", "price": "$18"},
    
    # Mike's products
    {"title": "Basic Tool Kit", "description": "Essential tools for basic home repairs and maintenance", "price": "$120"},
    {"title": "Cabinet Hardware Set", "description": "Modern cabinet handles and hinges for kitchen upgrades", "price": "$85"},
    {"title": "Wall Repair Kit", "description": "Everything needed for small wall repairs and touch-ups", "price": "$35"},
    
    # Lisa's products
    {"title": "Garden Starter Pack", "description": "Seeds, soil, and basic tools for new gardeners", "price": "$65"},
    {"title": "Sprinkler Heads (4-pack)", "description": "Replacement sprinkler heads for irrigation systems", "price": "$28"},
    {"title": "Organic Fertilizer", "description": "All-natural fertilizer for healthy plant growth", "price": "$32"},
    
    # Carlos's products
    {"title": "USB Recovery Drive", "description": "Bootable USB drive with recovery and diagnostic tools", "price": "$40"},
    {"title": "Ethernet Cable Pack", "description": "High-quality Cat6 ethernet cables in various lengths", "price": "$22"},
    {"title": "Wireless Router", "description": "High-performance wireless router for home networks", "price": "$180"}
]

SAMPLE_LEADS = [
    {"name": "Jennifer Smith", "email": "jennifer@email.com", "phone": "(555) 111-2222", "zip": "85379", "message": "I need Deep House Cleaning for my 3-bedroom home this weekend"},
    {"name": "Robert Johnson", "email": "rob.j@email.com", "phone": "(555) 333-4444", "zip": "85374", "message": "Looking for Bathroom Remodeling services. Can you provide an estimate?"},
    {"name": "Maria Garcia", "email": "maria.g@email.com", "phone": "(555) 555-6666", "zip": "85375", "message": "Need Lawn Mowing Service weekly for large backyard"},
    {"name": "David Chen", "email": "david.chen@email.com", "phone": "(555) 777-8888", "zip": "85376", "message": "Computer Repair needed - laptop won't start"},
    {"name": "Amanda Wilson", "email": "amanda.w@email.com", "phone": "(555) 999-0000", "zip": "85379", "message": "Interested in Weekly Maintenance Cleaning service"},
    {"name": "James Brown", "email": "j.brown@email.com", "phone": "(555) 222-3333", "zip": "85374", "message": "Need Kitchen Cabinet Repair - doors are sagging"},
    {"name": "Lisa Rodriguez", "email": "lisa.r@email.com", "phone": "(555) 444-5555", "zip": "85375", "message": "Garden Design consultation for front yard makeover"},
    {"name": "Michael Davis", "email": "mike.d@email.com", "phone": "(555) 666-7777", "zip": "85376", "message": "Virus Removal and computer cleanup needed"},
    {"name": "Sarah Thompson", "email": "sarah.t@email.com", "phone": "(555) 888-9999", "zip": "85379", "message": "Move-in cleaning for new house, also interested in Eco-Friendly Cleaning Kit"},
    {"name": "Christopher Lee", "email": "chris.lee@email.com", "phone": "(555) 101-2020", "zip": "85374", "message": "Plumbing Repairs - kitchen sink leak"},
    {"name": "Jessica Martinez", "email": "jessica.m@email.com", "phone": "(555) 303-4040", "zip": "85375", "message": "Tree Trimming for large oak tree in backyard"},
    {"name": "Kevin Anderson", "email": "kevin.a@email.com", "phone": "(555) 505-6060", "zip": "85376", "message": "Network Setup for home office, also need Ethernet Cable Pack"},
    {"name": "Nicole White", "email": "nicole.w@email.com", "phone": "(555) 707-8080", "zip": "85379", "message": "Office Cleaning service for small business"},
    {"name": "Daniel Taylor", "email": "daniel.t@email.com", "phone": "(555) 909-1010", "zip": "85374", "message": "Drywall Repair after water damage"},
    {"name": "Rachel Green", "email": "rachel.g@email.com", "phone": "(555) 121-3030", "zip": "85375", "message": "Sprinkler Installation for new landscaping project"}
]

# Sample service areas (ZIP codes) for providers
SAMPLE_SERVICE_AREAS = [
    # Sarah's service areas (CleanPro Services) - West Valley area
    {"zip_code": "85379", "radius_miles": 15},  # Surprise
    {"zip_code": "85374", "radius_miles": 10},  # Sun City West
    {"zip_code": "85375", "radius_miles": 12},  # Sun City
    {"zip_code": "85378", "radius_miles": 10},  # Sun City West
    {"zip_code": "85373", "radius_miles": 8},   # Sun City
    
    # Mike's service areas (HandyFix Solutions) - Northwest Phoenix
    {"zip_code": "85308", "radius_miles": 20},  # Glendale
    {"zip_code": "85301", "radius_miles": 15},  # Glendale
    {"zip_code": "85310", "radius_miles": 12},  # Glendale
    {"zip_code": "85302", "radius_miles": 18},  # Glendale
    
    # Lisa's service areas (Garden Care Plus) - Scottsdale area
    {"zip_code": "85260", "radius_miles": 15},  # Scottsdale
    {"zip_code": "85259", "radius_miles": 12},  # Scottsdale
    {"zip_code": "85262", "radius_miles": 10},  # Scottsdale
    {"zip_code": "85251", "radius_miles": 18},  # Scottsdale
    {"zip_code": "85254", "radius_miles": 14},  # Scottsdale
    
    # Carlos's service areas (Tech Support) - Central Phoenix
    {"zip_code": "85016", "radius_miles": 20},  # Phoenix
    {"zip_code": "85018", "radius_miles": 15},  # Phoenix
    {"zip_code": "85020", "radius_miles": 12},  # Phoenix
]

def add_sample_data():
    """Add sample data to the database"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("Adding sample providers...")
    provider_ids = []
    
    for i, provider in enumerate(SAMPLE_PROVIDERS):
        # Check if provider already exists
        cursor.execute("SELECT id FROM providers WHERE email = ?", (provider["email"],))
        existing_provider = cursor.fetchone()
        
        if existing_provider:
            print(f"  Provider {provider['business_name']} already exists, using existing ID...")
            provider_ids.append(existing_provider[0])
            continue
            
        cursor.execute("""
            INSERT INTO providers (email, password_hash, first_name, business_name, phone, base_zip, about, active, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
        """, (
            provider["email"],
            generate_password_hash("password123"),  # Default password for all test providers
            provider["first_name"],
            provider["business_name"], 
            provider["phone"],
            provider["base_zip"],
            provider["about"],
            datetime.datetime.utcnow().isoformat()
        ))
        provider_ids.append(cursor.lastrowid)
        print(f"  Added provider: {provider['business_name']}")
    
    print("\nAdding sample services...")
    
    for i, service in enumerate(SAMPLE_SERVICES):
        provider_id = provider_ids[i % len(provider_ids)]  # Use modulo to cycle through providers
        provider_name = SAMPLE_PROVIDERS[i % len(SAMPLE_PROVIDERS)]["first_name"]
        
        cursor.execute("""
            INSERT INTO services (title, description, price, posted_by, provider_id, active, created_at)
            VALUES (?, ?, ?, ?, ?, 1, ?)
        """, (
            service["title"],
            service["description"],
            service["price"],
            provider_name,
            provider_id,
            datetime.datetime.utcnow().isoformat()
        ))
        print(f"  Added service: {service['title']} (Provider: {provider_name})")
    
    print("\nAdding sample products...")
    products_per_provider = len(SAMPLE_PRODUCTS) // len(SAMPLE_PROVIDERS)
    
    for i, product in enumerate(SAMPLE_PRODUCTS):
        provider_id = provider_ids[i // products_per_provider]
        provider_name = SAMPLE_PROVIDERS[i // products_per_provider]["first_name"]
        
        cursor.execute("""
            INSERT INTO products (title, description, price, provider_id, active, created_at)
            VALUES (?, ?, ?, ?, 1, ?)
        """, (
            product["title"],
            product["description"],
            product["price"],
            provider_id,
            datetime.datetime.utcnow().isoformat()
        ))
        print(f"  Added product: {product['title']} (Provider: {provider_name})")
    
    print("\nAdding sample leads...")
    
    for i, lead in enumerate(SAMPLE_LEADS):
        provider_id = provider_ids[i % len(provider_ids)]  # Use modulo to cycle through providers
        provider_name = SAMPLE_PROVIDERS[i % len(provider_ids)]["first_name"]
        
        # Vary the creation dates to show trends
        days_ago = random.randint(0, 30)
        created_at = (datetime.datetime.utcnow() - datetime.timedelta(days=days_ago)).isoformat()
        
        cursor.execute("""
            INSERT INTO leads (name, email, phone, zip, message, provider_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            lead["name"],
            lead["email"],
            lead["phone"],
            lead["zip"],
            lead["message"],
            provider_id,
            created_at
        ))
        print(f"  Added lead: {lead['name']} (Provider: {provider_name})")
    
    print("\nAdding sample service areas...")
    areas_per_provider = 5  # Max 5 areas per provider
    
    for i, area in enumerate(SAMPLE_SERVICE_AREAS):
        provider_index = i // areas_per_provider
        if provider_index >= len(provider_ids):
            break  # Don't exceed available providers
            
        provider_id = provider_ids[provider_index]
        provider_name = SAMPLE_PROVIDERS[provider_index]["first_name"]
        
        # Check if this service area already exists
        cursor.execute("SELECT id FROM provider_zips WHERE provider_id = ? AND zip_code = ?", 
                      (provider_id, area["zip_code"]))
        if cursor.fetchone():
            print(f"  Service area {area['zip_code']} for {provider_name} already exists, skipping...")
            continue
        
        cursor.execute("""
            INSERT INTO provider_zips (provider_id, zip_code, radius_miles, created_at)
            VALUES (?, ?, ?, ?)
        """, (
            provider_id,
            area["zip_code"],
            area["radius_miles"],
            datetime.datetime.utcnow().isoformat()
        ))
        print(f"  Added service area: {area['zip_code']} ({area['radius_miles']} mi) for {provider_name}")
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ Sample data added successfully!")
    print(f"   - {len(SAMPLE_PROVIDERS)} providers")
    print(f"   - {len(SAMPLE_SERVICES)} services") 
    print(f"   - {len(SAMPLE_PRODUCTS)} products")
    print(f"   - {len(SAMPLE_LEADS)} leads")
    print(f"   - {len(SAMPLE_SERVICE_AREAS)} service areas")
    print(f"\nTest provider credentials:")
    for provider in SAMPLE_PROVIDERS:
        print(f"   Email: {provider['email']} | Password: password123")

if __name__ == "__main__":
    add_sample_data()