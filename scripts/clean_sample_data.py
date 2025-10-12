import sqlite3, os
DB = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'instance', 'site.db'))
print('DB path:', DB)
if not os.path.exists(DB):
    print('Database not found. Nothing to clean.')
    raise SystemExit(0)

conn = sqlite3.connect(DB)
c = conn.cursor()

# Clear UTM visit demo data
try:
    c.execute('DELETE FROM visits')
    print('Cleared visits table')
except Exception as e:
    print('visits table not found or cannot be cleared:', e)

# Clear obvious sample products
sample_titles = [
    'Wireless Router','Ethernet Cable Pack','USB Recovery Drive','Organic Fertilizer',
    'Sprinkler Heads (4-pack)','Garden Starter Pack','Wall Repair Kit','Cabinet Hardware Set',
    'Basic Tool Kit'
]
try:
    c.executemany('DELETE FROM products WHERE title = ?', [(t,) for t in sample_titles])
    print('Deleted products by exact titles (if present).')
except Exception as e:
    print('Could not delete by title:', e)

# Also remove admin-owned generic samples
try:
    c.execute("DELETE FROM products WHERE provider_id = 0")
    print('Deleted admin-owned products (provider_id=0).')
except Exception as e:
    print('Could not delete provider_id=0 products:', e)

conn.commit()
conn.close()
print('Cleanup complete.')
