-- Migration: Create visits table for UTM tracking and profit
CREATE TABLE IF NOT EXISTS visits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_id INTEGER,
    utm_source TEXT,
    utm_medium TEXT,
    utm_campaign TEXT,
    profit REAL DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
