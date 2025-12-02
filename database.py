import sqlite3
from config import DB_FILE

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("PRAGMA foreign_keys = ON;")
    
    # 1. BIBLIO MASTER
    c.execute("""
        CREATE TABLE IF NOT EXISTS biblio_master (
            biblio_id INTEGER PRIMARY KEY,
            title TEXT, author TEXT, edition TEXT, isbn TEXT,
            pub_place TEXT, pub_publisher TEXT, pub_year INTEGER,
            page_count INTEGER, language TEXT, item_type TEXT
        );
    """)

    # 2. PHYSICAL ITEMS (Enhanced)
    c.execute("""
        CREATE TABLE IF NOT EXISTS physical_items (
            item_id INTEGER PRIMARY KEY AUTOINCREMENT,
            biblio_id INTEGER NOT NULL,
            barcode TEXT,
            call_number TEXT,
            shelving_location TEXT,
            library_code TEXT,
            vendor TEXT,          -- Captured Intelligent Vendor
            bill_number TEXT,     -- Captured Intelligent Bill No
            price REAL,
            currency TEXT,
            bill_date DATE,
            date_acquired DATE,
            last_seen_date DATE,
            is_withdrawn INTEGER, is_lost INTEGER, is_damaged INTEGER, is_restricted INTEGER,
            FOREIGN KEY(biblio_id) REFERENCES biblio_master(biblio_id)
        );
    """)
    conn.commit()
    return conn