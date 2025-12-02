import json
import sqlite3
import re
from datetime import datetime

# CONFIGURATION
INPUT_FILE = 'vit_library_master.jsonl'  # Your JSONL file path
DB_FILE = 'comprehensive_library.db' # New DB name

def clean_item_type(raw_type):
    """Strips 'ddc' noise from item types."""
    if not raw_type: return "UNKNOWN"
    clean = raw_type.split()[0].upper()
    return clean

def extract_year(pub_string):
    """Extracts year safely."""
    if not pub_string: return None
    match = re.search(r'\b(19|20)\d{2}\b', pub_string)
    return int(match.group(0)) if match else None

def parse_holdings_952(holdings_str):
    """
    Extracts Copy data (Price, Barcode, Call No) from the messy 952 string.
    Returns None if the string is too short/empty.
    """
    if not holdings_str or not isinstance(holdings_str, str) or len(holdings_str) < 5:
        return None

    data = {
        'price': None,
        'date_acquired': None,
        'barcode': None,
        'call_number': None
    }
    
    tokens = holdings_str.split()

    # 1. PRICE (Look for decimal patterns like 295.00)
    for token in tokens:
        # Matches numbers with exactly 2 decimals, avoids dates
        if re.match(r'^\d+\.\d{2}$', token): 
            try:
                data['price'] = float(token)
                break
            except: pass

    # 2. DATE (ISO or UK format)
    for token in tokens:
        if re.match(r'\d{4}-\d{2}-\d{2}', token): # 2025-12-01
            data['date_acquired'] = token
            break
        elif re.match(r'\d{2}/\d{2}/\d{4}', token): # 01/12/2025
            try:
                dt = datetime.strptime(token, "%d/%m/%Y")
                data['date_acquired'] = dt.strftime("%Y-%m-%d")
                break
            except: pass

    # 3. BARCODE (Heuristic: Long number near end, distinct from price)
    for token in reversed(tokens): 
        if token.isdigit() and len(token) > 4:
            # Ensure it's not the price we just found (as integer)
            is_price = False
            if data['price'] and float(token) == data['price']: is_price = True
            
            if not is_price:
                data['barcode'] = token
                break

    # 4. CALL NUMBER (Heuristic: Contains decimals/colons, e.g. 621.7:744)
    for token in tokens:
        # Looks for Dewey patterns (digits dot digits) or colons
        if re.search(r'\d{3}\.', token) or ':' in token: 
            data['call_number'] = token
            break
            
    return data

def run_comprehensive_migration():
    print(f"Creating Comprehensive Database: {DB_FILE}...")
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # --- 1. DEFINE ROBUST SCHEMA ---
    # We enable Foreign Keys to ensure data integrity
    c.execute("PRAGMA foreign_keys = ON;")
    
    c.executescript("""
        -- Master Table for Bibliographic Data (The "Intellectual" Work)
        CREATE TABLE IF NOT EXISTS biblio_master (
            biblio_id INTEGER PRIMARY KEY,  -- Strictly the JSON 'id'
            title TEXT,                     -- 245
            author TEXT,                    -- 100
            edition TEXT,                   -- 250 (Added)
            isbn TEXT,                      -- 020
            publication_info TEXT,          -- 260 (Full Text)
            pub_year INTEGER,               -- 260 (Extracted)
            physical_desc TEXT,             -- 300 (Added)
            dewey_class TEXT,               -- 082 (Added)
            subject TEXT,                   -- 650 (Added)
            notes TEXT,                     -- 500 (Added)
            item_type TEXT,                 -- 942 (Cleaned)
            raw_json_dump TEXT              -- Full backup of the record
        );

        -- Child Table for Physical Items (The "Inventory")
        CREATE TABLE IF NOT EXISTS physical_items (
            item_id INTEGER PRIMARY KEY AUTOINCREMENT,
            biblio_id INTEGER NOT NULL,
            barcode TEXT,
            call_number TEXT,
            price REAL,
            date_acquired DATE,
            original_holding_string TEXT,   -- 952 (Full Text)
            FOREIGN KEY(biblio_id) REFERENCES biblio_master(biblio_id)
        );

        -- Indexes for fast searching later
        CREATE INDEX IF NOT EXISTS idx_title ON biblio_master(title);
        CREATE INDEX IF NOT EXISTS idx_author ON biblio_master(author);
        CREATE INDEX IF NOT EXISTS idx_barcode ON physical_items(barcode);
    """)

    print("Schema created. Starting ingestion...")

    count_biblio = 0
    count_items = 0
    errors = 0

    batch_biblio = []
    batch_items = []

    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            try:
                rec = json.loads(line)
                
                # --- EXTRACT BIBLIO DATA ---
                # Using .get() ensures we grab data if it exists, or None if not
                b_data = (
                    int(rec.get('id', 0)),              # biblio_id
                    rec.get('245', 'Untitled'),         # title
                    rec.get('100', None),               # author
                    rec.get('250', None),               # edition
                    rec.get('020', None),               # isbn
                    rec.get('260', None),               # publication_info
                    extract_year(rec.get('260', '')),   # pub_year
                    rec.get('300', None),               # physical_desc
                    rec.get('082', None),               # dewey_class
                    rec.get('650', None),               # subject
                    rec.get('500', None),               # notes
                    clean_item_type(rec.get('942')),    # item_type
                    line                                # raw_json_dump
                )
                batch_biblio.append(b_data)
                count_biblio += 1

                # --- EXTRACT ITEM DATA ---
                raw_952 = rec.get('952', '')
                parsed_item = parse_holdings_952(raw_952)
                
                # Even if parsing is partial, we store the record linked to the biblio
                if parsed_item:
                    i_data = (
                        int(rec.get('id', 0)),          # biblio_id (FK)
                        parsed_item['barcode'],
                        parsed_item['call_number'],
                        parsed_item['price'],
                        parsed_item['date_acquired'],
                        str(raw_952)                    # Store original string
                    )
                    batch_items.append(i_data)
                    count_items += 1

                # Batch Commit every 5000 records
                if len(batch_biblio) >= 5000:
                    c.executemany("""
                        INSERT OR REPLACE INTO biblio_master 
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, batch_biblio)
                    
                    c.executemany("""
                        INSERT INTO physical_items (biblio_id, barcode, call_number, price, date_acquired, original_holding_string)
                        VALUES (?,?,?,?,?,?)
                    """, batch_items)
                    
                    conn.commit()
                    batch_biblio = []
                    batch_items = []
                    print(f"Processed {i} records...")

            except Exception as e:
                errors += 1
                print(f"Error on line {i}: {e}")

        # Final Commit
        if batch_biblio:
            c.executemany("INSERT OR REPLACE INTO biblio_master VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", batch_biblio)
            c.executemany("INSERT INTO physical_items (biblio_id, barcode, call_number, price, date_acquired, original_holding_string) VALUES (?,?,?,?,?,?)", batch_items)
            conn.commit()

    conn.close()
    print(f"\n--- SUCCESS ---")
    print(f"Total Unique Titles (Biblio): {count_biblio}")
    print(f"Total Physical Items Found: {count_items}")
    print(f"Errors: {errors}")
    print(f"Data saved strictly to: {DB_FILE}")

if __name__ == "__main__":
    run_comprehensive_migration()