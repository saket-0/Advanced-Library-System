import json
import sqlite3
import re
from datetime import datetime

# CONFIGURATION
INPUT_FILE = 'vit_library_master.jsonl'
DB_FILE = 'comprehensive_library_v2.db'

def clean_item_type(raw_type):
    if not raw_type: return "UNKNOWN"
    return raw_type.split()[0].upper()

def extract_year(pub_string):
    if not pub_string: return None
    match = re.search(r'\b(19|20)\d{2}\b', pub_string)
    return int(match.group(0)) if match else None

def parse_holdings_maximalist(holdings_str):
    """
    Advanced Parser to extract Shelf, Location, and strict Price.
    Targeting patterns seen in the XML dump.
    """
    if not holdings_str or not isinstance(holdings_str, str) or len(holdings_str) < 5:
        return None

    data = {
        'price': None,
        'date_acquired': None,
        'barcode': None,
        'call_number': None,
        'shelving_location': None, # New: captures "IIF-R76-C2-A"
        'library_code': None       # New: captures "VIT"
    }
    
    tokens = holdings_str.split()

    # 1. SHELVING LOCATION (The "IIF-R76-C2-A" pattern)
    # Looks for alphanumeric segments separated by hyphens
    # Must have at least 2 hyphens to differentiate from dates/ISBNs
    # Example Regex logic: [Chars]-[Chars]-[Chars]
    for token in tokens:
        # Regex: Start with chars, hyphen, chars, hyphen, chars...
        if re.match(r'^[A-Za-z0-9]+-[A-Za-z0-9]+-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)?$', token):
            # Exclude things that look like dates (YYYY-MM-DD)
            if not re.match(r'^\d{4}-\d{2}-\d{2}$', token):
                data['shelving_location'] = token
                break

    # 2. PRICE (Strict extraction)
    # XML shows "250.00". We look for purely numeric with 2 decimals.
    for token in tokens:
        if re.match(r'^\d+\.\d{2}$', token): 
            try:
                data['price'] = float(token)
                break
            except: pass

    # 3. DATE ACQUIRED
    for token in tokens:
        if re.match(r'\d{4}-\d{2}-\d{2}', token): 
            data['date_acquired'] = token
            # Don't break immediately, as later dates might be the "Last Seen" date
            # But usually the first ISO date is acquisition or print date
    
    # 4. LIBRARY CODE (Heuristic)
    # Looking for common codes like "VIT". 
    # This is tricky in regex, so we look for uppercase strings of length 3-4 
    # that are NOT the ItemType (BK) or dates.
    for token in tokens:
        if token.isupper() and 3 <= len(token) <= 4 and token.isalpha():
            if token not in ["None", "Null", "TRUE", "FALSE"]:
                 # Just capture the first valid-looking code
                 data['library_code'] = token
                 break

    # 5. BARCODE (Long numeric near end)
    for token in reversed(tokens): 
        if token.isdigit() and len(token) > 4:
            # Ensure it's not the price
            if data['price'] and float(token) == data['price']: continue
            data['barcode'] = token
            break

    # 6. CALL NUMBER
    # Usually contains decimals (Dewey) or colons.
    for token in tokens:
        if (':' in token or re.search(r'\d{3}\.', token)) and token != data['shelving_location']:
            data['call_number'] = token
            break
            
    return data

def run_migration():
    print(f"Creating Maximalist Database: {DB_FILE}...")
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # --- UPDATED SCHEMA ---
    c.execute("PRAGMA foreign_keys = ON;")
    c.executescript("""
        CREATE TABLE IF NOT EXISTS biblio_master (
            biblio_id INTEGER PRIMARY KEY,
            title TEXT,
            author TEXT,
            edition TEXT,
            isbn TEXT,
            publication_info TEXT,
            pub_year INTEGER,
            physical_desc TEXT,
            dewey_class TEXT,
            subject TEXT,
            notes TEXT,
            item_type TEXT,
            raw_json_dump TEXT
        );

        CREATE TABLE IF NOT EXISTS physical_items (
            item_id INTEGER PRIMARY KEY AUTOINCREMENT,
            biblio_id INTEGER NOT NULL,
            barcode TEXT,
            call_number TEXT,
            shelving_location TEXT,  -- New Column
            library_code TEXT,       -- New Column
            price REAL,
            date_acquired DATE,
            original_holding_string TEXT,
            FOREIGN KEY(biblio_id) REFERENCES biblio_master(biblio_id)
        );
        
        CREATE INDEX IF NOT EXISTS idx_shelf ON physical_items(shelving_location);
    """)

    print("Schema updated. Starting comprehensive ingestion...")

    batch_biblio = []
    batch_items = []
    
    # Counters
    count_items = 0
    count_shelves_found = 0

    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            try:
                rec = json.loads(line)
                
                # --- BIBLIO ---
                b_data = (
                    int(rec.get('id', 0)),
                    rec.get('245', 'Untitled'),
                    rec.get('100', None),
                    rec.get('250', None),
                    rec.get('020', None),
                    rec.get('260', None),
                    extract_year(rec.get('260', '')),
                    rec.get('300', None),
                    rec.get('082', None),
                    rec.get('650', None),
                    rec.get('500', None),
                    clean_item_type(rec.get('942')),
                    line
                )
                batch_biblio.append(b_data)

                # --- ITEMS ---
                raw_952 = rec.get('952', '')
                parsed = parse_holdings_maximalist(raw_952)
                
                if parsed:
                    if parsed['shelving_location']: count_shelves_found += 1
                    
                    i_data = (
                        int(rec.get('id', 0)),
                        parsed['barcode'],
                        parsed['call_number'],
                        parsed['shelving_location'], # Storing the "IIF..." code
                        parsed['library_code'],      # Storing "VIT"
                        parsed['price'],
                        parsed['date_acquired'],
                        str(raw_952)
                    )
                    batch_items.append(i_data)
                    count_items += 1

                if len(batch_biblio) >= 5000:
                    c.executemany("INSERT OR REPLACE INTO biblio_master VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", batch_biblio)
                    c.executemany("INSERT INTO physical_items (biblio_id, barcode, call_number, shelving_location, library_code, price, date_acquired, original_holding_string) VALUES (?,?,?,?,?,?,?,?)", batch_items)
                    conn.commit()
                    batch_biblio = []
                    batch_items = []
                    print(f"Processed {i} records... (Found {count_shelves_found} shelf locations so far)")

            except Exception as e:
                pass # Silent fail for speed, or log if needed

        # Final Commit
        if batch_biblio:
            c.executemany("INSERT OR REPLACE INTO biblio_master VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", batch_biblio)
            c.executemany("INSERT INTO physical_items (biblio_id, barcode, call_number, shelving_location, library_code, price, date_acquired, original_holding_string) VALUES (?,?,?,?,?,?,?,?)", batch_items)
            conn.commit()

    conn.close()
    print(f"\n--- MIGRATION COMPLETE ---")
    print(f"Total Physical Items: {count_items}")
    print(f"Shelf Locations Extracted: {count_shelves_found}")
    print(f"Database: {DB_FILE}")

if __name__ == "__main__":
    run_migration()