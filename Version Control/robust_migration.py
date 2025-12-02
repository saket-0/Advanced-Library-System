import json
import sqlite3
import re
from datetime import datetime

# CONFIGURATION
INPUT_FILE = 'library_data.jsonl'
DB_FILE = 'robust_library_system.db'

class SmartParser:
    """
    A modular parser that uses context clues rather than just regex patterns.
    It separates 'Bill Dates' from 'Shelf Locations' by checking for alpha-characters.
    """
    
    def __init__(self, raw_string):
        self.raw = str(raw_string) if raw_string else ""
        self.tokens = self.raw.split()
        self.data = {
            'price': None,
            'currency': 'INR', # Default assumption based on data
            'barcode': None,
            'call_number': None,
            'shelving_location': None,
            'library_code': None,
            'bill_date': None,       # The old date (1984)
            'date_acquired': None,   # The middle date (2007)
            'last_seen_date': None,  # The new date (2019/2025)
            'vendor': None           # "SRI BALAJI..."
        }

    def parse(self):
        if not self.raw or len(self.raw) < 5:
            return None

        # --- MODULE 1: EXTRACT DATES & SORT THEM ---
        # We extract ALL dates first to remove them from the token pool
        # This prevents 15-12-1984 from being mistaken for a Shelf Code.
        date_pattern = re.compile(r'(\d{4}-\d{2}-\d{2}|\d{2}-\d{2}-\d{4}|\d{2}/\d{2}/\d{4})')
        found_dates = []
        
        cleaned_tokens = []
        for token in self.tokens:
            if date_pattern.match(token):
                # Normalize to YYYY-MM-DD
                try:
                    # Handle DD-MM-YYYY or DD/MM/YYYY
                    if '-' in token and token[2] == '-': 
                        d = datetime.strptime(token, "%d-%m-%Y")
                    elif '/' in token:
                        d = datetime.strptime(token, "%d/%m/%Y")
                    else:
                        d = datetime.strptime(token, "%Y-%m-%d")
                    found_dates.append(d)
                except:
                    pass # Invalid date, treat as text
            else:
                cleaned_tokens.append(token)
        
        # Sort dates: Oldest -> Bill/Pub, Middle -> Acquired, Newest -> Last Seen
        if found_dates:
            found_dates.sort()
            if len(found_dates) == 1:
                self.data['date_acquired'] = found_dates[0].date()
            elif len(found_dates) >= 2:
                # Heuristic: Oldest is Bill/Pub, Newest is Last Seen/Acquired
                self.data['bill_date'] = found_dates[0].date()
                self.data['last_seen_date'] = found_dates[-1].date()
                # If there's a middle date, that's likely the true acquisition
                if len(found_dates) > 2:
                    self.data['date_acquired'] = found_dates[1].date()
                else:
                    self.data['date_acquired'] = found_dates[-1].date()

        # --- MODULE 2: SHELVING LOCATION (Strict) ---
        # MUST contain at least one Letter to avoid matching dates.
        # Pattern: Characters-Characters-Characters (IIF-R17-C4-D)
        shelf_idx = -1
        for i, token in enumerate(cleaned_tokens):
            if '-' in token:
                # Count hyphens
                parts = token.split('-')
                if len(parts) >= 3:
                    # Check if any part has a letter (Critical fix for the 15-12-1984 bug)
                    if any(c.isalpha() for c in token):
                        self.data['shelving_location'] = token
                        shelf_idx = i
                        break
        
        # --- MODULE 3: PRICE & LIBRARY ---
        for i, token in enumerate(cleaned_tokens):
            # Price: Number with dot (25.00)
            if re.match(r'^\d+\.\d{2}$', token):
                try:
                    self.data['price'] = float(token)
                except: pass
            
            # Library: "VIT"
            if token == "VIT":
                self.data['library_code'] = "VIT"

        # --- MODULE 4: CALL NUMBER & BARCODE ---
        # These are usually found *after* the library code or shelf location
        # We scan the tokens again looking for Dewey patterns
        
        call_num_candidates = []
        barcode_candidates = []
        
        for i, token in enumerate(cleaned_tokens):
            # Skip price tokens
            if self.data['price'] and str(self.data['price']) in token: continue
            
            # Call Number Detection (Dewey or Colon classification)
            if ':' in token or re.match(r'\d{3}\.', token):
                # Check if next token is part of it (e.g., "621.7:744" + "BHA")
                full_call = token
                if i + 1 < len(cleaned_tokens):
                    next_t = cleaned_tokens[i+1]
                    # If next token is short, alpha, and all caps, it's likely the Cutter (BHA)
                    if next_t.isalpha() and next_t.isupper() and len(next_t) < 5:
                        full_call += " " + next_t
                self.data['call_number'] = full_call
            
            # Barcode Detection
            # Strict Rules: Numeric, Integer, NOT the price, NOT '0'
            if token.isdigit():
                val = int(token)
                if val > 0: # Ignore the '0' flags in the string
                    # If we found a price, ensure this isn't the integer part of the price
                    is_price_part = False
                    if self.data['price'] and int(self.data['price']) == val:
                        is_price_part = True
                    
                    if not is_price_part:
                        barcode_candidates.append(token)

        # Logic: The Barcode is usually the LAST distinct number in the sequence
        # or the one immediately following the Call Number.
        if barcode_candidates:
            # We take the last found integer that isn't a '0' flag.
            # In "621.7:744 BHA 42", 42 is at the end.
            self.data['barcode'] = barcode_candidates[-1]

        return self.data


# --- DATABASE HANDLER ---

def run_robust_migration():
    print(f"Initializing Robust Database: {DB_FILE}...")
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("PRAGMA foreign_keys = ON;")

    # SCHEMA: Added 'bill_date' and 'last_seen_date' for comprehensiveness
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
            item_type TEXT,
            raw_json_dump TEXT
        );

        CREATE TABLE IF NOT EXISTS physical_items (
            item_id INTEGER PRIMARY KEY AUTOINCREMENT,
            biblio_id INTEGER NOT NULL,
            barcode TEXT,
            call_number TEXT,
            shelving_location TEXT,
            library_code TEXT,
            price REAL,
            bill_date DATE,          -- NEW: For 1984
            date_acquired DATE,      -- NEW: For 2007
            last_seen_date DATE,     -- NEW: For 2019/2025
            original_holding_string TEXT,
            FOREIGN KEY(biblio_id) REFERENCES biblio_master(biblio_id)
        );
    """)

    print("Ingesting Data...")
    
    batch_biblio = []
    batch_items = []
    
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f):
            try:
                rec = json.loads(line)
                
                # --- BIBLIO ---
                # Simple extraction for the master record
                pub_str = rec.get('260', '')
                year_match = re.search(r'\b(19|20)\d{2}\b', pub_str)
                pub_year = int(year_match.group(0)) if year_match else None
                
                b_data = (
                    int(rec.get('id', 0)),
                    rec.get('245', 'Untitled'),
                    rec.get('100', None),
                    rec.get('250', None),
                    rec.get('020', None),
                    pub_str,
                    pub_year,
                    rec.get('300', None),
                    rec.get('082', None),
                    rec.get('650', None),
                    rec.get('942', '').split()[0],
                    line
                )
                batch_biblio.append(b_data)

                # --- ITEMS (SMART PARSE) ---
                raw_952 = rec.get('952', '')
                parser = SmartParser(raw_952)
                item_data = parser.parse()
                
                if item_data:
                    i_data = (
                        int(rec.get('id', 0)),
                        item_data['barcode'],
                        item_data['call_number'],
                        item_data['shelving_location'],
                        item_data['library_code'],
                        item_data['price'],
                        item_data['bill_date'],
                        item_data['date_acquired'],
                        item_data['last_seen_date'],
                        str(raw_952)
                    )
                    batch_items.append(i_data)

                # Batch commit
                if len(batch_biblio) >= 5000:
                    c.executemany("INSERT OR REPLACE INTO biblio_master VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch_biblio)
                    c.executemany("INSERT INTO physical_items (biblio_id, barcode, call_number, shelving_location, library_code, price, bill_date, date_acquired, last_seen_date, original_holding_string) VALUES (?,?,?,?,?,?,?,?,?,?)", batch_items)
                    conn.commit()
                    batch_biblio = []
                    batch_items = []
                    print(f"Processed {line_num}...")

            except Exception as e:
                # In production, log this to a file
                pass

        # Final commit
        if batch_biblio:
            c.executemany("INSERT OR REPLACE INTO biblio_master VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch_biblio)
            c.executemany("INSERT INTO physical_items (biblio_id, barcode, call_number, shelving_location, library_code, price, bill_date, date_acquired, last_seen_date, original_holding_string) VALUES (?,?,?,?,?,?,?,?,?,?)", batch_items)
            conn.commit()

    conn.close()
    print("Migration Complete. Database: robust_library_system.db")

if __name__ == "__main__":
    run_robust_migration()