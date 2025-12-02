import json
import sqlite3
import re
from datetime import datetime

# CONFIGURATION
INPUT_FILE = 'library_data.jsonl'
DB_FILE = 'robust_library_v3.db'

class SmartParser:
    def __init__(self, raw_string):
        self.raw = str(raw_string) if raw_string else ""
        self.tokens = self.raw.split()
        self.data = {
            'price': None,
            'currency': 'INR',       # Default
            'barcode': None,
            'call_number': None,
            'shelving_location': None,
            'library_code': None,
            'bill_date': None,
            'date_acquired': None,
            'last_seen_date': None,
            'last_seen_time': None,  # New: Capture 15:05:34
            'vendor': None
        }

    def parse(self):
        if not self.raw or len(self.raw) < 5:
            return None

        # --- STEP 1: SANITIZATION (Remove known "noise" patterns) ---
        clean_tokens = []
        
        # Regex patterns
        time_pat = re.compile(r'^\d{2}:\d{2}:\d{2}$') # Matches 15:05:34
        curr_pat = re.compile(r'^(INR|USD|EUR|GBP|RS\.?|RS)$', re.IGNORECASE)
        date_pat = re.compile(r'(\d{4}-\d{2}-\d{2}|\d{2}-\d{2}-\d{4}|\d{2}/\d{2}/\d{4})')
        
        dates_found = []

        for token in self.tokens:
            # 1. Catch TIME (The source of your Call Number error)
            if time_pat.match(token):
                self.data['last_seen_time'] = token
                continue # Do not add to clean_tokens
            
            # 2. Catch CURRENCY
            if curr_pat.match(token):
                self.data['currency'] = token.upper()
                continue # Do not add
            
            # 3. Catch DATES
            if date_pat.match(token):
                try:
                    # Normalize date
                    if '-' in token and token[2] == '-': d = datetime.strptime(token, "%d-%m-%Y")
                    elif '/' in token: d = datetime.strptime(token, "%d/%m/%Y")
                    else: d = datetime.strptime(token, "%Y-%m-%d")
                    dates_found.append(d)
                except:
                    pass # Keep token if it looked like a date but wasn't
                continue # Remove dates from pool

            clean_tokens.append(token)

        # Sort Dates
        if dates_found:
            dates_found.sort()
            self.data['bill_date'] = dates_found[0].date()      # Oldest
            self.data['last_seen_date'] = dates_found[-1].date() # Newest
            if len(dates_found) > 2:
                self.data['date_acquired'] = dates_found[1].date() # Middle
            elif len(dates_found) == 2:
                 self.data['date_acquired'] = dates_found[-1].date()

        # --- STEP 2: EXTRACT KNOWN ENTITIES FROM REMAINDER ---
        
        remaining_tokens = []
        for token in clean_tokens:
            is_handled = False
            
            # PRICE (Looks like 35.00)
            if re.match(r'^\d+\.\d{2}$', token):
                try:
                    self.data['price'] = float(token)
                    is_handled = True
                except: pass
            
            # LIBRARY (VIT)
            elif token == "VIT":
                self.data['library_code'] = "VIT"
                is_handled = True
                
            # SHELVING LOCATION (Alpha-Alpha-Alpha)
            # Must have hyphens and letters. MUST NOT look like a Call Number (621.7)
            elif '-' in token and any(c.isalpha() for c in token):
                parts = token.split('-')
                if len(parts) >= 3: 
                    self.data['shelving_location'] = token
                    is_handled = True

            if not is_handled:
                remaining_tokens.append(token)

        # --- STEP 3: CALL NUMBER & BARCODE (The Tricky Part) ---
        
        # We search the REMAINING tokens. 
        # Since Time and Currency are gone, they can't corrupt the data.
        
        call_candidates = []
        barcode_candidates = []

        for i, token in enumerate(remaining_tokens):
            # Call Number Detection (Dewey or Colon)
            # Looks for digits-dot-digits or contains colon
            # BUT: Ensure it's not just a stray number
            if (':' in token or '.' in token) and any(c.isdigit() for c in token):
                # Valid Call Number
                full_call = token
                # Check next token for Cutter (e.g., "BHA")
                if i + 1 < len(remaining_tokens):
                    next_t = remaining_tokens[i+1]
                    if next_t.isalpha() and next_t.isupper() and len(next_t) < 5:
                        full_call += " " + next_t
                self.data['call_number'] = full_call
            
            # Barcode Detection
            # Any integer that isn't the price
            if token.isdigit():
                val = int(token)
                # Ensure it's not the price
                is_price = False
                if self.data['price'] and int(self.data['price']) == val: is_price = True
                
                # Ignore 0 flags
                if val > 0 and not is_price:
                    barcode_candidates.append(token)

        # Select best barcode (usually the last distinct number)
        if barcode_candidates:
            self.data['barcode'] = barcode_candidates[-1]

        return self.data

def split_publication_info(full_str):
    """
    Splits "NONE CHAROTAR PUBLISHING HOUSE 1984" into parts.
    Heuristic: 
    1. Last word is Year (if numeric).
    2. First word is Place (if distinct/uppercase).
    3. Middle is Publisher.
    """
    if not full_str:
        return None, None, None
    
    parts = full_str.split()
    if not parts:
        return None, None, None
        
    year = None
    place = None
    publisher = None
    
    # 1. Extract Year (from end)
    if parts[-1].isdigit() and len(parts[-1]) == 4:
        year = int(parts.pop())
        
    # 2. Extract Place (from start)
    # Heuristic: Usually single word, Uppercase (MADURAI, NONE, NEWDELHI)
    if parts:
        place = parts.pop(0)
        
    # 3. Remainder is Publisher
    if parts:
        publisher = " ".join(parts)
        
    return place, publisher, year


def run_v3_migration():
    print(f"Initializing V3 Database: {DB_FILE}...")
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("PRAGMA foreign_keys = ON;")

    c.executescript("""
        CREATE TABLE IF NOT EXISTS biblio_master (
            biblio_id INTEGER PRIMARY KEY,
            title TEXT,
            author TEXT,
            edition TEXT,
            isbn TEXT,
            
            -- Publication Breakdown
            pub_place TEXT,         -- New
            pub_publisher TEXT,     -- New
            pub_year INTEGER,
            publication_raw TEXT,
            
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
            currency TEXT,           -- New: To store INR/USD explicitly
            
            bill_date DATE,
            date_acquired DATE,
            last_seen_date DATE,
            last_seen_time TEXT,     -- New: To store 15:05:34
            
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
                
                # --- BIBLIO SPLITTING ---
                raw_pub = rec.get('260', '')
                place, pub_name, p_year = split_publication_info(raw_pub)
                
                # If splitting failed to find year, try regex fallback
                if not p_year:
                    match = re.search(r'\b(19|20)\d{2}\b', raw_pub)
                    p_year = int(match.group(0)) if match else None

                b_data = (
                    int(rec.get('id', 0)),
                    rec.get('245', 'Untitled'),
                    rec.get('100', None),
                    rec.get('250', None),
                    rec.get('020', None),
                    place,      # pub_place
                    pub_name,   # pub_publisher
                    p_year,     # pub_year
                    raw_pub,    # publication_raw
                    rec.get('300', None),
                    rec.get('082', None),
                    rec.get('650', None),
                    rec.get('942', '').split()[0],
                    line
                )
                batch_biblio.append(b_data)

                # --- ITEMS SMART PARSE ---
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
                        item_data['currency'],
                        item_data['bill_date'],
                        item_data['date_acquired'],
                        item_data['last_seen_date'],
                        item_data['last_seen_time'],
                        str(raw_952)
                    )
                    batch_items.append(i_data)

                if len(batch_biblio) >= 5000:
                    c.executemany("INSERT OR REPLACE INTO biblio_master VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", batch_biblio)
                    c.executemany("INSERT INTO physical_items (biblio_id, barcode, call_number, shelving_location, library_code, price, currency, bill_date, date_acquired, last_seen_date, last_seen_time, original_holding_string) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch_items)
                    conn.commit()
                    batch_biblio = []
                    batch_items = []
                    print(f"Processed {line_num}...")

            except Exception as e:
                pass

        if batch_biblio:
            c.executemany("INSERT OR REPLACE INTO biblio_master VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", batch_biblio)
            c.executemany("INSERT INTO physical_items (biblio_id, barcode, call_number, shelving_location, library_code, price, currency, bill_date, date_acquired, last_seen_date, last_seen_time, original_holding_string) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch_items)
            conn.commit()

    conn.close()
    print("Migration V3 Complete. Database: robust_library_v3.db")

if __name__ == "__main__":
    run_v3_migration()