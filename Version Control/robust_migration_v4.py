import json
import sqlite3
import re
from datetime import datetime

# CONFIGURATION
INPUT_FILE = 'library_data.jsonl'
DB_FILE = 'library_master_v4.db'

class SmartParser:
    def __init__(self, raw_string, item_type_hint="BK"):
        self.raw = str(raw_string) if raw_string else ""
        self.tokens = self.raw.split()
        self.item_type_hint = item_type_hint # Helps distinguish E-books
        self.data = {
            'price': None,
            'currency': 'INR',
            'barcode': None,
            'call_number': None,
            'shelving_location': None,
            'library_code': None,
            'bill_date': None,
            'date_acquired': None,
            'last_seen_date': None,
            'last_seen_time': None,
            'vendor': None  # NEW: Extraction target
        }

    def parse(self):
        if not self.raw or len(self.raw) < 5:
            return None

        clean_tokens = []
        
        # --- PHASE 1: REMOVE KNOWN ENTITIES ---
        # We strip out dates, times, prices, and currency to isolate the "Vendor" text.
        
        time_pat = re.compile(r'^\d{2}:\d{2}:\d{2}$')
        curr_pat = re.compile(r'^(INR|USD|EUR|GBP|RS\.?|RS)$', re.IGNORECASE)
        date_pat = re.compile(r'(\d{4}-\d{2}-\d{2}|\d{2}-\d{2}-\d{4}|\d{2}/\d{2}/\d{4})')
        
        dates_found = []

        for token in self.tokens:
            if time_pat.match(token):
                self.data['last_seen_time'] = token
                continue
            if curr_pat.match(token):
                self.data['currency'] = token.upper()
                continue
            if date_pat.match(token):
                try:
                    if '-' in token and token[2] == '-': d = datetime.strptime(token, "%d-%m-%Y")
                    elif '/' in token: d = datetime.strptime(token, "%d/%m/%Y")
                    else: d = datetime.strptime(token, "%Y-%m-%d")
                    dates_found.append(d)
                except: pass
                continue
            
            clean_tokens.append(token)

        # Assign Dates
        if dates_found:
            dates_found.sort()
            self.data['bill_date'] = dates_found[0].date()
            self.data['last_seen_date'] = dates_found[-1].date()
            if len(dates_found) > 1:
                # Heuristic: The second date is often acquisition
                self.data['date_acquired'] = dates_found[1].date() if len(dates_found) > 2 else dates_found[-1].date()

        # --- PHASE 2: EXTRACT STRUCTURED DATA ---
        
        remaining_after_structure = []
        
        for token in clean_tokens:
            is_known = False
            
            # PRICE
            if re.match(r'^\d+\.\d{2}$', token):
                self.data['price'] = float(token)
                is_known = True
            # LIBRARY CODE
            elif token == "VIT":
                self.data['library_code'] = "VIT"
                is_known = True
            # SHELVING LOCATION (Alpha-Alpha-Alpha)
            elif '-' in token and any(c.isalpha() for c in token):
                parts = token.split('-')
                if len(parts) >= 3:
                    self.data['shelving_location'] = token
                    is_known = True
            
            if not is_known:
                remaining_after_structure.append(token)

        # --- PHASE 3: CALL NUMBER, BARCODE, VENDOR ---
        
        vendor_tokens = []
        call_found = False
        
        for i, token in enumerate(remaining_after_structure):
            
            # CALL NUMBER (The Anchor)
            if (':' in token or '.' in token) and any(c.isdigit() for c in token):
                # We found the call number. Everything BEFORE this (and after dates) is likely Vendor.
                full_call = token
                if i + 1 < len(remaining_after_structure):
                    next_t = remaining_after_structure[i+1]
                    if next_t.isalpha() and next_t.isupper() and len(next_t) < 5:
                        full_call += " " + next_t
                self.data['call_number'] = full_call
                call_found = True
                continue

            # BARCODE
            # Logic: If E-book, allow alphanumeric. If Book, prefer numeric.
            is_barcode = False
            if self.item_type_hint == "EB" or self.item_type_hint == "E-BOOK":
                # E-Book barcodes often look like EBS5251
                if len(token) > 3 and any(c.isdigit() for c in token):
                    self.data['barcode'] = token
                    is_barcode = True
            else:
                # Physical book barcodes are integers
                if token.isdigit():
                    val = int(token)
                    is_price = (self.data['price'] and int(self.data['price']) == val)
                    if val > 0 and not is_price:
                        self.data['barcode'] = token
                        is_barcode = True
            
            if is_barcode: continue

            # VENDOR ACCUMULATION
            # If we haven't found the call number yet, and it's uppercase text, it's likely Vendor.
            if not call_found and not is_barcode:
                # Filter out "NONE", "0", etc.
                if token not in ["0", "NONE", "NULL"] and len(token) > 1:
                    vendor_tokens.append(token)

        if vendor_tokens:
            self.data['vendor'] = " ".join(vendor_tokens)

        return self.data

def extract_pages(phys_desc_str):
    """ Extracts '600' from '600 p.' or '1-115p.' """
    if not phys_desc_str: return None
    # Look for digits followed by 'p'
    match = re.search(r'(\d+)\s*p', phys_desc_str)
    if match:
        return int(match.group(1))
    # Fallback: just find the first large number
    match_digits = re.search(r'\b\d+\b', phys_desc_str)
    if match_digits:
        return int(match_digits.group(0))
    return None

def extract_url(field_856):
    """ Extracts https link from the 856 field """
    if not field_856: return None
    # Regex to find http/https url inside tag or raw
    match = re.search(r'(https?://[^\s"<]+)', field_856)
    return match.group(1) if match else None

def split_publication_info(full_str):
    # Same splitter as V3
    if not full_str: return None, None, None
    parts = full_str.split()
    if not parts: return None, None, None
    year = None
    place = None
    if parts[-1].isdigit() and len(parts[-1]) == 4: year = int(parts.pop())
    if parts: place = parts.pop(0)
    publisher = " ".join(parts) if parts else None
    return place, publisher, year

def run_v4_migration():
    print(f"Initializing Master Database: {DB_FILE}...")
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
            pub_place TEXT,
            pub_publisher TEXT,
            pub_year INTEGER,
            page_count INTEGER,     -- NEW: Extracted Pages
            access_url TEXT,        -- NEW: For E-Books
            dewey_class TEXT,
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
            vendor TEXT,            -- NEW: Extracted Vendor
            price REAL,
            currency TEXT,
            bill_date DATE,
            date_acquired DATE,
            last_seen_date DATE,
            last_seen_time TEXT,
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
                raw_pub = rec.get('260', '')
                place, pub_name, p_year = split_publication_info(raw_pub)
                
                # Pages & URL extraction
                pages = extract_pages(rec.get('300', ''))
                url = extract_url(rec.get('856', ''))
                
                item_type = rec.get('942', '').split()[0]

                b_data = (
                    int(rec.get('id', 0)),
                    rec.get('245', 'Untitled'),
                    rec.get('100', None),
                    rec.get('250', None),
                    rec.get('020', None),
                    place,
                    pub_name,
                    p_year,
                    pages,    # New
                    url,      # New
                    rec.get('082', None),
                    item_type,
                    line
                )
                batch_biblio.append(b_data)

                # --- ITEMS ---
                raw_952 = rec.get('952', '')
                # Pass item type hint so we know if barcode should be alphanumeric
                parser = SmartParser(raw_952, item_type_hint=item_type)
                item_data = parser.parse()
                
                if item_data:
                    i_data = (
                        int(rec.get('id', 0)),
                        item_data['barcode'],
                        item_data['call_number'],
                        item_data['shelving_location'],
                        item_data['library_code'],
                        item_data['vendor'],  # New
                        item_data['price'],
                        item_data['currency'],
                        item_data['bill_date'],
                        item_data['date_acquired'],
                        item_data['last_seen_date'],
                        item_data['last_seen_time']
                    )
                    batch_items.append(i_data)

                if len(batch_biblio) >= 5000:
                    c.executemany("INSERT OR REPLACE INTO biblio_master VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", batch_biblio)
                    c.executemany("INSERT INTO physical_items (biblio_id, barcode, call_number, shelving_location, library_code, vendor, price, currency, bill_date, date_acquired, last_seen_date, last_seen_time) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch_items)
                    conn.commit()
                    batch_biblio = []
                    batch_items = []
                    print(f"Processed {line_num}...")

            except Exception as e:
                pass

        if batch_biblio:
             c.executemany("INSERT OR REPLACE INTO biblio_master VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", batch_biblio)
             c.executemany("INSERT INTO physical_items (biblio_id, barcode, call_number, shelving_location, library_code, vendor, price, currency, bill_date, date_acquired, last_seen_date, last_seen_time) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch_items)
             conn.commit()

    conn.close()
    print("Migration V4 Complete. Database: library_master_v4.db")

if __name__ == "__main__":
    run_v4_migration()