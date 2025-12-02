import json
import sqlite3
import re
from datetime import datetime

INPUT_FILE = 'library_data.jsonl'
DB_FILE = 'library_system_corrected.db'

# --- 1. NEW INTELLIGENT EXTRACTOR ---
def extract_biblio_data(record):
    """
    Intelligently separates ISBNs from Page Counts, even if they are mixed up.
    Returns: (pages, resolved_isbn)
    """
    raw_300 = record.get('300', '')
    original_isbn = record.get('020', '').strip()
    
    pages = None
    rescued_isbn = None

    # Logic to parse Field 300 (Description)
    if raw_300:
        # Check for explicit "p" or "pages" first (Strongest Signal)
        match_explicit = re.search(r'(\d+)\s*(-(\d+))?\s*(p|page)', raw_300, re.IGNORECASE)
        if match_explicit:
             # It's definitely pages (e.g., "600 p.")
            val = int(match_explicit.group(3)) if match_explicit.group(3) else int(match_explicit.group(1))
            pages = val
        else:
            # Fallback: Look for raw numbers
            match_raw = re.search(r'\b(\d+)\b', raw_300)
            if match_raw:
                val = int(match_raw.group(1))
                val_len = len(str(val))
                
                # DECISION TREE
                if 10 <= val <= 2000:
                    # Valid Page Count Range
                    pages = val
                elif (val_len == 10 or val_len == 13) and (str(val).startswith('978') or str(val).startswith('81')):
                    # It's an ISBN (10 or 13 digits, starts with 978 or India code 81)
                    rescued_isbn = str(val)

    # FINAL DECISION: Which ISBN to use?
    # Prefer the original 020 field. If empty, use the one we rescued from 300.
    final_isbn = original_isbn if original_isbn else rescued_isbn
    
    return pages, final_isbn

# --- [Keep your existing SmartParser class here] ---
# (I am omitting the SmartParser class code for brevity, 
#  copy it from the previous "robust_migration_final.py")
class SmartParser:
    def __init__(self, raw_string, item_type_hint="BK"):
        self.raw = str(raw_string) if raw_string else ""
        self.tokens = self.raw.split()
        self.item_type_hint = item_type_hint 
        self.data = {
            'price': None, 'currency': 'INR', 'barcode': None, 'call_number': None,
            'shelving_location': None, 'library_code': None, 'bill_date': None,
            'date_acquired': None, 'last_seen_date': None, 'last_seen_time': None, 'vendor': None 
        }

    def parse(self):
        if not self.raw or len(self.raw) < 5: return None
        clean_tokens = []
        time_pat = re.compile(r'^\d{2}:\d{2}:\d{2}$')
        curr_pat = re.compile(r'^(INR|USD|EUR|GBP|RS\.?|RS)$', re.IGNORECASE)
        date_pat = re.compile(r'(\d{4}-\d{2}-\d{2}|\d{2}-\d{2}-\d{4}|\d{2}/\d{2}/\d{4})')
        dates_found = []
        for token in self.tokens:
            if time_pat.match(token): self.data['last_seen_time'] = token; continue
            if curr_pat.match(token): self.data['currency'] = token.upper(); continue
            if date_pat.match(token):
                try:
                    if '-' in token and token[2] == '-': d = datetime.strptime(token, "%d-%m-%Y")
                    elif '/' in token: d = datetime.strptime(token, "%d/%m/%Y")
                    else: d = datetime.strptime(token, "%Y-%m-%d")
                    dates_found.append(d)
                except: pass
                continue
            clean_tokens.append(token)
        if dates_found:
            dates_found.sort(); self.data['bill_date'] = dates_found[0].date(); self.data['last_seen_date'] = dates_found[-1].date()
            if len(dates_found) > 1: self.data['date_acquired'] = dates_found[1].date() if len(dates_found) > 2 else dates_found[-1].date()
        
        remaining_after_structure = []
        for token in clean_tokens:
            is_known = False
            if re.match(r'^\d+\.\d{2}$', token): self.data['price'] = float(token); is_known = True
            elif token == "VIT": self.data['library_code'] = "VIT"; is_known = True
            elif '-' in token and any(c.isalpha() for c in token): 
                parts = token.split('-'); 
                if len(parts) >= 3: self.data['shelving_location'] = token; is_known = True
            if not is_known: remaining_after_structure.append(token)
            
        vendor_tokens = []
        call_found = False
        for i, token in enumerate(remaining_after_structure):
            if (':' in token or '.' in token) and any(c.isdigit() for c in token):
                full_call = token
                if i + 1 < len(remaining_after_structure):
                    next_t = remaining_after_structure[i+1]
                    if next_t.isalpha() and next_t.isupper() and len(next_t) < 5: full_call += " " + next_t
                self.data['call_number'] = full_call; call_found = True; continue
            is_barcode = False
            if self.item_type_hint in ["EB", "E-BOOK"]:
                if len(token) > 3 and any(c.isdigit() for c in token): self.data['barcode'] = token; is_barcode = True
            else:
                if token.isdigit():
                    val = int(token)
                    is_price = (self.data['price'] and int(self.data['price']) == val)
                    if val > 0 and not is_price: self.data['barcode'] = token; is_barcode = True
            if is_barcode: continue
            if not call_found and not is_barcode:
                if token not in ["0", "NONE", "NULL"] and len(token) > 1: vendor_tokens.append(token)
        if vendor_tokens: self.data['vendor'] = " ".join(vendor_tokens)
        return self.data

def split_publication_info(full_str):
    if not full_str: return None, None, None
    parts = full_str.split()
    if not parts: return None, None, None
    year = None; place = None
    if parts[-1].isdigit() and len(parts[-1]) == 4: year = int(parts.pop())
    if parts: place = parts.pop(0)
    publisher = " ".join(parts) if parts else None
    return place, publisher, year

def run_correction_migration():
    print(f"Initializing Corrected Database: {DB_FILE}...")
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("PRAGMA foreign_keys = ON;")

    c.executescript("""
        CREATE TABLE IF NOT EXISTS biblio_master (
            biblio_id INTEGER PRIMARY KEY,
            title TEXT,
            author TEXT,
            edition TEXT,
            isbn TEXT,            -- Now cleaner!
            pub_place TEXT,
            pub_publisher TEXT,
            pub_year INTEGER,
            page_count INTEGER,   -- Now cleaner!
            access_url TEXT,
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
            vendor TEXT,
            price REAL,
            currency TEXT,
            bill_date DATE,
            date_acquired DATE,
            last_seen_date DATE,
            last_seen_time TEXT,
            FOREIGN KEY(biblio_id) REFERENCES biblio_master(biblio_id)
        );
    """)

    batch_biblio = []
    batch_items = []
    
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f):
            try:
                rec = json.loads(line)
                
                # --- BIBLIO ---
                raw_pub = rec.get('260', '')
                place, pub_name, p_year = split_publication_info(raw_pub)
                
                # USE NEW INTELLIGENT EXTRACTOR
                pages, final_isbn = extract_biblio_data(rec)
                
                raw_856 = rec.get('856', '')
                url_match = re.search(r'(https?://[^\s"<]+)', raw_856)
                url = url_match.group(1) if url_match else None
                item_type = rec.get('942', '').split()[0]

                b_data = (
                    int(rec.get('id', 0)),
                    rec.get('245', 'Untitled'),
                    rec.get('100', None),
                    rec.get('250', None),
                    final_isbn,  # Using the corrected ISBN
                    place,
                    pub_name,
                    p_year,
                    pages,       # Using the corrected Pages
                    url,
                    rec.get('082', None),
                    item_type,
                    line
                )
                batch_biblio.append(b_data)

                # --- ITEMS ---
                raw_952 = rec.get('952', '')
                parser = SmartParser(raw_952, item_type_hint=item_type)
                item_data = parser.parse()
                if item_data:
                    i_data = (
                        int(rec.get('id', 0)),
                        item_data['barcode'],
                        item_data['call_number'],
                        item_data['shelving_location'],
                        item_data['library_code'],
                        item_data['vendor'],
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

            except Exception as e: pass

        if batch_biblio:
             c.executemany("INSERT OR REPLACE INTO biblio_master VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", batch_biblio)
             c.executemany("INSERT INTO physical_items (biblio_id, barcode, call_number, shelving_location, library_code, vendor, price, currency, bill_date, date_acquired, last_seen_date, last_seen_time) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch_items)
             conn.commit()

    conn.close()
    print("Correction Complete. Check library_system_corrected.db")

if __name__ == "__main__":
    run_correction_migration()