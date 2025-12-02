import json
import sqlite3
import re
from datetime import datetime

INPUT_FILE = 'library_data.jsonl'
DB_FILE = 'library_forensic_v6.db'

# --- MODULE 1: THE DATA HEALER ---
def heal_scientific_notation(text):
    """
    Detects Excel-style scientific notation (e.g., 9.78812E+12) 
    and converts it back to a full integer string (e.g., 9788122414837).
    """
    if not text: return text
    
    # Pattern: Digit dot Digits E + Digits
    pattern = re.compile(r'(\d\.\d+)[Ee]\+(\d+)')
    
    def replace_match(match):
        try:
            base = float(match.group(1))
            exponent = int(match.group(2))
            # Convert to integer string
            full_number = int(base * (10 ** exponent))
            return str(full_number)
        except:
            return match.group(0) # Return original if math fails

    return pattern.sub(replace_match, text)

# --- MODULE 2: INTELLIGENT BIBLIO EXTRACTOR ---
def extract_biblio_data(record):
    raw_300 = record.get('300', '')
    original_isbn = record.get('020', '').strip()
    
    pages = None
    rescued_isbn = None

    # Logic: First, try to find a VALID page count (10-5000)
    # We ignore numbers > 5000 immediately, treating them as potential ISBNs.
    
    # Regex to find all numbers in the string
    all_numbers = re.findall(r'\b\d+\b', raw_300)
    
    for num_str in all_numbers:
        val = int(num_str)
        length = len(num_str)
        
        # 1. Check for ISBN characteristics (10 or 13 digits, starts with 978 or 81)
        if (length == 13 and num_str.startswith('978')) or (length == 10 and num_str.startswith('81')):
            rescued_isbn = num_str
            continue # This is an ISBN, not pages.
            
        # 2. Check for realistic Page Count (10 to 5000)
        # We prioritize the number that is closest to a "normal" book size
        if 10 <= val <= 5000:
            pages = val

    # If the "pages" are still None, but we have text like "729120 p.", 
    # we treat it as garbage/typo and leave pages as None.
    
    # Decision: Use Rescued ISBN if Original is missing
    final_isbn = original_isbn if original_isbn else rescued_isbn
    
    return pages, final_isbn

def split_publication_info(full_str):
    """
    Handles: "NEW DELHI, NEW AGE INTERNATIONAL 2008"
    Splits by COMMA if present, otherwise fallback to spaces.
    """
    if not full_str: return None, None, None
    
    # 1. Extract Year (always at the end)
    year = None
    clean_str = full_str.strip()
    match_year = re.search(r'\b(19|20)\d{2}\b', clean_str)
    if match_year:
        year = int(match_year.group(0))
        # Remove year from string for easier splitting
        clean_str = clean_str.replace(match_year.group(0), '').strip()

    # 2. Extract Place and Publisher
    place = None
    publisher = None
    
    if ',' in clean_str:
        # Split by the FIRST comma
        parts = clean_str.split(',', 1)
        place = parts[0].strip()
        publisher = parts[1].strip()
    else:
        # Fallback: First word is Place, rest is Publisher
        parts = clean_str.split(None, 1)
        if parts:
            place = parts[0]
            if len(parts) > 1: publisher = parts[1]

    # Cleanup common noise
    if place and place.upper() in ["NONE", "NULL"]: place = None
    
    return place, publisher, year

# --- MODULE 3: SMART ITEM PARSER ---
class SmartParser:
    def __init__(self, raw_string, item_type_hint="BK"):
        # HEAL THE STRING FIRST
        self.raw = heal_scientific_notation(str(raw_string)) if raw_string else ""
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
            # CALL NUMBER LOGIC
            # Must NOT look like an ISBN (10-13 digits starting with 978)
            # This fixes the "9.78E+12" leaking into Call Number
            is_isbn_like = (token.startswith('978') and len(token) >= 10)
            
            if not is_isbn_like and ((':' in token or '.' in token) and any(c.isdigit() for c in token)):
                full_call = token
                if i + 1 < len(remaining_after_structure):
                    next_t = remaining_after_structure[i+1]
                    if next_t.isalpha() and next_t.isupper() and len(next_t) < 5: full_call += " " + next_t
                self.data['call_number'] = full_call; call_found = True; continue

            # BARCODE LOGIC
            is_barcode = False
            # Check if this token matches our "Healed" ISBN
            # Sometimes the barcode IS the ISBN in messy libraries
            if is_isbn_like: 
                 # If we haven't found a barcode yet, accept the ISBN as a barcode fallback
                 # OR if it's explicitly short (like '99' or '42')
                 pass 

            if token.isdigit():
                val = int(token)
                is_price = (self.data['price'] and int(self.data['price']) == val)
                if val > 0 and not is_price: self.data['barcode'] = token; is_barcode = True
            
            # E-Book Barcode override
            if self.item_type_hint in ["EB", "E-BOOK"] and len(token) > 3 and any(c.isdigit() for c in token):
                self.data['barcode'] = token; is_barcode = True
                
            if is_barcode: continue

            if not call_found and not is_barcode:
                if token not in ["0", "NONE", "NULL"] and len(token) > 1: vendor_tokens.append(token)
                
        if vendor_tokens: self.data['vendor'] = " ".join(vendor_tokens)
        return self.data

def run_forensic_migration():
    print(f"Initializing Forensic Database: {DB_FILE}...")
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("PRAGMA foreign_keys = ON;")

    c.executescript("""
        CREATE TABLE IF NOT EXISTS biblio_master (
            biblio_id INTEGER PRIMARY KEY,
            title TEXT, author TEXT, edition TEXT, isbn TEXT,
            pub_place TEXT, pub_publisher TEXT, pub_year INTEGER,
            page_count INTEGER, access_url TEXT, dewey_class TEXT,
            item_type TEXT, raw_json_dump TEXT
        );
        CREATE TABLE IF NOT EXISTS physical_items (
            item_id INTEGER PRIMARY KEY AUTOINCREMENT,
            biblio_id INTEGER NOT NULL,
            barcode TEXT, call_number TEXT, shelving_location TEXT,
            library_code TEXT, vendor TEXT, price REAL, currency TEXT,
            bill_date DATE, date_acquired DATE, last_seen_date DATE, last_seen_time TEXT,
            FOREIGN KEY(biblio_id) REFERENCES biblio_master(biblio_id)
        );
    """)

    batch_biblio = []
    batch_items = []
    
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f):
            try:
                rec = json.loads(line)
                
                # HEAL THE RAW JSON FIELD 952 BEFORE PROCESSING
                if '952' in rec:
                    rec['952'] = heal_scientific_notation(str(rec['952']))

                place, pub_name, p_year = split_publication_info(rec.get('260', ''))
                pages, final_isbn = extract_biblio_data(rec)
                
                raw_856 = rec.get('856', '')
                url_match = re.search(r'(https?://[^\s"<]+)', raw_856)
                url = url_match.group(1) if url_match else None
                item_type = rec.get('942', '').split()[0]

                batch_biblio.append((
                    int(rec.get('id', 0)), rec.get('245', 'Untitled'), rec.get('100', None),
                    rec.get('250', None), final_isbn, place, pub_name, p_year,
                    pages, url, rec.get('082', None), item_type, line
                ))

                parser = SmartParser(rec.get('952', ''), item_type_hint=item_type)
                item_data = parser.parse()
                if item_data:
                    batch_items.append((
                        int(rec.get('id', 0)), item_data['barcode'], item_data['call_number'],
                        item_data['shelving_location'], item_data['library_code'], item_data['vendor'],
                        item_data['price'], item_data['currency'], item_data['bill_date'],
                        item_data['date_acquired'], item_data['last_seen_date'], item_data['last_seen_time']
                    ))

                if len(batch_biblio) >= 5000:
                    c.executemany("INSERT OR REPLACE INTO biblio_master VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", batch_biblio)
                    c.executemany("INSERT INTO physical_items (biblio_id, barcode, call_number, shelving_location, library_code, vendor, price, currency, bill_date, date_acquired, last_seen_date, last_seen_time) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch_items)
                    conn.commit(); batch_biblio = []; batch_items = []

            except Exception as e: pass

        if batch_biblio:
             c.executemany("INSERT OR REPLACE INTO biblio_master VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", batch_biblio)
             c.executemany("INSERT INTO physical_items (biblio_id, barcode, call_number, shelving_location, library_code, vendor, price, currency, bill_date, date_acquired, last_seen_date, last_seen_time) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch_items)
             conn.commit()

    conn.close()
    print("Forensic Migration Complete. Check library_forensic_v6.db")

if __name__ == "__main__":
    run_forensic_migration()