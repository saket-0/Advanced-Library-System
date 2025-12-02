import json
import sqlite3
import re
from datetime import datetime

INPUT_FILE = 'library_data.jsonl'
DB_FILE = 'library_complete_v9.db'

# --- 1. LANGUAGE EXTRACTOR (From MARC 008) ---
def extract_language(record):
    """
    Extracts language code (e.g., 'eng', 'tam', 'hin') from the 008 control field.
    MARC 21 Standard: Positions 35-37 of the 008 tag.
    """
    f008 = record.get('008', '')
    if f008 and len(f008) >= 38:
        lang_code = f008[35:38].strip()
        # Filter out placeholders like '|||' or 'xxx' or 'und'
        if lang_code and lang_code not in ['|||', 'xxx', 'und']:
            return lang_code
    return None

# --- 2. UPDATED PUBLICATION SPLITTER ---
def split_publication_info(full_str):
    if not full_str: return None, None, None
    year = None
    clean_str = full_str.strip()
    match_year = re.search(r'\b(19|20)\d{2}\b', clean_str)
    if match_year:
        year = int(match_year.group(0))
        clean_str = clean_str.replace(match_year.group(0), '').strip()
    clean_str = clean_str.rstrip('.,;: ')
    place = None; publisher = None
    if clean_str.lower().startswith("none "):
        place = "NONE"; publisher = clean_str[5:].strip()
    elif ':' in clean_str:
        parts = clean_str.split(':', 1); place = parts[0].strip(); publisher = parts[1].strip()
    elif ',' in clean_str:
        parts = clean_str.split(',', 1); place = parts[0].strip(); publisher = parts[1].strip()
    else:
        parts = clean_str.split(None, 1)
        if parts:
            place = parts[0]
            if len(parts) > 1: publisher = parts[1]
    if place: place = place.rstrip('.,')
    if publisher: publisher = publisher.rstrip('.,')
    return place, publisher, year

# --- 3. HELPERS ---
def heal_scientific_notation(text):
    if not text: return text
    pattern = re.compile(r'(\d\.\d+)[Ee]\+(\d+)')
    def replace_match(match):
        try: return str(int(float(match.group(1)) * (10 ** int(match.group(2)))))
        except: return match.group(0)
    return pattern.sub(replace_match, text)

def extract_biblio_data(record):
    raw_300 = record.get('300', '')
    original_isbn = record.get('020', '').strip()
    pages = None; rescued_isbn = None
    all_numbers = re.findall(r'\b\d+\b', raw_300)
    for num_str in all_numbers:
        val = int(num_str); length = len(num_str)
        if (length == 13 and num_str.startswith('978')) or (length == 10 and num_str.startswith('81')):
            rescued_isbn = num_str; continue
        if 10 <= val <= 5000: pages = val
    return pages, (original_isbn if original_isbn else rescued_isbn)

# --- 4. BILL NUMBER AWARE PARSER ---
class SmartParser:
    def __init__(self, raw_string, item_type_hint="BK"):
        self.raw = heal_scientific_notation(str(raw_string)) if raw_string else ""
        self.tokens = self.raw.split()
        self.item_type_hint = item_type_hint 
        self.data = {
            'price': None, 'currency': 'INR', 'barcode': None, 'call_number': None,
            'shelving_location': None, 'library_code': None, 'bill_date': None,
            'date_acquired': None, 'last_seen_date': None, 'last_seen_time': None, 'vendor': None,
            'bill_number': None,  # NEW FIELD
            'status_withdrawn': 0, 'status_lost': 0, 'status_damaged': 0, 'status_not_for_loan': 0
        }

    def parse(self):
        if not self.raw or len(self.raw) < 5: return None
        
        # EXTRACT STATUS FLAGS
        if len(self.tokens) >= 4:
            try:
                flags = [self.tokens[0], self.tokens[1], self.tokens[2], self.tokens[3]]
                if all(f.isdigit() and len(f) == 1 for f in flags):
                    self.data['status_withdrawn'] = int(flags[0])
                    self.data['status_lost'] = int(flags[1])
                    self.data['status_damaged'] = int(flags[2])
                    self.data['status_not_for_loan'] = int(flags[3])
                    self.tokens = self.tokens[4:]
            except: pass

        # --- BILL NUMBER EXTRACTION LOGIC ---
        # Strategy: Identify the Bill Date (usually the first date in the string).
        # The token immediately preceding it is the Bill Number.
        
        date_pat = re.compile(r'(\d{4}-\d{2}-\d{2}|\d{2}-\d{2}-\d{4}|\d{2}/\d{2}/\d{4})')
        found_date_indices = []
        
        for i, token in enumerate(self.tokens):
            if date_pat.match(token):
                found_date_indices.append(i)
        
        if found_date_indices:
            # First date is usually bill date
            bill_date_idx = found_date_indices[0]
            
            # Check the token BEFORE the bill date
            if bill_date_idx > 0:
                candidate = self.tokens[bill_date_idx - 1]
                # If it's "NONE" or a number, grab it. 
                # Avoid if it's "0" (sometimes noise) unless explicit.
                if candidate.upper() == "NONE" or (candidate.isdigit() and candidate != "0") or candidate.isalnum():
                    self.data['bill_number'] = candidate

        # --- STANDARD EXTRACTION ---
        clean_tokens = []
        time_pat = re.compile(r'^\d{2}:\d{2}:\d{2}$')
        curr_pat = re.compile(r'^(INR|USD|EUR|GBP|RS\.?|RS)$', re.IGNORECASE)
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
        
        remaining = []
        for token in clean_tokens:
            is_known = False
            # Exclude bill number from further processing if we found it
            if self.data['bill_number'] and token == self.data['bill_number']:
                is_known = True

            if not is_known:
                if re.match(r'^\d+\.\d{2}$', token): self.data['price'] = float(token); is_known = True
                elif token == "VIT": self.data['library_code'] = "VIT"; is_known = True
                elif '-' in token and any(c.isalpha() for c in token): 
                    parts = token.split('-'); 
                    if len(parts) >= 3: self.data['shelving_location'] = token; is_known = True
            
            if not is_known: remaining.append(token)
            
        vendor_tokens = []; call_found = False
        for i, token in enumerate(remaining):
            is_isbn_like = (token.startswith('978') and len(token) >= 10)
            if not is_isbn_like and ((':' in token or '.' in token) and any(c.isdigit() for c in token)):
                full_call = token
                if i + 1 < len(remaining):
                    next_t = remaining[i+1]
                    if next_t.isalpha() and next_t.isupper() and len(next_t) < 5: full_call += " " + next_t
                self.data['call_number'] = full_call; call_found = True; continue
            
            is_barcode = False
            if self.item_type_hint in ["EB", "E-BOOK"] and len(token) > 3 and any(c.isdigit() for c in token):
                self.data['barcode'] = token; is_barcode = True
            if token.isdigit():
                val = int(token); is_price = (self.data['price'] and int(self.data['price']) == val)
                if val > 0 and not is_price: self.data['barcode'] = token; is_barcode = True
            if is_barcode: continue
            
            if not call_found and not is_barcode:
                if token not in ["0", "NONE", "NULL"] and len(token) > 1: vendor_tokens.append(token)
        if vendor_tokens: self.data['vendor'] = " ".join(vendor_tokens)
        return self.data

def run_complete_migration():
    print(f"Initializing Complete Database: {DB_FILE}...")
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("PRAGMA foreign_keys = ON;")
    c.executescript("""
        CREATE TABLE IF NOT EXISTS biblio_master (
            biblio_id INTEGER PRIMARY KEY, title TEXT, author TEXT, edition TEXT, isbn TEXT,
            pub_place TEXT, pub_publisher TEXT, pub_year INTEGER, page_count INTEGER, 
            access_url TEXT, language TEXT, -- NEW LANGUAGE FIELD
            dewey_class TEXT, item_type TEXT, raw_json_dump TEXT
        );
        CREATE TABLE IF NOT EXISTS physical_items (
            item_id INTEGER PRIMARY KEY AUTOINCREMENT, biblio_id INTEGER NOT NULL,
            barcode TEXT, call_number TEXT, shelving_location TEXT, library_code TEXT, vendor TEXT,
            bill_number TEXT, -- NEW BILL NUMBER FIELD
            price REAL, currency TEXT, bill_date DATE, date_acquired DATE, last_seen_date DATE, last_seen_time TEXT,
            is_withdrawn INTEGER DEFAULT 0, is_lost INTEGER DEFAULT 0, is_damaged INTEGER DEFAULT 0, is_restricted INTEGER DEFAULT 0,
            FOREIGN KEY(biblio_id) REFERENCES biblio_master(biblio_id)
        );
    """)
    batch_biblio = []; batch_items = []
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f):
            try:
                rec = json.loads(line)
                if '952' in rec: rec['952'] = heal_scientific_notation(str(rec['952']))
                
                place, pub_name, p_year = split_publication_info(rec.get('260', ''))
                pages, final_isbn = extract_biblio_data(rec)
                lang = extract_language(rec) # Extract Language
                raw_856 = rec.get('856', ''); url_match = re.search(r'(https?://[^\s"<]+)', raw_856)
                url = url_match.group(1) if url_match else None
                item_type = rec.get('942', '').split()[0]

                batch_biblio.append((
                    int(rec.get('id', 0)), rec.get('245', 'Untitled'), rec.get('100', None),
                    rec.get('250', None), final_isbn, place, pub_name, p_year,
                    pages, url, lang, rec.get('082', None), item_type, line
                ))
                parser = SmartParser(rec.get('952', ''), item_type_hint=item_type)
                item_data = parser.parse()
                if item_data:
                    batch_items.append((
                        int(rec.get('id', 0)), item_data['barcode'], item_data['call_number'],
                        item_data['shelving_location'], item_data['library_code'], item_data['vendor'],
                        item_data['bill_number'], # Insert Bill Number
                        item_data['price'], item_data['currency'], item_data['bill_date'],
                        item_data['date_acquired'], item_data['last_seen_date'], item_data['last_seen_time'],
                        item_data['status_withdrawn'], item_data['status_lost'], 
                        item_data['status_damaged'], item_data['status_not_for_loan']
                    ))
                if len(batch_biblio) >= 5000:
                    c.executemany("INSERT OR REPLACE INTO biblio_master VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", batch_biblio)
                    c.executemany("INSERT INTO physical_items (biblio_id, barcode, call_number, shelving_location, library_code, vendor, bill_number, price, currency, bill_date, date_acquired, last_seen_date, last_seen_time, is_withdrawn, is_lost, is_damaged, is_restricted) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", batch_items)
                    conn.commit(); batch_biblio = []; batch_items = []
            except Exception as e: pass
        if batch_biblio:
             c.executemany("INSERT OR REPLACE INTO biblio_master VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", batch_biblio)
             c.executemany("INSERT INTO physical_items (biblio_id, barcode, call_number, shelving_location, library_code, vendor, bill_number, price, currency, bill_date, date_acquired, last_seen_date, last_seen_time, is_withdrawn, is_lost, is_damaged, is_restricted) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", batch_items)
             conn.commit()
    conn.close()
    print("Complete Migration V9 Finished. Check library_complete_v9.db")

if __name__ == "__main__":
    run_complete_migration()