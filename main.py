import json
import re
from config import INPUT_FILE
from database import init_db
from smart_parser import IntelligentParser

# Simple Biblio Extractors
def get_language(rec):
    f008 = rec.get('008', '')
    return f008[35:38].strip() if len(f008) >= 38 else None

def get_year(rec):
    match = re.search(r'\b(19|20)\d{2}\b', rec.get('260', ''))
    return int(match.group(0)) if match else None

def run_migration():
    print("Starting Intelligent Migration V10...")
    conn = init_db()
    c = conn.cursor()
    
    batch_biblio = []
    batch_items = []
    
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            try:
                rec = json.loads(line)
                
                # --- BIBLIO DATA ---
                b_id = int(rec.get('id', 0))
                # Basic cleanup for ISBN/Pages (simplified for brevity)
                isbn = rec.get('020', '').strip()
                
                batch_biblio.append((
                    b_id, rec.get('245', 'Untitled'), rec.get('100', None),
                    rec.get('250', None), isbn, None, 
                    rec.get('260', ''), get_year(rec),
                    None, get_language(rec), rec.get('942', '').split()[0]
                ))

                # --- INTELLIGENT PARSING (952) ---
                raw_952 = rec.get('952', '')
                if raw_952:
                    parser = IntelligentParser(raw_952, item_type_hint=rec.get('942', ''))
                    item = parser.parse()
                    
                    batch_items.append((
                        b_id, item['barcode'], item['call_number'],
                        item['shelving_location'], item['library_code'],
                        item['vendor'], item['bill_number'],
                        item['price'], item['currency'],
                        item['bill_date'], item['date_acquired'], item['last_seen_date'],
                        item['status_flags'][0], item['status_flags'][1], 
                        item['status_flags'][2], item['status_flags'][3]
                    ))

                if len(batch_biblio) >= 5000:
                    c.executemany("INSERT OR REPLACE INTO biblio_master VALUES (?,?,?,?,?,?,?,?,?,?,?)", batch_biblio)
                    c.executemany("""INSERT INTO physical_items 
                        (biblio_id, barcode, call_number, shelving_location, library_code, vendor, bill_number, 
                        price, currency, bill_date, date_acquired, last_seen_date, 
                        is_withdrawn, is_lost, is_damaged, is_restricted) 
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", batch_items)
                    conn.commit()
                    batch_biblio = []; batch_items = []
                    print(f"Processed {i} records...")

            except Exception as e:
                pass # Skip corrupted lines

    # Final Commit
    if batch_biblio:
        c.executemany("INSERT OR REPLACE INTO biblio_master VALUES (?,?,?,?,?,?,?,?,?,?,?)", batch_biblio)
        c.executemany("""INSERT INTO physical_items 
            (biblio_id, barcode, call_number, shelving_location, library_code, vendor, bill_number, 
            price, currency, bill_date, date_acquired, last_seen_date, 
            is_withdrawn, is_lost, is_damaged, is_restricted) 
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", batch_items)
        conn.commit()

    conn.close()
    print("Migration Complete.")

if __name__ == "__main__":
    run_migration()