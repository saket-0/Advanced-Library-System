import json
import sqlite3
from tqdm import tqdm
from config import INPUT_FILE
from database import init_db
from smart_parser import IntelligentParser
from publisher_parser import AI_PublisherParser

# Initialize AI Parser
pub_ai = AI_PublisherParser()

def get_language(rec):
    f008 = rec.get('008', '')
    return f008[35:38].strip() if len(f008) >= 38 else None

def count_total_lines(filepath):
    print("Calculating dataset size...")
    with open(filepath, 'rb') as f:
        return sum(1 for _ in f)

def run_migration():
    total_records = count_total_lines(INPUT_FILE)
    print(f"Starting M4-Optimized Migration V13 on {total_records} records...")
    
    conn = init_db()
    c = conn.cursor()
    
    batch_biblio = []
    batch_items = []
    
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        # Using tqdm for the progress bar
        for line in tqdm(f, total=total_records, desc="Processing", unit="rec", colour="green"):
            try:
                rec = json.loads(line)
                b_id = int(rec.get('id', 0))
                
                # --- AI PUBLICATION PARSING ---
                raw_pub = rec.get('260', '')
                place, publisher, year = pub_ai.parse(raw_pub)

                # --- BIBLIO DATA ---
                batch_biblio.append((
                    b_id, 
                    rec.get('245', 'Untitled'), 
                    rec.get('100', None),
                    rec.get('250', None), 
                    rec.get('020', '').strip(), 
                    place, publisher, year,
                    None, 
                    get_language(rec), 
                    rec.get('942', '').split()[0],
                    line 
                ))

                # --- ITEM PARSING ---
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

                # --- BATCH COMMIT ---
                if len(batch_biblio) >= 5000: # M4 can handle larger batches easily
                    c.executemany("INSERT OR REPLACE INTO biblio_master VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch_biblio)
                    c.executemany("""INSERT INTO physical_items 
                        (biblio_id, barcode, call_number, shelving_location, library_code, vendor, bill_number, 
                        price, currency, bill_date, date_acquired, last_seen_date, 
                        is_withdrawn, is_lost, is_damaged, is_restricted) 
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", batch_items)
                    conn.commit()
                    batch_biblio = []; batch_items = []

            except Exception as e:
                pass 

    # Final Commit
    if batch_biblio:
        c.executemany("INSERT OR REPLACE INTO biblio_master VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch_biblio)
        c.executemany("""INSERT INTO physical_items 
            (biblio_id, barcode, call_number, shelving_location, library_code, vendor, bill_number, 
            price, currency, bill_date, date_acquired, last_seen_date, 
            is_withdrawn, is_lost, is_damaged, is_restricted) 
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", batch_items)
        conn.commit()

    conn.close()
    print("\nMigration Complete. Check library_fixed_v13.db")

if __name__ == "__main__":
    run_migration()