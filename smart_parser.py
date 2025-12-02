import re
from datetime import datetime
from config import PATTERNS

class IntelligentParser:
    def __init__(self, raw_string, item_type_hint="BK"):
        self.raw = self.heal_data(str(raw_string)) if raw_string else ""
        self.item_type_hint = item_type_hint
        self.tokens = []
        self.data = {
            'price': None, 'currency': 'INR', 'barcode': None, 'call_number': None,
            'shelving_location': None, 'library_code': None, 'vendor': None,
            'bill_number': None, 'bill_date': None, 'date_acquired': None,
            'last_seen_date': None, 'last_seen_time': None,
            'status_flags': [0, 0, 0, 0]
        }

    def heal_data(self, text):
        """Fixes scientific notation before tokenization."""
        return PATTERNS['scientific_notation'].sub(
            lambda m: str(int(float(m.group(1)) * (10 ** int(m.group(2))))), text
        )

    def tokenize(self):
        """
        V11 FIX: Regex Tokenizer.
        Captures "'2643,44,45'" as a single token instead of splitting by comma/space.
        """
        if not self.raw: return
        # findall returns a list of strings that match the pattern
        self.tokens = PATTERNS['tokenizer'].findall(self.raw)

    def parse(self):
        if not self.raw: return None
        self.tokenize()
        
        # --- 1. EXTRACT FLAGS (First 4 tokens) ---
        # Same as V9: If first 4 are single digits, they are flags.
        if len(self.tokens) >= 4:
            if all(t.isdigit() and len(t) == 1 for t in self.tokens[:4]):
                self.data['status_flags'] = [int(x) for x in self.tokens[:4]]
                self.tokens = self.tokens[4:]

        # --- 2. IDENTIFY DATES & ANCHORS (The V9 Strategy) ---
        date_indices = []
        clean_tokens = []
        
        for i, token in enumerate(self.tokens):
            # Time (Remove immediately)
            if PATTERNS['time'].match(token):
                self.data['last_seen_time'] = token
                continue
            # Currency (Remove immediately)
            if PATTERNS['currency'].match(token):
                self.data['currency'] = token.upper()
                continue
            # Dates (Store index for Anchoring)
            if PATTERNS['date'].match(token):
                try:
                    if '-' in token and token[2] == '-': d = datetime.strptime(token, "%d-%m-%Y")
                    elif '/' in token: d = datetime.strptime(token, "%d/%m/%Y")
                    else: d = datetime.strptime(token, "%Y-%m-%d")
                    
                    # We store the DATE and its Index in the current list
                    date_indices.append((len(clean_tokens), d)) 
                    clean_tokens.append(token) # Keep date in token list for now
                except: pass
                continue
            
            clean_tokens.append(token)
        
        self.tokens = clean_tokens

        # --- 3. BILL NUMBER EXTRACTION (The V9 Anchor) ---
        if date_indices:
            # Sort by Date Value
            sorted_dates = sorted(date_indices, key=lambda x: x[1])
            self.data['bill_date'] = sorted_dates[0][1].date()
            self.data['last_seen_date'] = sorted_dates[-1][1].date()
            if len(sorted_dates) > 1:
                # Logic: If >2 dates, middle one is acquired. If 2 dates, older is bill, newer is last seen.
                self.data['date_acquired'] = sorted_dates[1][1].date() if len(sorted_dates) > 2 else sorted_dates[-1][1].date()

            # V9 ANCHOR LOGIC: Look at the token BEFORE the first date
            # The "First Date" in the string (positional) is usually the Bill Date
            first_date_pos = date_indices[0][0] # Index of first date in token list
            
            if first_date_pos > 0:
                candidate = self.tokens[first_date_pos - 1]
                # Filter: Must not be "0", "NONE", or "VIT"
                if candidate not in ["0", "NONE", "VIT", "NULL"] and candidate != "STAC":
                     self.data['bill_number'] = candidate.replace('"', '').replace("'", "") # Clean quotes

        # --- 4. CONTEXT FILLING (Remaining tokens) ---
        unknowns = []
        for token in self.tokens:
            # Skip known entities
            if token == self.data['bill_number']: continue
            if any(str(d[1].date()) in token or d[1].strftime("%d/%m/%Y") in token for d in date_indices): continue
            
            if token == "VIT": 
                self.data['library_code'] = "VIT"
            elif PATTERNS['shelving'].match(token):
                self.data['shelving_location'] = token
            elif PATTERNS['price'].match(token):
                self.data['price'] = float(token)
            else:
                unknowns.append(token)

        # --- 5. VENDOR vs CALL NUMBER vs BARCODE ---
        vendor_parts = []
        for token in unknowns:
            # Is it a Barcode? (Numeric, > 3 digits, not Price)
            is_price = (self.data['price'] and str(int(self.data['price'])) == token)
            if token.isdigit() and len(token) > 3 and not is_price:
                if not self.data['barcode']: # Capture first viable number as barcode
                    self.data['barcode'] = token
                continue
            
            # Is it a Call Number? (Contains decimals or colons)
            if ('.' in token or ':' in token) and any(c.isdigit() for c in token):
                if not self.data['call_number']:
                    self.data['call_number'] = token
                continue

            # Leftovers are likely VENDOR
            if token not in PATTERNS['garbage'] and len(token) > 1:
                vendor_parts.append(token)
        
        if vendor_parts:
            self.data['vendor'] = " ".join(vendor_parts)

        return self.data