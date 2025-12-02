import re

# --- REGEX PATTERNS ---
PATTERNS = {
    # Matches "text" OR 'text' OR "'text'" OR simple text
    'tokenizer': re.compile(r'\"\'[^\"]+\'\"|\"[^\"]+\"|\'[^\']+\'|\S+'),
    
    'scientific_notation': re.compile(r'(\d\.\d+)[Ee]\+(\d+)'),
    # Standard Date formats (DD-MM-YYYY or YYYY-MM-DD)
    'date': re.compile(r'(\d{4}-\d{2}-\d{2}|\d{2}-\d{2}-\d{4}|\d{2}/\d{2}/\d{4})'),
    'time': re.compile(r'^\d{2}:\d{2}:\d{2}$'),
    'currency': re.compile(r'^(INR|USD|EUR|GBP|RS\.?|RS)$', re.IGNORECASE),
    'price': re.compile(r'^\d+\.\d{2}$'),
    # Shelving: Chars-Chars-Chars (e.g., IIF-R76-C5-F)
    'shelving': re.compile(r'^[A-Z0-9]+-[A-Z0-9]+-[A-Z0-9]+'),
    # Garbage tokens to ignore
    'garbage': ["0", "NONE", "NULL", "STAC", "STACK", "GEN", "REF"]
}

INPUT_FILE = 'library_data.jsonl'
DB_FILE = 'library_fixed_v11.db'