import spacy
import re
import sys

# Load the efficient model (CPU Optimized)
# The M4 chip runs this blazingly fast.
try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    print("Error: Model 'en_core_web_sm' not found.")
    print("Please run: python -m spacy download en_core_web_sm")
    sys.exit(1)

class AI_PublisherParser:
    def __init__(self):
        self.year_pattern = re.compile(r'\b(19|20)\d{2}\b')
        self.noise_pattern = re.compile(r'\b(NONE|NULL|X+|\|+)\b', re.IGNORECASE)
        
        # Hardcoded fixes for common Indian data entry habits
        self.typo_fixes = {
            "NEWDELHI": "NEW DELHI",
            "N.DELHI": "NEW DELHI",
            "MADRAS": "CHENNAI",
            "BOMBAY": "MUMBAI",
            "CALCUTTA": "KOLKATA",
            "BANGALORE": "BENGALURU"
        }

    def parse(self, text):
        if not text:
            return None, None, None

        # 1. EXTRACT YEAR (Regex is faster than AI)
        year = None
        match_year = self.year_pattern.search(text)
        if match_year:
            year = int(match_year.group(0))
            text = text.replace(match_year.group(0), "")

        # 2. CLEANUP
        clean_text = text.strip().strip(".,:;")
        clean_text = self.noise_pattern.sub("", clean_text)
        
        # Typo correction
        clean_text_upper = clean_text.upper()
        for bad, good in self.typo_fixes.items():
            if bad in clean_text_upper:
                # Use regex to replace keeping case if possible, or just force fix
                clean_text = re.sub(bad, good, clean_text, flags=re.IGNORECASE)
        
        clean_text = clean_text.strip(" ,.-")
        if not clean_text:
            return None, None, year

        # 3. AI ENTITY RECOGNITION (M4 CPU)
        doc = nlp(clean_text)
        
        place = []
        publisher = []
        
        for ent in doc.ents:
            if ent.label_ == "GPE": # Geo-Political Entity (City/Country)
                place.append(ent.text)
            elif ent.label_ in ["ORG", "PERSON"]: # Organization
                publisher.append(ent.text)
        
        # 4. FALLBACK & CLEANUP
        final_place = ", ".join(place) if place else None
        final_pub = ", ".join(publisher) if publisher else None
        
        # If AI missed the publisher, treat the remainder as publisher
        if not final_pub:
            leftover = clean_text
            if final_place:
                leftover = leftover.replace(final_place, "").strip(" ,-")
            if leftover:
                final_pub = leftover

        return final_place, final_pub, year