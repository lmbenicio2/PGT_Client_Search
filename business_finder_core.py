
import os
import re
import csv
import difflib
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from html import unescape
from urllib.parse import quote_plus, urljoin, urlparse, unquote, parse_qs

import pandas as pd
import requests
from bs4 import BeautifulSoup
from openpyxl import Workbook
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


TIMEOUT = 20
REQUEST_DELAY_SEARCH_PAGE = 0.15
REQUEST_DELAY_PROFILE_BATCH = 0.15
MAX_WORKERS = 6
SAVE_EVERY_N_ROWS = 25

CATEGORY_MATCH_THRESHOLD = 0.20
EXACT_CATEGORY_MATCH_THRESHOLD = 0.50

EMAIL_LOOKUP_TIMEOUT = 8
EMAIL_LOOKUP_MAX_WORKERS = 6
CONTACT_PAGE_PATHS = ["", "/contact", "/contact-us", "/about", "/about-us"]
BAD_EMAIL_PREFIXES = (
    "privacy@", "support@cloudflare", "noreply@", "no-reply@",
    "info@bbb.org", "help@", "support@",
)

ADDRESS_WORD_NORMALIZATION = {
    "street": "st", "st.": "st", "avenue": "ave", "ave.": "ave",
    "boulevard": "blvd", "blvd.": "blvd", "road": "rd", "rd.": "rd",
    "drive": "dr", "dr.": "dr", "lane": "ln", "ln.": "ln",
    "court": "ct", "ct.": "ct", "circle": "cir", "cir.": "cir",
    "place": "pl", "pl.": "pl", "parkway": "pkwy", "pkwy.": "pkwy",
    "highway": "hwy", "hwy.": "hwy", "terrace": "ter", "ter.": "ter",
    "suite": "ste", "ste.": "ste", "apartment": "apt", "apt.": "apt",
    "unit": "unit", "building": "bldg", "bldg.": "bldg",
    "north": "n", "south": "s", "east": "e", "west": "w",
}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

BBB_BASE = "https://www.bbb.org"
BBB_CATEGORIES_URL = f"{BBB_BASE}/us/categories"

US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC"
]

CATEGORY_ALIASES = {
    "restaurants": [
        "restaurant", "dining", "food", "pizza", "cafe", "bistro",
        "grill", "bbq", "bar", "tavern", "eatery", "diner", "sushi",
        "chinese", "mexican", "italian", "thai", "seafood", "steakhouse",
        "burger", "sandwich", "buffet", "catering", "bakery", "deli",
        "fast food", "carry out", "takeout", "breakfast", "brunch",
    ],
    "restaurant": [
        "restaurant", "dining", "food", "pizza", "cafe", "bistro",
        "grill", "bbq", "bar", "tavern", "eatery", "diner", "sushi",
        "chinese", "mexican", "italian", "thai", "seafood", "steakhouse",
        "burger", "sandwich", "buffet", "catering", "bakery", "deli",
        "fast food", "carry out", "takeout", "breakfast", "brunch",
    ],
    "plumber": ["plumb", "plumbing", "pipe", "drain", "sewer", "water heater", "hvac", "heating", "cooling"],
    "plumbers": ["plumb", "plumbing", "pipe", "drain", "sewer", "water heater", "hvac", "heating", "cooling"],
    "electrician": ["electric", "electrical", "wiring", "lighting", "panel", "circuit", "generator"],
    "electricians": ["electric", "electrical", "wiring", "lighting", "panel", "circuit", "generator"],
    "painters": ["paint", "painting", "coat", "finish", "stain", "drywall", "wall"],
    "painter": ["paint", "painting", "coat", "finish", "stain", "drywall", "wall"],
    "roofing contractors": ["roof", "roofing", "shingle", "gutter", "siding", "exterior"],
    "lawn maintenance": ["lawn", "landscape", "grass", "mowing", "garden", "tree", "yard", "irrigation"],
    "pest control": ["pest", "exterminator", "bug", "insect", "rodent", "termite", "ant"],
    "moving companies": ["moving", "mover", "relocation", "storage", "truck", "hauling"],
    "auto repairs": ["auto", "car", "vehicle", "mechanic", "repair", "tire", "brake", "oil"],
    "dentists": ["dental", "dentist", "orthodont", "tooth", "teeth", "oral", "smile"],
    "cleaning services": ["cleaning", "maid", "janitorial", "housekeeping", "sanitation"],
    "attorneys & lawyers": ["attorney", "lawyer", "legal", "law", "counsel"],
    "real estate": ["real estate", "realty", "realtor", "property", "homes", "rental"],
    "insurance companies": ["insurance", "insure", "coverage", "policy", "claim"],
    "tax return preparation": ["tax", "accounting", "bookkeeping", "financial", "cpa", "payroll"],
    "home health care": ["home health", "caregiver", "nursing", "elderly care", "senior care"],
    "physical therapists": ["physical therapy", "rehabilitation", "orthopedic", "sports medicine"],
    "veterinarians": ["veterinar", "vet ", "animal hospital", "pet clinic", "animal care"],
    "tutoring": ["tutor", "education", "learning", "academic", "school", "teaching"],
    "photography": ["photo", "photogr", "portrait", "wedding photo", "studio", "image"],
    "catering": ["cater", "catering", "event food", "banquet", "food service"],
    "hotels": ["hotel", "motel", "inn", "lodge", "resort", "accommodation", "stay"],
    "hair salons": ["hair", "salon", "barber", "beauty", "haircut", "styling"],
    "fitness centers": ["fitness", "gym", "workout", "exercise", "health club", "yoga", "pilates"],
    "child care": ["child care", "daycare", "preschool", "nursery", "babysit", "kids"],
    "tree service": ["tree", "arborist", "stump", "trimming", "pruning", "removal"],
    "solar energy contractors": ["solar", "photovoltaic", "renewable energy", "panel"],
    "junk removal": ["junk", "debris", "hauling", "removal", "dumpster", "trash"],
}

FALLBACK_CATEGORIES = sorted([
    "ACLS Certification", "AIDS Clinics", "AIDS Research", "ATVs",
    "Abortion Alternatives", "Abortion Services", "Above Ground Pools",
    "Access Control Systems", "Accountant", "Accounting",
    "Acoustic Ceiling Removal", "Acoustical Ceiling Contractors",
    "Acting Classes", "Acupressure", "Acupuncturist",
    "Adhesives", "Adult Care", "Adult Day Care", "Adult Family Homes",
    "Advertising", "Advertising Agencies", "Aerial Photographers",
    "Air Conditioning Cleaning", "Air Conditioning Contractors",
    "Air Conditioning Repair", "Air Duct Cleaning", "Air Duct Systems",
    "Air Filters", "Air Purification Systems", "Air Quality Services",
    "Airbag Repair", "Aircraft Maintenance", "Airport Transportation",
    "Alarm Systems", "Alcohol Testing", "Alcoholism Treatments",
    "Allergist", "Alternative Medicine", "Alternator Repair",
    "Ambulance Services", "Animal Hospitals", "Animal Removal",
    "Animal Rescue", "Animal Shelter", "Answering Service",
    "Antique Dealers", "Antique Restoration", "Antiques",
    "App Developers", "Appliance Installation", "Appliance Rental",
    "Appliance Repair", "Appliance Sales", "Appraiser",
    "Arborist", "Architect", "Archery Classes",
    "Artificial Intelligence", "Artificial Turfs",
    "Asbestos Removal", "Asbestos Testing",
    "Asphalt", "Asphalt Repair", "Asphalt Roofing",
    "Assisted Living Facilities", "Associations",
    "Attorneys & Lawyers", "Attorneys & Lawyers - Real Estate",
    "Auctioneer", "Audio Visual Consultants", "Audio Visual Equipment",
    "Audiologist", "Autism Therapy",
    "Auto Accessories", "Auto Air Conditioning", "Auto Alarms",
    "Auto Body Repair and Painting", "Auto Brokers", "Auto Detailing",
    "Auto Financing", "Auto Insurance", "Auto Lube",
    "Auto Parts", "Auto Rentals and Leasing", "Auto Repair Consultants",
    "Auto Repairs", "Auto Salvage", "Auto Services",
    "Auto Transportation", "Auto Upholstery", "Auto Warranty Plans",
    "Automated Teller Machines", "Automatic Door Installation",
    "Automotive Transmission Repair", "Awnings",
    "Baby Accessories", "Baby Furniture", "Baby Proofing",
    "Background Checks", "Bail Bond Services", "Bakeries",
    "Balloon Decorating", "Bankruptcy Attorneys", "Banks",
    "Banquet Facilities", "Bar Equipment", "Barbers",
    "Bathroom Remodeling", "Battery Dealers", "Beauty Schools",
    "Beauty Supplies", "Bed & Breakfast", "Bedding Manufacturers",
    "Beer Distributors", "Bicycle Dealers", "Bicycle Repair",
    "Billing Services", "Blood Banks", "Boat Dealers",
    "Boat Repair", "Boat Storage", "Bookkeeping",
    "Book Stores", "Bowling Centers", "Brake Service",
    "Building Contractors", "Building Inspection", "Building Materials",
    "Business Brokers", "Business Coaching", "Business Consultants",
    "Business Credit", "Business Forms", "Business Furniture",
    "Business Services",
    "Cabinet Makers", "Camping Equipment", "Car Rentals",
    "Car Wash", "Carpet & Rug Cleaners", "Carpet & Rug Dealers",
    "Catering", "Ceiling Contractors", "Cell Phone Repair",
    "Child Care", "Chiropractors", "Christmas Trees",
    "Churches", "Civil Engineers", "Cleaning Equipment",
    "Cleaning Services", "Clothing Stores", "Coffee Shops",
    "Colleges & Universities", "Commercial Cleaning", "Commercial Real Estate",
    "Computer Dealers", "Computer Repair", "Computer Software",
    "Concrete Contractors", "Concierge Services", "Construction Services",
    "Counselors", "Credit Repair Services", "Credit Unions",
    "Dance Lessons", "Data Recovery", "Debt Collection",
    "Dentists", "Detective Agencies", "Digital Marketing",
    "Door & Window", "Drywall Contractors", "Dry Cleaning",
    "Electrician", "Electricians", "Electrical Contractors",
    "Electronics Repair", "Electronics Stores",
    "Email Marketing", "Emergency Physicians", "Employment Agencies",
    "Environmental Consultants", "Estate Planning", "Event Planning",
    "Excavation Contractors", "Eye Care",
    "Fencing Contractors", "Financial Planning", "Fire Damage Restoration",
    "Fire Protection Equipment", "Fitness Centers", "Flooring Contractors",
    "Flower Shops", "Food Trucks", "Foundation Contractors",
    "Freight Forwarding", "Funeral Services", "Furniture Repair",
    "Furniture Stores",
    "Gas Stations", "General Contractor", "Glass Contractors",
    "Golf Courses", "Graphic Designers", "Grocery Stores",
    "Gutters & Downspouts", "Generator Installation",
    "Hair Salons", "Handyman Services", "Hardware Stores",
    "Health Clubs", "Heating & Cooling", "Home Builders",
    "Home Health Care", "Home Inspection", "Home Security",
    "Hospitals", "Hotels", "House Cleaning",
    "HVAC Contractors",
    "Immigration Attorneys", "Insulation Contractors",
    "Insurance Companies", "Interior Design", "Internet Service Providers",
    "IT Services", "Irrigation Systems",
    "Janitorial Services", "Jewelry Stores", "Junk Removal",
    "Karate & Martial Arts", "Kitchen Remodeling",
    "Land Surveyors", "Landscape Contractors", "Lawn Maintenance", "Lighting Consultants",
    "Laundry", "Legal Document Services", "Limousine Services", "Locksmiths",
    "Maid Services", "Mattress Stores", "Medical Labs",
    "Mental Health Services", "Metal Fabrication",
    "Mold & Water Damage", "Mortgage Brokers", "Moving Companies",
    "Music Lessons", "Music Stores",
    "Notary Services", "Nursing Homes",
    "Oil Change & Lube", "Optometrists", "Orthodontists",
    "Painters", "Painting Contractors", "Parking Lots", "Paving Contractors",
    "Payroll Services", "Pediatricians", "Pest Control",
    "Pet Grooming", "Pet Stores", "Photography",
    "Physical Therapists", "Pizza", "Plastic Surgery",
    "Plumber", "Plumbers", "Podiatrists", "Pool Service",
    "Printing Services", "Private Schools", "Property Management",
    "Psychiatrists", "Psychologists",
    "Real Estate", "Real Estate Appraisers", "Rental Agents",
    "Restaurants", "Roofing Contractors",
    "Security Services", "Siding Contractors", "Sign Manufacturers",
    "Solar Energy Contractors", "Storage", "Swimming Pool Contractors",
    "Tax Consultants", "Tax Return Preparation", "Title Companies", "Tire Dealers",
    "Towing", "Trade Schools", "Travel Agencies", "Tree Service", "Tutoring",
    "Urgent Care",
    "Veterinarians",
    "Waste Management", "Web Design", "Web Hosting",
    "Window Cleaning", "Window Contractors",
    "Yoga Instruction",
])

MAIN_CATEGORY_MAP = {
    "Auto Repairs": [
        "Auto Repairs", "Auto Body Repair and Painting", "Auto Repair Consultants",
        "Auto Services", "Auto Air Conditioning", "Automotive Transmission Repair",
        "Brake Service", "Oil Change & Lube", "Tire Dealers", "Auto Lube",
        "Auto Detailing", "Auto Accessories", "Auto Parts", "Towing",
        "Auto Upholstery", "Auto Alarms", "Auto Salvage", "Auto Rentals and Leasing",
        "Auto Financing", "Auto Warranty Plans", "Auto Insurance", "Car Wash",
        "Auto Transportation", "Auto Brokers",
    ],
    "Business Services": [
        "Business Services", "Business Consultants", "Business Coaching",
        "Business Brokers", "Advertising", "Advertising Agencies", "Digital Marketing",
        "Email Marketing", "Graphic Designers", "Web Design", "Web Hosting",
        "IT Services", "Computer Repair", "Computer Software", "Computer Dealers",
        "Data Recovery", "Printing Services", "Concierge Services", "Answering Service",
        "Billing Services", "Background Checks", "Employment Agencies",
        "Internet Service Providers", "App Developers", "Artificial Intelligence",
        "Business Credit", "Business Forms", "Business Furniture",
    ],
    "General Contractor": [
        "General Contractor", "Building Contractors", "Home Builders",
        "Concrete Contractors", "Foundation Contractors", "Drywall Contractors",
        "Ceiling Contractors", "Kitchen Remodeling", "Bathroom Remodeling",
        "Cabinet Makers", "Flooring Contractors", "Door & Window", "Glass Contractors",
        "Insulation Contractors", "Building Inspection", "Building Materials",
        "Asphalt", "Asphalt Repair", "Asphalt Roofing", "Interior Design",
        "Metal Fabrication",
    ],
    "Painting Contractors": ["Painting Contractors", "Painters"],
    "Roofing Contractors": [
        "Roofing Contractors", "Asphalt Roofing", "Gutters & Downspouts",
        "Siding Contractors", "Window Contractors",
    ],
    "Plumber": [
        "Plumber", "Plumbers", "Heating & Cooling", "HVAC Contractors",
        "Air Conditioning Contractors", "Air Conditioning Repair",
        "Air Conditioning Cleaning", "Air Duct Cleaning", "Air Duct Systems",
        "Air Filters", "Air Purification Systems", "Air Quality Services",
    ],
    "Lawn Maintenance": [
        "Lawn Maintenance", "Landscape Contractors", "Tree Service",
        "Irrigation Systems", "Arborist",
    ],
    "Tax Return Preparation": [
        "Tax Return Preparation", "Tax Consultants", "Accountant", "Accounting",
        "Bookkeeping", "Payroll Services", "Financial Planning", "Banks",
        "Credit Unions", "Mortgage Brokers", "Insurance Companies", "Title Companies",
        "Notary Services", "Debt Collection",
    ],
    "Construction Services": [
        "Construction Services", "Excavation Contractors", "Paving Contractors",
        "Fencing Contractors", "Swimming Pool Contractors", "Solar Energy Contractors",
        "Handyman Services", "Home Inspection", "Home Security", "Locksmiths",
        "Pest Control", "Pool Service", "Window Cleaning", "House Cleaning",
        "Cleaning Services", "Commercial Cleaning", "Maid Services",
        "Janitorial Services", "Junk Removal", "Mold & Water Damage",
        "Fire Damage Restoration", "Security Services",
    ],
    "Electrician": [
        "Electrician", "Electricians", "Electrical Contractors",
        "Lighting Consultants", "Generator Installation", "Alarm Systems",
        "Access Control Systems",
    ],
}

def load_cities_by_state_from_csv_obj(file_obj) -> dict:
    cities_by_state = {}
    if file_obj is None:
        return cities_by_state
    if hasattr(file_obj, "seek"):
        file_obj.seek(0)
    text = file_obj.read()
    if isinstance(text, bytes):
        text = text.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(text.splitlines())
    for row in reader:
        city = (row.get("city") or "").strip()
        state_id = (row.get("state_id") or "").strip().upper()
        if not city or not state_id:
            continue
        cities_by_state.setdefault(state_id, []).append(city)
    for state_id in list(cities_by_state):
        cities_by_state[state_id] = sorted(set(cities_by_state[state_id]), key=str.lower)
    return cities_by_state

def slugify_bbb_category(category: str) -> str:
    text = (category or "").strip().lower().replace("&", " and ")
    text = re.sub(r"[/'\"]+", " ", text)
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return re.sub(r"-{2,}", "-", text).strip("-")

def singularize_token(token: str) -> str:
    token = (token or "").strip().lower()
    if len(token) <= 3:
        return token
    if token.endswith("ies") and len(token) > 4:
        return token[:-3] + "y"
    if token.endswith("es") and len(token) > 4 and not token.endswith(("ses", "xes", "zes")):
        return token[:-2]
    if token.endswith("s") and not token.endswith("ss"):
        return token[:-1]
    return token

def normalize_category_phrase(text: str) -> str:
    text = (text or "").strip().lower().replace("&", " and ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    replacements = {
        r"\bcontractors?\b": "contractor",
        r"\brepairs?\b": "repair",
        r"\bservices?\b": "service",
        r"\brestaurants?\b": "restaurant",
        r"\belectricians?\b": "electrician",
        r"\bplumbers?\b": "plumber",
        r"\bpainters?\b": "painting",
    }
    for pat, rep in replacements.items():
        text = re.sub(pat, rep, text)
    return re.sub(r"\s+", " ", text).strip()

def category_tokens(text: str) -> set:
    base = normalize_category_phrase(text)
    tokens = set()
    for token in base.split():
        if len(token) > 1:
            tokens.add(token)
            tokens.add(singularize_token(token))
    return {t for t in tokens if t}

def get_alias_keywords(category: str) -> list:
    key = (category or "").strip().lower()
    if key in CATEGORY_ALIASES:
        return CATEGORY_ALIASES[key]
    norm = normalize_category_phrase(key)
    if norm in CATEGORY_ALIASES:
        return CATEGORY_ALIASES[norm]
    for alias_key, keywords in CATEGORY_ALIASES.items():
        if alias_key in norm or norm in alias_key:
            return keywords
    return []

def category_similarity(query_category: str, candidate_text: str) -> float:
    q = normalize_category_phrase(query_category)
    c = normalize_category_phrase(candidate_text)
    if not q or not c:
        return 0.0
    if q == c:
        return 1.0
    if q in c or c in q:
        return 0.92

    alias_keywords = get_alias_keywords(query_category)
    if alias_keywords:
        c_lower = c.lower()
        for kw in alias_keywords:
            if kw.lower() in c_lower:
                return 0.75

    q_tokens = category_tokens(q)
    c_tokens = category_tokens(c)
    if not q_tokens or not c_tokens:
        return 0.0

    overlap = len(q_tokens & c_tokens)
    union = len(q_tokens | c_tokens)
    jaccard = overlap / union if union else 0.0
    coverage = overlap / len(q_tokens) if q_tokens else 0.0
    seq = difflib.SequenceMatcher(None, q, c).ratio()
    score = max(seq * 0.65 + jaccard * 0.35, coverage * 0.75 + seq * 0.25)
    return min(score, 1.0)

class ExcelWriter:
    def __init__(self, output_path: str):
        self.output_path = output_path
        self.wb = Workbook(write_only=False)
        self.ws = self.wb.active
        self.ws.title = "Businesses"
        self.ws.append([
            "Main Category", "Subcategory", "City", "State",
            "Business Name", "Address", "Phone Number", "Email", "Website",
        ])
        self.rows_written = 1

    def append_row(self, row: list):
        self.ws.append(row)
        self.rows_written += 1
        if self.rows_written % SAVE_EVERY_N_ROWS == 0:
            self.save()

    def save(self):
        self.wb.save(self.output_path)

class BusinessSearchClient:
    def __init__(self):
        self.session = self._build_session()

    @staticmethod
    def _build_session() -> requests.Session:
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})
        retries = Retry(
            total=3, backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def fetch_all_bbb_categories(self) -> list:
        all_categories = set()
        try:
            main_html = self._get_html(BBB_CATEGORIES_URL)
            main_soup = BeautifulSoup(main_html, "html.parser")
            for a in main_soup.select("a[href*='/us/category/']"):
                text = self._clean(a.get_text(" ", strip=True))
                if text and 2 < len(text) < 100:
                    all_categories.add(text)
        except Exception:
            pass
        result = sorted(c for c in all_categories if c)
        return result if len(result) > 10 else FALLBACK_CATEGORIES

    def search_bbb(self, business_type: str, city: str, state_code: str, logger=None) -> list:
        results = []
        seen_items = set()
        global_seen_profile_urls = set()
        candidate_urls = []

        candidate_urls.extend(self._collect_profile_urls_from_category_pages(
            business_type, city, state_code, global_seen_profile_urls, logger
        ))
        candidate_urls.extend(self._collect_profile_urls_from_search_pages(
            business_type, city, state_code, global_seen_profile_urls, logger
        ))

        alias_keywords = get_alias_keywords(business_type)
        for extra_term in list(dict.fromkeys(alias_keywords))[:3]:
            if logger:
                logger(f"Alias search: '{extra_term}' for '{business_type}'")
            candidate_urls.extend(self._collect_profile_urls_from_search_pages(
                extra_term, city, state_code, global_seen_profile_urls, logger
            ))

        deduped_urls = []
        seen_urls = set()
        for url in candidate_urls:
            if url and url not in seen_urls:
                seen_urls.add(url)
                deduped_urls.append(url)

        batch_results = self._fetch_profiles_parallel(
            deduped_urls, requested_city=city,
            requested_state=state_code, requested_category=business_type, logger=logger
        )

        ranked = []
        for item in batch_results:
            location_score = self._score_location_match(item, city, state_code)
            category_score = item.get("_category_score", 0.0)
            if location_score <= 0 or category_score < CATEGORY_MATCH_THRESHOLD:
                continue
            item["_location_score"] = location_score
            ranked.append(item)

        ranked.sort(key=lambda x: (
            -x.get("_category_score", 0.0),
            -x.get("_location_score", 0),
            (x.get("business_name") or "").lower()
        ))

        for item in ranked:
            key = self._dedupe_key(item)
            if key not in seen_items:
                seen_items.add(key)
                results.append(item)
        return results

    def enrich_missing_emails(self, items: list, logger=None) -> list:
        domain_cache = {}
        targets = []
        for item in items:
            if item.get("email"):
                continue
            website = (item.get("website") or "").strip()
            if website:
                targets.append(item)

        with ThreadPoolExecutor(max_workers=EMAIL_LOOKUP_MAX_WORKERS) as executor:
            future_to_item = {}
            for item in targets:
                domain = self._get_domain(item.get("website", ""))
                if not domain:
                    continue
                if domain in domain_cache:
                    item["email"] = domain_cache[domain]
                    continue
                future_to_item[executor.submit(self._find_email_from_website, item.get("website", ""))] = item

            for future in as_completed(future_to_item):
                item = future_to_item[future]
                domain = self._get_domain(item.get("website", ""))
                try:
                    found_email = future.result()
                    domain_cache[domain] = found_email or ""
                    item["email"] = found_email or ""
                    if found_email and logger:
                        logger(f"Website email found: {item.get('business_name', '[No name]')} -> {found_email}")
                except Exception:
                    domain_cache[domain] = ""
                    item["email"] = ""
        return items

    def _collect_profile_urls_from_category_pages(self, business_type, city, state_code, global_seen_profile_urls, logger=None):
        slugs_to_try = []
        for candidate in [business_type, normalize_category_phrase(business_type)]:
            slug = slugify_bbb_category(candidate)
            if slug and slug not in slugs_to_try:
                slugs_to_try.append(slug)

        city_slug = slugify_bbb_category(city)
        collected = []

        for slug in slugs_to_try[:3]:
            page = 1
            while True:
                category_url = f"{BBB_BASE}/us/{state_code.lower()}/{city_slug}/category/{slug}"
                if page > 1:
                    category_url += f"?page={page}"
                try:
                    html = self._get_html(category_url)
                except Exception:
                    break
                soup = BeautifulSoup(html, "html.parser")
                urls = self._collect_candidate_profile_urls_from_search_page(soup, global_seen_profile_urls)
                if not urls:
                    break
                if logger and page == 1:
                    logger(f"Category page '{slug}' yielded {len(urls)} candidate profiles")
                collected.extend(urls)
                next_link = soup.select_one("a[rel='next']") or soup.select_one("a.next")
                if not next_link:
                    break
                page += 1
                time.sleep(REQUEST_DELAY_SEARCH_PAGE)
        return collected

    def _collect_profile_urls_from_search_pages(self, business_type, city, state_code, global_seen_profile_urls, logger=None):
        urls, page, empty_pages = [], 1, 0
        while True:
            search_url = (
                f"{BBB_BASE}/search?find_country=USA"
                f"&find_text={quote_plus(business_type)}"
                f"&find_loc={quote_plus(f'{city}, {state_code}')}&page={page}"
            )
            try:
                html = self._get_html(search_url)
            except Exception as e:
                if logger:
                    logger(f"Search page error on page {page}: {e}")
                break
            soup = BeautifulSoup(html, "html.parser")
            candidate_urls = self._collect_candidate_profile_urls_from_search_page(soup, global_seen_profile_urls)
            if logger:
                logger(f"Search page {page} for '{business_type}': {len(candidate_urls)} candidates")
            if not candidate_urls:
                empty_pages += 1
            else:
                empty_pages = 0
                urls.extend(candidate_urls)
            if empty_pages >= 2 and page > 1:
                break
            next_link = soup.select_one("a[rel='next']") or soup.select_one("a.next")
            if not next_link:
                break
            page += 1
            time.sleep(REQUEST_DELAY_SEARCH_PAGE)
        return urls

    def _fetch_profiles_parallel(self, profile_urls, requested_city, requested_state, requested_category, logger=None):
        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_url = {
                executor.submit(self._fetch_and_parse_profile, url, requested_category): url
                for url in profile_urls
            }
            for future in as_completed(future_to_url):
                try:
                    item = future.result()
                    if item:
                        item["_location_score"] = self._score_location_match(item, requested_city, requested_state)
                        results.append(item)
                except Exception as e:
                    if logger:
                        logger(f"Profile parse error: {future_to_url[future]} | {e}")
        time.sleep(REQUEST_DELAY_PROFILE_BATCH)
        return results

    def _fetch_and_parse_profile(self, profile_url, requested_category=""):
        html = self._get_html(profile_url)
        return self._parse_bbb_profile(html, profile_url, requested_category)

    def _parse_bbb_profile(self, html, profile_url, requested_category=""):
        soup = BeautifulSoup(html, "html.parser")
        data = {
            "business_name": "", "address": "", "phone": "", "email": "",
            "website": "", "profile_url": profile_url, "url_city": "",
            "url_state": "", "profile_categories": [], "category_text": "",
            "_category_score": 0.0,
        }
        url_state, url_city = self._extract_city_state_from_bbb_url(profile_url)
        data["url_state"], data["url_city"] = url_state, url_city

        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            raw = script.string or script.get_text(" ", strip=True)
            if not raw:
                continue
            if not data["business_name"]:
                m = re.search(r'"name"\s*:\s*"([^"]{2,300})"', raw)
                if m:
                    data["business_name"] = self._clean_json_text(m.group(1))
            if not data["phone"]:
                m = re.search(r'"telephone"\s*:\s*"([^"]{7,60})"', raw)
                if m:
                    data["phone"] = self._clean_json_text(m.group(1))
            if not data["address"]:
                street = self._json_field(raw, "streetAddress")
                city = self._json_field(raw, "addressLocality")
                state = self._json_field(raw, "addressRegion")
                postal = self._json_field(raw, "postalCode")
                parts = [p for p in [street, city, state, postal] if p]
                if parts:
                    data["address"] = ", ".join(parts)

        data["website"] = self._extract_business_website_from_page(soup, profile_url)
        if not data["business_name"]:
            h1 = soup.find("h1")
            data["business_name"] = self._text(h1)
        if not data["phone"]:
            tel = soup.select_one("a[href^='tel:']")
            if tel:
                data["phone"] = tel.get("href", "").replace("tel:", "").strip()
        if not data["address"]:
            full_text = soup.get_text(" ", strip=True)
            m = re.search(r"\b\d{1,6}\s+[^,]{2,120},\s*[^,]{2,60},\s*[A-Z]{2}[,\s]+\d{5}(?:-\d{4})?\b", full_text)
            if m:
                data["address"] = m.group(0)

        data["profile_categories"] = self._extract_profile_categories(soup, html)
        data["category_text"] = " | ".join(data["profile_categories"])
        data["_category_score"] = self._score_category_match(requested_category, data)

        for k, v in list(data.items()):
            if k == "profile_categories":
                data[k] = [self._clean(x) for x in v if self._clean(x)]
            elif not k.startswith("_"):
                data[k] = self._clean(v)
        return data

    def _extract_profile_categories(self, soup, html):
        found = []
        selectors = ["a[href*='/category/']", "a[href*='/categories/']", "nav a", "ol.breadcrumb a", "[data-testid*='category'] a"]
        for selector in selectors:
            for a in soup.select(selector):
                text = self._clean(a.get_text(" ", strip=True))
                href = (a.get("href") or "").lower()
                if text and ("/category/" in href or "/categories/" in href or len(text.split()) <= 6):
                    found.append(text)

        label_patterns = [
            r'This company offers\s*([^\.]{3,300})',
            r'Products and Services\s*([^\.]{3,300})',
            r'Business Categories\s*([^\n\r]{3,300})',
            r'Type of Entity\s*([^\n\r]{3,200})',
        ]
        text_blob = BeautifulSoup(html, "html.parser").get_text("\n", strip=True)
        for pat in label_patterns:
            for m in re.finditer(pat, text_blob, re.I):
                chunk = self._clean(m.group(1))
                if chunk:
                    found.extend([x.strip() for x in re.split(r"[;,|/]", chunk) if x.strip()])

        deduped, seen = [], set()
        for text in found:
            norm = normalize_category_phrase(text)
            if norm and 3 <= len(norm) <= 80 and norm not in seen:
                seen.add(norm)
                deduped.append(text)
        return deduped

    def _score_category_match(self, requested_category, item):
        requested_category = (requested_category or "").strip()
        if not requested_category:
            return 1.0
        candidates = []
        categories = item.get("profile_categories", []) or []
        candidates.extend(categories)
        cat_text = item.get("category_text", "")
        if cat_text:
            candidates.append(cat_text)
        candidates.append(item.get("business_name", ""))
        profile_url = item.get("profile_url", "")
        url_category_slug = self._extract_category_slug_from_url(profile_url)
        if url_category_slug:
            candidates.append(url_category_slug)
        best = 0.0
        for text in candidates:
            if text:
                best = max(best, category_similarity(requested_category, text))
        return round(best, 4)

    @staticmethod
    def _extract_category_slug_from_url(profile_url: str) -> str:
        m = re.search(r'/profile/([^/]+)/', profile_url or "")
        return m.group(1).replace("-", " ").strip() if m else ""

    def _extract_business_website_from_page(self, soup, profile_url):
        preferred_links = []
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            text = self._clean(a.get_text(" ", strip=True)).lower()
            aria = self._clean(a.get("aria-label", "")).lower()
            title = self._clean(a.get("title", "")).lower()
            marker = " ".join([text, aria, title])
            if any(word in marker for word in ["website", "visit website", "visit site", "business website", "visit"]):
                preferred_links.append(href)

        all_links = preferred_links + [a.get("href", "").strip() for a in soup.select("a[href]")]
        seen = set()
        for href in all_links:
            if not href or href in seen:
                continue
            seen.add(href)
            real_url = self._normalize_possible_business_url(href, profile_url)
            if real_url:
                return real_url
        return ""

    def _normalize_possible_business_url(self, href: str, profile_url: str) -> str:
        href = (href or "").strip()
        if not href or href.startswith(("mailto:", "tel:", "#", "javascript:")):
            return ""
        full_url = urljoin(profile_url, href)
        parsed = urlparse(full_url)
        domain = parsed.netloc.lower().replace("www.", "")
        query = parse_qs(parsed.query)
        for key in ["url", "to", "redirect", "target"]:
            if key in query and query[key]:
                candidate = unquote(query[key][0]).strip()
                if candidate.startswith(("http://", "https://")):
                    c_domain = urlparse(candidate).netloc.lower().replace("www.", "")
                    if c_domain and "bbb.org" not in c_domain and self._looks_like_business_website(candidate):
                        return candidate
        if parsed.scheme in ("http", "https") and "bbb.org" not in domain and self._looks_like_business_website(full_url):
            return full_url
        return ""

    @staticmethod
    def _extract_city_state_from_bbb_url(url: str):
        try:
            parsed = urlparse(url)
            parts = [p for p in parsed.path.split("/") if p]
            if len(parts) >= 4 and parts[0].lower() == "us":
                return parts[1].upper(), unquote(parts[2]).replace("-", " ").strip()
        except Exception:
            pass
        return "", ""

    @staticmethod
    def _extract_city_state_from_address(address: str):
        if not address:
            return "", ""
        address = re.sub(r"\s+", " ", address.strip())
        for pat in [
            r",\s*([^,]+?)\s*,\s*([A-Z]{2})\s*,?\s*\d{5}(?:-\d{4})?$",
            r",\s*([^,]+?)\s*,\s*([A-Z]{2})\s*$",
            r"\b([A-Za-z\s]+?)\s+([A-Z]{2})\s+\d{5}(?:-\d{4})?$",
        ]:
            m = re.search(pat, address)
            if m:
                return m.group(1).strip(), m.group(2).strip()
        return "", ""

    @staticmethod
    def _normalize_text(value: str) -> str:
        value = (value or "").strip().lower().replace("-", " ")
        value = re.sub(r"[^a-z0-9\s]", "", value)
        return re.sub(r"\s+", " ", value).strip()

    @staticmethod
    def _normalize_business_name_for_dedupe(name: str) -> str:
        name = (name or "").strip().lower().replace("&", " and ")
        name = re.sub(r"[^a-z0-9\s]", " ", name)
        name = re.sub(r"\b(inc|llc|l\.l\.c|corp|corporation|co|company|ltd|limited)\b", " ", name)
        return re.sub(r"\s+", " ", name).strip()

    @staticmethod
    def _normalize_address_for_dedupe(address: str) -> str:
        address = (address or "").strip().lower()
        if not address:
            return ""
        address = unescape(address).replace("#", " ")
        address = re.sub(r"[\r\n\t]", " ", address)
        address = re.sub(r"[^a-z0-9\s,/-]", " ", address)
        address = re.sub(r"\b(\d{5})-\d{4}\b", r"\1", address)
        tokens = re.split(r"\s+", address)
        normalized_tokens = []
        for token in tokens:
            token = token.strip(" ,")
            if token:
                normalized_tokens.append(ADDRESS_WORD_NORMALIZATION.get(token, token))
        address = " ".join(normalized_tokens)
        address = re.sub(r"\s+", " ", address).strip()
        address = address.replace(" ,", ",").replace(", ", ",")
        return re.sub(r",+", ",", address)

    def _is_nearby_city_match(self, result_city: str, requested_city: str) -> bool:
        rc = self._normalize_text(result_city)
        qc = self._normalize_text(requested_city)
        return bool(rc and qc and (rc == qc or qc in rc or rc in qc))

    def _score_location_match(self, item: dict, requested_city: str, requested_state: str) -> int:
        requested_city_norm = self._normalize_text(requested_city)
        requested_state_norm = requested_state.strip().upper()
        addr_city, addr_state = self._extract_city_state_from_address(item.get("address", ""))
        url_city = item.get("url_city", "")
        url_state = item.get("url_state", "")
        for city_value, state_value in [(addr_city, addr_state), (url_city, url_state)]:
            if not city_value or not state_value:
                continue
            city_norm = self._normalize_text(city_value)
            state_norm = state_value.strip().upper()
            if state_norm != requested_state_norm:
                continue
            if city_norm == requested_city_norm:
                return 3
            if self._is_nearby_city_match(city_value, requested_city):
                return 2
            return 1
        return 0

    def _find_email_from_website(self, website: str) -> str:
        website = (website or "").strip()
        if not website:
            return ""
        if not website.startswith(("http://", "https://")):
            website = "https://" + website

        seen_urls = set()
        checked_homepage = False

        for path in CONTACT_PAGE_PATHS:
            candidate_url = urljoin(website.rstrip("/") + "/", path.lstrip("/"))
            if candidate_url in seen_urls:
                continue
            seen_urls.add(candidate_url)
            try:
                html = self._get_html_generic(candidate_url, timeout=EMAIL_LOOKUP_TIMEOUT)
                if not html:
                    continue
                email = self._extract_email_from_html(html, candidate_url)
                if email:
                    return email

                if not checked_homepage:
                    checked_homepage = True
                    soup = BeautifulSoup(html, "html.parser")
                    likely_links = []
                    for a in soup.select("a[href]"):
                        href = (a.get("href") or "").strip()
                        text = self._clean(a.get_text(" ", strip=True)).lower()
                        aria = self._clean(a.get("aria-label", "")).lower()
                        title = self._clean(a.get("title", "")).lower()
                        marker = " ".join([href.lower(), text, aria, title])
                        if any(k in marker for k in ["contact", "about", "support", "team", "staff"]):
                            full_link = urljoin(candidate_url, href)
                            if self._same_domain(website, full_link):
                                likely_links.append(full_link)
                    for extra_url in likely_links[:5]:
                        if extra_url in seen_urls:
                            continue
                        seen_urls.add(extra_url)
                        try:
                            extra_html = self._get_html_generic(extra_url, timeout=EMAIL_LOOKUP_TIMEOUT)
                            email = self._extract_email_from_html(extra_html, extra_url)
                            if email:
                                return email
                        except Exception:
                            continue
            except Exception:
                continue
        return ""

    def _get_html_generic(self, url: str, timeout: int = 8) -> str:
        response = self.session.get(url, timeout=timeout, allow_redirects=True)
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", "").lower()
        if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
            return ""
        return response.text

    def _extract_email_from_html(self, html: str, page_url: str = "") -> str:
        if not html:
            return ""
        page_domain = self._get_domain(page_url)
        soup = BeautifulSoup(html, "html.parser")

        for a in soup.select("a[href^='mailto:']"):
            href = (a.get("href") or "").strip()
            match = re.search(r'mailto:([^?\s]+)', href, re.I)
            if match:
                email = match.group(1).strip().lower()
                if self._is_valid_business_email(email, page_domain):
                    return email

        text = soup.get_text(" ", strip=True)
        email_matches = re.findall(r'\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b', text, re.I)
        for email in email_matches:
            email = email.strip().lower().rstrip(".,;:")
            if self._is_valid_business_email(email, page_domain):
                return email
        return ""

    def _is_valid_business_email(self, email: str, page_domain: str = "") -> bool:
        if not email:
            return False
        email = email.lower().strip()
        bad_domains = ["example.com", "email.com", "domain.com", "godaddy.com", "wix.com", "sentry.io", "cloudflare.com", "bbb.org"]
        if any(email.startswith(prefix) for prefix in BAD_EMAIL_PREFIXES):
            return False
        if any(email.endswith("@" + d) or email.endswith(d) for d in bad_domains):
            return False
        if page_domain:
            email_domain = email.split("@")[-1].lower()
            if page_domain not in email_domain and email_domain not in page_domain:
                return False
        return True

    def _same_domain(self, base_url: str, other_url: str) -> bool:
        return self._get_domain(base_url) == self._get_domain(other_url)

    def _get_domain(self, website: str) -> str:
        website = (website or "").strip()
        if not website:
            return ""
        try:
            if not website.startswith(("http://", "https://")):
                website = "https://" + website
            parsed = urlparse(website)
            return parsed.netloc.lower().replace("www.", "")
        except Exception:
            return ""

    def _collect_candidate_profile_urls_from_search_page(self, soup, global_seen_profile_urls):
        candidates, local_seen = [], set()
        for a in soup.select("a[href*='/us/']"):
            profile_url = self._extract_link(a, BBB_BASE)
            if not profile_url or "/profile/" not in profile_url or "/search?" in profile_url:
                continue
            if profile_url in local_seen or profile_url in global_seen_profile_urls:
                continue
            local_seen.add(profile_url)
            global_seen_profile_urls.add(profile_url)
            candidates.append(profile_url)
        return candidates

    def _get_html(self, url: str) -> str:
        response = self.session.get(url, timeout=TIMEOUT)
        response.raise_for_status()
        return response.text

    @staticmethod
    def _text(el) -> str:
        return re.sub(r"\s+", " ", el.get_text(" ", strip=True)).strip() if el else ""

    @staticmethod
    def _extract_link(el, base: str) -> str:
        return urljoin(base, el.get("href").strip()) if el and el.get("href") else ""

    @staticmethod
    def _json_field(raw: str, field: str) -> str:
        m = re.search(rf'"{re.escape(field)}"\s*:\s*"([^"]{{1,200}})"', raw)
        return unescape(m.group(1)) if m else ""

    @staticmethod
    def _clean(v) -> str:
        if not isinstance(v, str):
            v = str(v) if v else ""
        return re.sub(r"\s+", " ", v.replace("\n", " ").replace("\r", " ").strip())

    @staticmethod
    def _clean_json_text(text: str) -> str:
        return unescape(text.replace("\\/", "/")).strip()

    @staticmethod
    def _looks_like_business_website(url: str) -> bool:
        lowered = url.lower()
        bad_parts = [
            "facebook.com", "instagram.com", "linkedin.com", "twitter.com", "x.com",
            "youtube.com", "bbb.org", "google.com", "mapquest.com", "yelp.com"
        ]
        return not any(b in lowered for b in bad_parts)

    @staticmethod
    def _dedupe_key(item: dict) -> tuple:
        raw_name = item.get("business_name") or ""
        raw_phone = item.get("phone") or ""
        raw_address = item.get("address") or ""
        raw_website = item.get("website") or ""
        name = BusinessSearchClient._normalize_business_name_for_dedupe(raw_name)
        phone = re.sub(r"\D", "", raw_phone)
        address = BusinessSearchClient._normalize_address_for_dedupe(raw_address)
        website = raw_website.strip().lower()
        domain = ""
        if website:
            try:
                parsed = urlparse(website if website.startswith(("http://", "https://")) else f"https://{website}")
                domain = parsed.netloc.lower().replace("www.", "")
            except Exception:
                domain = website
        if name and domain:
            return ("name_domain", name, domain)
        if name and phone:
            return ("name_phone", name, phone)
        if name and address:
            return ("name_address", name, address)
        if domain and phone:
            return ("domain_phone", domain, phone)
        if domain:
            return ("domain_only", domain)
        if phone:
            return ("phone_only", phone)
        if name:
            return ("name_only", name)
        return ("fallback", raw_name.strip().lower(), raw_address.strip().lower(), raw_website.strip().lower())

def make_output_path_unique(path: str) -> str:
    if not path:
        path = "business_results.xlsx"
    folder = os.path.dirname(path) or os.getcwd()
    base = os.path.splitext(os.path.basename(path))[0]
    ext = os.path.splitext(path)[1] or ".xlsx"
    if not os.path.exists(path):
        return path
    ts = datetime.now().strftime("%H%M%S")
    new_path = os.path.join(folder, f"{base}_{ts}{ext}")
    counter = 1
    while os.path.exists(new_path):
        new_path = os.path.join(folder, f"{base}_{ts}_{counter}{ext}")
        counter += 1
    return new_path

def build_subcategory_plan(selected_mains, selected_subs=None, use_all_subcategories=True):
    plan = {}
    selected_subs = selected_subs or []
    for main_cat in selected_mains:
        subs = MAIN_CATEGORY_MAP.get(main_cat, [main_cat])
        if use_all_subcategories:
            chosen = subs
        else:
            chosen = [s for s in subs if s in selected_subs]
        if chosen:
            plan[main_cat] = chosen
    return plan

def run_search_plan(search_plan, cities, state, output_path, enrich_emails=True, logger=print):
    output_path = make_output_path_unique(output_path)
    writer = ExcelWriter(output_path)
    client = BusinessSearchClient()

    saved = 0
    seen_rows = set()
    global_seen_profile_urls = set()

    for current_city in cities:
        logger(f"=== City: {current_city}, {state} ===")
        for main_cat, subcategories in search_plan.items():
            logger(f"Main category: {main_cat}")
            for subcategory in subcategories:
                logger(f"Searching subcategory: {subcategory}")
                businesses = client.search_bbb(
                    business_type=subcategory,
                    city=current_city,
                    state_code=state,
                    logger=logger,
                )
                if enrich_emails:
                    businesses = client.enrich_missing_emails(businesses, logger=logger)

                logger(f"Found {len(businesses)} valid record(s) for '{subcategory}' in {current_city}")
                new_in_sub = 0

                for item in businesses:
                    row_key = client._dedupe_key(item)
                    if row_key in seen_rows:
                        continue
                    seen_rows.add(row_key)

                    extracted_city, extracted_state = client._extract_city_state_from_address(item.get("address", ""))
                    final_city = extracted_city or item.get("url_city", "") or ""
                    final_state = extracted_state or item.get("url_state", "") or state

                    writer.append_row([
                        main_cat,
                        subcategory,
                        final_city,
                        final_state,
                        item.get("business_name", ""),
                        item.get("address", ""),
                        item.get("phone", ""),
                        item.get("email", ""),
                        item.get("website", ""),
                    ])
                    saved += 1
                    new_in_sub += 1

                writer.save()
                logger(f"Saved {new_in_sub} new row(s) from '{subcategory}' in {current_city}")

    writer.save()
    logger(f"Done. File saved: {output_path}")
    return output_path, saved
