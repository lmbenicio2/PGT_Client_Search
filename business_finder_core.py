import os
import re
import csv
import difflib
import time
import queue
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from html import unescape
from urllib.parse import quote_plus, urljoin, urlparse, unquote, parse_qs

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from openpyxl import Workbook
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

import sys
import pandas as pd


def resource_path(relative_path):
    if hasattr(sys, "_MEIPASS"):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath("."), relative_path)


logo_file = resource_path("company_logo.png")
cities_file = resource_path("cities.csv")
CITY_CSV_PATH = cities_file
logo_path = logo_file

print("cities_file:", cities_file)
print("cities exists:", os.path.exists(cities_file))
print("logo_file:", logo_file)
print("logo exists:", os.path.exists(logo_file))

try:
    df_cities = pd.read_csv(CITY_CSV_PATH)
except Exception as e:
    print(f"Error reading cities CSV: {e}")
    df_cities = pd.DataFrame()

# ------------------------------------------------------------
# Automatic Business Finder - BBB Only
# BBB-style city search + multi-select main/subcategories
# ------------------------------------------------------------
# Install:
#   pip install requests beautifulsoup4 openpyxl
# ------------------------------------------------------------

TIMEOUT = 20
REQUEST_DELAY_SEARCH_PAGE = 0.15
REQUEST_DELAY_PROFILE_BATCH = 0.15
MAX_WORKERS = 6
SAVE_EVERY_N_ROWS = 25

# ----------------------------------------------------------------
# CATEGORY MATCHING THRESHOLDS
# LOWERED significantly so broader categories (e.g. "Restaurants")
# still match businesses even when the word isn't in their name/URL.
# A profile is accepted if ANY scored candidate passes the threshold.
# ----------------------------------------------------------------
CATEGORY_MATCH_THRESHOLD = 0.20
EXACT_CATEGORY_MATCH_THRESHOLD = 0.50

# Email enrichment settings
EMAIL_LOOKUP_TIMEOUT = 8
EMAIL_LOOKUP_MAX_WORKERS = 6
CONTACT_PAGE_PATHS = [
    "",
    "/contact",
    "/contact-us",
    "/about",
    "/about-us",
]
BAD_EMAIL_PREFIXES = (
    "privacy@",
    "support@cloudflare",
    "noreply@",
    "no-reply@",
    "info@bbb.org",
    "help@",
    "support@",
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

def load_cities_by_state_from_csv(csv_path: str) -> dict:
    cities_by_state = {}
    if not os.path.exists(csv_path):
        return cities_by_state
    try:
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                city = (row.get("city") or "").strip()
                state_id = (row.get("state_id") or "").strip().upper()
                if not city or not state_id:
                    continue
                if state_id not in cities_by_state:
                    cities_by_state[state_id] = []
                cities_by_state[state_id].append(city)
        for state_id in cities_by_state:
            cities_by_state[state_id] = sorted(set(cities_by_state[state_id]), key=str.lower)
    except Exception:
        return {}
    return cities_by_state


US_CITIES_BY_STATE = load_cities_by_state_from_csv(CITY_CSV_PATH)

# ----------------------------------------------------------------
# KEY FIX: Expanded alias map so "Restaurants" → many food keywords,
# and other broad categories map to terms found on BBB pages.
# ----------------------------------------------------------------
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

US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC"
]

BBB_POPULAR_MAIN_CATEGORIES = [
    "Auto Repairs",
    "Business Services",
    "General Contractor",
    "Painting Contractors",
    "Roofing Contractors",
    "Plumber",
    "Lawn Maintenance",
    "Tax Return Preparation",
    "Construction Services",
    "Electrician",
]

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


def load_cities_by_state_from_csv(csv_path: str) -> dict:
    cities_by_state = {}
    if not os.path.exists(csv_path):
        return cities_by_state
    try:
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                city = (row.get("city") or "").strip()
                state_id = (row.get("state_id") or "").strip().upper()
                if not city or not state_id:
                    continue
                if state_id not in cities_by_state:
                    cities_by_state[state_id] = []
                cities_by_state[state_id].append(city)
        for state_id in cities_by_state:
            cities_by_state[state_id] = sorted(set(cities_by_state[state_id]), key=str.lower)
    except Exception:
        return {}
    return cities_by_state


US_CITIES_BY_STATE = load_cities_by_state_from_csv(CITY_CSV_PATH)


def slugify_bbb_category(category: str) -> str:
    text = (category or "").strip().lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[/'\"]+", " ", text)
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return text


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
    text = (text or "").strip().lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\bcontractors?\b", "contractor", text)
    text = re.sub(r"\brepairs?\b", "repair", text)
    text = re.sub(r"\bservices?\b", "service", text)
    text = re.sub(r"\brestaurants?\b", "restaurant", text)
    text = re.sub(r"\belectricians?\b", "electrician", text)
    text = re.sub(r"\bplumbers?\b", "plumber", text)
    text = re.sub(r"\bpainters?\b", "painting", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def category_tokens(text: str) -> set:
    base = normalize_category_phrase(text)
    if not base:
        return set()
    tokens = set()
    for token in base.split():
        if len(token) <= 1:
            continue
        tokens.add(token)
        tokens.add(singularize_token(token))
    return {t for t in tokens if t}


def get_alias_keywords(category: str) -> list:
    """
    Return alias keywords for a category so broad terms like 'restaurant'
    can match businesses that don't literally say 'restaurant' in their name.
    """
    key = category.strip().lower()
    # Try exact match first
    if key in CATEGORY_ALIASES:
        return CATEGORY_ALIASES[key]
    # Try normalized
    norm = normalize_category_phrase(key)
    if norm in CATEGORY_ALIASES:
        return CATEGORY_ALIASES[norm]
    # Try partial match (e.g. "restaurants" matches "restaurant")
    for alias_key, keywords in CATEGORY_ALIASES.items():
        if alias_key in norm or norm in alias_key:
            return keywords
    return []


def category_similarity(query_category: str, candidate_text: str) -> float:
    """
    Compute similarity between the requested category and any candidate text
    (business name, profile URL, category tags from the page, etc.).

    KEY FIX: We now also check alias keywords. This means a search for
    "Restaurants" will still match a business whose BBB page says "Pizza"
    or "Italian Food" in its category section.
    """
    q = normalize_category_phrase(query_category)
    c = normalize_category_phrase(candidate_text)
    if not q or not c:
        return 0.0

    if q == c:
        return 1.0
    if q in c or c in q:
        return 0.92

    # --- Alias keyword check (the main new logic) ---
    alias_keywords = get_alias_keywords(query_category)
    if alias_keywords:
        c_lower = c.lower()
        for kw in alias_keywords:
            if kw.lower() in c_lower:
                # Found an alias match → give a solid but not perfect score
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
            "Main Category",
            "Subcategory",
            "City",
            "State",
            "Business Name",
            "Address",
            "Phone Number",
            "Email",
            "Website",
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
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def fetch_all_bbb_categories(self) -> list:
        all_categories = set()
        letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        try:
            main_html = self._get_html(BBB_CATEGORIES_URL)
            main_soup = BeautifulSoup(main_html, "html.parser")
            for a in main_soup.select("a[href*='/us/category/']"):
                text = self._clean(a.get_text(" ", strip=True))
                if text and 2 < len(text) < 100:
                    all_categories.add(text)
            for letter in letters:
                try:
                    url = f"{BBB_CATEGORIES_URL}/{letter.lower()}"
                    html = self._get_html(url)
                    soup = BeautifulSoup(html, "html.parser")
                    for a in soup.select("a[href*='/us/category/']"):
                        text = self._clean(a.get_text(" ", strip=True))
                        if text and 2 < len(text) < 100:
                            all_categories.add(text)
                    time.sleep(0.05)
                except Exception:
                    continue
        except Exception:
            pass
        result = sorted(c for c in all_categories if c)
        return result if len(result) > 10 else FALLBACK_CATEGORIES

    def search_bbb(
        self,
        business_type: str,
        city: str,
        state_code: str,
        stop_checker=None,
        global_seen_profile_urls=None,
        logger=None,
    ) -> list:
        results = []
        seen_items = set()

        if global_seen_profile_urls is None:
            global_seen_profile_urls = set()

        candidate_urls = []

        # Strategy 1: BBB category page (slug-based)
        candidate_urls.extend(
            self._collect_profile_urls_from_category_pages(
                business_type=business_type,
                city=city,
                state_code=state_code,
                stop_checker=stop_checker,
                global_seen_profile_urls=global_seen_profile_urls,
                logger=logger,
            )
        )

        # Strategy 2: BBB search (text query)
        candidate_urls.extend(
            self._collect_profile_urls_from_search_pages(
                business_type=business_type,
                city=city,
                state_code=state_code,
                stop_checker=stop_checker,
                global_seen_profile_urls=global_seen_profile_urls,
                logger=logger,
            )
        )

        # Strategy 3: Alias keyword searches (NEW)
        # For broad categories like "Restaurants", also search alias terms
        # so we cast a wider net on BBB.
        alias_keywords = get_alias_keywords(business_type)
        if alias_keywords:
            # Pick top 3 distinct alias terms to avoid hammering BBB
            extra_terms = list(dict.fromkeys(alias_keywords))[:3]
            for extra_term in extra_terms:
                if stop_checker and stop_checker():
                    break
                if logger:
                    logger(f"  Alias search: '{extra_term}' for category '{business_type}'")
                candidate_urls.extend(
                    self._collect_profile_urls_from_search_pages(
                        business_type=extra_term,
                        city=city,
                        state_code=state_code,
                        stop_checker=stop_checker,
                        global_seen_profile_urls=global_seen_profile_urls,
                        logger=logger,
                    )
                )

        deduped_urls = []
        seen_urls = set()
        for url in candidate_urls:
            if url and url not in seen_urls:
                seen_urls.add(url)
                deduped_urls.append(url)

        if logger:
            logger(f"  Total unique BBB profile candidates for '{business_type}' in {city}, {state_code}: {len(deduped_urls)}")

        if not deduped_urls:
            return []

        batch_results = self._fetch_profiles_parallel(
            profile_urls=deduped_urls,
            requested_city=city,
            requested_state=state_code,
            requested_category=business_type,
            stop_checker=stop_checker,
            logger=logger,
        )

        ranked = []
        for item in batch_results:
            location_score = self._score_location_match(item, city, state_code)
            category_score = item.get("_category_score", 0.0)

            if location_score <= 0:
                continue

            # KEY FIX: Use the lowered threshold. BBB search already filters
            # by category — if a result came back from BBB for our query,
            # it is almost certainly in that category. We still exclude
            # truly unrelated profiles (score < 0.20) but allow broad ones.
            if category_score < CATEGORY_MATCH_THRESHOLD:
                if logger:
                    logger(f"    Filtered (low category score {category_score:.2f}): {item.get('business_name','?')}")
                continue

            item["_location_score"] = location_score
            ranked.append(item)

        ranked.sort(
            key=lambda x: (
                -x.get("_category_score", 0.0),
                -x.get("_location_score", 0),
                (x.get("business_name") or "").lower()
            )
        )

        for item in ranked:
            key = self._dedupe_key(item)
            if key not in seen_items:
                seen_items.add(key)
                results.append(item)

        if logger:
            exact_cat = sum(1 for x in results if x.get("_category_score", 0) >= EXACT_CATEGORY_MATCH_THRESHOLD)
            logger(f"  Kept {len(results)} business(es) after validation [strong category match: {exact_cat}]")

        return results

    def _collect_profile_urls_from_category_pages(
        self,
        business_type: str,
        city: str,
        state_code: str,
        stop_checker=None,
        global_seen_profile_urls=None,
        logger=None,
    ) -> list:
        if global_seen_profile_urls is None:
            global_seen_profile_urls = set()

        slugs_to_try = []
        for candidate in [
            business_type,
            normalize_category_phrase(business_type),
            singularize_token(normalize_category_phrase(business_type).split()[-1]) if business_type else ""
        ]:
            slug = slugify_bbb_category(candidate)
            if slug and slug not in slugs_to_try:
                slugs_to_try.append(slug)

        city_slug = slugify_bbb_category(city)
        collected = []

        for slug in slugs_to_try[:3]:
            if stop_checker and stop_checker():
                break

            page = 1
            while True:
                if stop_checker and stop_checker():
                    break

                category_url = f"{BBB_BASE}/us/{state_code.lower()}/{city_slug}/category/{slug}"
                if page > 1:
                    category_url += f"?page={page}"

                try:
                    html = self._get_html(category_url)
                except Exception:
                    break

                soup = BeautifulSoup(html, "html.parser")
                urls = self._collect_candidate_profile_urls_from_search_page(
                    soup=soup,
                    global_seen_profile_urls=global_seen_profile_urls,
                )

                if logger and page == 1:
                    logger(f"  Category page '{slug}' yielded {len(urls)} candidate profile(s) on page 1")

                if not urls:
                    break

                collected.extend(urls)

                next_link = soup.select_one("a[rel='next']") or soup.select_one("a.next")
                if not next_link:
                    break
                page += 1
                time.sleep(REQUEST_DELAY_SEARCH_PAGE)

        return collected

    def _collect_profile_urls_from_search_pages(
        self,
        business_type: str,
        city: str,
        state_code: str,
        stop_checker=None,
        global_seen_profile_urls=None,
        logger=None,
    ) -> list:
        if global_seen_profile_urls is None:
            global_seen_profile_urls = set()

        urls = []
        page = 1
        empty_pages = 0

        while True:
            if stop_checker and stop_checker():
                break

            search_url = (
                f"{BBB_BASE}/search"
                f"?find_country=USA"
                f"&find_text={quote_plus(business_type)}"
                f"&find_loc={quote_plus(f'{city}, {state_code}')}"
                f"&page={page}"
            )

            try:
                html = self._get_html(search_url)
            except Exception as e:
                if logger:
                    logger(f"Search page error on page {page}: {e}")
                break

            soup = BeautifulSoup(html, "html.parser")
            candidate_urls = self._collect_candidate_profile_urls_from_search_page(
                soup=soup,
                global_seen_profile_urls=global_seen_profile_urls,
            )

            if logger:
                logger(f"  Search page {page} for '{business_type}': {len(candidate_urls)} candidate profile(s)")

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

    def enrich_missing_emails(self, items: list, stop_checker=None, logger=None) -> list:
        domain_cache = {}
        targets = []

        for item in items:
            if item.get("email"):
                continue
            website = (item.get("website") or "").strip()
            if not website:
                continue
            targets.append(item)

        if not targets:
            return items

        with ThreadPoolExecutor(max_workers=EMAIL_LOOKUP_MAX_WORKERS) as executor:
            future_to_item = {}

            for item in targets:
                if stop_checker and stop_checker():
                    break

                domain = self._get_domain(item.get("website", ""))
                if not domain:
                    continue

                if domain in domain_cache:
                    item["email"] = domain_cache[domain]
                    continue

                future = executor.submit(self._find_email_from_website, item.get("website", ""))
                future_to_item[future] = item

            for future in as_completed(future_to_item):
                if stop_checker and stop_checker():
                    break

                item = future_to_item[future]
                domain = self._get_domain(item.get("website", ""))

                try:
                    found_email = future.result()
                    domain_cache[domain] = found_email or ""
                    item["email"] = found_email or ""
                    if found_email and logger:
                        logger(f"    Website email found: {item.get('business_name', '[No name]')} -> {found_email}")
                except Exception as e:
                    domain_cache[domain] = ""
                    item["email"] = ""
                    if logger:
                        logger(f"    Email lookup error for {item.get('website', '')}: {e}")

        return items

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
        email_matches = re.findall(
            r'\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b',
            text, re.I
        )

        for email in email_matches:
            email = email.strip().lower().rstrip(".,;:")
            if self._is_valid_business_email(email, page_domain):
                return email

        return ""

    def _is_valid_business_email(self, email: str, page_domain: str = "") -> bool:
        if not email:
            return False
        email = email.lower().strip()
        bad_domains = [
            "example.com", "email.com", "domain.com", "godaddy.com",
            "wix.com", "sentry.io", "cloudflare.com", "bbb.org",
        ]
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

    def _collect_candidate_profile_urls_from_search_page(
        self,
        soup: BeautifulSoup,
        global_seen_profile_urls: set,
    ) -> list:
        candidates = []
        local_seen = set()

        for a in soup.select("a[href*='/us/']"):
            profile_url = self._extract_link(a, BBB_BASE)
            if not profile_url:
                continue
            if "/profile/" not in profile_url or "/search?" in profile_url:
                continue
            if profile_url in local_seen or profile_url in global_seen_profile_urls:
                continue

            local_seen.add(profile_url)
            global_seen_profile_urls.add(profile_url)
            candidates.append(profile_url)

        return candidates

    def _fetch_profiles_parallel(
        self,
        profile_urls: list,
        requested_city: str,
        requested_state: str,
        requested_category: str,
        stop_checker=None,
        logger=None,
    ) -> list:
        results = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_url = {
                executor.submit(self._fetch_and_parse_profile, url, requested_category): url
                for url in profile_urls
            }

            for future in as_completed(future_to_url):
                if stop_checker and stop_checker():
                    break

                url = future_to_url[future]
                try:
                    item = future.result()
                    if not item:
                        continue

                    score = self._score_location_match(item, requested_city, requested_state)
                    item["_location_score"] = score
                    results.append(item)

                except Exception as e:
                    if logger:
                        logger(f"    Profile parse error: {url} | {e}")

        time.sleep(REQUEST_DELAY_PROFILE_BATCH)
        return results

    def _fetch_and_parse_profile(self, profile_url: str, requested_category: str = ""):
        html = self._get_html(profile_url)
        return self._parse_bbb_profile(html, profile_url, requested_category)

    def _parse_bbb_profile(self, html: str, profile_url: str, requested_category: str = "") -> dict:
        soup = BeautifulSoup(html, "html.parser")

        data = {
            "business_name": "",
            "address": "",
            "phone": "",
            "email": "",
            "website": "",
            "profile_url": profile_url,
            "url_city": "",
            "url_state": "",
            "profile_categories": [],
            "category_text": "",
            "_category_score": 0.0,
        }

        url_state, url_city = self._extract_city_state_from_bbb_url(profile_url)
        data["url_state"] = url_state
        data["url_city"] = url_city

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
            m = re.search(
                r"\b\d{1,6}\s+[^,]{2,120},\s*[^,]{2,60},\s*[A-Z]{2}[,\s]+\d{5}(?:-\d{4})?\b",
                full_text
            )
            if m:
                data["address"] = m.group(0)

        data["profile_categories"] = self._extract_profile_categories(soup, html)
        data["category_text"] = " | ".join(data["profile_categories"])

        # KEY FIX: Score category using ALL available text sources including
        # the profile categories, business name, and also the URL-embedded
        # category slug (which BBB puts in the profile URL itself).
        data["_category_score"] = self._score_category_match(requested_category, data)

        for k, v in list(data.items()):
            if k == "profile_categories":
                data[k] = [self._clean(x) for x in v if self._clean(x)]
            elif not k.startswith("_"):
                data[k] = self._clean(v)

        return data

    def _extract_profile_categories(self, soup: BeautifulSoup, html: str) -> list:
        found = []

        selectors = [
            "a[href*='/category/']",
            "a[href*='/categories/']",
            "nav a",
            "ol.breadcrumb a",
            "[data-testid*='category'] a",
        ]
        for selector in selectors:
            for a in soup.select(selector):
                text = self._clean(a.get_text(" ", strip=True))
                href = (a.get("href") or "").lower()
                if not text:
                    continue
                if "/category/" in href or "/categories/" in href or len(text.split()) <= 6:
                    found.append(text)

        # Also extract "This company offers..." and similar labels
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

        deduped = []
        seen = set()
        for text in found:
            norm = normalize_category_phrase(text)
            if not norm:
                continue
            if len(norm) < 3 or len(norm) > 80:
                continue
            if norm in seen:
                continue
            seen.add(norm)
            deduped.append(text)
        return deduped

    def _score_category_match(self, requested_category: str, item: dict) -> float:
        """
        KEY FIX: Score the category match against ALL available text from the
        profile, including:
          - Profile categories extracted from the page
          - Business name
          - Profile URL slug (BBB encodes the category in the URL)
          - Full category_text
          - The search query itself (if BBB returned this via a direct search,
            it's almost certainly relevant, so we give a floor score)

        For broad categories like "Restaurants", the profile categories on
        BBB pages will say things like "Pizza", "Mexican Restaurant", etc.
        Our alias keyword system will catch those.
        """
        requested_category = (requested_category or "").strip()
        if not requested_category:
            return 1.0

        candidates = []

        # Profile categories (most reliable signal)
        categories = item.get("profile_categories", []) or []
        candidates.extend(categories)

        # Category text blob
        cat_text = item.get("category_text", "")
        if cat_text:
            candidates.append(cat_text)

        # Business name (sometimes contains the category, e.g. "Luigi's Pizza")
        candidates.append(item.get("business_name", ""))

        # BBB profile URL contains the category slug, e.g.:
        # /us/il/champaign/profile/restaurants/rosatis-pizza-...
        # Extract the slug between /profile/ and the business name
        profile_url = item.get("profile_url", "")
        url_category_slug = self._extract_category_slug_from_url(profile_url)
        if url_category_slug:
            candidates.append(url_category_slug)

        best = 0.0
        for text in candidates:
            if not text:
                continue
            score = category_similarity(requested_category, text)
            if score > best:
                best = score

        # Floor: if BBB returned this result from a direct search query for
        # the requested category, give it at minimum a passing score.
        # This prevents us from rejecting perfectly valid results just because
        # the business name doesn't contain the category word.
        if best < CATEGORY_MATCH_THRESHOLD and url_category_slug:
            # The URL slug is the strongest signal from BBB itself
            slug_score = category_similarity(requested_category, url_category_slug.replace("-", " "))
            if slug_score > best:
                best = slug_score

        return round(best, 4)

    @staticmethod
    def _extract_category_slug_from_url(profile_url: str) -> str:
        """
        BBB profile URLs look like:
        /us/il/champaign/profile/restaurants/rosatis-pizza-0694-90107068
        Extract the category part: "restaurants"
        """
        if not profile_url:
            return ""
        m = re.search(r'/profile/([^/]+)/', profile_url)
        if m:
            return m.group(1).replace("-", " ").strip()
        return ""

    def _extract_business_website_from_page(self, soup: BeautifulSoup, profile_url: str) -> str:
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
        if not href:
            return ""
        if href.startswith(("mailto:", "tel:", "#", "javascript:")):
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
                    if c_domain and "bbb.org" not in c_domain:
                        if self._looks_like_business_website(candidate):
                            return candidate

        if parsed.scheme in ("http", "https"):
            if "bbb.org" in domain:
                return ""
            if self._looks_like_business_website(full_url):
                return full_url

        return ""

    @staticmethod
    def _extract_city_state_from_bbb_url(url: str):
        try:
            parsed = urlparse(url)
            parts = [p for p in parsed.path.split("/") if p]
            if len(parts) >= 4 and parts[0].lower() == "us":
                state = parts[1].upper()
                city = unquote(parts[2]).replace("-", " ").strip()
                return state, city
        except Exception:
            pass
        return "", ""

    @staticmethod
    def _extract_city_state_from_address(address: str):
        """
        FIX: Parse city and state robustly from structured address strings.
        Returns (city, state).
        """
        if not address:
            return "", ""

        address = re.sub(r"\s+", " ", address.strip())

        # Pattern: "..., City, ST 12345" or "..., City, ST, 12345"
        m = re.search(r",\s*([^,]+?)\s*,\s*([A-Z]{2})\s*,?\s*\d{5}(?:-\d{4})?$", address)
        if m:
            return m.group(1).strip(), m.group(2).strip()

        # Pattern: "..., City, ST" (no zip)
        m = re.search(r",\s*([^,]+?)\s*,\s*([A-Z]{2})\s*$", address)
        if m:
            return m.group(1).strip(), m.group(2).strip()

        # Pattern: "City ST 12345" at end
        m = re.search(r"\b([A-Za-z\s]+?)\s+([A-Z]{2})\s+\d{5}(?:-\d{4})?$", address)
        if m:
            return m.group(1).strip(), m.group(2).strip()

        return "", ""

    @staticmethod
    def _normalize_text(value: str) -> str:
        value = (value or "").strip().lower().replace("-", " ")
        value = re.sub(r"[^a-z0-9\s]", "", value)
        value = re.sub(r"\s+", " ", value).strip()
        return value

    @staticmethod
    def _normalize_business_name_for_dedupe(name: str) -> str:
        name = (name or "").strip().lower()
        name = name.replace("&", " and ")
        name = re.sub(r"[^a-z0-9\s]", " ", name)
        name = re.sub(r"\b(inc|llc|l\.l\.c|corp|corporation|co|company|ltd|limited)\b", " ", name)
        name = re.sub(r"\s+", " ", name).strip()
        return name

    @staticmethod
    def _normalize_address_for_dedupe(address: str) -> str:
        address = (address or "").strip().lower()
        if not address:
            return ""
        address = unescape(address)
        address = address.replace("#", " ")
        address = re.sub(r"[\r\n\t]", " ", address)
        address = re.sub(r"[^a-z0-9\s,/-]", " ", address)
        address = re.sub(r"\b(\d{5})-\d{4}\b", r"\1", address)
        tokens = re.split(r"\s+", address)
        normalized_tokens = []
        for token in tokens:
            token = token.strip(" ,")
            if not token:
                continue
            normalized_tokens.append(ADDRESS_WORD_NORMALIZATION.get(token, token))
        address = " ".join(normalized_tokens)
        address = re.sub(r"\b(ste|apt|unit|bldg)\s+", r"\1 ", address)
        address = re.sub(r"\s+", " ", address).strip()
        address = address.replace(" ,", ",").replace(", ", ",")
        address = re.sub(r",+", ",", address)
        return address

    def _is_nearby_city_match(self, result_city: str, requested_city: str) -> bool:
        rc = self._normalize_text(result_city)
        qc = self._normalize_text(requested_city)
        if not rc or not qc:
            return False
        if rc == qc:
            return True
        if qc in rc or rc in qc:
            return True
        return False

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

    def _get_html(self, url: str) -> str:
        response = self.session.get(url, timeout=TIMEOUT)
        response.raise_for_status()
        return response.text

    @staticmethod
    def _text(el) -> str:
        if not el:
            return ""
        return re.sub(r"\s+", " ", el.get_text(" ", strip=True)).strip()

    @staticmethod
    def _extract_link(el, base: str) -> str:
        if not el or not el.get("href"):
            return ""
        return urljoin(base, el.get("href").strip())

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
                parsed = urlparse(
                    website if website.startswith(("http://", "https://"))
                    else f"https://{website}"
                )
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


class AutocompleteCombobox(ttk.Combobox):
    def set_completion_list(self, completion_list):
        self._completion_list = sorted(completion_list, key=str.lower)
        self["values"] = self._completion_list
        self.bind("<KeyRelease>", self._handle_keyrelease)

    def _handle_keyrelease(self, event):
        if event.keysym in ("BackSpace", "Left", "Right", "Up", "Down", "Return", "Tab", "Escape"):
            return
        value = self.get().strip().lower()
        if not value:
            self["values"] = self._completion_list
            return
        filtered = [item for item in self._completion_list if value in item.lower()]
        self["values"] = filtered if filtered else self._completion_list


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Automatic Business Finder to Excel")
        self.geometry("1180x780")
        self.minsize(1000, 700)

        self.log_queue = queue.Queue()
        self.worker = None
        self.stop_requested = False
        self.client = BusinessSearchClient()

        self.all_categories = []
        self.filtered_main_categories = []
        self.filtered_subcategories = []
        self.selected_cities = []

        if not US_CITIES_BY_STATE:
            print(f"WARNING: No cities loaded from CSV: {CITY_CSV_PATH}")

        self._build_ui()
        self.after(150, self._flush_logs)
        self.after(300, self.load_categories_async)

    def _build_ui(self):
        outer = ttk.Frame(self)
        outer.pack(fill="both", expand=True)

        self.canvas = tk.Canvas(outer, highlightthickness=0)
        self.v_scroll = ttk.Scrollbar(outer, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self.v_scroll.set)

        self.v_scroll.pack(side="right", fill="y")
        self.canvas.pack(side="left", fill="both", expand=True)

        self.scrollable_frame = ttk.Frame(self.canvas)
        self.canvas_window = self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")

        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        )
        self.canvas.bind(
            "<Configure>",
            lambda e: self.canvas.itemconfig(self.canvas_window, width=e.width)
        )
        self.bind_all("<MouseWheel>", self._on_mousewheel)

        root = ttk.Frame(self.scrollable_frame, padding=14)
        root.pack(fill="both", expand=True)

        ttk.Label(
            root,
            text="Automatic Business Finder - PGT Marketing",
            font=("Arial", 16, "bold")
        ).pack(anchor="w", pady=(0, 10))

        filters = ttk.LabelFrame(root, text="Search Filters", padding=10)
        filters.pack(fill="x", pady=6)

        main_frame = ttk.LabelFrame(filters, text="BBB Categories (type any category, multi-select)", padding=8)
        main_frame.pack(fill="x", pady=(0, 8))

        main_search_row = ttk.Frame(main_frame)
        main_search_row.pack(fill="x", pady=(0, 4))

        ttk.Label(main_search_row, text="Find category:").pack(side="left", padx=(0, 6))
        self.main_search_var = tk.StringVar()
        self.main_search_var.trace_add("write", self._on_main_search_changed)

        self.main_search_entry = ttk.Entry(main_search_row, textvariable=self.main_search_var, width=40)
        self.main_search_entry.pack(side="left", padx=(0, 8))

        ttk.Button(main_search_row, text="Select all", command=self._select_all_main).pack(side="left", padx=(0, 6))
        ttk.Button(main_search_row, text="Clear", command=self._clear_all_main).pack(side="left", padx=(0, 6))
        ttk.Button(main_search_row, text="Reload from BBB", command=self.load_categories_async).pack(side="left")

        main_list_frame = ttk.Frame(main_frame)
        main_list_frame.pack(fill="x")

        self.main_listbox = tk.Listbox(
            main_list_frame,
            height=6,
            selectmode="extended",
            exportselection=False,
            font=("Arial", 11),
            relief="solid",
            borderwidth=1
        )
        self.main_listbox.pack(side="left", fill="x", expand=True)
        self.main_listbox.bind("<<ListboxSelect>>", self._on_main_selection_change)

        main_scroll = ttk.Scrollbar(main_list_frame, orient="vertical", command=self.main_listbox.yview)
        main_scroll.pack(side="right", fill="y")
        self.main_listbox.configure(yscrollcommand=main_scroll.set)

        self.main_info_var = tk.StringVar(value="0 main categories selected")
        ttk.Label(main_frame, textvariable=self.main_info_var).pack(anchor="w", pady=(4, 0))

        sub_frame = ttk.LabelFrame(filters, text="Subcategories inside selected main categories (multi-select)", padding=8)
        sub_frame.pack(fill="x", pady=(0, 8))

        sub_search_row = ttk.Frame(sub_frame)
        sub_search_row.pack(fill="x", pady=(0, 4))

        ttk.Label(sub_search_row, text="Find subcategory:").pack(side="left", padx=(0, 6))
        self.sub_search_var = tk.StringVar()
        self.sub_search_var.trace_add("write", self._on_sub_search_changed)

        self.sub_search_entry = ttk.Entry(sub_search_row, textvariable=self.sub_search_var, width=40)
        self.sub_search_entry.pack(side="left", padx=(0, 8))

        ttk.Button(sub_search_row, text="Select all shown", command=self._select_all_sub).pack(side="left", padx=(0, 6))
        ttk.Button(sub_search_row, text="Clear", command=self._clear_all_sub).pack(side="left", padx=(0, 6))
        self.use_all_sub_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            sub_search_row,
            text="Use all subcategories from selected main categories",
            variable=self.use_all_sub_var
        ).pack(side="left", padx=(10, 0))

        sub_list_frame = ttk.Frame(sub_frame)
        sub_list_frame.pack(fill="x")

        self.sub_listbox = tk.Listbox(
            sub_list_frame,
            height=8,
            selectmode="extended",
            exportselection=False,
            font=("Arial", 11),
            relief="solid",
            borderwidth=1
        )
        self.sub_listbox.pack(side="left", fill="x", expand=True)
        self.sub_listbox.bind("<<ListboxSelect>>", self._on_sub_selection_change)

        sub_scroll = ttk.Scrollbar(sub_list_frame, orient="vertical", command=self.sub_listbox.yview)
        sub_scroll.pack(side="right", fill="y")
        self.sub_listbox.configure(yscrollcommand=sub_scroll.set)

        self.sub_info_var = tk.StringVar(value="0 subcategories shown")
        ttk.Label(sub_frame, textvariable=self.sub_info_var).pack(anchor="w", pady=(4, 0))

        loc_frame = ttk.Frame(filters)
        loc_frame.pack(fill="x", pady=6)

        ttk.Label(loc_frame, text="State:").grid(row=0, column=0, sticky="w")
        self.state_var = tk.StringVar(value="IL")
        self.state_combo = ttk.Combobox(
            loc_frame,
            textvariable=self.state_var,
            values=US_STATES,
            state="readonly",
            width=8
        )
        self.state_combo.grid(row=0, column=1, sticky="w", padx=(6, 16))
        self.state_combo.bind("<<ComboboxSelected>>", self._on_state_changed)

        ttk.Label(loc_frame, text="City:").grid(row=0, column=2, sticky="w")
        self.city_var = tk.StringVar(value="")
        self.city_combo = AutocompleteCombobox(loc_frame, textvariable=self.city_var, width=30)
        self.city_combo.grid(row=0, column=3, sticky="w", padx=(6, 6))

        ttk.Button(loc_frame, text="Add city", command=self._add_selected_city).grid(row=0, column=4, padx=(0, 6))
        ttk.Button(loc_frame, text="Remove selected", command=self._remove_selected_city).grid(row=0, column=5, padx=(0, 6))
        ttk.Button(loc_frame, text="Clear cities", command=self._clear_selected_cities).grid(row=0, column=6, padx=(0, 12))

        self.search_all_state_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            loc_frame,
            text="Search all cities in selected state",
            variable=self.search_all_state_var
        ).grid(row=0, column=7, sticky="w")

        selected_city_frame = ttk.Frame(loc_frame)
        selected_city_frame.grid(row=1, column=2, columnspan=6, sticky="ew", pady=(8, 0))

        ttk.Label(selected_city_frame, text="Selected cities:").pack(anchor="w")

        city_list_frame = ttk.Frame(selected_city_frame)
        city_list_frame.pack(fill="x")

        self.selected_cities_listbox = tk.Listbox(
            city_list_frame,
            height=4,
            selectmode="extended",
            exportselection=False,
            font=("Arial", 10),
            relief="solid",
            borderwidth=1
        )
        self.selected_cities_listbox.pack(side="left", fill="x", expand=True)

        city_scroll = ttk.Scrollbar(city_list_frame, orient="vertical", command=self.selected_cities_listbox.yview)
        city_scroll.pack(side="right", fill="y")
        self.selected_cities_listbox.configure(yscrollcommand=city_scroll.set)

        self.city_info_var = tk.StringVar(value="0 cities selected")
        ttk.Label(selected_city_frame, textvariable=self.city_info_var).pack(anchor="w", pady=(4, 0))

        self._load_cities_for_state(self.state_var.get())

        output = ttk.LabelFrame(root, text="Excel Output", padding=10)
        output.pack(fill="x", pady=6)

        ttk.Label(output, text="Save as:").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        self.output_var = tk.StringVar(value=self._default_output_path())
        ttk.Entry(output, textvariable=self.output_var, width=74).grid(row=0, column=1, sticky="ew", pady=4)
        output.columnconfigure(1, weight=1)
        ttk.Button(output, text="Browse", command=self.choose_output_file).grid(row=0, column=2, padx=(10, 0), pady=4)

        actions = ttk.Frame(root)
        actions.pack(fill="x", pady=(8, 6))
        self.start_btn = ttk.Button(actions, text="▶ Start search", command=self.start_search)
        self.start_btn.pack(side="left")
        self.stop_btn = ttk.Button(actions, text="■ Stop", command=self.stop_search, state="disabled")
        self.stop_btn.pack(side="left", padx=8)

        self.status_var = tk.StringVar(value="Ready — loading categories...")
        ttk.Label(actions, textvariable=self.status_var).pack(side="left", padx=18)

        logs = ttk.LabelFrame(root, text="Log", padding=10)
        logs.pack(fill="both", expand=True, pady=6)

        self.log_text = tk.Text(logs, height=6, wrap="word")
        self.log_text.pack(fill="both", expand=True)
        self.log_text.configure(state="disabled")

    def _on_mousewheel(self, event):
        try:
            self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        except Exception:
            pass

    def load_categories_async(self):
        self.main_listbox.delete(0, tk.END)
        self.main_listbox.insert(tk.END, "⏳ Loading categories from BBB.org...")
        self.status_var.set("Fetching BBB categories...")
        threading.Thread(target=self._load_categories_worker, daemon=True).start()

    def _load_categories_worker(self):
        try:
            cats = self.client.fetch_all_bbb_categories()
        except Exception as e:
            self.log(f"Category load error: {e}")
            cats = FALLBACK_CATEGORIES
        self.all_categories = sorted(set(cats))
        self.after(0, self._apply_categories)

    def _apply_categories(self):
        self.filtered_main_categories = self.all_categories[:]
        self._refresh_main_listbox()
        self._refresh_subcategory_listbox()
        self.log(f"Loaded {len(self.all_categories)} BBB categories.")
        self.status_var.set("Categories loaded — type and select one or more categories.")

    def _refresh_main_listbox(self):
        selected_before = set(self._get_selected_main_categories())
        self.main_listbox.delete(0, tk.END)
        for cat in self.filtered_main_categories:
            self.main_listbox.insert(tk.END, cat)
        for i, cat in enumerate(self.filtered_main_categories):
            if cat in selected_before:
                self.main_listbox.selection_set(i)
        self.main_info_var.set(f"{len(self._get_selected_main_categories())} main categories selected")

    def _refresh_subcategory_listbox(self):
        selected_before = set(self._get_selected_subcategories())
        selected_mains = self._get_selected_main_categories()

        all_subs = []
        for main_cat in selected_mains:
            if main_cat in MAIN_CATEGORY_MAP:
                subs = MAIN_CATEGORY_MAP.get(main_cat, [])
                valid = [c for c in subs if c in self.all_categories]
                all_subs.extend(valid if valid else subs)
            else:
                all_subs.append(main_cat)

        all_subs = sorted(set(all_subs), key=str.lower)

        q = self.sub_search_var.get().strip().lower()
        if q:
            self.filtered_subcategories = [s for s in all_subs if q in s.lower()]
        else:
            self.filtered_subcategories = all_subs

        self.sub_listbox.delete(0, tk.END)
        for sub in self.filtered_subcategories:
            self.sub_listbox.insert(tk.END, sub)

        for i, sub in enumerate(self.filtered_subcategories):
            if sub in selected_before:
                self.sub_listbox.selection_set(i)

        selected_sub_count = len(self._get_selected_subcategories())
        self.sub_info_var.set(f"{len(self.filtered_subcategories)} subcategories shown | {selected_sub_count} selected")

    def _on_main_search_changed(self, *_):
        q = self.main_search_var.get().strip().lower()
        if not q:
            self.filtered_main_categories = self.all_categories[:]
        else:
            self.filtered_main_categories = [c for c in self.all_categories if q in c.lower()]
        self._refresh_main_listbox()
        self._refresh_subcategory_listbox()

    def _on_sub_search_changed(self, *_):
        self._refresh_subcategory_listbox()

    def _on_main_selection_change(self, event=None):
        self.main_info_var.set(f"{len(self._get_selected_main_categories())} main categories selected")
        self._refresh_subcategory_listbox()

    def _on_sub_selection_change(self, event=None):
        self.sub_info_var.set(
            f"{len(self.filtered_subcategories)} subcategories shown | {len(self._get_selected_subcategories())} selected"
        )

    def _select_all_main(self):
        self.main_listbox.selection_set(0, tk.END)
        self._on_main_selection_change()

    def _clear_all_main(self):
        self.main_listbox.selection_clear(0, tk.END)
        self._on_main_selection_change()

    def _select_all_sub(self):
        self.sub_listbox.selection_set(0, tk.END)
        self._on_sub_selection_change()

    def _clear_all_sub(self):
        self.sub_listbox.selection_clear(0, tk.END)
        self._on_sub_selection_change()

    def _get_selected_main_categories(self):
        return [self.main_listbox.get(i) for i in self.main_listbox.curselection()]

    def _get_selected_subcategories(self):
        return [self.sub_listbox.get(i) for i in self.sub_listbox.curselection()]

    def _build_subcategory_plan(self):
        selected_mains = self._get_selected_main_categories()
        if not selected_mains:
            return []

        chosen_subs = set(self._get_selected_subcategories())
        plan = []

        for main_cat in selected_mains:
            if main_cat in MAIN_CATEGORY_MAP:
                subs = MAIN_CATEGORY_MAP.get(main_cat, [])
                valid = [c for c in subs if c in self.all_categories]
                valid = valid if valid else subs

                if self.use_all_sub_var.get():
                    use_subs = valid
                else:
                    use_subs = [s for s in valid if s in chosen_subs]

                for sub in use_subs:
                    plan.append((main_cat, sub))
            else:
                if self.use_all_sub_var.get():
                    plan.append((main_cat, main_cat))
                else:
                    if main_cat in chosen_subs:
                        plan.append((main_cat, main_cat))

        deduped = []
        seen = set()
        for main_cat, sub in plan:
            key = (main_cat, sub)
            if key not in seen:
                seen.add(key)
                deduped.append((main_cat, sub))
        return deduped

    def _on_state_changed(self, event=None):
        self._load_cities_for_state(self.state_var.get())
        self._clear_selected_cities()

    def _load_cities_for_state(self, state_code: str):
        cities = US_CITIES_BY_STATE.get(state_code, [])
        self.city_combo.set_completion_list(cities)
        current_city = self.city_var.get().strip()
        if current_city in cities:
            self.city_var.set(current_city)
        elif cities:
            self.city_var.set(cities[0])
        else:
            self.city_var.set("")
            self.log(f"No cities found in CSV for state {state_code}")

    def _add_selected_city(self):
        city = self.city_var.get().strip()
        state = self.state_var.get().strip()
        valid_cities = US_CITIES_BY_STATE.get(state, [])

        if not city:
            return

        if valid_cities and city not in valid_cities:
            messagebox.showerror("Invalid city", f"'{city}' is not in the city list for {state}.")
            return

        if city not in self.selected_cities:
            self.selected_cities.append(city)
            self.selected_cities = sorted(self.selected_cities, key=str.lower)
            self._refresh_selected_cities_listbox()

    def _remove_selected_city(self):
        indices = list(self.selected_cities_listbox.curselection())
        if not indices:
            return
        cities_to_remove = [self.selected_cities_listbox.get(i) for i in indices]
        self.selected_cities = [c for c in self.selected_cities if c not in cities_to_remove]
        self._refresh_selected_cities_listbox()

    def _clear_selected_cities(self):
        self.selected_cities = []
        self._refresh_selected_cities_listbox()

    def _refresh_selected_cities_listbox(self):
        self.selected_cities_listbox.delete(0, tk.END)
        for city in self.selected_cities:
            self.selected_cities_listbox.insert(tk.END, city)
        self.city_info_var.set(f"{len(self.selected_cities)} cities selected")

    def _get_selected_city_list(self):
        if self.search_all_state_var.get():
            return US_CITIES_BY_STATE.get(self.state_var.get().strip(), [])
        if self.selected_cities:
            return self.selected_cities[:]
        single_city = self.city_var.get().strip()
        return [single_city] if single_city else []

    def _default_output_path(self):
        return os.path.join(os.getcwd(), "business_results.xlsx")

    def _make_output_path_unique(self, path: str) -> str:
        if not path:
            return self._default_output_path()
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

    def choose_output_file(self):
        path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel workbook", "*.xlsx")],
            initialfile=os.path.basename(self.output_var.get())
        )
        if path:
            self.output_var.set(path)

    def log(self, msg: str):
        self.log_queue.put(msg)

    def _flush_logs(self):
        try:
            while not self.log_queue.empty():
                msg = self.log_queue.get_nowait()
                self.log_text.configure(state="normal")
                self.log_text.insert(tk.END, msg + "\n")
                self.log_text.see(tk.END)
                self.log_text.configure(state="disabled")
            self.after(150, self._flush_logs)
        except tk.TclError:
            pass

    def start_search(self):
        selected_mains = self._get_selected_main_categories()
        search_plan = self._build_subcategory_plan()
        city = self.city_var.get().strip()
        state = self.state_var.get().strip()
        output_path = self.output_var.get().strip()
        search_all_state = self.search_all_state_var.get()
        city_list = self._get_selected_city_list()

        if not selected_mains:
            messagebox.showerror("No main category selected", "Select one or more main categories.")
            return

        if not search_plan:
            if self.use_all_sub_var.get():
                messagebox.showerror("No subcategories", "No subcategories found for the selected main categories.")
            else:
                messagebox.showerror("No subcategory selected", "Select one or more subcategories or enable 'Use all subcategories'.")
            return

        if not state:
            messagebox.showerror("Missing state", "Select a state.")
            return

        if not city_list:
            messagebox.showerror("Missing city", "Select or add one or more cities, or enable all-state search.")
            return

        if not output_path.lower().endswith(".xlsx"):
            messagebox.showerror("Invalid output", "Output file must end with .xlsx")
            return

        safe_output_path = self._make_output_path_unique(output_path)
        self.output_var.set(safe_output_path)

        if self.worker and self.worker.is_alive():
            messagebox.showinfo("Running", "A search is already running.")
            return

        self.stop_requested = False
        self.start_btn.configure(state="disabled")
        self.stop_btn.configure(state="normal")
        self.status_var.set("Searching...")

        self.worker = threading.Thread(
            target=self._run_search,
            args=(search_plan, city, state, safe_output_path, search_all_state, city_list),
            daemon=True
        )
        self.worker.start()

    def stop_search(self):
        self.stop_requested = True
        self.log("Stop requested — finishing current step...")

    def _run_search(self, search_plan, city: str, state: str, output_path: str,
                    search_all_state: bool = False, city_list=None):
        writer = ExcelWriter(output_path)
        saved = 0
        seen_rows = set()
        global_seen_profile_urls = set()

        try:
            if search_all_state:
                city_list = US_CITIES_BY_STATE.get(state, [])
                if not city_list:
                    self.log(f"No city list found for state {state}.")
                    self.status_var.set("Error")
                    return
                self.log(f"Searching ALL cities in {state} ({len(city_list)} cities)")
            else:
                city_list = city_list or ([city] if city else [])
                self.log(f"Searching selected cities in {state}: {', '.join(city_list)}")

            self.log(f"Searching {len(search_plan)} subcategory search(es):")
            for main_cat, sub in search_plan:
                self.log(f"  - {main_cat}  ->  {sub}")

            total_jobs = len(city_list) * len(search_plan)
            job_idx = 0

            for current_city in city_list:
                if self.stop_requested:
                    break

                self.log(f"=== City: {current_city}, {state} ===")

                for main_cat, subcategory in search_plan:
                    if self.stop_requested:
                        break

                    job_idx += 1
                    self.log(f"[{job_idx}/{total_jobs}] Searching: city='{current_city}' | main='{main_cat}' | sub='{subcategory}'")

                    businesses = self.client.search_bbb(
                        business_type=subcategory,
                        city=current_city,
                        state_code=state,
                        stop_checker=lambda: self.stop_requested,
                        global_seen_profile_urls=global_seen_profile_urls,
                        logger=self.log,
                    )

                    businesses = self.client.enrich_missing_emails(
                        businesses,
                        stop_checker=lambda: self.stop_requested,
                        logger=self.log,
                    )

                    self.log(f"Found {len(businesses)} VALID record(s) for '{subcategory}' in {current_city}")

                    new_in_sub = 0
                    for item in businesses:
                        row_key = self.client._dedupe_key(item)
                        if row_key in seen_rows:
                            continue

                        seen_rows.add(row_key)

                        # FIX: City and State in Excel always come from the
                        # extracted address first, then URL, never from the
                        # search query city.
                        extracted_city, extracted_state = self.client._extract_city_state_from_address(
                            item.get("address", "")
                        )
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
                        self.log(
                            f"#{saved}: {item.get('business_name')} | "
                            f"{final_city}, {final_state} | "
                            f"cat_score={item.get('_category_score', 0):.2f}"
                        )

                        if self.stop_requested:
                            break

                    writer.save()
                    self.log(f"Saved {new_in_sub} new row(s) from '{subcategory}' in {current_city}")

            writer.save()
            label = "Stopped" if self.stop_requested else "Done"
            self.status_var.set(f"{label}. Saved {saved} row(s).")
            self.log(f"{label}. File saved: {output_path}")

        except Exception as e:
            self.status_var.set("Error")
            self.log(f"Error: {e}")
            try:
                messagebox.showerror("Search error", str(e))
            except Exception:
                pass
        finally:
            try:
                writer.save()
            except Exception:
                pass
            self.start_btn.configure(state="normal")
            self.stop_btn.configure(state="disabled")


if __name__ == "__main__":
    app = App()
    app.mainloop()
