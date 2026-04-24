"""Path-parsing utilities for extracting metadata from S3/file paths.

Used by graph-analyzer (UC19, UC24, UC26) and catalog-reconciler to derive
territory, licensee, company, crew origin, recipient, and date from paths.
"""

import re


# ── Lookup tables ─────────────────────────────────────────────────────────────

TERRITORY_PATTERNS = {
    "us": "United States", "usa": "United States", "uk": "United Kingdom",
    "gb": "United Kingdom", "de": "Germany", "fr": "France", "jp": "Japan",
    "au": "Australia", "ca": "Canada", "emea": "EMEA", "apac": "APAC",
    "latam": "Latin America", "global": "Global",
}

LOCATION_MARKERS = {
    "la": "Los Angeles", "nyc": "New York", "london": "London",
    "toronto": "Toronto", "vancouver": "Vancouver", "berlin": "Berlin",
    "paris": "Paris", "mumbai": "Mumbai", "tokyo": "Tokyo",
    "sydney": "Sydney", "atlanta": "Atlanta", "chicago": "Chicago",
}


# ── Extraction functions ──────────────────────────────────────────────────────

def extract_recipient(path):
    """Extract delivery recipient from s3_path."""
    parts = path.lower().replace("\\", "/").split("/")
    keywords = {"deliveries", "delivery", "output", "distribution", "clients"}
    for i, part in enumerate(parts):
        if part in keywords and i + 1 < len(parts):
            return parts[i + 1].replace("_", " ").replace("-", " ").title()
    return ""


def extract_date(path):
    """Extract YYYY-MM-DD date from path."""
    match = re.search(r"(\d{4}-\d{2}-\d{2})", path)
    return match.group(1) if match else ""


def extract_territory(path):
    """Extract territory from s3_path."""
    parts = re.split(r"[/\-_]", path.lower())
    for part in parts:
        if part in TERRITORY_PATTERNS:
            return TERRITORY_PATTERNS[part]
    # Check compound patterns
    for part in parts:
        if part.startswith("territory_") or part.startswith("region-"):
            code = part.split("_")[-1].split("-")[-1]
            if code in TERRITORY_PATTERNS:
                return TERRITORY_PATTERNS[code]
    return ""


def extract_licensee(path):
    """Extract licensee from s3_path."""
    parts = path.replace("\\", "/").split("/")
    keywords = {"licensee", "client", "syndication", "partner", "distributor"}
    for i, part in enumerate(parts):
        if part.lower() in keywords and i + 1 < len(parts):
            return parts[i + 1].replace("_", " ").replace("-", " ").title()
    return ""


def extract_company(path):
    """Extract production company from s3_path."""
    parts = path.replace("\\", "/").split("/")
    keywords = {"studio", "production", "company", "producer", "vendor", "facility"}
    skip = {"media", "content", "assets", "video", "raw", "master", "output", ""}

    for i, part in enumerate(parts):
        if part.lower() in keywords and i + 1 < len(parts):
            return parts[i + 1].replace("_", " ").replace("-", " ").title()

    # Fallback: first meaningful directory
    for part in parts:
        if part.lower() not in skip and not part.startswith("."):
            return part.replace("_", " ").replace("-", " ").title()
    return "Unknown"


def extract_crew_origin(path):
    """Extract crew/production location from s3_path."""
    parts = re.split(r"[/\-_]", path.lower())
    for part in parts:
        if part in LOCATION_MARKERS:
            return LOCATION_MARKERS[part]
    return ""


def classify_contribution(rel_type):
    """Classify relationship type into contribution category."""
    rt = (rel_type or "").lower()
    if rt in ("source", "original", "raw"):
        return "source_footage"
    elif rt in ("audio", "music", "sound"):
        return "audio"
    elif rt in ("vfx", "effects", "cgi"):
        return "visual_effects"
    elif rt in ("edit", "cut", "conform"):
        return "editorial"
    elif rt in ("grade", "color", "di"):
        return "color_grade"
    return "source_material"
