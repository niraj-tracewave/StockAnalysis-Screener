import calendar
import os
import re
import unicodedata
from collections import defaultdict, deque

import aiohttp
import pandas as pd
import pyotp
import uuid
import base64
import requests
from bs4 import BeautifulSoup

from app.core.config import get_settings
from app.core.constants import PARENT_CHILD_MAP, PARENT_CHILD_MAP_NBFC_INDAS, PARENT_CHILD_MAP_GI, PARENT_CHILD_MAP_LI

settings = get_settings()

ANGEL_NSE_MAP = {}

async def generate_otp_from_pyotp():
    user_uuid = uuid.uuid4()
    secret = base64.b32encode(user_uuid.bytes).decode('utf-8').rstrip("=")
    totp = pyotp.TOTP(secret, interval=1)
    otp = totp.now()
    return otp, secret

async def verify_otp(request_dict):
    is_production = settings.is_production
    if not is_production:
        return True
    totp = pyotp.TOTP(request_dict.get("secret"), interval=1)
    return totp.verify(request_dict.get("otp"), valid_window=int(settings.otp_valid_window))

async def generate_otp(request_dict):
    is_production = settings.is_production
    if not is_production:
        return "123456", "CMVIOOIYA5GQDK23FIMP55MQPE"
    otp, secret = await generate_otp_from_pyotp()
    return otp, secret

async def send_otp(otp, secret, request_dict):
    is_production = settings.is_production
    if is_production:
        url = f"http://ahd.sendsmsbox.com/api/mt/SendSMS?user=tracewave&password=tracewave14&senderid=TRNSWV&channel=Trans&DCS=0&flashsms=0&number={request_dict.get('mobile_number')}&text=Hello customer,\nYour OTP for login is {otp}. This OTP is valid for 5 minutes.Please do not share it with anyone.\nTRACEWAVE##&Peid=0&DLTTemplateId=1707173510885060059"
        response = requests.get(url)

from datetime import datetime, date


async def parse_qtr(qtr: str) -> str:
    formats = [
        "%B %Y",     # March 2021
        "%b %Y",     # Mar 2021 (just in case)
        "%d %b %Y",  # 11 Feb 2022
        "%d %B %Y",  # 11 February 2022
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(qtr.strip(), fmt)
            return dt.strftime("%b-%y")
        except ValueError:
            continue

    raise ValueError(f"Unsupported date format: {qtr}")

# def build_response(periods):
#     response = {
#         "periods": [],
#         "sections": {}
#     }
#
#     for period in periods:
#         response["periods"].append(
#             period.period_date.strftime("%b %Y")
#         )
#
#         for section in period.sections:
#             sec = response["sections"].setdefault(
#                 section.key,
#                 {
#                     "key": section.key,
#                     "label": section.label,
#                     "total": [],
#                     "format": "number" if section.value_type == "number" else None,
#                     "children": {}
#                 }
#             )
#
#             sec["total"].append(float(section.total_value))
#
#             for c in section.children:
#                 sec["children"].setdefault(c.label, []).append(float(c.value))
#
#     # convert children dict → list
#     final_sections = []
#     for sec in response["sections"].values():
#         if sec["children"]:
#             sec["children"] = [
#                 {"label": k, "values": v}
#                 for k, v in sec["children"].items()
#             ]
#         else:
#             sec.pop("children")
#         if sec["format"] is None:
#             sec.pop("format")
#         final_sections.append(sec)
#
#     response["sections"] = final_sections
#     return response

async def parse_period_to_date(period_str: str) -> date:
    # "Sep-25" → 2025-09-30
    month_str, year_str = period_str.split("-")
    month = {
        "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
        "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
        "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
    }[month_str]

    year = 2000 + int(year_str)
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, last_day)

def fetch_nse_scrip_code(nse_symbol, listing_at_group):
    url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    response = requests.get(url, timeout=30)
    df = pd.DataFrame(response.json())

    df = df[df["exch_seg"].isin(["NSE", "BSE"])]

    lookup = {}
    for _, row in df.iterrows():
        lookup[(row["symbol"], row["exch_seg"])] = row["token"]

    symbol = nse_symbol

    token = None

    if listing_at_group in ["NSE", "NSE, BSE"]:
        token = lookup.get((f"{symbol}-EQ", "NSE")) or lookup.get((f"{symbol}-SM", "NSE")) or lookup.get(
            (f"{symbol}-SQ", "NSE")) or lookup.get((f"{symbol}-ST", "NSE")) or lookup.get((f"{symbol}-BE", "NSE"))

    if listing_at_group in ["BSE", "NSE, BSE"]:
        token = lookup.get((symbol, "BSE")) or lookup.get((f"{symbol}-SM", "BSE")) or lookup.get(
            (f"{symbol}-SQ", "BSE")) or lookup.get((f"{symbol}-ST", "BSE")) or lookup.get((f"{symbol}-BE", "BSE"))

    return token


async def fetch_top_50_company_from_nse():
    url = "https://www.nseindia.com/api/NextApi/apiClient/indexTrackerApi?functionName=getContributionData&&index=NIFTY 50&&noofrecords=0&&flag=1"
    headers = {
        "authority": "www.nseindia.com",
        "method": "GET",
        "path": "/api/NextApi/apiClient/indexTrackerApi?functionName=getContributionData&&index=NIFTY%2050&&noofrecords=0&&flag=1",
        "scheme": "https",
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "if-none-match": "\"17i5kdsty1y7dp\"",
        "priority": "u=1, i",
        "referer": "https://www.nseindia.com/index-tracker/NIFTY 50",
        "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Linux\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    }
    session = requests.Session()
    response = session.get(url, headers=headers, timeout=30)

    top_50_company = response.json()
    top_50_company_symbols = []
    if top_50_company:
        top_50_company = top_50_company.get("data")
        for row in top_50_company:
            top_50_company_symbols.append(row["icSymbol"])

    return top_50_company_symbols

def sync_fetch_top_50_company_from_nse():
    url = "https://www.nseindia.com/api/NextApi/apiClient/indexTrackerApi?functionName=getContributionData&&index=NIFTY 50&&noofrecords=0&&flag=1"
    headers = {
        "authority": "www.nseindia.com",
        "method": "GET",
        "path": "/api/NextApi/apiClient/indexTrackerApi?functionName=getContributionData&&index=NIFTY%2050&&noofrecords=0&&flag=1",
        "scheme": "https",
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "if-none-match": "\"17i5kdsty1y7dp\"",
        "priority": "u=1, i",
        "referer": "https://www.nseindia.com/index-tracker/NIFTY 50",
        "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Linux\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    }
    session = requests.Session()
    response = session.get(url, headers=headers, timeout=30)

    top_50_company = response.json()
    top_50_company_symbols = []
    if top_50_company:
        top_50_company = top_50_company.get("data")
        for row in top_50_company:
            top_50_company_symbols.append(row["icSymbol"])

    return top_50_company_symbols



def load_angel_map():
    global ANGEL_NSE_MAP

    with open("OpenAPIScripMaster.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    ANGEL_NSE_MAP = {
        item["symbol"]: item["token"]
        for item in data
        if item.get("exch_seg") == "NSE"
    }

    print("Angel NSE map reloaded")

async def fetch_json_from_angle_one():
    url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=30) as response:
            response.raise_for_status()
            data = await response.json()

    # write to file (file write is sync, but fine for most cases)
    with open("OpenAPIScripMaster.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    load_angel_map()

    print("JSON file saved successfully")

import json

def normalize_symbol(symbol):
    if not symbol:
        return None

    # only process symbols like RELIANCE-EQ
    if "-" in symbol:
        return symbol.split("-")[0].strip()

    if not "-" in symbol:
        return symbol

    # if already RELIANCE → skip
    return None

def filter_exchange_data_from_file(file_path):
    """
    Read JSON file and return filtered data where:
    - exch_seg is NSE or BSE
    - instrumenttype is empty
    """
    allowed_exchanges = {"NSE", "BSE"}

    with open(file_path, "r") as f:
        data = json.load(f)

    filtered = [
        normalize_symbol(item.get("symbol")) for item in data
        if item.get("exch_seg") in allowed_exchanges
        and item.get("instrumenttype") == ""
    ]

    filtered = list(set(filtered))

    return filtered

async def fetch_symbols_from_covered_symbol_json():
    file_path = "covered_symbols.json"
    skipped_symbols = {}

    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as f:
                data = json.load(f)

            old_skipped = set(data.get("skipped", []))

            print(f"{len(skipped_symbols)} skipped from json")
            return old_skipped

        except Exception as e:
            return {}
    return {}

async def fetch_symbols_from_covered_symbol_json_for_quarterly_result():
    file_path = "covered_symbols_quarterly_result.json"
    skipped_symbols = {}

    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as f:
                data = json.load(f)

            old_skipped = set(data.get("processing", []))
            old_unsaved = set(data.get("unsaved", []))
            old_error = set(data.get("error", []))

            print(f"{len(skipped_symbols)} skipped from json")
            return old_skipped | old_unsaved | old_error

        except Exception as e:
            return {}
    return {}

async def normalize(text):
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip().lower()

async def inject_values_into_hierarchy(structure, values_json):

    value_lookup = {
        await normalize(item["heading"]): item["value"]
        for item in values_json
        if item.get("value") is not None
    }

    async def traverse(nodes):
        for node in nodes:
            key = await normalize(node["heading"])

            if key in value_lookup:
                node["value"] = value_lookup[key]

            await traverse(node["child"])

    await traverse(structure)
    return structure
#
# async def gi_inject_values_into_hierarchy(structure, values_json):
#
#     value_lookup = defaultdict(list)
#
#     # Step 1: build lookup with heading ONLY (raw source)
#     for item in values_json:
#         if item.get("value") is not None:
#             key = await normalize(item["heading"])
#             value_lookup[key].append(item["value"])
#
#     # Step 2: traverse with index tracking
#     used_index = defaultdict(int)
#
#     async def traverse(nodes):
#         for node in nodes:
#             key = await normalize(node["heading"])
#
#             if key in value_lookup:
#                 idx = used_index[key]
#
#                 if idx < len(value_lookup[key]):
#                     node["value"] = value_lookup[key][idx]
#                     used_index[key] += 1   # move pointer
#
#             await traverse(node["child"])
#
#     await traverse(structure)
#     return structure


async def gi_inject_values_into_hierarchy(structure, values_json):

    value_lookup = defaultdict(deque)

    for item in values_json:
        if item.get("heading"):
            key = await normalize(item["heading"])
            value_lookup[key].append(item["value"])

    normalized_map = {}
    for parent, children in PARENT_CHILD_MAP_GI.items():
        p_norm = await normalize(parent)
        normalized_map[p_norm] = set([await normalize(c) for c in children])

    async def traverse(nodes, parent=None):
        for node in nodes:
            child_key = await normalize(node["heading"])
            parent_key = await normalize(parent) if parent else None

            # ✅ STRICT: only assign if mapping matches
            if parent_key in normalized_map:
                if child_key in normalized_map[parent_key]:
                    if value_lookup[child_key]:
                        node["value"] = value_lookup[child_key].popleft()

            await traverse(node["child"], node["heading"])

    await traverse(structure)
    return structure


async def li_inject_values_into_hierarchy(structure, values_json):

    # -------------------------
    # STEP 1 — build lookup (heading → queue of values)
    # -------------------------
    value_lookup = defaultdict(deque)

    for item in values_json:
        if item.get("heading"):
            key = await normalize(item["heading"])
            value_lookup[key].append(item.get("value"))

    # -------------------------
    # STEP 2 — normalize parent-child map
    # -------------------------
    normalized_map = {}
    for parent, children in PARENT_CHILD_MAP_GI.items():
        p_norm = await normalize(parent)
        normalized_map[p_norm] = set([await normalize(c) for c in children])

    # -------------------------
    # STEP 3 — track index per (parent, child)
    # -------------------------
    usage_counter = defaultdict(int)

    # -------------------------
    # STEP 4 — recursive traversal
    # -------------------------
    async def traverse(nodes, parent=None):
        parent_key = await normalize(parent) if parent else None

        for node in nodes:
            child_key = await normalize(node["heading"])

            # unique key for duplicate tracking
            unique_key = (parent_key, child_key)

            # ✅ STRICT mapping check
            if parent_key in normalized_map:
                if child_key in normalized_map[parent_key]:

                    if value_lookup[child_key]:
                        index = usage_counter[unique_key]

                        # assign sequential value safely
                        try:
                            node["value"] = value_lookup[child_key][index]
                        except IndexError:
                            node["value"] = None

                        usage_counter[unique_key] += 1

            # 🔁 recurse
            if node.get("child"):
                await traverse(node["child"], node["heading"])

    # -------------------------
    # STEP 5 — run traversal
    # -------------------------
    await traverse(structure)

    return structure

# async def gi_build_hierarchy(flat_data, parent_child_map):
#     import json
#     from collections import defaultdict
#
#     if isinstance(flat_data, str):
#         flat_data = json.loads(flat_data)
#
#     # -------------------------
#     # STEP 1 — create parent containers
#     # -------------------------
#     parent_nodes = {}
#     for parent in parent_child_map.keys():
#         p_key = await normalize(parent)
#         parent_nodes[p_key] = {
#             "heading": parent.title(),
#             "value": None,
#             "child": []
#         }
#
#     # -------------------------
#     # STEP 2 — build child → MULTIPLE parents lookup
#     # -------------------------
#     child_to_parents = defaultdict(list)
#
#     for parent, children in parent_child_map.items():
#         p_key = await normalize(parent)
#         for child in children:
#             c_key = await normalize(child)
#             child_to_parents[c_key].append(p_key)
#
#     # -------------------------
#     # STEP 3 — insert nested parents
#     # -------------------------
#     for parent, children in parent_child_map.items():
#         parent_key = await normalize(parent)
#
#         for child in children:
#             child_key = await normalize(child)
#
#             if child_key == parent_key:
#                 continue
#
#             if child_key in parent_nodes:
#                 parent_nodes[parent_key]["child"].append(parent_nodes[child_key])
#
#     # -------------------------
#     # STEP 4 — SECTION-AWARE parsing (🔥 MAIN FIX)
#     # -------------------------
#     current_section = None
#
#     for row in flat_data:
#         if not isinstance(row, dict):
#             continue
#
#         heading_raw = row.get("heading")
#         if not heading_raw:
#             continue
#
#         heading_raw = heading_raw.strip()
#         heading = await normalize(heading_raw)
#         value = row.get("value")
#
#         # ✅ detect section (parent)
#         if heading in parent_nodes:
#             current_section = heading
#             continue
#
#         # skip if no active section
#         if not current_section:
#             continue
#
#         # skip parent rows with no value
#         if heading in parent_nodes and value is None:
#             continue
#
#         # ✅ ONLY attach to CURRENT SECTION (IMPORTANT FIX)
#         if heading in child_to_parents:
#             if current_section in child_to_parents[heading]:
#
#                 node = {
#                     "heading": heading,
#                     "value": value,
#                     "child": []
#                 }
#
#                 parent_nodes[current_section]["child"].append(node)
#
#     # -------------------------
#     # STEP 5 — find root nodes
#     # -------------------------
#     all_children = set(child_to_parents.keys())
#
#     roots = []
#     for parent in parent_child_map:
#         p_key = await normalize(parent)
#         if p_key not in all_children:
#             roots.append(parent_nodes[p_key])
#
#     return roots

async def gi_build_hierarchy(flat_data, parent_child_map):
    import json
    from collections import defaultdict

    if isinstance(flat_data, str):
        flat_data = json.loads(flat_data)

    # -------------------------
    # STEP 1 — create parent containers
    # -------------------------
    parent_nodes = {}
    for parent in parent_child_map.keys():
        p_key = await normalize(parent)
        parent_nodes[p_key] = {
            "heading": parent.title(),
            "value": None,
            "child": []
        }

    # -------------------------
    # STEP 2 — build child → MULTIPLE parents lookup
    # -------------------------
    child_to_parents = defaultdict(list)

    for parent, children in parent_child_map.items():
        p_key = await normalize(parent)
        for child in children:
            c_key = await normalize(child)
            child_to_parents[c_key].append(p_key)

    # -------------------------
    # STEP 3 — insert nested parents
    # -------------------------
    for parent, children in parent_child_map.items():
        parent_key = await normalize(parent)

        for child in children:
            child_key = await normalize(child)

            if child_key == parent_key:
                continue

            if child_key in parent_nodes:
                parent_nodes[parent_key]["child"].append(parent_nodes[child_key])

    # -------------------------
    # STEP 4 — AUTO-DETECT SECTIONS
    # -------------------------
    SECTION_KEYS = set()
    for parent, children in parent_child_map.items():
        if children:
            SECTION_KEYS.add(await normalize(parent))

    current_section = None

    # 🔥 track inserted children per section
    section_child_tracker = defaultdict(set)

    # -------------------------
    # STEP 5 — PROCESS FLAT DATA
    # -------------------------
    for row in flat_data:
        if not isinstance(row, dict):
            continue

        heading_raw = row.get("heading")
        if not heading_raw:
            continue

        heading = await normalize(heading_raw.strip())
        value = row.get("value")

        # detect section
        if heading in SECTION_KEYS:
            current_section = heading
            continue

        # standalone parent
        if heading in parent_nodes:
            parent_nodes[heading]["value"] = value
            continue

        if not current_section:
            continue

        # attach valid child
        if heading in child_to_parents:
            if current_section in child_to_parents[heading]:

                node = {
                    "heading": heading,
                    "value": value,
                    "child": []
                }

                parent_nodes[current_section]["child"].append(node)

                # track this child
                section_child_tracker[current_section].add(heading)

    # -------------------------
    # STEP 6 — ADD MISSING CHILDREN (🔥 MAIN FIX)
    # -------------------------
    for parent, children in parent_child_map.items():
        p_key = await normalize(parent)

        for child in children:
            c_key = await normalize(child)

            # skip nested parent case
            if c_key in parent_nodes:
                continue

            # if not already added → add with None
            if c_key not in section_child_tracker[p_key]:
                parent_nodes[p_key]["child"].append({
                    "heading": c_key,
                    "value": None,
                    "child": []
                })

    # -------------------------
    # STEP 7 — find root nodes
    # -------------------------
    all_children = set(child_to_parents.keys())

    roots = []
    for parent in parent_child_map:
        p_key = await normalize(parent)
        if p_key not in all_children:
            roots.append(parent_nodes[p_key])

    return roots

async def build_hierarchy(flat_data, parent_child_map):
    import json

    if isinstance(flat_data, str):
        flat_data = json.loads(flat_data)

    # -------------------------
    # STEP 1 — create parent containers
    # -------------------------
    parent_nodes = {
        await normalize(parent): {
            "heading": parent.title(),
            "value": None,
            "child": []
        }
        for parent in parent_child_map.keys()
    }

    # -------------------------
    # STEP 2 — build child → parent lookup
    # -------------------------
    child_to_parent = {
        await normalize(child): await normalize(parent)
        for parent, childs in parent_child_map.items()
        for child in childs
    }

    # -------------------------
    # STEP 3 — insert nested parents first
    # (IMPORTANT)
    # -------------------------
    for parent, children in parent_child_map.items():
        parent_key = await normalize(parent)

        for child in children:
            child_key = await normalize(child)

            # IMPORTANT: prevent self-reference loop
            if child_key == parent_key:
                continue

            if child_key in parent_nodes:
                parent_nodes[parent_key]["child"].append(parent_nodes[child_key])

    # -------------------------
    # STEP 4 — now attach flat rows in order
    # -------------------------
    for row in flat_data:
        if not isinstance(row, dict):
            continue
        if row.get("heading") is None:
            continue
        heading_raw = row.get("heading", "").strip()
        heading = await normalize(heading_raw)
        value = row.get("value")

        if heading in parent_nodes:
            if value is None:
                continue

        node = {
            "heading": heading,
            "value": value,
            "child": []
        }

        if heading in child_to_parent:
            parent_key = child_to_parent[heading]
            parent_nodes[parent_key]["child"].append(node)

    # -------------------------
    # STEP 5 — find real roots
    # -------------------------
    all_children = set(child_to_parent.keys())

    roots = []
    for parent in parent_child_map:
        if await normalize(parent) not in all_children:
            roots.append(parent_nodes[await normalize(parent)])

    return roots


async def safe_build(data, file_url=None):
    import json
    if isinstance(data, str):
        data = json.loads(data)

    if file_url and "NBFC_INDAS" in file_url:
        return await build_hierarchy(data, PARENT_CHILD_MAP_NBFC_INDAS)
    if file_url and "GI" in file_url:
        return await gi_build_hierarchy(data, PARENT_CHILD_MAP_GI)
    if file_url and "LI" in file_url:
        return await gi_build_hierarchy(data, PARENT_CHILD_MAP_LI)
    return await build_hierarchy(data, PARENT_CHILD_MAP)

async def parse_numeric(text):
    if not text:
        return None

    text = text.strip()

    is_negative = text.startswith("(") and text.endswith(")")
    text = text.replace("(", "").replace(")", "").replace(",", "")

    try:
        value = float(text)
        return -value if is_negative else value
    except ValueError:
        return None
async def extract_text(tag):
    if tag.find("b"):
        return tag.find("b").get_text(strip=True)
    return tag.get_text(" ", strip=True)

async def get_current_valuess(ths):
    if not ths:
        return None

    # ❌ Skip header row
    first_text = ths[0].get_text(strip=True).lower()
    if "particulars" in first_text:
        return None

    # 🔍 Extract all numeric values from all <th>
    values = []

    for th in ths:
        # Prefer <b> tags (NSE pattern)
        bs = th.find_all("b")
        for b in bs:
            text = b.get_text(strip=True)
            if re.search(r"\d", text):
                values.append(text)

        # fallback if no <b>
        if not bs:
            text = th.get_text(" ", strip=True)
            if re.search(r"\d", text):
                values.append(text)

    # ❗ Remove serial number (like 6, 10, 17)
    if values and len(values[0]) <= 3:
        values = values[1:]

    # ✅ Return ONLY current value
    return values[0] if values else None


async def extract_heading(tag):
    """Extract clean heading text"""
    if not tag:
        return None
    heading = tag.get_text(" ", strip=True)
    return heading

async def extract_heading_one(tag):
    """Extract clean heading text"""
    if tag:
        ths = tag.find_all("th")
        if ths:
            for b in ths[0].find("b"):
                return b.get_text(strip=True)
        return None

    return None

async def extract_numeric_values(tag):
    """Extract all numeric values from a tag (handles nested/broken HTML)"""
    if not tag:
        return []

    values = []

    # First try: <b> tags (NSE pattern)
    for b in tag.find_all("b"):
        text = b.get_text(strip=True)
        if re.search(r"\d", text):
            values.append(text)

    # Fallback: direct text (if no <b>)
    if not values:
        text = tag.get_text(" ", strip=True)
        if re.search(r"\d", text):
            values.append(text)

    return values


async def parse_row_dynamic(ths):
    """Fully dynamic parser for NSE table rows"""

    if not ths:
        return None
    # ❌ Skip header rows
    first_text = ths[0].get_text(strip=True).lower()
    # if "particulars" in first_text:
    #     return None
    print(first_text, "-------------7777", ths)
    data = {
        "heading": None,
        "current_value": None,
        "previous_value": None,
    }

    # ✅ Step 1: Extract heading (usually 2nd th)
    if len(ths) >= 2:
        data["heading"] = await extract_heading(ths[1])
    elif len(ths) == 1:
        data["heading"] = await extract_heading_one(ths[0])

    # ✅ Step 2: Extract all numeric values from ALL ths
    all_values = []
    for th in ths:
        vals = await extract_numeric_values(th)
        all_values.extend(vals)

    # ❗ Remove serial number (like 6, 10, 17)
    if all_values and len(all_values[0]) <= 3:
        all_values = all_values[1:]

    # ✅ Assign values dynamically
    if len(all_values) >= 1:
        data["current_value"] = all_values[0]

    if len(all_values) >= 2:
        data["previous_value"] = all_values[1]

    return data

async def get_heading_from_row(row):
    texts = []

    # get all text pieces
    for t in row.stripped_strings:
        # skip pure numbers
        if re.fullmatch(r"\d+(\.\d+)?", t):
            continue
        if re.fullmatch(r"\([a-zA-ZivxIVX]+\)", t):
            continue

        texts.append(t)

    # remove first number if it's serial (like 16)
    if texts and texts[0].isdigit():
        texts = texts[1:]

    # join remaining → heading
    return " ".join(texts) if texts else None

async def fetch_th_tr_from_table(rows_data):
    final_data = []
    for row in rows_data:
        tds = row.find_all("td", recursive=False)
        ths = row.find_all("th", recursive=False)
        if not tds and not ths:
            continue

        ths = row.find_all("th", recursive=False)
        tds = row.find_all("td")
        f_json: dict[str, str | None] = {
            "heading": None,
            "value": None,
        }
        if ths:
            if len(ths) > 1:
                th = ths[1]
                for td in th.find_all("td"):
                    td.extract()
                section_name = th.get_text(strip=True)
            else:
                section_name = None
            # section_name = ths[1].get_text(strip=True) if len(ths) > 1 else None
            f_json['heading'] = section_name
            if tds:
                if len(tds) == 3:
                    section_name = await extract_text(tds[0]) if len(tds) > 1 else None
                    f_json['heading'] = section_name
                    text = tds[1].get_text(strip=True) if len(tds) > 1 else None
                    value = await parse_numeric(text)
                elif len(tds) == 2 and len(ths) == 2:
                    section_name = await extract_text(ths[1]) if len(ths) == 2 else None
                    f_json['heading'] = section_name
                    text = tds[0].get_text(strip=True) if len(tds) == 2 else None
                    value = await parse_numeric(text)
                elif len(tds) == 2 and len(ths) == 1:
                    section_name = await get_heading_from_row(ths[0]) if len(ths) == 1 else None
                    f_json['heading'] = re.sub(r'\s+\d[\d,]*\.\d+', '', section_name).strip()
                    text = tds[0].get_text(strip=True) if len(tds) == 2 else None

                    value = await parse_numeric(text)

                else:
                    text = await extract_text(tds[0]) if len(tds) > 1 else None
                    value = await parse_numeric(text)
                f_json['value'] = value
            else:
                j_value = await parse_row_dynamic(ths)
                if j_value:
                    j_heading = j_value.get("heading")
                    if j_heading:
                        result = j_heading.split()
                        if result:
                            remove_vals = {j_value.get('current_value'), j_value.get('previous_value')}

                            data = [x for x in result if x not in remove_vals]
                            heading = " ".join(data)
                        if heading:
                            f_json['heading'] = heading
                    value = await parse_numeric(j_value.get("current_value"))
                    f_json['value'] = value

            final_data.append(f_json)
    return final_data


async def fetch_th_tr_from_gi_table(rows_data):
    final_data = []
    for row in rows_data:
        tds = row.find_all("td", recursive=False)
        ths = row.find_all("th", recursive=False)
        if not tds and not ths:
            continue

        ths = row.find_all("th", recursive=False)
        tds = row.find_all("td")
        f_json: dict[str, str | None] = {
            "heading": None,
            "value": None,
        }
        if tds:
            if len(tds) == 3:
                section_name = await extract_text(tds[1]) if len(tds) > 1 else None
                f_json['heading'] = section_name
            elif len(tds) == 1:
                section_name = await extract_text(tds[0]) if len(tds) == 1 else None
                f_json['heading'] = section_name

            elif len(tds) == 4:
                section_name = await extract_text(tds[1]) if len(tds) == 4 else None
                f_json['heading'] = section_name
                text = tds[2].get_text(strip=True) if len(tds) == 4 else None
                value = await parse_numeric(text)
                f_json['value'] = value
            elif len(tds) == 2 and len(ths) == 1:
                section_name = await get_heading_from_row(ths[0]) if len(ths) == 1 else None
                f_json['heading'] = re.sub(r'\s+\d[\d,]*\.\d+', '', section_name).strip()
                text = tds[0].get_text(strip=True) if len(tds) == 2 else None

                value = await parse_numeric(text)
                f_json['value'] = value

            final_data.append(f_json)
    return final_data

async def fetch_th_tr_from_li_table(rows_data):
    final_data = []
    previous_raw = None
    for row in rows_data:
        tds = row.find_all("td", recursive=False)
        ths = row.find_all("th", recursive=False)
        if not tds and not ths:
            continue

        ths = row.find_all("th", recursive=False)
        tds = row.find_all("td")
        f_json: dict[str, str | None] = {
            "heading": None,
            "value": None,
        }
        titles = {"Gross NPAs" : "Shareholders Gross NPAs",
                  "Net NPAs": "Shareholders Net NPAs",
                  "Percentage of Gross NPAs": "Shareholders Percentage of Gross NPAs",
                  "Percentage of Net NPAs": "Shareholders Percentage of Net NPAs",
                  "Without unrealised gains":  "Shareholders Without unrealised gains",
                  "With unrealised gains": "Shareholders With unrealised gains",}
        if tds:
            if len(ths) == 2 and len(tds) == 2:
                section_name = await extract_text(ths[1]) if len(ths) > 1 else None
                f_json['heading'] = section_name
                text = tds[0].get_text(strip=True) if len(tds) > 1 else None
                value = await parse_numeric(text)
                f_json['value'] = value
            elif len(ths) == 1 and len(tds) == 3:
                section_name = await extract_text(tds[0]) if len(tds) > 1 else None
                f_json['heading'] = section_name
                text = tds[1].get_text(strip=True) if len(tds) > 1 else None
                value = await parse_numeric(text)
                f_json['value'] = value
            elif len(ths) == 2 and len(tds) == 1:
                section_name = await extract_text(ths[1]) if len(ths) > 1 else None
                f_json['heading'] = section_name
        else:
            if len(ths) == 3:
                section_name = await extract_text(ths[1]) if len(ths) > 1 else None
                f_json['heading'] = section_name
            elif len(ths) == 4:
                section_name = await extract_text(ths[1]) if len(ths) > 1 else None
                f_json['heading'] = section_name
            elif len(ths) == 2:
                section_name = await extract_text(ths[1]) if len(ths) > 1 else None
                f_json['heading'] = section_name
            elif len(ths) == 1:
                section_name = await extract_text(ths[0]) if len(ths) > 0 else None
                f_json['heading'] = section_name

        final_data.append(f_json)
    return final_data

async def fetch_bse_th_tr_from_table(rows_data):
    final_data = []
    for row in rows_data:
        tds = row.find_all("td", recursive=False)
        ths = row.find_all("th", recursive=False)
        if not tds and not ths:
            continue

        tds = row.find_all("td")
        f_json = {
            "heading": None,
            "value": None,
        }
        if tds:
            section_name = tds[1].get_text(strip=True) if len(tds) > 1 else None
            f_json['heading'] = section_name
            if tds:
                value=None
                if len(tds) == 4:
                    section_name = tds[1].get_text(strip=True) if len(tds) > 1 else None
                    f_json['heading'] = section_name
                    value_tag = tds[2].find("ix:nonfraction")
                    text = value_tag.get_text(strip=True) if value_tag else tds[2].get_text(strip=True)
                    value = await parse_numeric(text)
                f_json['value'] = value
            final_data.append(f_json)
    return final_data


async def extract_table_as_dict(soup, table):
    data = {}

    for row in table.find_all("tr"):
        cols = row.find_all(["td", "th"])

        if len(cols) >= 2:
            key = cols[0].get_text(strip=True)
            value = cols[1].get_text(strip=True)

            data[key] = value

    return data

async def fetch_integrated_filing_financials_data_from_nse(url):
    try:
        structured_with_values = []
        session = requests.Session()

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nseindia.com/",
            "Connection": "keep-alive"
        }

        # first hit homepage to get cookies
        session.get("https://www.nseindia.com", headers=headers)

        path = url.split("nsearchives.nseindia.com")[-1]

        headers = {
            "authority": "nsearchives.nseindia.com",
            "method": "GET",
            "path": path,
            "scheme": "https",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "max-age=0",
            "if-none-match": "W/\"46855-1768223605988\"",
            "priority": "u=0, i",
            "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Linux\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
        }
        resp = session.get(url, headers=headers)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            heading = soup.find("h3", string=lambda x: x and "General information" in x)
            gITable = heading.find_next("table")
            table_data = await extract_table_as_dict(soup, gITable)
            value = table_data.get("Level of rounding used in financial results", "Crores")

            if "GI" in url:
                tables = soup.find_all("table", class_="stockExchnageTableLastColwidth")
                # print(tables)
                table = None
                total_rows = []
                if tables and len(tables) > 1:
                    for table1 in tables[:4]:
                        table = table1
                        rows = [
                            tr for tr in table.find_all("tr")
                            if tr.get_text(strip=True)
                        ]
                        total_rows.extend(rows)
                else:
                    tables = soup.find_all("table")
                    for table1 in tables[:5]:
                        table = table1
                        rows = [
                            tr for tr in table.find_all("tr")
                            if tr.get_text(strip=True)
                        ]
                        total_rows.extend(rows)
                final_data = await fetch_th_tr_from_gi_table(total_rows)
                # print(final_data, "---------------------f---------------------------")
                structured = await safe_build(final_data, url)
                structured_with_values = await gi_inject_values_into_hierarchy(
                    structured,
                    final_data
                )
                return structured_with_values, value
            elif "LI" in url:
                total_rows = []
                tables = soup.find_all("table")
                for table1 in tables[3:5]:
                    table = table1
                    rows = [
                        tr for tr in table.find_all("tr")
                        if tr.get_text(strip=True)
                    ]
                    total_rows.extend(rows)
                final_data = await fetch_th_tr_from_li_table(total_rows)
                structured = await safe_build(final_data, url)
                structured_with_values = await li_inject_values_into_hierarchy(
                    structured,
                    final_data
                )
                return structured_with_values, value
            else:
                tables = soup.find_all("table", class_="stockExchnageTableLastColwidth")
                other_tables = soup.find_all("table", class_="customTablewidth3Col")
                table = None
                if tables and len(tables) > 1:
                    for table1 in tables[:1]:
                        table = table1
                elif other_tables:
                    for table1 in other_tables[:1]:
                        table = table1
                else:
                    tables = soup.find_all("table")
                    for table1 in tables[1:2]:
                        table = table1
                rows = [
                    tr for tr in table.find_all("tr")
                    if tr.get_text(strip=True)
                ]

                final_data = await fetch_th_tr_from_table(rows)
                structured = await safe_build(final_data, url)
                structured_with_values = await inject_values_into_hierarchy(
                    structured,
                    final_data
                )
                return structured_with_values, value

        return structured_with_values, None
    except Exception as e:
        return [], None


async def build_node(item, node_map, quarter_index, amount_type):
    heading = item.get("heading")
    value = item.get("value")
    children = item.get("child", [])
    # convert lakhs → crores
    if amount_type == "Lakhs":
        if isinstance(value, (int, float)):
            # value = round(value / 100, 2)
            if value.is_integer() and value not in [1,2,3,4,5,6,7,8,9,10]:
                value = round(value / 100, 2)
    elif amount_type == "Crores":
        if isinstance(value, (int, float)):
            # value = round(value / 1_00_00_000, 2)
            if value.is_integer() and value not in [1,2,3,4,5,6,7,8,9,10]:
                value = value / 1_00_00_000

    if heading not in node_map:
        node_map[heading] = {
            "key": heading.lower().replace(" ", "_"),
            "label": heading,
            "type": "group" if children else "single",
            "unit": "Rs Cr",
            "values": [],
            "children": {}
        }

    node = node_map[heading]

    # IMPORTANT: create placeholders for previous quarters
    if len(node["values"]) <= quarter_index:
        node["values"].extend([None] * (quarter_index + 1 - len(node["values"])))

    # set ONLY this quarter value
    node["values"][quarter_index] = value

    # process children recursively
    for child in children:
        await build_node(child, node["children"], quarter_index, amount_type)

async def bse_build_node(item, node_map, quarter_index, amount_type):
    heading = item.get("heading")
    value = item.get("value")
    children = item.get("child", [])

    if amount_type == "Millions":
        if isinstance(value, (int, float)):
            value = round(value / 10)
    elif amount_type == "Lakhs":
        if isinstance(value, (int, float)):
            value = round(value / 100, 2)
    elif amount_type == "Crores":
        value = value


    if heading not in node_map:
        node_map[heading] = {
            "key": heading.lower().replace(" ", "_"),
            "label": heading,
            "type": "group" if children else "single",
            "unit": "Rs Cr",
            "values": [],
            "children": {}
        }

    node = node_map[heading]

    # IMPORTANT: create placeholders for previous quarters
    if len(node["values"]) <= quarter_index:
        node["values"].extend([None] * (quarter_index + 1 - len(node["values"])))

    # set ONLY this quarter value
    node["values"][quarter_index] = value

    # process children recursively
    for child in children:
        await bse_build_node(child, node["children"], quarter_index, amount_type)

async def convert_to_quarterly_format(response_list):
    headers = []
    root_map = {}

    for quarter_index, quarter in enumerate(response_list):

        meta = quarter[-1]
        headers.append(meta.get("date"))

        for item in quarter[:-1]:
            await build_node(item, root_map, quarter_index, meta.get("amount_type"))

    # convert children dict → list
    def finalize(node):
        if node["children"]:
            node["children"] = [finalize(child) for child in node["children"].values()]
        else:
            node.pop("children", None)
        return node

    rows = [finalize(node) for node in root_map.values()]

    return {
        "headers": headers,
        "rows": rows
    }

async def li_build_node(item, quarter_index):
    node = {
        "key": await normalize(item["heading"]),
        "label": item["heading"],
        "values": [None] * (quarter_index + 1),
        "type": "group" if item.get("child") else "single"
    }

    # set value
    if item.get("value") is not None:
        node["values"][quarter_index] = item["value"]

    if item.get("child"):
        node["children"] = [
            await li_build_node(child, quarter_index)
            for child in item["child"]
        ]

    return node


async def li_inject_into_existing(existing_node, new_node, quarter_index):

    # expand values list
    while len(existing_node["values"]) <= quarter_index:
        existing_node["values"].append(None)

    # assign value
    if new_node.get("value") is not None:
        existing_node["values"][quarter_index] = new_node["value"]

    # handle children (IMPORTANT: index-based, not key-based)
    if "children" in existing_node and new_node.get("child"):

        for i, child in enumerate(new_node["child"]):

            if i < len(existing_node["children"]):
                await li_inject_into_existing(
                    existing_node["children"][i],
                    child,
                    quarter_index
                )

async def li_convert_to_quarterly_format(response_list):
    headers = []
    root_nodes = []

    for quarter_index, quarter in enumerate(response_list):

        meta = quarter[-1]
        headers.append(meta.get("date"))

        for i, item in enumerate(quarter[:-1]):

            # first quarter → build structure
            if quarter_index == 0:
                node = await build_node(item, quarter_index)
                root_nodes.append(node)

            # next quarters → inject values
            else:
                await li_inject_into_existing(root_nodes[i], item, quarter_index)

    return {
        "headers": headers,
        "rows": root_nodes
    }

async def bse_convert_to_quarterly_format(response_list):
    headers = []
    root_map = {}

    for quarter_index, quarter in enumerate(response_list):

        meta = quarter[-1]
        headers.append(meta.get("date"))

        for item in quarter[:-1]:
            await bse_build_node(item, root_map, quarter_index, meta.get("amount_type"))

    # convert children dict → list
    def finalize(node):
        if node["children"]:
            node["children"] = [finalize(child) for child in node["children"].values()]
        else:
            node.pop("children", None)
        return node

    rows = [finalize(node) for node in root_map.values()]

    return {
        "headers": headers,
        "rows": rows
    }


async def fetch_bse_integrated_filing_financials_data_from(url):
    try:
        structured_with_values = []
        session = requests.Session()

        headers = {
              "authority": "www.bseindia.com",
              "method": "GET",
              "path": "/",
              "scheme": "https",
              "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
              "accept-encoding": "gzip, deflate, br, zstd",
              "accept-language": "en-US,en;q=0.9",
              "cache-control": "max-age=0",
              "priority": "u=0, i",
              "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
              "sec-ch-ua-mobile": "?0",
              "sec-ch-ua-platform": "\"Linux\"",
              "sec-fetch-dest": "document",
              "sec-fetch-mode": "navigate",
              "sec-fetch-site": "none",
              "sec-fetch-user": "?1",
              "upgrade-insecure-requests": "1",
              "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
            }
        # # first hit homepage to get cookies
        session.get("https://www.bseindia.com/", headers=headers)

        path = url.split("www.bseindia.com/")[-1]

        headers = {
            "authority": "www.bseindia.com",
            "method": "GET",
            "path": path,
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-US,en;q=0.9",
            "accept-encoding": "gzip, deflate, br, zstd",
            "cache-control": "max-age=0",
            "priority": "u=0, i",
            "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Linux\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
        }
        resp = session.get(url, headers=headers)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            amount_type = soup.find("td", string="Level of rounding").find_next("td").text.strip()
            table = soup.select_one("h2:-soup-contains('Financial Results') + p + table")

            if table:
                rows = [
                    tr for tr in table.find_all("tr")
                    if tr.get_text(strip=True)
                ]
                final_data = await fetch_bse_th_tr_from_table(rows)
                structured = await safe_build(final_data)
                structured_with_values = await inject_values_into_hierarchy(
                    structured,
                    final_data
                )
                return structured_with_values, amount_type

            return [], None

        return structured_with_values
    except Exception as e:
        return [], None

def get_today_file():
    today = datetime.now().strftime("%Y-%m-%d")
    return f"processed_symbols_{today}.json"


async def load_processed_symbols():
    file = get_today_file()
    if os.path.exists(file):
        with open(file, "r") as f:
            content = f.read().strip()
            if not content:
                return set()

            data = json.loads(content)

            processed = data.get("processed_symbols", [])
            current = data.get("current_processed_symbols", [])

            return set(processed) | set(current)
    return set()


async def save_processed_symbol(symbols, key):
    file = get_today_file()

    data = {
        "processed_symbols": [],
        "current_processed_symbols": []
    }

    if os.path.exists(file):
        try:
            with open(file, "r") as f:
                content = f.read().strip()
                if content:
                    data = json.loads(content)
        except json.JSONDecodeError:
            pass

    if key == "current_processed_symbols":
        data["current_processed_symbols"].extend(symbols)
        data["current_processed_symbols"] = list(set(data["current_processed_symbols"]))

    elif key == "processed_symbols":
        # add to processed
        data["processed_symbols"].extend(symbols)
        data["processed_symbols"] = list(set(data["processed_symbols"]))

        # remove from current
        current_set = set(data.get("current_processed_symbols", []))
        current_set -= set(symbols)
        data["current_processed_symbols"] = list(current_set)

    with open(file, "w") as f:
        json.dump(data, f, indent=4)

async def save_quarterly_result_processed_symbol(symbols, key):
    file = "covered_symbols_quarterly_result.json"

    data = {
        "processing": [],
        "unsaved": [],
        "error": []
    }

    if os.path.exists(file):
        try:
            with open(file, "r") as f:
                content = f.read().strip()
                if content:
                    data = json.loads(content)
        except json.JSONDecodeError:
            pass

    if key == "processing":
        data["processing"].extend(symbols)
        data["processing"] = list(set(data["processing"]))

    if key == "unsaved":
        data["unsaved"].extend(symbols)
        data["unsaved"] = list(set(data["unsaved"]))

    if key == "error":
        data["error"].extend(symbols)
        data["error"] = list(set(data["error"]))

    elif key == "remove_processing":
        current_set = set(data.get("processing", []))
        current_set -= set(symbols)
        data["processing"] = list(current_set)

    with open(file, "w") as f:
        json.dump(data, f, indent=4)

async def parse_financial_name(text: str):
    parts = text.split("-")

    result = {
        "type": parts[0].lower(),          # Standalone / Consolidated
        "month": parts[1],         # Dec / Sep / Mar
        "period": parts[2].lower(),        # Qtr / Hly / Ann
        "year": int(parts[3])      # 2025
    }

    return result

async def update_nse_bse_scrip_code_load_processed_symbols():
    file = "update_nse_bse_scrip_code.json"
    if os.path.exists(file):
        with open(file, "r") as f:
            content = f.read().strip()
            if not content:
                return set()

            data = json.loads(content)

            processed = data.get("processed_symbols", [])
            current = data.get("current_processed_symbols", [])

            return set(processed) | set(current)
    return set()

async def update_nse_bse_scrip_code_save_processed_symbol(symbols, key):
    file = "update_nse_bse_scrip_code.json"

    data = {
        "processed_symbols": [],
        "current_processed_symbols": []
    }

    if os.path.exists(file):
        try:
            with open(file, "r") as f:
                content = f.read().strip()
                if content:
                    data = json.loads(content)
        except json.JSONDecodeError:
            pass

    if key == "current_processed_symbols":
        data["current_processed_symbols"].extend(symbols)
        data["current_processed_symbols"] = list(set(data["current_processed_symbols"]))

    elif key == "processed_symbols":
        data["processed_symbols"].extend(symbols)
        data["processed_symbols"] = list(set(data["processed_symbols"]))

        # remove from current
        current_set = set(data.get("current_processed_symbols", []))
        current_set -= set(symbols)
        data["current_processed_symbols"] = list(current_set)

    with open(file, "w") as f:
        json.dump(data, f, indent=4)

def get_custom_today_file(file_name):
    today = datetime.now().strftime("%Y-%m-%d")
    return f"{file_name}_{today}.json"

async def update_nse_bse_price_data_load_processed_symbols(file_name):
    file = get_custom_today_file(file_name)
    if os.path.exists(file):
        with open(file, "r") as f:
            content = f.read().strip()
            if not content:
                return set()

            data = json.loads(content)

            processed = data.get("processed_symbols", [])
            current = data.get("current_processed_symbols", [])
            error = data.get("error", [])

            return set(processed) | set(current) | set(error)
    return set()

async def update_nse_bse_price_data_save_processed_symbol(symbols, key, file_name):
    file = get_custom_today_file(file_name)

    data = {
        "processed_symbols": [],
        "current_processed_symbols": [],
        "error": []
    }

    if os.path.exists(file):
        try:
            with open(file, "r") as f:
                content = f.read().strip()
                if content:
                    data = json.loads(content)
        except json.JSONDecodeError:
            pass

    if key == "current_processed_symbols":
        data["current_processed_symbols"].extend(symbols)
        data["current_processed_symbols"] = list(set(data["current_processed_symbols"]))

    elif key == "processed_symbols":
        data["processed_symbols"].extend(symbols)
        data["processed_symbols"] = list(set(data["processed_symbols"]))

        # remove from current
        current_set = set(data.get("current_processed_symbols", []))
        current_set -= set(symbols)
        data["current_processed_symbols"] = list(current_set)

    elif key == "error":
        data["error"].extend(symbols)
        data["error"] = list(set(data["error"]))

        current_set = set(data.get("current_processed_symbols", []))
        current_set -= set(symbols)
        data["current_processed_symbols"] = list(current_set)

    with open(file, "w") as f:
        json.dump(data, f, indent=4)