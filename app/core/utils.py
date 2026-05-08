import calendar
import os
import re
import unicodedata
from collections import defaultdict, deque
from datetime import datetime
from decimal import Decimal

import aiohttp
import pandas as pd
import pyotp
import uuid
import base64
import requests
from bs4 import BeautifulSoup

from app.apis.models.stock_data import ShareHoldingPeriod, ShareHoldingSectionChild, ShareHoldingSection
from app.core.config import get_settings
from app.core.constants import PARENT_CHILD_MAP, PARENT_CHILD_MAP_NBFC_INDAS, PARENT_CHILD_MAP_GI, PARENT_CHILD_MAP_LI, \
    PARENT_CHILD_MAP_INDAS, PARENT_CHILD_MAP_BANKING, PARENT_CHILD_MAP_INDAS_BSE, PARENT_CHILD_MAP_BANKING_BSE, \
    PARENT_CHILD_MAP_BSE_NBFC, PARENT_CHILD_MAP_BSE_GI, PARENT_CHILD_MAP_BSE_LI

settings = get_settings()

ANGEL_NSE_MAP = {}

def run_async_task(coro):
    import asyncio

    # loop = asyncio.new_event_loop()
    # try:
    #     asyncio.set_event_loop(loop)
    #     return loop.run_until_complete(coro)
    # finally:
    #     loop.run_until_complete(loop.shutdown_asyncgens())
    #     loop.close()
    return asyncio.run(coro)

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
        if item.get("exch_seg") == "NSE" or item.get("exch_seg") == "BSE"
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
            old_processed = set(data.get("processed_symbols", []))

            print(f"{len(skipped_symbols)} skipped from json")
            return old_skipped | old_unsaved | old_error | old_processed

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

async def bse_gi_inject_values_into_hierarchy(structure, values_json):

    value_lookup = defaultdict(deque)

    for item in values_json:
        if item.get("heading"):
            key = await normalize(item["heading"])
            value_lookup[key].append(item["value"])

    normalized_map = {}
    for parent, children in PARENT_CHILD_MAP_BSE_GI.items():
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

async def bse_li_inject_values_into_hierarchy(structure, values_json):

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


async def bse_build_hierarchy(flat_data, parent_child_map):
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
    if file_url and "_GI_" in file_url:
        return await gi_build_hierarchy(data, PARENT_CHILD_MAP_GI)
    if file_url and "_LI_" in file_url:
        return await gi_build_hierarchy(data, PARENT_CHILD_MAP_LI)
    if file_url and "_INDAS_" in file_url:
        return await build_hierarchy(data, PARENT_CHILD_MAP_INDAS)
    if file_url and "_BANKING_" in file_url:
        return await build_hierarchy(data, PARENT_CHILD_MAP_BANKING)
    return await build_hierarchy(data, PARENT_CHILD_MAP)

async def bse_safe_build(data, file_url=None, bse_format=None):
    import json
    if isinstance(data, str):
        data = json.loads(data)

    if bse_format == "NBFC":
        return await build_hierarchy(data, PARENT_CHILD_MAP_BSE_NBFC)
    if bse_format == "General Insurance":
        return await gi_build_hierarchy(data, PARENT_CHILD_MAP_BSE_GI)
    if bse_format == "Life Insurance":
        return await gi_build_hierarchy(data, PARENT_CHILD_MAP_BSE_LI)
    if file_url and "_Ind_As_" in file_url:
        return await bse_build_hierarchy(data, PARENT_CHILD_MAP_INDAS_BSE)
    if bse_format == "Banking":
        return await bse_build_hierarchy(data, PARENT_CHILD_MAP_BANKING_BSE)
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
                  "With unrealised gains": "Shareholders With unrealised gains"}
        if tds:
            if len(ths) == 2 and len(tds) == 2:
                section_name = await extract_text(ths[1]) if len(ths) > 1 else None
                f_json['heading'] = section_name

                if previous_raw and previous_raw == "NPA ratios: (for shareholders' fund)" and titles.get(section_name):
                    f_json["heading"] = titles.get(section_name) or section_name
                text = tds[0].get_text(strip=True) if len(tds) > 1 else None
                value = await parse_numeric(text)
                f_json['value'] = value
                if section_name == "NPA ratios: (for shareholders' fund)":
                    previous_raw = section_name
            elif len(ths) == 1 and len(tds) == 3:
                section_name = await extract_text(tds[0]) if len(tds) > 1 else None
                f_json['heading'] = section_name
                if previous_raw and previous_raw == "NPA ratios: (for shareholders' fund)" and titles.get(section_name):
                    f_json["heading"] = titles.get(section_name)
                text = tds[1].get_text(strip=True) if len(tds) > 1 else None
                value = await parse_numeric(text)
                f_json['value'] = value
                if section_name == "NPA ratios: (for shareholders' fund)":
                    previous_raw = section_name
            elif len(ths) == 2 and len(tds) == 1:
                section_name = await extract_text(ths[1]) if len(ths) > 1 else None
                f_json['heading'] = section_name
                if previous_raw and previous_raw == "NPA ratios: (for shareholders' fund)" and titles.get(section_name):
                    f_json["heading"] = titles.get(section_name)
                if section_name == "NPA ratios: (for shareholders' fund)":
                    previous_raw = section_name
        else:
            if len(ths) == 3:
                section_name = await extract_text(ths[1]) if len(ths) > 1 else None
                f_json['heading'] = section_name
                if previous_raw and previous_raw == "NPA ratios: (for shareholders' fund)" and titles.get(section_name):
                    f_json["heading"] = titles.get(section_name)
                if section_name == "NPA ratios: (for shareholders' fund)":
                    previous_raw = section_name
            elif len(ths) == 4:
                section_name = await extract_text(ths[1]) if len(ths) > 1 else None
                f_json['heading'] = section_name
                if previous_raw and previous_raw == "NPA ratios: (for shareholders' fund)" and titles.get(section_name):
                    f_json["heading"] = titles.get(section_name)
                text = ths[2].get_text(strip=True) if len(ths) > 1 else None
                value = await parse_numeric(text)
                f_json['value'] = value
                if section_name == "NPA ratios: (for shareholders' fund)":
                    previous_raw = section_name
            elif len(ths) == 2:
                section_name = await extract_text(ths[1]) if len(ths) > 1 else None
                f_json['heading'] = section_name
                if previous_raw and previous_raw == "NPA ratios: (for shareholders' fund)" and titles.get(section_name):
                    f_json["heading"] = titles.get(section_name)
                if section_name == "NPA ratios: (for shareholders' fund)":
                    previous_raw = section_name
            elif len(ths) == 1:
                section_name = await extract_text(ths[0]) if len(ths) > 0 else None
                f_json['heading'] = section_name
                if previous_raw and previous_raw == "NPA ratios: (for shareholders' fund)" and titles.get(section_name):
                    f_json["heading"] = titles.get(section_name)
                if section_name == "NPA ratios: (for shareholders' fund)":
                    previous_raw = section_name

        final_data.append(f_json)
    return final_data

async def bse_fetch_th_tr_from_li_table(rows_data):
    final_data = []
    previous_raw = None
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
        titles = {"Gross NPAs": "Shareholders Gross NPAs",
                  "Net NPAs": "Shareholders Net NPAs",
                  "Percentage of Gross NPAs": "Shareholders Percentage of Gross NPAs",
                  "Percentage of Net NPAs": "Shareholders Percentage of Net NPAs",
                  "Without unrealised gains": "Shareholders Without unrealised gains",
                  "With unrealised gains": "Shareholders With unrealised gains"}
        if tds:
            section_name = tds[1].get_text(strip=True) if len(tds) > 1 else None
            f_json['heading'] = section_name
            if section_name == "NPA ratios: (for shareholder's fund)":
                previous_raw = section_name
            if tds:
                value = None
                if len(tds) == 4:
                    section_name = tds[1].get_text(strip=True) if len(tds) > 1 else None
                    if previous_raw and previous_raw == "NPA ratios: (for shareholder's fund)" and titles.get(
                            section_name):
                        f_json["heading"] = titles.get(section_name) or section_name
                    value_tag = tds[2].find("ix:nonfraction")
                    if value_tag:
                        text = value_tag.get_text(strip=True) if value_tag else tds[2].get_text(strip=True)
                        sign = value_tag.get("sign")
                        if sign == "-":
                            text = "-" + text
                        value = await parse_numeric(text)
                    f_json['value'] = value
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
                    if value_tag:
                        text = value_tag.get_text(strip=True) if value_tag else tds[2].get_text(strip=True)
                        sign = value_tag.get("sign")
                        if sign == "-":
                            text = "-" + text
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

            if "_GI_" in url:
                tables = soup.find_all("table", class_="stockExchnageTableLastColwidth")
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
                structured = await safe_build(final_data, url)
                structured_with_values = await gi_inject_values_into_hierarchy(
                    structured,
                    final_data
                )
                return structured_with_values, value, "GI"
            elif "_LI_" in url:
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
                return structured_with_values, value, "LI"
            elif "_INDAS_" in url:
                tables = soup.find_all("table", class_="stockExchnageTableLastColwidth")
                table = None
                if tables and len(tables) > 1:
                    for table1 in tables[:1]:
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
                return structured_with_values, value, "INDAS"
            elif "_BANKING_" in url:
                other_tables = soup.find_all("table", class_="customTablewidth3Col")
                table = None
                if other_tables:
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
                return structured_with_values, value, "BANKING"
            elif "_NBFC_" in url:
                tables = soup.find_all("table", class_="stockExchnageTableLastColwidth")
                table = None
                if tables and len(tables) > 1:
                    for table1 in tables[:1]:
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
                return structured_with_values, value, "NBFC"
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
                return structured_with_values, value, "Other"

        return structured_with_values, None, None
    except Exception as e:
        return [], None, None


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
            if value.is_integer() and value not in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
                value = round(value / 10, 2)

    if amount_type == "Lakhs":
        if isinstance(value, (int, float)):
            # value = round(value / 100, 2)
            if value.is_integer() and value not in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
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

async def bse_gl_convert_to_quarterly_format(response_list):
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

async def li_build_node(item, quarter_index, amount_type):
    value = item.get("value")
    # -------------------------
    # APPLY AMOUNT CONVERSION
    # -------------------------
    if amount_type == "Lakhs":
        if isinstance(value, (int, float)):
            if value.is_integer() and value not in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
                value = value / 100
    elif amount_type == "Crores":
        if isinstance(value, (int, float)):
            if value.is_integer() and value not in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
                value = value / 1_00_00_000

    node = {
        "key": await normalize(item["heading"]),
        "label": item["heading"],
        "values": [None] * (quarter_index + 1),
        "type": "group" if item.get("child") else "single"
    }

    # set value
    if item.get("value") is not None:
        node["values"][quarter_index] = value

    if item.get("child"):
        node["children"] = [
            await li_build_node(child, quarter_index, amount_type)
            for child in item["child"]
        ]

    return node

async def bse_banking_build_node(item, quarter_index, amount_type):
    value = item.get("value")
    # -------------------------
    # APPLY AMOUNT CONVERSION
    # -------------------------
    if amount_type == "Lakhs":
        if isinstance(value, (int, float)):
            if value.is_integer() and value not in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
                value = value / 100
    elif amount_type == "Crores":
        if isinstance(value, (int, float)):
            if value.is_integer() and value not in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
                value = value / 1_00_00_000

    node = {
        "key": await normalize(item["heading"]),
        "label": item["heading"],
        "values": [None] * (quarter_index + 1),
        "type": "group" if item.get("child") else "single"
    }

    # set value
    if item.get("value") is not None:
        node["values"][quarter_index] = value

    if item.get("child"):
        node["children"] = [
            await bse_banking_build_node(child, quarter_index, amount_type)
            for child in item["child"]
        ]

    return node


async def li_inject_into_existing(existing_node, new_node, quarter_index, amount_type):

    # expand values list
    while len(existing_node["values"]) <= quarter_index:
        existing_node["values"].append(None)

    value = new_node.get("value")

    # -------------------------
    # APPLY SAME CONVERSION
    # -------------------------
    if amount_type == "Lakhs":
        if isinstance(value, (int, float)):
            if float(value).is_integer() and value not in range(1, 11):
                value = value / 100

    elif amount_type == "Crores":
        if isinstance(value, (int, float)):
            if float(value).is_integer() and value not in range(1, 11):
                value = value / 1_00_00_000

    # assign value
    if value is not None:
        existing_node["values"][quarter_index] = value

    # handle children (IMPORTANT: index-based, not key-based)
    if "children" in existing_node and new_node.get("child"):

        for i, child in enumerate(new_node["child"]):

            if i < len(existing_node["children"]):
                await li_inject_into_existing(
                    existing_node["children"][i],
                    child,
                    quarter_index, amount_type
                )

async def bse_banking_inject_into_existing(existing_node, new_node, quarter_index, amount_type):

    # expand values list
    while len(existing_node["values"]) <= quarter_index:
        existing_node["values"].append(None)

    value = new_node.get("value")

    # -------------------------
    # APPLY SAME CONVERSION
    # -------------------------
    if amount_type == "Lakhs":
        if isinstance(value, (int, float)):
            if float(value).is_integer() and value not in range(1, 11):
                value = value / 100

    elif amount_type == "Crores":
        if isinstance(value, (int, float)):
            if float(value).is_integer() and value not in range(1, 11):
                value = value / 1_00_00_000

    # assign value
    if value is not None:
        existing_node["values"][quarter_index] = value

    # handle children (IMPORTANT: index-based, not key-based)
    if "children" in existing_node and new_node.get("child"):

        for i, child in enumerate(new_node["child"]):

            if i < len(existing_node["children"]):
                await bse_banking_inject_into_existing(
                    existing_node["children"][i],
                    child,
                    quarter_index, amount_type
                )

async def bse_nbfc_inject_into_existing(existing_node, new_node, quarter_index, amount_type):

    # expand values
    while len(existing_node["values"]) <= quarter_index:
        existing_node["values"].append(None)

    value = new_node.get("value")

    if amount_type == "Lakhs":
        if isinstance(value, (int, float)):
            if float(value).is_integer() and value not in range(1, 11):
                value = value / 100

    elif amount_type == "Crores":
        if isinstance(value, (int, float)):
            if float(value).is_integer() and value not in range(1, 11):
                value = value / 1_00_00_000

    if value is not None:
        existing_node["values"][quarter_index] = value

    # =========================
    # ✅ FIX: KEY-BASED CHILD MATCHING
    # =========================
    if "children" in existing_node and new_node.get("child"):

        # build map from existing children
        child_map = {
            child["key"]: child
            for child in existing_node["children"]
        }

        for child in new_node["child"]:

            child_key = await normalize(child["heading"])
            existing_child = child_map.get(child_key)

            if existing_child:
                await bse_nbfc_inject_into_existing(
                    existing_child,
                    child,
                    quarter_index,
                    amount_type
                )

async def get_format_type(response_list):
    if not response_list or not response_list[0]:
        return None  # or "other" if you prefer
    return response_list[0][-1].get("format")

async def decide_quarterly_format(response_list):
    frmt = await get_format_type(response_list)
    result = {}
    if frmt in ["GI", "Other", "INDAS", "BANKING", "NBFC"]:
        result = await convert_to_quarterly_format(response_list)
    elif frmt in ["LI"]:
        result = await li_convert_to_quarterly_format(response_list)
    return result

async def bse_decide_quarterly_format(response_list):
    frmt = await get_format_type(response_list)
    result = {}
    if frmt in ["Other", "INDAS"]:
        result = await bse_convert_to_quarterly_format(response_list)
    elif frmt in ["BANKING"]:
        result = await bse_banking_convert_to_quarterly_format(response_list)
    elif frmt in ["NBFC"]:
        result = await bse_nbfc_convert_to_quarterly_format(response_list)
    elif frmt in ["General Insurance"]:
        result = await bse_gl_convert_to_quarterly_format(response_list)
    elif frmt in ["Life Insurance"]:
        result = await li_convert_to_quarterly_format(response_list)
    return result

async def li_convert_to_quarterly_format(response_list):
    headers = []
    root_nodes = []

    for quarter_index, quarter in enumerate(response_list):

        meta = quarter[-1]
        headers.append(meta.get("date"))

        for i, item in enumerate(quarter[:-1]):

            # first quarter → build structure
            if quarter_index == 0:
                node = await li_build_node(item, quarter_index, meta.get("amount_type"))
                root_nodes.append(node)

            # next quarters → inject values
            else:
                await li_inject_into_existing(root_nodes[i], item, quarter_index, meta.get("amount_type"))

    return {
        "headers": headers,
        "rows": root_nodes
    }

async def bse_banking_convert_to_quarterly_format(response_list):
    headers = []
    root_nodes = []

    for quarter_index, quarter in enumerate(response_list):

        meta = quarter[-1]
        headers.append(meta.get("date"))

        for i, item in enumerate(quarter[:-1]):

            # first quarter → build structure
            if quarter_index == 0:
                node = await bse_banking_build_node(item, quarter_index, meta.get("amount_type"))
                root_nodes.append(node)

            # next quarters → inject values
            else:
                await bse_banking_inject_into_existing(root_nodes[i], item, quarter_index, meta.get("amount_type"))

    return {
        "headers": headers,
        "rows": root_nodes
    }

async def fill_missing_values(node, quarter_index):
    # If this quarter value is missing → append None
    if len(node["values"]) <= quarter_index:
        node["values"].append(None)

    # Do same for children
    for child in node.get("children", []):
        await fill_missing_values(child, quarter_index)

async def bse_nbfc_convert_to_quarterly_format(response_list):
    headers = []
    root_nodes = []
    key_map = {}   # ✅ NEW

    for quarter_index, quarter in enumerate(response_list):

        meta = quarter[-1]
        headers.append(meta.get("date"))

        for i, item in enumerate(quarter[:-1]):

            key = await normalize(item["heading"])   # ✅ NEW

            # -------------------------
            # FIRST QUARTER → BUILD
            # -------------------------
            if quarter_index == 0:
                node = await bse_banking_build_node(
                    item, quarter_index, meta.get("amount_type")
                )
                root_nodes.append(node)

                key_map[node["key"]] = node   # ✅ NEW

            # -------------------------
            # NEXT QUARTERS → FIXED INJECTION
            # -------------------------
            else:
                existing_node = key_map.get(key)   # ✅ FIX

                if existing_node:
                    await bse_nbfc_inject_into_existing(
                        existing_node,
                        item,
                        quarter_index,
                        meta.get("amount_type")
                    )

        for node in root_nodes:
            await fill_missing_values(node, quarter_index)

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
            heading = soup.find(["h1", "h2"], string=lambda x: x and "Financial Results".lower() in x.lower())
            result_type = None
            if heading:
                text = heading.get_text(strip=True)
                result_type = text.split("-")[-1].strip()
            if "_Ind_As_" in url:
                table = soup.select_one("h2:-soup-contains('Financial Results') + p + table")

                if table:
                    rows = [
                        tr for tr in table.find_all("tr")
                        if tr.get_text(strip=True)
                    ]
                    final_data = await fetch_bse_th_tr_from_table(rows)
                    structured = await bse_safe_build(final_data, url)
                    structured_with_values = await inject_values_into_hierarchy(
                        structured,
                        final_data
                    )
                    return structured_with_values, amount_type, "INDAS"
            if "Banking" == result_type:
                table = soup.select_one("h2:-soup-contains('Financial Results') + p + table")

                if table:
                    rows = [
                        tr for tr in table.find_all("tr")
                        if tr.get_text(strip=True)
                    ]
                    final_data = await fetch_bse_th_tr_from_table(rows)
                    structured = await bse_safe_build(final_data, bse_format="Banking")
                    structured_with_values = await inject_values_into_hierarchy(
                        structured,
                        final_data
                    )
                    return structured_with_values, amount_type, "BANKING"

            if "NBFC" == result_type:
                table = soup.select_one("h2:-soup-contains('Financial Results') + p + table")

                if table:
                    rows = [
                        tr for tr in table.find_all("tr")
                        if tr.get_text(strip=True)
                    ]
                    final_data = await fetch_bse_th_tr_from_table(rows)
                    structured = await bse_safe_build(final_data, bse_format="NBFC")
                    structured_with_values = await inject_values_into_hierarchy(
                        structured,
                        final_data
                    )
                    return structured_with_values, amount_type, "NBFC"

            if "General Insurance".lower() in result_type:
                table = soup.select_one("h2:-soup-contains('financial results') + p + table")

                if table:
                    rows = [
                        tr for tr in table.find_all("tr")
                        if tr.get_text(strip=True)
                    ]
                    final_data = await fetch_bse_th_tr_from_table(rows)
                    structured = await bse_safe_build(final_data, bse_format="General Insurance")
                    structured_with_values = await bse_gi_inject_values_into_hierarchy(
                        structured,
                        final_data
                    )
                    return structured_with_values, amount_type, "General Insurance"

            if "Life Insurance".lower() in result_type:
                table = soup.select_one("h2:-soup-contains('financial results') + p + table")

                if table:
                    rows = [
                        tr for tr in table.find_all("tr")
                        if tr.get_text(strip=True)
                    ]
                    start_index = None
                    for i, tr in enumerate(rows):
                        text = tr.get_text(" ", strip=True).lower()
                        if "shareholder's account" in text:
                            start_index = i
                            break

                    if start_index is not None:
                        rows = rows[start_index + 1:]
                    final_data = await bse_fetch_th_tr_from_li_table(rows)
                    structured = await bse_safe_build(final_data, bse_format="Life Insurance")
                    structured_with_values = await bse_li_inject_values_into_hierarchy(
                        structured,
                        final_data
                    )
                    return structured_with_values, amount_type, "Life Insurance"

            return [], None, None

        return structured_with_values, None, None
    except Exception as e:
        return [], None, None

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
        "error": [],
        "processed_symbols": []
    }

    if os.path.exists(file):
        try:
            with open(file, "r") as f:
                content = f.read().strip()
                if content:
                    data = json.loads(content)
        except json.JSONDecodeError:
            pass

    if key == "processed_symbols":
        data["processed_symbols"].extend(symbols)
        data["processed_symbols"] = list(set(data["processed_symbols"]))

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

async def update_nse_bse_newly_listed_stock_save_processed_symbol(symbols, key, file_name):
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


async def fetch_newly_listed_stock_symbols_from_covered_symbol_json(file_name):
    file_path = get_custom_today_file(file_name)
    skipped_symbols = {}

    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as f:
                data = json.load(f)

            processed_symbols = set(data.get("processed_symbols", []))

            print(f"{len(skipped_symbols)} skipped from json")
            return processed_symbols

        except Exception as e:
            return {}
    return {}

async def fetch_symbols_from_covered_symbol_json_for_shareholder_result(file_name):
    file_path  = get_custom_today_file(file_name)

    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as f:
                data = json.load(f)

            processing = set(data.get("processing", []))
            data_not_available = set(data.get("data_not_available", []))
            error = set(data.get("error", []))
            processed = set(data.get("processed_symbols", []))

            return processing | data_not_available | error | processed

        except Exception as e:
            return {}
    return {}

async def fetch_symbols_from_covered_symbol_json_for_balance_sheet_and_profit_loss_and_cash_flow(file_name):
    file_path  = get_custom_today_file(file_name)

    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as f:
                data = json.load(f)

            processing = set(data.get("processing", []))
            data_not_available = set(data.get("data_not_available", []))
            error = set(data.get("error", []))
            processed = set(data.get("processed_symbols", []))

            return processing | data_not_available | error | processed

        except Exception as e:
            return {}
    return {}

async def update_nse_bse_shareholder_save_processed_symbol(symbols, key, file_name):
    file = get_custom_today_file(file_name)
    data = {
        "processing": [],
        "data_not_available": [],
        "processed_symbols": [],
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

    elif key == "processed_symbols":
        data["processed_symbols"].extend(symbols)
        data["processed_symbols"] = list(set(data["processed_symbols"]))

        current_set = set(data.get("processing", []))
        current_set -= set(symbols)
        data["processing"] = list(current_set)

    elif key == "error":
        data["error"].extend(symbols)
        data["error"] = list(set(data["error"]))

        current_set = set(data.get("processing", []))
        current_set -= set(symbols)
        data["processing"] = list(current_set)

    elif key == "data_not_available":
        data["data_not_available"].extend(symbols)
        data["data_not_available"] = list(set(data["data_not_available"]))

        current_set = set(data.get("processing", []))
        current_set -= set(symbols)
        data["processing"] = list(current_set)

    with open(file, "w") as f:
        json.dump(data, f, indent=4)


async def update_nse_bse_balance_sheet_and_profit_loss_and_cash_flow_save_processed_symbol(symbols, key, file_name):
    file = get_custom_today_file(file_name)
    data = {
        "processing": [],
        "data_not_available": [],
        "processed_symbols": [],
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

    elif key == "processed_symbols":
        data["processed_symbols"].extend(symbols)
        data["processed_symbols"] = list(set(data["processed_symbols"]))

        current_set = set(data.get("processing", []))
        current_set -= set(symbols)
        data["processing"] = list(current_set)

    elif key == "error":
        data["error"].extend(symbols)
        data["error"] = list(set(data["error"]))

        current_set = set(data.get("processing", []))
        current_set -= set(symbols)
        data["processing"] = list(current_set)

    elif key == "data_not_available":
        data["data_not_available"].extend(symbols)
        data["data_not_available"] = list(set(data["data_not_available"]))

        current_set = set(data.get("processing", []))
        current_set -= set(symbols)
        data["processing"] = list(current_set)

    with open(file, "w") as f:
        json.dump(data, f, indent=4)



async def parse_date(date_str: str):
    try:
        return datetime.strptime(date_str, "%d %b %Y").date()
    except ValueError:
        dt = datetime.strptime(date_str, "%B %Y")
        last_day = calendar.monthrange(dt.year, dt.month)[1]
        return f"{last_day} {dt.strftime('%b %Y')}"

async def save_multiple_shareholding(session, company_id, api_response):
    """
    api_response = list of period objects OR single object
    """

    if isinstance(api_response, dict):
        api_response = [api_response]

    for item in api_response:
        result = item.get("result", {})
        data = result.get("data", {})

        date_str = result.get("date")
        if not date_str:
            continue

        period_date = datetime.strptime(date_str, "%d-%b-%Y").date()

        period = session.query(ShareHoldingPeriod).filter_by(
            company_id=company_id,
            period_date=period_date,
            period_type="quarterly"
        ).first()

        if not period:
            period = ShareHoldingPeriod(
                company_id=company_id,
                period_date=period_date,
                period_type="quarterly"
            )
            session.add(period)
            session.flush()

        existing_section = session.query(ShareHoldingSection).filter_by(
            period_id=period.id,
            key="promoters"
        ).one_or_none()

        if existing_section:
            continue

        promoter_list = data.get("promoter", [])

        children = []
        total_value = Decimal("0")

        for row in promoter_list:
            entity_type = row.get("ENTITY_TYPE")

            if entity_type in ["Promoter", "Promoter Group"]:
                label = row.get("COL_I")
                value = Decimal(row.get("COL_XI") or 0)

                # if value > 0:
                children.append(
                    ShareHoldingSectionChild(
                        label=label,
                        value=value
                    )
                )

        for row in promoter_list:
            if "Sub-Total" in (row.get("COL_I") or ""):
                total_value = Decimal(row.get("COL_XI") or 0)
                break

        children.sort(key=lambda x: x.value, reverse=True)

        promoter_section = ShareHoldingSection(
            period_id=period.id,
            key="promoters",
            label="Promoters",
            value_type="percent",
            total_value=total_value,
            children=children
        )

        session.add(promoter_section)

async def save_bse_multiple_shareholding(session, company_id, api_response):
    if isinstance(api_response, dict):
        api_response = [api_response]

    for item in api_response:
        promoter_list = item.get("promoter", [])
        summary = item.get("summary", {})
        date_str = item.get("date")

        if not date_str:
            continue
        period_date = await parse_date(date_str)
        period = session.query(ShareHoldingPeriod).filter_by(
            company_id=company_id,
            period_date=period_date,
            period_type="quarterly"
        ).first()

        if not period:
            period = ShareHoldingPeriod(
                company_id=company_id,
                period_date=period_date,
                period_type="quarterly"
            )
            session.add(period)
            session.flush()

        existing_section = session.query(ShareHoldingSection).filter_by(
            period_id=period.id,
            key="promoters"
        ).one_or_none()

        if existing_section:
            continue

        children = []
        for row in promoter_list:
            name = row.get("name")
            value = Decimal(row.get("shareholding_percent") or 0)

            children.append(
                ShareHoldingSectionChild(
                    label=name,
                    value=value
                )
            )

        children.sort(key=lambda x: x.value, reverse=True)

        total_value = Decimal(summary.get("shareholding_percent") or 0)

        promoter_section = ShareHoldingSection(
            period_id=period.id,
            key="promoters",
            label="Promoters",
            value_type="percent",
            total_value=total_value,
            children=children
        )

        session.add(promoter_section)


async def save_multiple_dii_shareholding(session, company_id, api_response):

    if isinstance(api_response, dict):
        api_response = [api_response]

    for item in api_response:
        result = item.get("result", {})
        data = result.get("data", {})

        date_str = result.get("date")
        if not date_str:
            continue

        period_date = datetime.strptime(date_str, "%d-%b-%Y").date()

        period = session.query(ShareHoldingPeriod).filter_by(
            company_id=company_id,
            period_date=period_date,
            period_type="quarterly"
        ).first()

        if not period:
            period = ShareHoldingPeriod(
                company_id=company_id,
                period_date=period_date,
                period_type="quarterly"
            )
            session.add(period)
            session.flush()

        existing_section = session.query(ShareHoldingSection).filter_by(
            period_id=period.id,
            key="diis"
        ).one_or_none()

        if existing_section:
            continue

        dii_list = data.get("public_shareholder", [])

        children = []
        total_value = Decimal("0")
        dii_category_lists = ['mutual funds', 'venture capital funds', 'alternate investment funds', 'banks',
                              'insurance  companies', 'provident funds/ pension funds', 'asset reconstruction companies',
                              'sovereign wealth funds', 'nbfcs registered with rbi', 'other financial institutions', 'any other (specify)']

        for row in dii_list:
            name = (row.get("COL_I") or "").strip()
            value = Decimal(row.get("COL_XI") or 0)

            name_lower = name.lower()

            if not name or name == "-":
                continue

            if "sub-total (b)(1)" in name_lower:
                total_value = value
                break

            if "institutions (domestic)" in name_lower:
                continue

            if name.lower() not in dii_category_lists:
                children.append(
                    ShareHoldingSectionChild(
                        label=name,
                        value=value,
                    )
                )

        children.sort(key=lambda x: x.value, reverse=True)

        dii_section = ShareHoldingSection(
            period_id=period.id,
            key="diis",
            label="DIIs",
            value_type="percent",
            total_value=total_value,
            children=children
        )

        session.add(dii_section)


async def save_bse_multiple_dii_shareholding(session, company_id, api_response):

    if isinstance(api_response, dict):
        api_response = [api_response]

    for item in api_response:
        public_list = item.get("public", [])
        date_str = item.get("date")

        if not date_str:
            continue

        period_date = await parse_date(date_str)
        period = session.query(ShareHoldingPeriod).filter_by(
            company_id=company_id,
            period_date=period_date,
            period_type="quarterly"
        ).first()

        if not period:
            period = ShareHoldingPeriod(
                company_id=company_id,
                period_date=period_date,
                period_type="quarterly"
            )
            session.add(period)
            session.flush()

        existing_section = session.query(ShareHoldingSection).filter_by(
            period_id=period.id,
            key="diis"
        ).one_or_none()

        if existing_section:
            continue

        children = []
        total_value = Decimal("0")

        for row in public_list:
            name = (row.get("name") or "").strip()
            if row.get("is_bold") and name.lower() != 'sub total b1':
                continue
            percent = row.get("shareholding_percent")

            name_lower = name.lower()

            if not name or name == "-":
                continue

            if "sub total b1" in name_lower:
                total_value = percent
                break

            children.append(
                ShareHoldingSectionChild(
                    label=name,
                    value=percent,
                )
            )

        children.sort(key=lambda x: x.value, reverse=True)

        public_section = ShareHoldingSection(
            period_id=period.id,
            key="diis",
            label="DIIs",
            value_type="percent",
            total_value=total_value,
            children=children
        )

        session.add(public_section)


async def save_multiple_fii_shareholding(session, company_id, api_response):

    if isinstance(api_response, dict):
        api_response = [api_response]

    for item in api_response:
        result = item.get("result", {})
        data = result.get("data", {})

        date_str = result.get("date")
        if not date_str:
            continue

        period_date = datetime.strptime(date_str, "%d-%b-%Y").date()

        period = session.query(ShareHoldingPeriod).filter_by(
            company_id=company_id,
            period_date=period_date,
            period_type="quarterly"
        ).first()

        if not period:
            period = ShareHoldingPeriod(
                company_id=company_id,
                period_date=period_date,
                period_type="quarterly"
            )
        session.add(period)
        session.flush()

        existing_section = session.query(ShareHoldingSection).filter_by(
            period_id=period.id,
            key="fiis"
        ).one_or_none()

        if existing_section:
            continue

        fii_list = data.get("public_shareholder", [])

        children = []
        total_value = Decimal("0")
        fii_category_lists = ['foreign direct investment', 'foreign venture capital investors', 'sovereign wealth funds',
                              'foreign portfolio investors category i', 'foreign portfolio investors category ii',
                              'overseas depositories (holding drs) (balancing figure)', 'any other (specify)', 'other']

        start_fii = False

        for row in fii_list:
            name = (row.get("COL_I") or "").strip()
            name_lower = name.lower()


            if "institutions (foreign)" in name_lower:
                start_fii = True
                continue

            if not start_fii:
                continue

            value = Decimal(row.get("COL_XI") or 0)


            if not name or name == "-":
                continue

            if "sub-total (b)(2)" in name_lower:
                total_value = value
                break

            if "institutions (foreign)" in name_lower:
                continue

            if name.lower() not in fii_category_lists:
                children.append(
                    ShareHoldingSectionChild(
                        label=name,
                        value=value,
                    )
                )

        children.sort(key=lambda x: x.value, reverse=True)

        fii_section = ShareHoldingSection(
            period_id=period.id,
            key="fiis",
            label="FIIs",
            value_type="percent",
            total_value=total_value,
            children=children
        )

        session.add(fii_section)

async def save_bse_multiple_fii_shareholding(session, company_id, api_response):

    if isinstance(api_response, dict):
        api_response = [api_response]

    for item in api_response:
        public_list = item.get("public", [])
        date_str = item.get("date")

        if not date_str:
            continue

        period_date = await parse_date(date_str)
        period = session.query(ShareHoldingPeriod).filter_by(
            company_id=company_id,
            period_date=period_date,
            period_type="quarterly"
        ).first()

        if not period:
            period = ShareHoldingPeriod(
                company_id=company_id,
                period_date=period_date,
                period_type="quarterly"
            )
            session.add(period)
            session.flush()

        existing_section = session.query(ShareHoldingSection).filter_by(
            period_id=period.id,
            key="fiis"
        ).one_or_none()

        if existing_section:
            continue

        children = []
        total_value = "0.0"

        start_fii = False

        for row in public_list:
            name = (row.get("name") or "").strip()
            if "b2) institutions (foreign)" in name.lower():
                start_fii = True
                continue

            if not start_fii:
                continue

            if row.get("is_bold") and name.lower() != 'sub total b2':
                continue

            percent = row.get("shareholding_percent")

            name_lower = name.lower()

            if not name or name == "-":
                continue

            if "sub total b2" in name_lower:
                total_value = percent
                break

            children.append(
                ShareHoldingSectionChild(
                    label=name,
                    value=percent,
                )
            )

        children.sort(key=lambda x: x.value, reverse=True)
        public_section = ShareHoldingSection(
            period_id=period.id,
            key="fiis",
            label="FIIs",
            value_type="percent",
            total_value=total_value,
            children=children
        )

        session.add(public_section)

async def save_multiple_government_shareholding(session, company_id, api_response):

    if isinstance(api_response, dict):
        api_response = [api_response]

    for item in api_response:
        result = item.get("result", {})
        data = result.get("data", {})

        date_str = result.get("date")
        if not date_str:
            continue

        period_date = datetime.strptime(date_str, "%d-%b-%Y").date()

        period = session.query(ShareHoldingPeriod).filter_by(
            company_id=company_id,
            period_date=period_date,
            period_type="quarterly"
        ).first()

        if not period:
            period = ShareHoldingPeriod(
                company_id=company_id,
                period_date=period_date,
                period_type="quarterly"
            )
        session.add(period)
        session.flush()

        existing_section = session.query(ShareHoldingSection).filter_by(
            period_id=period.id,
            key="government"
        ).one_or_none()

        if existing_section:
            continue

        government_list = data.get("public_shareholder", [])

        children = []
        total_value = Decimal("0")
        government_category_lists = ['central government / state government(s)', 'central government / president of india',
                              'state government / governor', 'shareholding by companies or bodies corporate where central / state government is a promoter',
                              ]

        start_government = False

        for row in government_list:
            name = (row.get("COL_I") or "").strip()
            name_lower = name.lower()


            if "central government / state government(s)" in name_lower:
                start_government = True
                continue

            if not start_government:
                continue

            value = Decimal(row.get("COL_XI") or 0)

            if not name or name == "-":
                continue

            if "sub-total (b)(3)" in name_lower:
                total_value = value
                break

            if "institutions (foreign)" in name_lower:
                continue

            if name.lower() not in government_category_lists:
                children.append(
                    ShareHoldingSectionChild(
                        label=name,
                        value=value,
                    )
                )

        children.sort(key=lambda x: x.value, reverse=True)

        fii_section = ShareHoldingSection(
            period_id=period.id,
            key="government",
            label="Government",
            value_type="percent",
            total_value=total_value,
            children=children
        )

        session.add(fii_section)

async def save_bse_multiple_government_shareholding(session, company_id, api_response):

    if isinstance(api_response, dict):
        api_response = [api_response]

    for item in api_response:
        public_list = item.get("public", [])
        date_str = item.get("date")

        if not date_str:
            continue

        period_date = await parse_date(date_str)
        period = session.query(ShareHoldingPeriod).filter_by(
            company_id=company_id,
            period_date=period_date,
            period_type="quarterly"
        ).first()

        if not period:
            period = ShareHoldingPeriod(
                company_id=company_id,
                period_date=period_date,
                period_type="quarterly"
            )
            session.add(period)
            session.flush()

        existing_section = session.query(ShareHoldingSection).filter_by(
            period_id=period.id,
            key="government"
        ).one_or_none()

        if existing_section:
            continue

        children = []
        total_value = "0.0"

        start_government = False

        for row in public_list:
            name = (row.get("name") or "").strip()
            if "b3) central government/ state government(s)/ president of india" in name.lower():
                start_government = True
                continue

            if not start_government:
                continue

            if row.get("is_bold") and name.lower() != 'sub total b3':
                continue

            percent = row.get("shareholding_percent")

            name_lower = name.lower()

            if not name or name == "-":
                continue

            if "sub total b3" in name_lower:
                total_value = percent
                break

            children.append(
                ShareHoldingSectionChild(
                    label=name,
                    value=percent,
                )
            )

        children.sort(key=lambda x: x.value, reverse=True)
        public_section = ShareHoldingSection(
            period_id=period.id,
            key="government",
            label="Government",
            value_type="percent",
            total_value=total_value,
            children=children
        )

        session.add(public_section)

async def save_multiple_public_shareholding(session, company_id, api_response):

    if isinstance(api_response, dict):
        api_response = [api_response]

    for item in api_response:
        result = item.get("result", {})
        data = result.get("data", {})

        date_str = result.get("date")
        if not date_str:
            continue

        period_date = datetime.strptime(date_str, "%d-%b-%Y").date()

        period = session.query(ShareHoldingPeriod).filter_by(
            company_id=company_id,
            period_date=period_date,
            period_type="quarterly"
        ).first()

        if not period:
            period = ShareHoldingPeriod(
                company_id=company_id,
                period_date=period_date,
                period_type="quarterly"
            )
        session.add(period)
        session.flush()

        existing_section = session.query(ShareHoldingSection).filter_by(
            period_id=period.id,
            key="public"
        ).one_or_none()

        if existing_section:
            continue

        public_list = data.get("public_shareholder", [])

        children = []
        total_value = Decimal("0")
        public_category_lists = ['non-institutions', 'associate companies / subsidiaries',
                              'directors and their relatives (excluding independent directors and nominee directors)', 'key managerial personnel',
                              'relatives of promoters (other than "immediate relatives" of promoters disclosed under "promoter and promoter group" category)',
                              'trusts where any person belonging to "promoter and promoter group" category is "trustee", "beneficiary", or "author of the trust"',
                              'investor education and protection fund (iepf)', 'resident individuals holding nominal share capital up to Rs. 2 lakhs', 'resident individuals holding nominal share capital in excess of Rs. 2 lakhs',
                              'non resident indians (nris)', 'foreign nationals', 'foreign companies', 'bodies corporate', 'any other (specify)',
                              'clearing members', 'esop or esos or esps', 'employees', 'huf', 'trusts', 'llp', 'others', 'foreign portfolio investor (category - iii)', 'overseas corporate bodies', 'unclaimed or suspense or escrow account'
                              'independent director or his relatives', 'societies'
                              ]

        public_government = False

        for row in public_list:
            name = (row.get("COL_I") or "").strip()
            name_lower = name.lower()


            if "non-institutions" in name_lower:
                public_government = True
                continue

            if not public_government:
                continue

            value = Decimal(row.get("COL_XI") or 0)

            if not name or name == "-":
                continue

            if "sub-total (b)(4)" in name_lower:
                total_value = value
                break

            if name.lower() not in public_category_lists:
                children.append(
                    ShareHoldingSectionChild(
                        label=name,
                        value=value,
                    )
                )

        children.sort(key=lambda x: x.value, reverse=True)

        fii_section = ShareHoldingSection(
            period_id=period.id,
            key="public",
            label="Public",
            value_type="percent",
            total_value=total_value,
            children=children
        )

        session.add(fii_section)

async def save_bse_multiple_public_shareholding(session, company_id, api_response):

    if isinstance(api_response, dict):
        api_response = [api_response]

    for item in api_response:
        public_list = item.get("public", [])
        date_str = item.get("date")

        if not date_str:
            continue

        period_date = await parse_date(date_str)
        period = session.query(ShareHoldingPeriod).filter_by(
            company_id=company_id,
            period_date=period_date,
            period_type="quarterly"
        ).first()

        if not period:
            period = ShareHoldingPeriod(
                company_id=company_id,
                period_date=period_date,
                period_type="quarterly"
            )
            session.add(period)
            session.flush()

        existing_section = session.query(ShareHoldingSection).filter_by(
            period_id=period.id,
            key="public"
        ).one_or_none()

        if existing_section:
            continue

        children = []
        total_value = "0.0"

        start_public = False

        public_category_lists = ['non-institutions', 'associate companies / subsidiaries',
                                 'directors and their relatives (excluding independent directors and nominee directors)',
                                 'key managerial personnel',
                                 'relatives of promoters (other than "immediate relatives" of promoters disclosed under "promoter and promoter group" category)',
                                 'trusts where any person belonging to "promoter and promoter group" category is "trustee", "beneficiary", or "author of the trust"',
                                 'investor education and protection fund (iepf)',
                                 'resident individuals holding nominal share capital up to Rs. 2 lakhs',
                                 'resident individuals holding nominal share capital in excess of Rs. 2 lakhs',
                                 'non resident indians (nris)', 'foreign nationals', 'foreign companies',
                                 'bodies corporate', 'any other (specify)', 'resident individuals',
                                 'clearing members', 'esop or esos or esps', 'employees', 'huf', 'trusts', 'llp',
                                 'others', 'foreign portfolio investor (category - iii)', 'overseas corporate bodies',
                                 'unclaimed or suspense or escrow account'
                                 'independent director or his relatives', 'societies'
                                 ]

        for row in public_list:
            name = (row.get("name") or "").strip()
            if "b4) non-institutions" in name.lower():
                start_public = True
                continue

            if not start_public:
                continue

            if row.get("is_bold") and name.lower() != 'sub total b4':
                continue

            percent = row.get("shareholding_percent")

            name_lower = name.lower()

            if not name or name == "-":
                continue

            if "sub total b4" in name_lower:
                total_value = percent
                break

            if name.lower() not in public_category_lists:
                children.append(
                    ShareHoldingSectionChild(
                        label=name,
                        value=percent,
                    )
                )

        children.sort(key=lambda x: x.value, reverse=True)
        public_section = ShareHoldingSection(
            period_id=period.id,
            key="public",
            label="Public",
            value_type="percent",
            total_value=total_value,
            children=children
        )

        session.add(public_section)

async  def to_year_month(date_str):
    from datetime import datetime

    for fmt in ("%d %b %Y", "%d %B %Y", "%b %Y", "%B %Y"):
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.year, dt.month
        except ValueError:
            continue

async def make_key(label: str) -> str:
    """Convert a human label to snake_case key.
    e.g. 'Long Term Borrowings' -> 'long_term_borrowings'
    """
    key = label.lower().strip()
    key = re.sub(r"[^a-z0-9\s]", "", key)
    key = re.sub(r"\s+", "_", key)
    return re.sub(r"_+", "_", key).strip("_")


async def clean_value(val: str):
    """Convert string cell to int / float / None."""
    val = val.strip().replace(",", "").replace("\xa0", "")
    if val in ("", "-", "--", "N/A", "NA"):
        return None
    try:
        return int(val)
    except ValueError:
        pass
    try:
        return float(val)
    except ValueError:
        return val


async def get_unit(label_cell) -> str:
    """Read unit from data-original-title tooltip, default Rs Cr."""
    el = label_cell.find(attrs={"data-original-title": True})
    if el:
        m = re.search(r"\(([^)]+)\)", el.get("data-original-title", ""))
        if m:
            return m.group(1)
    return "Rs Cr"


async def is_child_row(tr) -> bool:
    """True if row is indented (padding-left style or child/sub class)."""
    classes = " ".join(tr.get("class", [])).lower()
    if "child" in classes or "sub" in classes:
        return True
    first_td = tr.find(["th", "td"])
    if first_td:
        style = first_td.get("style", "")
        if "padding-left" in style or "text-indent" in style:
            return True
    return False


async def extract_table_data(table) -> dict:
    """
    Parse table rows and return structured output.

    Classification:
      thead tr.bgColor          -> period column headers
      tbody tr.bgColor          -> section divider (skip)
      td[colspan]  / 1 cell     -> section divider (skip)
      is_child_row() == True    -> child of the previous top-level row
      everything else           -> top-level row (single or group)
    """
    headers = []
    rows    = []   # final top-level rows

    thead = table.find("thead")
    tbody = table.find("tbody")

    if thead:
        for cell in thead.find("tr").find_all(["th", "td"])[1:]:
            t = cell.get_text(strip=True)
            if t:
                headers.append(t)

    raw = []
    rows_source = tbody.find_all("tr") if tbody else table.find_all("tr")

    for tr in rows_source:
        cells = tr.find_all(["th", "td"])
        if not cells:
            continue

        label_cell = cells[0]
        label = label_cell.get_text(strip=True)

        if not label or label.lower() == "particulars":
            continue

        if label_cell.get("colspan") or len(cells) == 1:
            continue

        tr_cls = [c.lower() for c in tr.get("class", [])]
        values = [await clean_value(c.get_text(strip=True)) for c in cells[1:]]

        if "bgcolor" in tr_cls:
            raw.append({
                "key":    await make_key(label),
                "label":  label,
                "unit":   await get_unit(label_cell),
                "values": values,
                "_is_bgcolor": True,
                "_child": False,
            })
        else:
            raw.append({
                "key":    await make_key(label),
                "label":  label,
                "unit":   await get_unit(label_cell),
                "values": values,
                "_is_bgcolor": False,
                "_child": await is_child_row(tr),
            })

    current_bgcolor_group = None

    for item in raw:
        is_bgcolor  = item.pop("_is_bgcolor")
        is_child    = item.pop("_child")

        if is_bgcolor:
            item["type"]     = "group"
            item["children"] = []
            rows.append(item)
            current_bgcolor_group = item

        elif is_child:
            if current_bgcolor_group and current_bgcolor_group["children"]:
                parent = current_bgcolor_group["children"][-1]
                parent["type"] = "group"
                if "children" not in parent:
                    parent["children"] = []
                item["type"] = "single"
                parent["children"].append(item)
            elif current_bgcolor_group:
                item["type"] = "single"
                current_bgcolor_group["children"].append(item)

        else:
            item["type"] = "single"
            if current_bgcolor_group is not None:
                current_bgcolor_group["children"].append(item)
            else:
                rows.append(item)

    return {"headers": headers, "rows": rows}

async def parse_balance_sheet(soup: BeautifulSoup) -> dict:
    """
    Navigate DOM path:
      div.companyinfo
        > div#mainContent_pnlCompanyDetails
          > div#balance
            > table
    """
    ci = soup.find("div", class_="companyinfo")
    if not ci:
        return {'headers': [], 'rows': []}
    pd_ = ci.find("div", id="mainContent_pnlCompanyDetails")
    if not pd_:
        return {'headers': [], 'rows': []}
    bd = pd_.find("div", id="balance")
    if not bd:
        return {'headers': [], 'rows': []}
    tbl = bd.find("table")
    if not tbl:
        return {'headers': [], 'rows': []}
    return await extract_table_data(tbl)

async def parse_profit_loss(soup : BeautifulSoup) -> dict:
    """
    Accepts either a BeautifulSoup object or a raw HTML string.

    DOM path:
      div.companyinfo
        > div#mainContent_pnlCompanyDetails
          > div#profit          <-- only difference from balance sheet
            > table
    """
    ci = soup.find("div", class_="companyinfo")
    if not ci:
        return {'headers': [], 'rows': []}
    pd_ = ci.find("div", id="mainContent_pnlCompanyDetails")
    if not pd_:
        return {'headers': [], 'rows': []}
    pf = pd_.find("div", id="profit")
    if not pf:
        return {'headers': [], 'rows': []}
    tbl = pf.find("table")
    if not tbl:
        return {'headers': [], 'rows': []}

    return await extract_table_data(tbl)   # reuse exact same parser

async def parse_cash_flow(soup: BeautifulSoup) -> dict:
    """
    Navigate DOM path:
      div.companyinfo
        > div#mainContent_pnlCompanyDetails
          > div#mainContent_cashflows
            > table
    """
    ci = soup.find("div", class_="companyinfo")
    if not ci:
        return {'headers': [], 'rows': []}
    pd_ = ci.find("div", id="mainContent_pnlCompanyDetails")
    if not pd_:
        return {'headers': [], 'rows': []}
    bd = pd_.find("div", id="mainContent_cashflows")
    if not bd:
        return {'headers': [], 'rows': []}
    tbl = bd.find("table")
    if not tbl:
        return {'headers': [], 'rows': []}
    return await extract_table_data(tbl)

async def make_short_company_name(text):

    words = text.split()

    if len(words) == 1:
        return text.lower()

    text = ' '.join(words[:-1])   # remove last word
    text = ' '.join(text.split()[:2])  # take first 3 words and join

    return text

async def find_by_scripcode(results: list, scripcode: int) -> dict | None:
    """
    results = [{"compname": "...", "SCRIPCODE": 523840, ...}, ...]
    Returns the matching item or None.
    """
    for item in results:
        if scripcode and item.get("SCRIPCODE") == int(scripcode):
            return item
    return None

# async def split_purpose_into_rows(row, purpose_col):
#     """
#     Extracts ALL individual dividend entries from a combined purpose string.
#     Works for 2, 3, 4 or more dividends in the same string.
#     """
#     purpose = str(row[purpose_col])
#
#     # Strategy 1: Split on explicit separators: '/', 'And', '+'
#     parts = re.split(r'\s*/\s*|\s+[Aa]nd\s+|\s+\+\s+', purpose)
#     parts = [p.strip() for p in parts if p.strip()]
#
#     # Strategy 2: If still 1 part, split on keyword boundary after "Per Share"
#     # e.g. "Interim Dividend Rs 10 Per Share Special Dividend Rs 66 Per Share"
#     if len(parts) < 2:
#         # Insert a delimiter before each dividend keyword that follows "Share"
#         tagged = re.sub(
#             r'(Per\s+(?:Equity\s+)?Share\.?)\s+(?=(?:Interim|Special|Final|Annual|Dividend)\b)',
#             r'\1|||',
#             purpose,
#             flags=re.IGNORECASE
#         )
#         parts = [p.strip() for p in tagged.split('|||') if p.strip()]
#
#     # Strategy 3: Regex — find every "Dividend Rs X Per Share" chunk directly
#     if len(parts) < 2:
#         parts = re.findall(
#             r'(?:(?:1st|2nd|3rd|\d+th|Third|Second|First)\s+)?'
#             r'(?:Interim|Special|Final|Annual)?\s*'
#             r'(?:Interim|Special|Final|Annual)?\s*'
#             r'Dividend\s*[-\u2013]?\s*Rs\.?\s*[\d.]+(?:/-)?'
#             r'\s*Per\s+(?:Equity\s+)?Share(?:\s*\(Purpose\s+Revised\))?',
#             purpose,
#             flags=re.IGNORECASE
#         )
#
#     rows = []
#     for part in parts:
#         part = part.strip()
#         amt_match = re.search(r"Rs\.?\s*([\d.]+)", part, re.IGNORECASE)
#         if not amt_match:
#             continue  # skip parts with no Rs amount
#
#         new_row = row.copy()
#         new_row[purpose_col] = part
#         new_row["_amount"] = float(amt_match.group(1))
#
#         # Classify type
#         p_lower = part.lower()
#         if "special" in p_lower:
#             new_row["_type"] = "Special"
#         elif "interim" in p_lower:
#             new_row["_type"] = "Interim"
#         elif any(k in p_lower for k in ["final", "annual", "agm"]):
#             new_row["_type"] = "Final/Annual"
#         else:
#             new_row["_type"] = "Dividend"
#
#         rows.append(new_row)
#
#     # Absolute fallback: return original row untouched
#     if not rows:
#         amt_match = re.search(r"Rs\.?\s*([\d.]+)", purpose, re.IGNORECASE)
#         row["_amount"] = float(amt_match.group(1)) if amt_match else 0.0
#         row["_type"] = "Dividend"
#         rows = [row]
#
#     return rows

# async def split_purpose_into_rows(row, purpose_col):
#     purpose = str(row[purpose_col]).strip()
#     parts = await _split_purpose(purpose)
#
#     rows = []
#     for part in parts:
#         part = part.strip()
#         # BSE format: "Dividend - Rs. - 4.2500" (dash before number)
#         # Legacy format: "Dividend Rs 5 Per Share"
#         amt_match = re.search(r"Rs\.?\s*[-\u2013]?\s*([\d.]+)", part, re.IGNORECASE)
#         if not amt_match:
#             continue
#
#         new_row = row.copy()
#         new_row[purpose_col] = part
#         new_row["_amount"] = float(amt_match.group(1))
#
#         p_lower = part.lower()
#         if new_row["_amount"] == 0 or "nil" in p_lower:
#             new_row["_type"] = "NIL"
#         elif "special" in p_lower:
#             new_row["_type"] = "Special"
#         elif "interim" in p_lower:
#             new_row["_type"] = "Interim"
#         elif any(k in p_lower for k in ["final", "annual", "agm"]):
#             new_row["_type"] = "Final/Annual"
#         else:
#             new_row["_type"] = "Dividend"
#
#         rows.append(new_row)
#
#     if not rows:
#         amt_match = re.search(r"Rs\.?\s*[-\u2013]?\s*([\d.]+)", purpose, re.IGNORECASE)
#         row["_amount"] = float(amt_match.group(1)) if amt_match else 0.0
#         row["_type"] = "Dividend"
#         rows = [row]
#
#     return rows
#
#
# async def _split_purpose(s):
#     """Split a combined purpose string into individual dividend parts."""
#
#     # Strategy 1: slash-split — only when BOTH sides look like dividend entries
#     slash_parts = re.split(r'\s*/\s*', s)
#     dividend_like = lambda p: bool(re.search(r'dividend', p, re.IGNORECASE))
#     if len(slash_parts) >= 2 and sum(dividend_like(p) for p in slash_parts) >= 2:
#         return [p for p in slash_parts if dividend_like(p) or re.search(r'Rs', p, re.IGNORECASE)]
#
#     # Strategy 2: "And" / "+" separators
#     and_parts = re.split(r'\s+[Aa]nd\s+|\s+\+\s+', s)
#     if len(and_parts) >= 2 and all(
#         re.search(r'Rs|dividend', p, re.IGNORECASE) for p in and_parts
#     ):
#         return and_parts
#
#     # Strategy 3: boundary after "Per Share" before next dividend keyword
#     tagged = re.sub(
#         r'(Per\s+(?:Equity\s+)?Share\.?)\s+(?=(?:Interim|Special|Final|Annual|Dividend)\b)',
#         r'\1|||',
#         s, flags=re.IGNORECASE
#     )
#     tag_parts = [p.strip() for p in tagged.split('|||') if p.strip()]
#     if len(tag_parts) >= 2:
#         return tag_parts
#
#     # Strategy 4: direct regex match — handles BSE "Rs. - N.NNNN" format
#     bse_rx = re.compile(
#         r'(?:(?:1st|2nd|3rd|\d+th|First|Second|Third)\s+)?'
#         r'(?:Nil\s+)?(?:Interim|Special|Final|Annual|AGM)?\s*'
#         r'(?:Interim|Special|Final|Annual)?\s*'
#         r'Dividend\s*[-\u2013]?\s*Rs\.?\s*[-\u2013]?\s*[\d.]+'
#         r'(?:/-)?(?:\s*Per\s+(?:Equity\s+)?Share)?'
#         r'(?:\s*\(Purpose\s+Revised\))?',
#         re.IGNORECASE
#     )
#     matches = bse_rx.findall(s)
#     if len(matches) >= 2:
#         return matches
#
#     return [s]

AMOUNT_RE = re.compile(
    r"(?:Rs\.?|Re\.?|INR)\s*[-\u2013]?\s*([\d.]+)",
    re.IGNORECASE
)

DIVIDEND_SPLIT_RX = re.compile(
    r'(?:(?:1st|2nd|3rd|\d+th|First|Second|Third)\s+)?'
    r'(?:Nil\s+)?(?:Interim|Special|Final|Annual|AGM)?\s*'
    r'(?:Interim|Special|Final|Annual)?\s*'
    r'Dividend\s*[-\u2013]?\s*(?:Rs\.?|Re\.?|INR)\s*[-\u2013]?\s*[\d.]+'
    r'(?:/-)?(?:\s*Per\s+(?:Equity\s+)?Share)?'
    r'(?:\s*\(Purpose\s+Revised\))?',
    re.IGNORECASE
)

async def _has_amount(p):
    return bool(AMOUNT_RE.search(p))

async def _dividend_like(p):
    return bool(re.search(r'dividend', p, re.IGNORECASE))

async def _classify_type(p_lower, amount):
    if amount == 0 or "nil" in p_lower:
        return "NIL"
    if "special" in p_lower:
        return "Special"
    if "interim" in p_lower:
        return "Interim"
    if any(k in p_lower for k in ["final", "annual", "agm"]):
        return "Final/Annual"
    return "Dividend"


async def _split_purpose(s):
    # Strategy 1: slash-split — only when BOTH sides look like dividend entries
    slash_parts = re.split(r'\s*/\s*', s)
    if len(slash_parts) >= 2 and sum(await _dividend_like(p) for p in slash_parts) >= 2:
        return [p for p in slash_parts if await _dividend_like(p) or await _has_amount(p)]

    # Strategy 2: "And" / "+" separators
    and_parts = re.split(r'\s+[Aa]nd\s+|\s+\+\s+', s)
    if len(and_parts) >= 2 and all(
        await _has_amount(p) or await _dividend_like(p) for p in and_parts
    ):
        return and_parts

    # Strategy 3: boundary after "Per Share" before next dividend keyword
    tagged = re.sub(
        r'(Per\s+(?:Equity\s+)?Share\.?)\s+(?=(?:Interim|Special|Final|Annual|Dividend)\b)',
        r'\1|||',
        s, flags=re.IGNORECASE
    )
    tag_parts = [p.strip() for p in tagged.split('|||') if p.strip()]
    if len(tag_parts) >= 2:
        return tag_parts

    # Strategy 4: direct regex — handles BSE "Rs./Re. - N.NNNN" and "Re 0.50 Per Share"
    matches = DIVIDEND_SPLIT_RX.findall(s)
    if len(matches) >= 2:
        return matches

    return [s]


async def split_purpose_into_rows(row, purpose_col):
    purpose = str(row[purpose_col]).strip()
    parts = await _split_purpose(purpose)

    rows = []
    for part in parts:
        part = part.strip()
        amt_match = AMOUNT_RE.search(part)
        if not amt_match:
            continue

        new_row = row.copy()
        new_row[purpose_col] = part
        new_row["_amount"] = float(amt_match.group(1))
        new_row["_type"] = await _classify_type(part.lower(), new_row["_amount"])
        rows.append(new_row)

    # Fallback: return original row with best-effort parse
    if not rows:
        amt_match = AMOUNT_RE.search(purpose)
        row["_amount"] = float(amt_match.group(1)) if amt_match else 0.0
        row["_type"] = await _classify_type(purpose.lower(), row["_amount"])
        rows = [row]

    return rows

async def fetch_dividend_values(data):
    df = pd.DataFrame(data)

    # ── Auto-detect column names ─────────────────────────────────────────────
    purpose_col = next((c for c in df.columns if any(k in c.lower() for k in ["subject", "purpose", "desc"])), None)
    ex_date_col = next((c for c in df.columns if "ex" in c.lower() and "date" in c.lower()), None)
    rec_date_col = next((c for c in df.columns if "rec" in c.lower() and "date" in c.lower()), None)
    series_col = next((c for c in df.columns if "series" in c.lower()), None)
    symbol_col = next((c for c in df.columns if "symbol" in c.lower()), None)

    if purpose_col is None:
        return df

    # ── Filter dividends only ────────────────────────────────────────────────
    mask = df[purpose_col].str.lower().str.contains(
        "dividend|interim|special", na=False
    )
    df_div = df[mask].copy()

    if df_div.empty:
        return df_div

    # ── Parse ex-date and sort ───────────────────────────────────────────────
    if ex_date_col:
        df_div[ex_date_col] = pd.to_datetime(
            df_div[ex_date_col], errors="coerce", dayfirst=True
        )
        df_div = df_div.sort_values(ex_date_col, ascending=False)

    # ── Split ALL combined entries into individual rows ──────────────────────
    split_rows = []
    for _, row in df_div.iterrows():
        split_rows.extend(await split_purpose_into_rows(row, purpose_col))

    df_split = pd.DataFrame(split_rows).reset_index(drop=True)

    # ── Build final clean dataframe ──────────────────────────────────────────
    col_map = {}
    if symbol_col:   col_map[symbol_col] = "Symbol"
    if series_col:   col_map[series_col] = "Series"
    col_map[purpose_col] = "Purpose"
    col_map["_type"] = "Type"
    if ex_date_col:  col_map[ex_date_col] = "Ex-Date"
    if rec_date_col: col_map[rec_date_col] = "Record Date"
    col_map["_amount"] = "Amount (Rs)"

    available = [c for c in col_map if c in df_split.columns]
    df_final = df_split[available].rename(columns=col_map)

    return df_final


async def fetch_li_key_values(li_table):
    """
    Extracts key-value pairs from LI financial table
    e.g. {"Share capital": "6,32,500.00", "Reserves and surplus": "1,35,01,552.00"}
    """
    result = {}

    if li_table is None:
        return result

    rows = li_table.find_all("tr")

    for tr in rows:
        ths = tr.find_all("th")
        tds = tr.find_all("td")


        if ths and tds:
            label = None
            for th in ths:
                text = th.get_text(strip=True)
                if text and not text.isdigit():
                    label = text
                    break

            if label is None:
                continue

            value = tds[-1].get_text(strip=True)
            if label and value:
                result[label] = value

    return result

async def fetch_indas_key_values(li_table):
    """
    Extracts key-value pairs from LI financial table
    e.g. {"Share capital": "6,32,500.00", "Reserves and surplus": "1,35,01,552.00"}
    """
    result = {}
    if li_table is None:
        return result
    rows = li_table.find_all("tr")
    for tr in rows:
        ths = tr.find_all("th")
        tds = tr.find_all("td")
        if ths and tds:
            label = None
            for th in ths:
                print(th, "--hh--")
                # Handle text inside <b> tag or direct text
                b_tag = th.find("b")
                if b_tag:
                    text = b_tag.get_text(strip=True)
                    print(text, "----b-------")
                else:
                    text = th.get_text(strip=True)
                    print(text, "--tt--")
                if text and not text.isdigit():
                    label = text
                    break
                print("----------------------")

            if label is None:
                continue
            print(label, "-----fff---fff----")
            # Handle value inside <b> tag or direct text
            last_td = tds[-1]
            b_tag = last_td.find("b")
            if b_tag:
                value = b_tag.get_text(strip=True)
            else:
                value = last_td.get_text(strip=True)
            if label and value:
                result[label] = value
    return result

async def fetch_integrated_filing_financials_data_from_nse_for_book_value(url):
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

            if "_LI_" in url:
                target_table = None
                target_heading = "Format for financial results by life insurance companies filed with stock exchanges"

                if value == "Crores":
                    tables = soup.find_all("table")
                    for table in tables:
                        rows = table.find_all(
                            "tr",
                            class_=lambda cls: cls and "main-row" in cls.split()
                        )
                        for row in rows:
                            th = row.find("th")
                            h3 = th.find("h3") if th else None
                            if h3 and target_heading in h3.get_text(" ", strip=True):
                                table_text = table.get_text()
                                if "Sources of Funds" in table_text:
                                    target_table = table
                                    break

                                break
                        if target_table:
                            break
                else:
                    flex_divs = soup.find_all("div", class_="d-flex-table-head")
                    for div in reversed(flex_divs):
                        h3 = div.find("h3",
                                      string=lambda x: x and "Format for financial results by life insurance" in x)
                        if h3:
                            # Verify the next sibling table actually has "Sources of Funds"
                            next_table = div.find_next_sibling("table")
                            if next_table:
                                table_text = next_table.get_text()
                                if "Sources of Funds" in table_text:
                                    target_table = next_table
                                    break
                result = await fetch_li_key_values(target_table)
                return result, value, "LI"

            elif "INDAS" in url:
                target_table = None
                if value == "Crores":
                    tables = soup.find_all("table", class_="gridtable")
                    for table in tables:
                        rows = table.find_all(
                            "tr",
                            class_=lambda cls: cls and "main-row" in cls.split()
                        )
                        for row in rows:
                            th = row.find("th")
                            h3 = th.find("h3") if th else None
                            if h3 and "Statement of Asset and Liabilities" in h3.get_text(" ", strip=True):
                                target_table = table
                                break
                        if target_table:
                            break
                    result = await fetch_indas_key_values(target_table)
                else:
                    asset_heading = soup.find("h3",string=lambda x: x and "Statement of Asset and Liabilities" in x)
                    if asset_heading:
                        for sibling in asset_heading.find_all_next("table"):
                            classes = sibling.get("class", [])
                            if "gridtable" in classes and "stockExchnageTableLastColwidth" in classes:
                                target_table = sibling
                                break
                    result = await fetch_li_key_values(target_table)
                print(result)
                return result, value, "INDAS"


        return structured_with_values, None, None
    except Exception as e:
        return [], None, None
