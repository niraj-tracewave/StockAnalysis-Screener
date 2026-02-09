import calendar

import aiohttp
import pandas as pd
import pyotp
import uuid
import base64

import requests
import json
from app.core.config import get_settings


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