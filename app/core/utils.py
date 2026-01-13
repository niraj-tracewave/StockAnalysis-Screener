import calendar

import pyotp
import uuid
import base64

import requests

from app.core.config import get_settings


settings = get_settings()

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