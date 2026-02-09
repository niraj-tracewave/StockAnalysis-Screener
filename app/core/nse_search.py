import re

import aiohttp
import requests
from bs4 import BeautifulSoup

from app.core import utils

url = "https://www.nseindia.com/api/search/autocomplete"
headers ={
    "authority": "www.nseindia.com",
    "method": "GET",
    "scheme": "https",
    "accept": "application/json, text/javascript, */*; q=0.01",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "priority": "u=1, i",
    "referer": "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
    "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Linux\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest"
  }

async def get_nse_code_from_angel(symbol: str):
    return utils.ANGEL_NSE_MAP.get(symbol)

async def fetch_nse_data(search):
    company_list = []

    timeout = aiohttp.ClientTimeout(total=10)

    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        async with session.get(
                f"{url}",
                params={"q": search},
        ) as response:
            if response.status != 200:
                return company_list

            data = await response.json()
            for symbol_data in data.get("symbols", []):
                activeSeries = symbol_data.get("activeSeries")
                if activeSeries:
                    series = activeSeries[0]
                    nse_code = await get_nse_code_from_angel(f"{symbol_data.get("symbol")}-{series}")
                    company_list.append({
                        "symbol": symbol_data.get("symbol"),
                        "company_name": symbol_data.get("symbol_info"),
                        "url": symbol_data.get("url"),
                        "platform": "NSE",
                        "nse_code": nse_code,
                        "bse_code": None
                    })

    return company_list

async def fetch_nse_exact_symbol_data(search):
    company_list = []

    timeout = aiohttp.ClientTimeout(total=10)

    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        async with session.get(
                f"{url}",
                params={"q": search},
        ) as response:
            if response.status != 200:
                return company_list

            data = await response.json()
            if data.get("symbols", []):
                symbol_data = next((item for item in data.get("symbols") if item["symbol"] == search), None)
                nse_code = get_nse_code_from_angel(symbol_data)
                company_list.append({
                        "symbol": symbol_data.get("symbol"),
                        "company_name": symbol_data.get("symbol_info"),
                        "url": symbol_data.get("url"),
                        "platform": "NSE",
                        "nse_code": nse_code,
                        "bse_code": None
                    })

    return company_list

bse_url = 'https://api.bseindia.com/Msource/1D/getQouteSearch.aspx'

bse_headers = {
  "Accept": "application/json, text/plain, */*",
  "Accept-Encoding": "gzip, deflate, br, zstd",
  "Accept-Language": "en-US,en;q=0.9",
  "Cache-Control": "no-cache",
  "Origin": "https://www.bseindia.com",
  "Pragma": "no-cache",
  "Referer": "https://www.bseindia.com/",
  "Sec-CH-UA": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
  "Sec-CH-UA-Mobile": "?0",
  "Sec-CH-UA-Platform": "\"Linux\"",
  "Sec-Fetch-Dest": "empty",
  "Sec-Fetch-Mode": "cors",
  "Sec-Fetch-Site": "same-site",
  "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
}

async def fetch_bse_data(search):
    response = requests.get(f"{bse_url}?Type=EQ&text={search}&flag=site", headers={**bse_headers, "path": f"/Msource/1D/getQouteSearch.aspx?Type=EQ&text={search}&flag=site"})
    company_list = []
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")

        for li in soup.select("li.quotemenu"):
            a = li.find("a")
            if not a:
                continue

            url = a.get("href").strip("/")

            parts = url.split("/")

            if len(parts) < 4:
                continue

            company_slug = parts[1]
            symbol = parts[2].upper()
            security_code = parts[3]

            company_name = company_slug.replace("-", " ").upper()

            company_list.append({
                "company_name": company_name,
                "symbol": symbol,
                "bse_code": security_code,
                "url": f"/{url}/",
                "platform": "BSE",
                "nse_code": None,
            })
        return company_list
    return company_list