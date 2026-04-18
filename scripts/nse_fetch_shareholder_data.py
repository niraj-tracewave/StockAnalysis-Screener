import asyncio
from urllib.parse import urlparse

import aiohttp
from collections import defaultdict

BASE_URL = "https://www.nseindia.com/api"

HEADERS = {
    "authority": "www.nseindia.com",
    "accept": "*/*",
    "method": "GET",
    "scheme": "https",
    "accept-language": "en-US,en;q=0.9",
    "priority": "u=1, i",
    "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Linux\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
}


# ---------------------------
# FETCH FUNCTION
# ---------------------------
async def fetch(session, url):
    parsed = urlparse(url)

    path = parsed.path.lstrip("/") + "?" + parsed.query

    print(path, parsed.query)
    async with session.get(url, headers={**HEADERS, "path":path, "referer": f"https://www.nseindia.com/companies-listing/corporate-filings-shareholding-pattern?{parsed.query}"}) as response:
        return await response.json()


# ---------------------------
# NSE API CALLS
# ---------------------------
async def fetch_all(ndsId):
    async with aiohttp.ClientSession() as session:

        # Required APIs (from your screenshots)
        urls = {
            # "summary": f"{BASE_URL}/corporate-share-holdings-equities?symbol={symbol}&section=summary",
            "promoter": f"{BASE_URL}/corporate-share-holdings-equities?ndsId={ndsId}&index=promoter",
            # "public": f"{BASE_URL}/corporate-share-holdings-equities?symbol={symbol}&index=public",
            # "non_public": f"{BASE_URL}/corporate-share-holdings-equities?symbol={symbol}&index=non-public"
        }

        tasks = {k: fetch(session, v) for k, v in urls.items()}
        results = await asyncio.gather(*tasks.values())

        return dict(zip(tasks.keys(), results))


async def main_nse_fetch_shareholding_data_using_api(ndsId):
    data = await fetch_all(ndsId)
    return data

import aiohttp

integrated_filing_url = "https://www.nseindia.com/api/corporate-share-holdings-master"

shareholder_list_headers = {
    "authority": "www.nseindia.com",
    "method": "GET",
    "scheme": "https",
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "priority": "u=1, i",
    "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Linux\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
}
async def fetch_data(symbol, index):
    original_url = f"{integrated_filing_url}?index={index}&symbol={symbol}"
    async with aiohttp.ClientSession() as session:
        async with session.get(original_url, headers={**shareholder_list_headers, "path": f"/api/corporate-share-holdings-master?index={index}&symbol={symbol}",
                                                      "referer": f"https://www.nseindia.com/companies-listing/corporate-filings-shareholding-pattern?symbol={symbol}&tabIndex={index}"}) as response:
            response.raise_for_status()
            data = await response.json()
            return data


async def main_nse_fetch_shareholding_list(symbol, index):
    data = await fetch_data(symbol, index)
    return data
