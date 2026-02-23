import aiohttp

integrated_filing_url = "https://www.nseindia.com/api/integrated-filing-results"

integrated_filing_url_header = {
    "authority": "www.nseindia.com",
    "method": "GET",
    "scheme": "https",
    "accept": "*/*",
    # "accept-encoding": "gzip, deflate, br, zstd",
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

async def fetch_data(symbol, identifier):
    original_url = f"{integrated_filing_url}?symbol={symbol}&type=Integrated%20Filing-%20Financials&page=1&size=20"
    async with aiohttp.ClientSession() as session:
        async with session.get(original_url, headers={**integrated_filing_url_header, "path": f"/api/integrated-filing-results?symbol={symbol}&type=Integrated%20Filing-%20Financials&page=1&size=20",
                                                      "referer": f"https://www.nseindia.com/companies-listing/corporate-integrated-filing?symbol={symbol}&tabIndex={identifier}&&integratedType=integratedfilingfinancials"}) as response:
            response.raise_for_status()
            data = await response.json()
            return data

async def main_fetch_integrated_filing_financials(symbol, identifier):
    data = await fetch_data(symbol, identifier)
    return data