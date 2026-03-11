import aiohttp

integrated_filing_url = "https://api.bseindia.com/BseIndiaAPI/api/Integratedfinancedata/w"

integrated_filing_url_header = {
    "authority": "api.bseindia.com",
    "method": "GET",
    "scheme": "https",
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "origin": "https://www.bseindia.com",
    "priority": "u=1, i",
    "referer": "https://www.bseindia.com/",
    "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Linux\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
  }

async def fetch_data(symbol):
    original_url = f"{integrated_filing_url}?scripcode={symbol}"
    async with aiohttp.ClientSession() as session:
        async with session.get(original_url, headers={**integrated_filing_url_header, "path": f"/BseIndiaAPI/api/Integratedfinancedata/w?scripcode={symbol}",
                                                      }) as response:
            response.raise_for_status()
            data = await response.json()
            return data


async def main_bse_fetch_integrated_filing_financials(symbol):
    data = await fetch_data(symbol)
    return data