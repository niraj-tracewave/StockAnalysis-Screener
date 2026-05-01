import aiohttp

URL = f'https://ticker.finology.in/company/'

async def fetch_bse_shareholding_list(symbol):
    url = f"{URL}{symbol}?mode=C"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.text()
            return data


async def main_balance_sheet_html(symbol):
    data = await fetch_bse_shareholding_list(symbol)
    return data


async def fetch_balance_sheet_standalone_list(symbol):
    url = f"{URL}{symbol}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.text()
            return data


async def main_balance_sheet_standalone_html(symbol):
    data = await fetch_balance_sheet_standalone_list(symbol)
    return data


FIND_COMPANY_URL = 'https://ticker.finology.in/GetSearchData.ashx'

HEADERS = {
  "authority": "ticker.finology.in",
  "method": "GET",
  "scheme": "https",
  "accept": "application/json, text/javascript, */*; q=0.01",
  "accept-language": "en-US,en;q=0.9",
  "priority": "u=1, i",
  "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
  "sec-ch-ua-mobile": "?0",
  "sec-ch-ua-platform": "\"Linux\"",
  "sec-fetch-dest": "empty",
  "sec-fetch-mode": "cors",
  "sec-fetch-site": "same-origin",
  "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
  "x-requested-with": "XMLHttpRequest"
}

async def find_company(c_name):
    url = f"{FIND_COMPANY_URL}?q={c_name}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers={**HEADERS, "path": f"/GetSearchData.ashx?q={c_name}"}) as response:
            response.raise_for_status()
            data = await response.json()
            return data

async def main_find_company_json(c_name):
    data = await find_company(c_name)
    return data