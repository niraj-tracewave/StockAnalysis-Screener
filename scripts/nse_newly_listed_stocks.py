import aiohttp

nse_newly_listed_stock_url = "https://www.nseindia.com/api/new-listing-today-ipo"

headers = {
    "authority": "www.nseindia.com",
    "method": "GET",
    "path": "/api/new-listing-today-ipo?index=NewListing",
    "scheme": "https",
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "priority": "u=1, i",
    "referer": "https://www.nseindia.com/market-data/new-stock-exchange-listings-today",
    "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
}

async def fetch_data():
    original_url = f"{nse_newly_listed_stock_url}?index=NewListing"
    async with aiohttp.ClientSession() as session:
        async with session.get(original_url, headers=headers) as response:
            response.raise_for_status()
            data = await response.json()
            return data


async def main_nse_newly_listed_stocks():
    data = await fetch_data()
    return data