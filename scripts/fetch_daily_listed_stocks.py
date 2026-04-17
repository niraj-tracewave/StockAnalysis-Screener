from datetime import datetime
from zoneinfo import ZoneInfo

import aiohttp

nse_newly_listed_stock_url = "https://api.ipo-trend.com/ipo/new-ipo-list?category=listed&page=1&page_size=30&platform=Android"

async def fetch_data():
    current_date = datetime.now(ZoneInfo("Asia/Kolkata")).date()

    original_url = f"{nse_newly_listed_stock_url}&listing_date={current_date}"
    async with aiohttp.ClientSession() as session:
        async with session.get(original_url) as response:
            response.raise_for_status()
            data = await response.json()
            return data


async def main_newly_listed_stocks():
    data = await fetch_data()
    return data