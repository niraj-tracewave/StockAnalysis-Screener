from datetime import datetime
from zoneinfo import ZoneInfo

import aiohttp
import asyncio
import ujson


class RawBSEClient:
    BASE = "https://api.bseindia.com/BseIndiaAPI/api"

    def __init__(self):
        self.session = None

    async def init(self):
        connector = aiohttp.TCPConnector(
            ttl_dns_cache=7200,
            limit=20,
            limit_per_host=5,
            ssl=False,
            enable_cleanup_closed=True,
        )

        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=12, connect=3, sock_read=5),
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) "
                    "AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/141 Safari/537.36"
                ),
                "Accept": "application/json, text/plain, */*",
                "Referer": "https://www.bseindia.com/",
            },
            json_serialize=ujson.dumps,
        )

    async def close(self):
        if self.session:
            await self.session.close()

    async def _get(self, endpoint, params=None):
        """Return raw JSON response for ANY endpoint."""
        try:
            async with self.session.get(f"{self.BASE}/{endpoint}", params=params) as r:
                r.raise_for_status()
                raw = await r.read()
                return ujson.loads(raw)
        except Exception:
            return None   # keep going even if BSE returns errors

    # -------------------------------------------------------
    # ALL BSE ENDPOINTS (RAW MODE)
    # -------------------------------------------------------

    async def fetch_all(self, scripcode: str, days: str):
        """
        Calls EVERY relevant BSE endpoint SAME as browser.
        Raw JSON only. No transformations.
        """

        endpoints = {
            "scriptHeader": ("StockReachGraph/w", {
                "flag": days,
                "scripcode": scripcode,
                "fromdate": "",
                "todate": "",
                "seriesid": ""
            }),

        }

        results = {}

        async with asyncio.TaskGroup() as tg:
            tasks = {
                name: tg.create_task(self._get(ep, params))
                for name, (ep, params) in endpoints.items()
            }

        # Collect output
        for key, task in tasks.items():
            results[key] = task.result()

        return results


# ---------------------- Runner ----------------------

async def main_fetch_stock_price_for_bse_graph(scrip, days):
    client = RawBSEClient()
    await client.init()
    try:
        return await client.fetch_all(scrip, days)
    finally:
        await client.close()


import aiohttp

url = "https://api.bseindia.com/BseIndiaAPI/api/StockReachGraph/w"
headers = {
    "authority": "api.bseindia.com",
    "accept": "*/*",
    "method": "GET",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "origin": "https://www.bseindia.com",
    "priority": "u=0, i",
    "referer": "https://www.bseindia.com/",
    "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Linux\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
  }


async def fetch_data(token, current_date):
    original_url = f"{url}?scripcode={token}&flag=1&fromdate={19960201}&todate={current_date}&seriesid="
    async with aiohttp.ClientSession() as session:
        async with session.get(original_url, headers={**headers, "path": f"/BseIndiaAPI/api/StockReachGraph/w?scripcode={token}&flag=1&fromdate={19960201}&todate={current_date}&seriesid="}) as response:
            response.raise_for_status()
            data = await response.json()
            return data

async def new_main_fetch_stock_price_for_bse_graph(token):
    ist_now = datetime.now(ZoneInfo("Asia/Kolkata"))

    current_date = ist_now.strftime("%Y%m%d")
    data = await fetch_data(token, current_date)
    return data
