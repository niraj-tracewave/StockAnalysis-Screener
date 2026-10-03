import aiohttp
import asyncio
import os
import random
import time
import ujson
from aiolimiter import AsyncLimiter

from app.core.proxy_pool import load_proxy_pool

#bse new changes done
class RawBSEClient:
    BASE = "https://api.bseindia.com/BseIndiaAPI/api"

    def __init__(self):
        self.session = None
        self.proxy_pool = load_proxy_pool("bse", default_concurrency=4)
        self.rate_limiter = AsyncLimiter(
            max(1, int(os.environ.get("BSE_REQUESTS_PER_SECOND", "4"))),
            time_period=1,
        )

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
                "Origin": "https://www.bseindia.com",
                "Referer": "https://www.bseindia.com/",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-site",
            },
            json_serialize=ujson.dumps,
        )

    async def close(self):
        if self.session:
            await self.session.close()
        await self.proxy_pool.close()

    async def _get(self, endpoint, params=None):
        """Return raw JSON response for ANY endpoint."""
        if not self.session:
            raise RuntimeError("RawBSEClient.init() must be called first")
        retries = max(1, int(os.environ.get("EXCHANGE_RETRIES", "3")))
        for attempt in range(retries):
            try:
                async with self.rate_limiter, self.proxy_pool.lease() as proxy_endpoint:
                    started = time.monotonic()
                    try:
                        async with self.session.get(
                            f"{self.BASE}/{endpoint}",
                            params=params,
                            proxy=proxy_endpoint.url,
                        ) as response:
                            response.raise_for_status()
                            raw = await response.read()
                            await self.proxy_pool.success(
                                proxy_endpoint,
                                latency_ms=(time.monotonic() - started) * 1000,
                            )
                            return ujson.loads(raw)
                    except Exception as exc:
                        await self.proxy_pool.failure(proxy_endpoint, type(exc).__name__)
            except Exception:
                pass
            if attempt + 1 < retries:
                await asyncio.sleep((0.35 * (2**attempt)) + random.random() * 0.25)
        return None

    # -------------------------------------------------------
    # ALL BSE ENDPOINTS (RAW MODE)
    # -------------------------------------------------------

    async def fetch_all(self, scripcode: str):
        """
        Calls EVERY relevant BSE endpoint SAME as browser.
        Raw JSON only. No transformations.
        """

        endpoints = {
            "leftMenu": ("LeftMenunew/w", {
                "quotetype": "LMN",
                "scripcode": scripcode,
            }),

            "header": ("ComHeadernew/w", {
                "quotetype": "",
                "scripcode": scripcode,
                "seriesid": "",
            }),

            "scriptHeader": ("getScripHeaderData/w", {
                "Debtflag": "",
                "scripcode": scripcode,
                "seriesid": "",
            }),

            "stockTrading": ("StockTrading/w", {
                "flag": "",
                "quotetype": "EQ",
                "scripcode":scripcode,
            }),

            "priceGraph": ("StockReachGraph/w", {
                "scripcode": scripcode,
                "flag": "0",
                "fromdate": "",
                "todate": "",
                "seriesid": "",
            }),

            # Additional endpoints exactly as browser calls them
            "deliveryData": ("ComtradDelivery/w", {
                "scripcode": scripcode
            }),

            "corpActions": ("CorpAction/w", {
                "scripcode": scripcode
            }),

            "announcements": ("AnnSubCategory/GetListOfCorpAnn/w", {
                "scripcode": scripcode
            }),

            "filings": ("GetFilings/w", {
                "scripcode": scripcode
            }),

            "results": ("TabResults_PAR/w", {
                "scripcode": scripcode,
                "tabtype": "RESULTS"
            }),

            "shareholding": ("ComHeadernew/w", {
                "scripcode": scripcode,
                "tabtype": "SHAREHOLDING"
            }),

            "news": ("TabResults_PAR/w", {
                "scripcode": scripcode,
                "tabtype": "NEWS"
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

async def main(scrip):
    client = RawBSEClient()
    await client.init()
    try:
        return await client.fetch_all(scrip)
    finally:
        await client.close()
