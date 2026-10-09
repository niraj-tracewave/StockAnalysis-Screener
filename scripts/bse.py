import aiohttp
import asyncio
import os
import random
import time
import ujson
from aiolimiter import AsyncLimiter

from app.core.proxy_pool import load_proxy_pool

USE_BSE_PROXY = os.environ.get("USE_BSE_PROXY", "false").lower() in ("true", "1", "yes")

BROWSER_PROFILES = [
    {
        "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
        "sec_ch_ua_platform": '"Linux"',
    },
    {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
        "sec_ch_ua_platform": '"Windows"',
    },
    {
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
        "sec_ch_ua_platform": '"macOS"',
    },
    {
        "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="139", "Not=A?Brand";v="24", "Google Chrome";v="139"',
        "sec_ch_ua_platform": '"Linux"',
    },
    {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="139", "Not=A?Brand";v="24", "Google Chrome";v="139"',
        "sec_ch_ua_platform": '"Windows"',
    },
]


def get_dynamic_bse_headers(scripcode: str = "") -> dict[str, str]:
    profile = random.choice(BROWSER_PROFILES)
    referer = f"https://www.bseindia.com/stock-share-price/sp/{scripcode}/" if scripcode else "https://www.bseindia.com/"
    return {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "origin": "https://www.bseindia.com",
        "referer": referer,
        "priority": "u=1, i",
        "sec-ch-ua": profile["sec_ch_ua"],
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": profile["sec_ch_ua_platform"],
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": profile["user_agent"],
    }


#bse new changes done
class RawBSEClient:
    BASE = "https://api.bseindia.com/BseIndiaAPI/api"

    def __init__(self):
        self.session = None
        # Proxy code commented out as requested (uncomment or set USE_BSE_PROXY=true when proxies are ready)
        # self.proxy_pool = load_proxy_pool("bse", default_concurrency=4) if USE_BSE_PROXY else None
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
            headers=get_dynamic_bse_headers(),
            json_serialize=ujson.dumps,
        )

    async def close(self):
        if self.session:
            await self.session.close()
        # if hasattr(self, "proxy_pool") and self.proxy_pool:
        #     await self.proxy_pool.close()

    async def _get(self, endpoint, params=None):
        """Return raw JSON response for ANY endpoint."""
        if not self.session:
            raise RuntimeError("RawBSEClient.init() must be called first")
        retries = max(1, int(os.environ.get("EXCHANGE_RETRIES", "3")))
        scripcode = str((params or {}).get("scripcode", "")).strip()
        for attempt in range(retries):
            try:
                # Proxy code commented out: executing direct request without proxy pool
                headers = get_dynamic_bse_headers(scripcode)
                async with self.rate_limiter:
                    async with self.session.get(
                        f"{self.BASE}/{endpoint}",
                        params=params,
                        headers=headers,
                        # proxy=proxy_endpoint.url,
                    ) as response:
                        response.raise_for_status()
                        raw = await response.read()
                        return ujson.loads(raw)
            except Exception as e:
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
