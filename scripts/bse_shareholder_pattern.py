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
            limit=200,
            limit_per_host=50,
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

    async def fetch_all(self, scripcode: str):
        """
        Calls EVERY relevant BSE endpoint SAME as browser.
        Raw JSON only. No transformations.
        """

        endpoints = {
            "share_holder": ("SHPQNewFormat/w", {
                "scripcode": scripcode,
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

async def main_fetch_stock_share_holder_pattern(scrip):
    client = RawBSEClient()
    await client.init()

    data = await client.fetch_all(scrip)

    await client.close()
    return data
