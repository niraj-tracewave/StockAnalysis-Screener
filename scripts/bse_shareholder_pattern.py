import aiohttp
import asyncio
import ujson


from scripts.bse import get_dynamic_bse_headers


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
            headers=get_dynamic_bse_headers(),
            json_serialize=ujson.dumps,
        )

    async def close(self):
        if self.session:
            await self.session.close()

    async def _get(self, endpoint, params=None):
        """Return raw JSON response for ANY endpoint."""
        scripcode = str((params or {}).get("scripcode", "")).strip()
        try:
            headers = get_dynamic_bse_headers(scripcode)
            async with self.session.get(f"{self.BASE}/{endpoint}", params=params, headers=headers) as r:
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
    try:
        return await client.fetch_all(scrip)
    finally:
        await client.close()
