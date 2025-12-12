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
            "leftMenu": ("LeftMenunew/w", {
                "quotetype": "LMN",
                "scripcode": scripcode,
            }),

            "header": ("ComHeadernew/w", {
                "quotetype": "EQ",
                "scripcode": scripcode,
                "seriesid": "",
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

            "results": ("ComHeadernew/w", {
                "scripcode": scripcode,
                "tabtype": "RESULTS"
            }),

            "shareholding": ("ComHeadernew/w", {
                "scripcode": scripcode,
                "tabtype": "SHAREHOLDING"
            }),

            "news": ("ComHeadernew/w", {
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

async def main():
    client = RawBSEClient()
    await client.init()

    scrip = "501831"  # COASTCORP
    data = await client.fetch_all(scrip)

    filename = f"BSE_{scrip}_RAW.json"
    with open(filename, "w") as f:
        f.write(ujson.dumps(data, indent=4))

    print(f"\n🔥 RAW BSE JSON SAVED → {filename}\n")

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
