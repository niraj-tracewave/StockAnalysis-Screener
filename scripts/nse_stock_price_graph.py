import aiohttp
import asyncio
import ujson


class FastNSEClient:
    BASE = "https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"

    def __init__(self):
        self.session: aiohttp.ClientSession | None = None

    async def init(self):
        # Ultra-fast TCP connector tuning
        connector = aiohttp.TCPConnector(
            ttl_dns_cache=3600,     # cache DNS for 1 hour
            limit=20,               # keep requests quick without a burst from one static IP
            limit_per_host=5,
            enable_cleanup_closed=True,
            ssl=False
        )

        timeout = aiohttp.ClientTimeout(
            total=10,
            connect=3,
            sock_read=5,
            sock_connect=3
        )

        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            json_serialize=ujson.dumps,
            headers={
                "User-Agent":
                    "Mozilla/5.0 (X11; Linux x86_64) "
                    "AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/141 Safari/537.36",
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.nseindia.com/get-quote/equity/"
            }
        )

        # Get cookies fast without downloading large content
        async with self.session.get("https://www.nseindia.com", allow_redirects=True):
            pass

    async def close(self):
        await self.session.close()

    async def _call(self, params: dict, safe=False):
        try:
            async with self.session.get(self.BASE, params=params) as r:
                if not safe:
                    r.raise_for_status()
                    return await r.json(loads=ujson.loads)

                # Safe mode
                try:
                    r.raise_for_status()
                    return await r.json(loads=ujson.loads)
                except:
                    return None

        except:
            if safe:
                return None
            raise

    # --- Basic API wrappers ---
    # async def n(self, s): return await self._call({"functionName": "getSymbolName", "symbol": s})
    async def m(self, s): return await self._call({"functionName": "getMetaData", "symbol": s})
    async def sd(self, s, series, mkt="N"):
        return await self._call({
            "functionName": "getSymbolData",
            "symbol": s,
            "series": series,
            "marketType": mkt
        })

    async def stock_chart_price(self, s, days):
        return await self._call({
            "functionName": "getSymbolChartData",
            "symbol": s,
            "days": days
        }, safe=True)

    # --- MASTER FAST FETCH ---
    async def full(self, symbol: str, days=None):
        meta = await self.m(symbol)
        series = meta.get("activeSeries", ["EQ"])[0]

        # Parallel getSymbolData + next requests
        symbol_data = await self.sd(symbol, series)
        identifier = symbol_data["equityResponse"][0]["metaData"]["identifier"]

        # Fire all in parallel using TaskGroup (faster than asyncio.gather)
        async with asyncio.TaskGroup() as tg:
            t_stock_chart_price = tg.create_task(self.stock_chart_price(identifier, days))

        return {
            "symbol": symbol,
            "companyName": meta.get("companyName"),
            "isin": meta.get("isin"),
            "chart": t_stock_chart_price.result(),
        }



async def main_fetch_stock_price_for_graph(symbol, days):
    c = FastNSEClient()
    await c.init()
    try:
        return await c.full(symbol, days)
    finally:
        await c.close()



import aiohttp

url = "https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
headers = {
    "authority": "www.nseindia.com",
    "method": "GET",
    "scheme": "https",
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "if-none-match": "\"dus2ivngaa48k4\"",
    "priority": "u=1, i",
    "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Linux\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
}



async def fetch_data(days, identifier, symbol, cname):
    original_url = f"{url}?functionName=getSymbolChartData&symbol={identifier}&days={days}"
    async with aiohttp.ClientSession() as session:
        async with session.get(original_url, headers={**headers, "path": f"/api/NextApi/apiClient/GetQuoteApi?functionName=getSymbolChartData&symbol=TCSEQN&days=30Y",
                                                      "referer": f"https://www.nseindia.com/get-quote/equity/{symbol}/{cname}"}) as response:
            response.raise_for_status()
            data = await response.json()
            return data

async def new_main_fetch_stock_price_for_graph(days, identifier, symbol, cname):
    data = await fetch_data(days, identifier, symbol, cname)
    return data
