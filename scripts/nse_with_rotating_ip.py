# import aiohttp
# import asyncio
# import ujson
# from urllib.parse import urlencode
#
#
# class FastNSEClient:
#     NSE_BASE = "https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
#     SCRAPINGBEE_BASE = "https://app.scrapingbee.com/api/v1/"
#
#     def __init__(self, scrapingbee_api_key: str):
#         self.session: aiohttp.ClientSession | None = None
#         self.api_key = "YA4JORSPAD7B29DDTRDM5VPFVG3U36IUL3RW76U09VVV9J6QN8H7XOX65B2RVRP9LQGKL7B89L41JKQD"
#
#     async def init(self):
#         connector = aiohttp.TCPConnector(
#             ttl_dns_cache=3600,
#             limit=100,
#             limit_per_host=20,
#             enable_cleanup_closed=True,
#             ssl=False
#         )
#
#         timeout = aiohttp.ClientTimeout(
#             total=15,
#             connect=5,
#             sock_read=10,
#             sock_connect=5
#         )
#
#         self.session = aiohttp.ClientSession(
#             connector=connector,
#             timeout=timeout,
#             json_serialize=ujson.dumps
#         )
#
#     async def close(self):
#         await self.session.close()
#
#     # -------------------------------------------------
#     # ScrapingBee URL builder
#     # -------------------------------------------------
#     def _scrapingbee_url(self, params: dict):
#         target_url = f"{self.NSE_BASE}?{urlencode(params)}"
#
#         return self.SCRAPINGBEE_BASE, {
#             "api_key": self.api_key,
#             "url": target_url,
#             # "country_code": "in",
#             # "premium_proxy": "true",
#             "render_js": "false"
#         }
#
#     # -------------------------------------------------
#     # ScrapingBee call with auto IP rotation retry
#     # -------------------------------------------------
#     async def _call(self, params: dict, safe=False, retries=3):
#         attempt = 0
#
#         while attempt < retries:
#             try:
#                 scrapingbee_url, bee_params = self._scrapingbee_url(params)
#
#                 async with self.session.get(scrapingbee_url, params=bee_params) as r:
#
#                     # NSE blocked / rate limit → retry new IP
#                     if r.status in (401, 403, 429, 500, 502, 503):
#                         attempt += 1
#                         await asyncio.sleep(0.5)
#                         continue
#
#                     r.raise_for_status()
#                     return await r.json(loads=ujson.loads)
#
#             except Exception:
#                 attempt += 1
#                 if attempt >= retries:
#                     if safe:
#                         return None
#                     raise
#                 await asyncio.sleep(0.5)
#
#     # --- Basic API wrappers ---
#     async def n(self, s): return await self._call({"functionName": "getSymbolName", "symbol": s})
#     async def m(self, s): return await self._call({"functionName": "getMetaData", "symbol": s})
#
#     async def sd(self, s, series, mkt="N"):
#         return await self._call({
#             "functionName": "getSymbolData",
#             "symbol": s,
#             "series": series,
#             "marketType": mkt
#         })
#
#     async def reg(self, s, series):
#         return await self._call({"functionName": "getRegDetails", "symbol": s, "series": series})
#
#     async def chart(self, i):
#         return await self._call({"functionName": "getSymbolChartData", "symbol": i, "days": "1D"})
#
#     async def yr(self, i): return await self._call({"functionName": "getYearwiseData", "symbol": i})
#     async def idx(self, s): return await self._call({"functionName": "getIndexList", "symbol": s})
#
#     async def ann(self, s):
#         return await self._call({
#             "functionName": "getCorporateAnnouncement",
#             "symbol": s,
#             "marketApiType": "equities",
#             "noOfRecords": 3
#         }, safe=True)
#
#     async def brs(self, s):
#         return await self._call({"functionName": "getCorpBrs", "symbol": s}, safe=True)
#
#     async def ar(self, s):
#         return await self._call({
#             "functionName": "getCorpAnnualReport",
#             "symbol": s,
#             "marketApiType": "equities",
#             "noOfRecords": 6
#         }, safe=True)
#
#     async def shp(self, s):
#         return await self._call({
#             "functionName": "getShareholdingPattern",
#             "symbol": s,
#             "noOfRecords": 5
#         }, safe=True)
#
#     async def fin(self, s):
#         return await self._call({"functionName": "getFinancialStatus", "symbol": s}, safe=True)
#
#     async def board(self, s):
#         return await self._call({
#             "functionName": "getBoardMeet",
#             "symbol": s,
#             "type": "W",
#             "noOfRecords": 4
#         }, safe=True)
#
#     # --- MASTER FAST FETCH ---
#     async def full(self, symbol: str):
#         meta = await self.m(symbol)
#         series = meta.get("activeSeries", ["EQ"])[0]
#
#         symbol_data = await self.sd(symbol, series)
#         identifier = symbol_data["equityResponse"][0]["metaData"]["identifier"]
#
#         async with asyncio.TaskGroup() as tg:
#             t_name = tg.create_task(self.n(symbol))
#             t_reg = tg.create_task(self.reg(symbol, series))
#             # t_chart = tg.create_task(self.chart(identifier))
#             # t_year = tg.create_task(self.yr(identifier))
#             # t_idx = tg.create_task(self.idx(symbol))
#             # t_ann = tg.create_task(self.ann(symbol))
#             t_brs = tg.create_task(self.brs(symbol))
#             # t_ar = tg.create_task(self.ar(symbol))
#             # t_shp = tg.create_task(self.shp(symbol))
#             # t_fin = tg.create_task(self.fin(symbol))
#             # t_board = tg.create_task(self.board(symbol))
#
#         return {
#             "symbol": symbol,
#             "companyName": meta.get("companyName"),
#             "isin": meta.get("isin"),
#             "symbolName": t_name.result(),
#             "metaData": meta,
#             "symbolData": symbol_data,
#             "regulatory": t_reg.result(),
#             # "chart": t_chart.result(),
#             "chart": None,
#             # "yearwise": t_year.result(),
#             "yearwise": None,
#             # "indexList": t_idx.result(),
#             "indexList": None,
#             # "corporateAnnouncements": t_ann.result(),
#             "corporateAnnouncements": None,
#             "companyInfo": t_brs.result(),
#             # "annualReports": t_ar.result(),
#             "annualReports": None,
#             # "shareholdingPattern": t_shp.result(),
#             "shareholdingPattern": None,
#             # "financialStatus": t_fin.result(),
#             "financialStatus": None,
#             # "boardMeetings": t_board.result(),
#             "boardMeetings": None,
#         }
#
#
# # ------------------------------
# # Runner
# # ------------------------------
# async def main(symbol):
#     c = FastNSEClient(scrapingbee_api_key="YOUR_SCRAPINGBEE_API_KEY")
#     await c.init()
#
#     data = await c.full(symbol)
#
#     await c.close()
#     return data



import aiohttp
import asyncio
import ujson
from urllib.parse import urlencode

from app.core.config import get_settings

settings = get_settings()

class FastNSEClient:
    NSE_BASE = "https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
    SCRAPINGBEE_BASE = "https://app.scrapingbee.com/api/v1/"

    HEADERS = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Referer": "https://www.nseindia.com/"
    }

    def __init__(self, scrapingbee_api_key: str):
        self.session: aiohttp.ClientSession | None = None
        self.api_key = scrapingbee_api_key  # fixed (no hardcoding)

    # -------------------------------------------------
    # SESSION INIT
    # -------------------------------------------------
    async def init(self):
        connector = aiohttp.TCPConnector(
            ttl_dns_cache=3600,
            limit=20,
            limit_per_host=5,
            enable_cleanup_closed=True,
            ssl=False
        )

        timeout = aiohttp.ClientTimeout(
            total=15,
            connect=5,
            sock_read=10,
            sock_connect=5
        )

        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            json_serialize=ujson.dumps
        )

    async def close(self):
        if self.session:
            await self.session.close()

    # -------------------------------------------------
    # URL BUILDERS
    # -------------------------------------------------
    def _direct_url(self, params: dict):
        return f"{self.NSE_BASE}?{urlencode(params)}", None

    def _scrapingbee_url(self, params: dict):
        target_url = f"{self.NSE_BASE}?{urlencode(params)}"
        return self.SCRAPINGBEE_BASE, {
            "api_key": self.api_key,
            "url": target_url,
            "render_js": "false"
        }

    # -------------------------------------------------
    # SMART CALLER
    # Direct NSE → if blocked → ScrapingBee
    # -------------------------------------------------
    async def _call(self, params: dict, safe=False, retries=3):
        attempt = 0
        use_proxy = False  # start with own IP

        while attempt < retries:
            try:
                if use_proxy:
                    url, req_params = self._scrapingbee_url(params)
                else:
                    url, req_params = self._direct_url(params)

                async with self.session.get(
                    url,
                    params=req_params,
                    headers=self.HEADERS
                ) as r:

                    # If NSE blocks → switch to proxy
                    if r.status in (401, 403, 429, 500, 502, 503):
                        attempt += 1

                        # first failure → move to proxy
                        if not use_proxy:
                            use_proxy = True

                        await asyncio.sleep(0.4)
                        continue

                    r.raise_for_status()
                    return await r.json(loads=ujson.loads)

            except Exception:
                attempt += 1
                use_proxy = True  # fallback to proxy on error

                if attempt >= retries:
                    if safe:
                        return None
                    raise

                await asyncio.sleep(0.4)

    # -------------------------------------------------
    # NSE API WRAPPERS
    # -------------------------------------------------
    async def n(self, s):
        return await self._call({"functionName": "getSymbolName", "symbol": s})

    async def m(self, s):
        return await self._call({"functionName": "getMetaData", "symbol": s})

    async def sd(self, s, series, mkt="N"):
        return await self._call({
            "functionName": "getSymbolData",
            "symbol": s,
            "series": series,
            "marketType": mkt
        })

    async def reg(self, s, series):
        return await self._call({
            "functionName": "getRegDetails",
            "symbol": s,
            "series": series
        })

    async def chart(self, i):
        return await self._call({
            "functionName": "getSymbolChartData",
            "symbol": i,
            "days": "1D"
        })

    async def yr(self, i):
        return await self._call({"functionName": "getYearwiseData", "symbol": i})

    async def idx(self, s):
        return await self._call({"functionName": "getIndexList", "symbol": s})

    async def ann(self, s):
        return await self._call({
            "functionName": "getCorporateAnnouncement",
            "symbol": s,
            "marketApiType": "equities",
            "noOfRecords": 3
        }, safe=True)

    async def brs(self, s):
        return await self._call({"functionName": "getCorpBrs", "symbol": s}, safe=True)

    async def ar(self, s):
        return await self._call({
            "functionName": "getCorpAnnualReport",
            "symbol": s,
            "marketApiType": "equities",
            "noOfRecords": 6
        }, safe=True)

    async def shp(self, s):
        return await self._call({
            "functionName": "getShareholdingPattern",
            "symbol": s,
            "noOfRecords": 5
        }, safe=True)

    async def fin(self, s):
        return await self._call({"functionName": "getFinancialStatus", "symbol": s}, safe=True)

    async def board(self, s):
        return await self._call({
            "functionName": "getBoardMeet",
            "symbol": s,
            "type": "W",
            "noOfRecords": 4
        }, safe=True)

    # -------------------------------------------------
    # MASTER FAST FETCH
    # -------------------------------------------------
    async def full(self, symbol: str):
        meta = await self.m(symbol)
        series = meta.get("activeSeries", ["EQ"])[0]
        marketType = meta.get("marketType", "N")

        symbol_data = await self.sd(symbol, series, marketType)
        identifier = symbol_data["equityResponse"][0]["metaData"]["identifier"]

        async with asyncio.TaskGroup() as tg:
            t_name = tg.create_task(self.n(symbol))
            t_reg = tg.create_task(self.reg(symbol, series))
            t_brs = tg.create_task(self.brs(symbol))

        return {
            "symbol": symbol,
            "companyName": meta.get("companyName"),
            "isin": meta.get("isin"),
            "symbolName": t_name.result(),
            "metaData": meta,
            "symbolData": symbol_data,
            "regulatory": t_reg.result(),
            "chart": None,
            "yearwise": None,
            "indexList": None,
            "corporateAnnouncements": None,
            "companyInfo": t_brs.result(),
            "annualReports": None,
            "shareholdingPattern": None,
            "financialStatus": None,
            "boardMeetings": None,
        }


# -------------------------------------------------
# RUNNER
# -------------------------------------------------
async def main(symbol: str):
    client = FastNSEClient(scrapingbee_api_key=settings.SCRAPINGBEE_API_KEY)
    await client.init()

    try:
        data = await client.full(symbol)
        return data
    finally:
        await client.close()
