import aiohttp
import asyncio
import os
import random
import time
import ujson
from aiolimiter import AsyncLimiter

from app.core.proxy_pool import load_proxy_pool


class FastNSEClient:
    BASE = "https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"

    def __init__(self):
        self.session: aiohttp.ClientSession | None = None
        self.proxy_pool = load_proxy_pool("nse", default_concurrency=4)
        self.rate_limiter = AsyncLimiter(
            max(1, int(os.environ.get("NSE_REQUESTS_PER_SECOND", "4"))),
            time_period=1,
        )

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
                    "AppleWebKit/537.36"
                    "(KHTML, like Gecko) Chrome/141 Safari/537.36",
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.nseindia.com/get-quote/equity/"
            }
        )

    async def close(self):
        if self.session:
            await self.session.close()
        await self.proxy_pool.close()

    async def _call(self, params: dict, safe=False):
        if not self.session:
            raise RuntimeError("FastNSEClient.init() must be called first")
        retries = max(1, int(os.environ.get("EXCHANGE_RETRIES", "3")))
        last_error = None
        for attempt in range(retries):
            try:
                async with self.rate_limiter, self.proxy_pool.lease() as endpoint:
                    started = time.monotonic()
                    try:
                        async with self.session.get(
                            self.BASE,
                            params=params,
                            proxy=endpoint.url,
                        ) as response:
                            response.raise_for_status()
                            result = await response.json(loads=ujson.loads, content_type=None)
                            await self.proxy_pool.success(
                                endpoint,
                                latency_ms=(time.monotonic() - started) * 1000,
                            )
                            return result
                    except Exception as exc:
                        last_error = exc
                        await self.proxy_pool.failure(endpoint, type(exc).__name__)
            except Exception as exc:
                last_error = exc
            if attempt + 1 < retries:
                await asyncio.sleep((0.35 * (2**attempt)) + random.random() * 0.25)
        if safe:
            return None
        raise last_error or RuntimeError("NSE request failed")

    # --- Basic API wrappers ---
    async def n(self, s): return await self._call({"functionName": "getSymbolName", "symbol": s})
    async def m(self, s): return await self._call({"functionName": "getMetaData", "symbol": s})
    async def sd(self, s, series, mkt="N"):
        return await self._call({
            "functionName": "getSymbolData",
            "symbol": s,
            "series": series,
            "marketType": mkt
        })
    async def reg(self, s, series):
        return await self._call({"functionName": "getRegDetails", "symbol": s, "series": series})
    async def chart(self, i):
        return await self._call({"functionName": "getSymbolChartData", "symbol": i, "days": "1D"})
    async def yr(self, i): return await self._call({"functionName": "getYearwiseData", "symbol": i})
    async def idx(self, s): return await self._call({"functionName": "getIndexList", "symbol": s})

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

    async def safe_call(self, coro, name):
        try:
            return await coro
        except Exception as e:
            print(f"{name} failed:", e)
            return None

    # --- MASTER FAST FETCH ---
    async def full(self, symbol: str):
        meta = await self.m(symbol)
        series = meta.get("activeSeries", ["EQ"])[0]
        marketType = meta.get("marketType", "N")

        # Parallel getSymbolData + next requests
        symbol_data = await self.sd(symbol, series, marketType)
        identifier = symbol_data["equityResponse"][0]["metaData"]["identifier"]

        # Fire all in parallel using TaskGroup (faster than asyncio.gather)
        async with asyncio.TaskGroup() as tg:
            t_name = tg.create_task(self.safe_call(self.n(symbol), "n"))
            t_reg = tg.create_task(self.safe_call(self.reg(symbol, series), "reg"))
            t_chart = tg.create_task(self.safe_call(self.chart(identifier), "chart"))
            t_year = tg.create_task(self.safe_call(self.yr(identifier), "yr"))
            t_idx = tg.create_task(self.safe_call(self.idx(symbol), "idx"))
            t_ann = tg.create_task(self.safe_call(self.ann(symbol), "ann"))
            t_brs = tg.create_task(self.safe_call(self.brs(symbol), "brs"))
            t_ar = tg.create_task(self.safe_call(self.ar(symbol), "ar"))
            t_shp = tg.create_task(self.safe_call(self.shp(symbol), "shp"))
            t_fin = tg.create_task(self.safe_call(self.fin(symbol), "fin"))
            t_board = tg.create_task(self.safe_call(self.board(symbol), "board"))

        return {
            "symbol": symbol,
            "companyName": meta.get("companyName"),
            "isin": meta.get("isin"),
            "symbolName": t_name.result(),
            "metaData": meta,
            "symbolData": symbol_data,
            "regulatory": t_reg.result(),
            "chart": t_chart.result(),
            "yearwise": t_year.result(),
            "indexList": t_idx.result(),
            "corporateAnnouncements": t_ann.result(),
            "companyInfo": t_brs.result(),
            "annualReports": t_ar.result(),
            "shareholdingPattern": t_shp.result(),
            "financialStatus": t_fin.result(),
            "boardMeetings": t_board.result(),
        }



async def main(symbol):
    c = FastNSEClient()
    await c.init()
    try:
        return await c.full(symbol)
    finally:
        await c.close()
