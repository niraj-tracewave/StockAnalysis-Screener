"""Fast, bounded NSE/BSE quote ingestion.

The exchange clients reuse one HTTP session per shard, acquire a proxy lease
for each request, and persist a whole shard in one transaction.  Yahoo is not
used anywhere in this module.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

import aiohttp
from aiolimiter import AsyncLimiter
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from app.apis.models.stock_data import CompanyStock, KeyDetailsForCS
from app.core.config import get_settings
from app.core.proxy_pool import AsyncProxyPool, load_proxy_pool
from app.db.postgres.sync_session import SessionLocalSync


logger = logging.getLogger("exchange_data_pipeline")

Provider = Literal["nse", "bse"]
NSE_QUOTE_URL = "https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
BSE_API_ROOT = "https://api.bseindia.com/BseIndiaAPI/api"


class PermanentExchangeError(RuntimeError):
    """The requested exchange instrument is not currently quoteable."""


@dataclass(frozen=True, slots=True)
class ExchangeTarget:
    company_id: int
    provider: Provider
    symbol: str
    series: str = "EQ"

    @property
    def key(self) -> str:
        return self.symbol


@dataclass(slots=True)
class ExchangeQuote:
    company_id: int
    provider: Provider
    symbol: str
    current_price: float
    high_price: float | None
    low_price: float | None
    market_cap: float | None
    pe_ratio: float | None
    roe: float | None
    face_value: float | None
    fetched_at: datetime
    macro_sector: str | None = None
    sector: str | None = None
    industry: str | None = None
    basic_industry: str | None = None


@dataclass(slots=True)
class ExchangeStats:
    requested: int = 0
    fetched: int = 0
    failed: int = 0
    permanent_failed: int = 0
    details_upserted: int = 0
    companies_updated: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "requested": self.requested,
            "fetched": self.fetched,
            "failed": self.failed,
            "permanent_failed": self.permanent_failed,
            "details_upserted": self.details_upserted,
            "companies_updated": self.companies_updated,
        }


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = str(value).strip().replace(",", "")
    if not cleaned or cleaned in {"-", "--", "NA", "N/A", "null"}:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, list) and value and isinstance(value[0], dict):
        return value[0]
    return {}


def parse_nse_quote(
    target: ExchangeTarget,
    metadata: dict[str, Any],
    symbol_data: dict[str, Any],
) -> ExchangeQuote:
    responses = symbol_data.get("equityResponse") or []
    if not responses or not isinstance(responses[0], dict):
        raise PermanentExchangeError(f"NSE returned no equity quote for {target.symbol}")

    response = responses[0]
    quote_meta = _mapping(response.get("metaData"))
    trade = _mapping(response.get("tradeInfo"))
    security = _mapping(response.get("secInfo"))
    current = _number(trade.get("lastPrice"))
    high = _number(quote_meta.get("dayHigh"))
    low = _number(quote_meta.get("dayLow"))
    if current is None or current <= 0:
        raise PermanentExchangeError(f"NSE returned no usable price for {target.symbol}")
    if high is not None and low is not None and high < low:
        raise ValueError(f"NSE returned an invalid high/low range for {target.symbol}")

    total_market_cap = _number(trade.get("totalMarketCap"))
    return ExchangeQuote(
        company_id=target.company_id,
        provider="nse",
        symbol=target.symbol,
        current_price=current,
        high_price=high,
        low_price=low,
        market_cap=round(total_market_cap / 10_000_000, 2) if total_market_cap else None,
        pe_ratio=_number(security.get("pdSymbolPe")),
        roe=None,
        face_value=_number(trade.get("faceValue")),
        fetched_at=datetime.now(timezone.utc),
        macro_sector=security.get("macro") or None,
        sector=security.get("sector") or None,
        industry=security.get("industryInfo") or None,
        basic_industry=security.get("basicIndustry") or None,
    )


def parse_bse_quote(
    target: ExchangeTarget,
    header_payload: dict[str, Any],
    price_payload: dict[str, Any],
    trading_payload: dict[str, Any],
) -> ExchangeQuote:
    header = _mapping(header_payload)
    price_root = _mapping(price_payload)
    price = _mapping(price_root.get("Header")) or price_root
    trading = _mapping(trading_payload)

    current = _number(price.get("LTP") or price.get("CurrVal") or header.get("LTP"))
    high = _number(price.get("High") or price.get("HighRate"))
    low = _number(price.get("Low") or price.get("LowRate"))
    if current is None or current <= 0:
        raise PermanentExchangeError(f"BSE returned no usable price for {target.symbol}")
    if high is not None and low is not None and high < low:
        raise ValueError(f"BSE returned an invalid high/low range for {target.symbol}")

    return ExchangeQuote(
        company_id=target.company_id,
        provider="bse",
        symbol=target.symbol,
        current_price=current,
        high_price=high,
        low_price=low,
        market_cap=_number(trading.get("MktCapFull") or trading.get("MktCapFF")),
        pe_ratio=_number(header.get("PE")),
        roe=_number(header.get("ROE")),
        face_value=_number(header.get("FaceVal")),
        fetched_at=datetime.now(timezone.utc),
    )


def load_exchange_targets(
    provider: Provider,
    *,
    limit: int | None = None,
    offset: int = 0,
) -> tuple[list[ExchangeTarget], int]:
    if provider == "nse":
        predicate = (
            CompanyStock.primary_exchange == "NSE",
            CompanyStock.nse_symbol.is_not(None),
        )
        symbol_column = CompanyStock.nse_symbol
    elif provider == "bse":
        predicate = (
            CompanyStock.primary_exchange == "BSE",
            CompanyStock.bse_code.is_not(None),
            func.length(CompanyStock.bse_code) == 6,
        )
        symbol_column = CompanyStock.bse_code
    else:
        raise ValueError(f"Unsupported exchange provider: {provider}")

    db = SessionLocalSync()
    try:
        total = db.scalar(select(func.count(CompanyStock.id)).where(*predicate)) or 0
        statement = (
            select(CompanyStock.id, symbol_column, CompanyStock.stock_format)
            .where(*predicate)
            .order_by(CompanyStock.id)
            .offset(max(0, offset))
        )
        if limit is not None:
            statement = statement.limit(max(0, limit))
        rows = db.execute(statement).all()
        targets = [
            ExchangeTarget(
                company_id=row.id,
                provider=provider,
                symbol=str(row[1]).strip(),
                series=str(row.stock_format or "EQ").strip() or "EQ",
            )
            for row in rows
            if row[1]
        ]
        return targets, int(total)
    finally:
        db.close()


def load_exchange_targets_by_keys(
    provider: Provider,
    keys: list[str],
) -> tuple[list[ExchangeTarget], int]:
    if not keys:
        return [], 0
    if provider == "nse":
        column = CompanyStock.nse_symbol
        predicate = CompanyStock.primary_exchange == "NSE"
    elif provider == "bse":
        column = CompanyStock.bse_code
        predicate = CompanyStock.primary_exchange == "BSE"
    else:
        raise ValueError(f"Unsupported exchange provider: {provider}")

    db = SessionLocalSync()
    try:
        rows = db.execute(
            select(CompanyStock.id, column, CompanyStock.stock_format).where(
                predicate,
                column.in_(keys),
            )
        ).all()
        by_key = {
            str(row[1]): ExchangeTarget(
                company_id=row.id,
                provider=provider,
                symbol=str(row[1]),
                series=str(row.stock_format or "EQ"),
            )
            for row in rows
        }
        return [by_key[key] for key in keys if key in by_key], len(rows)
    finally:
        db.close()


class _ExchangeHttpClient:
    provider: Provider

    def __init__(
        self,
        *,
        concurrency: int,
        timeout_seconds: int,
        retries: int,
        requests_per_second: int,
        proxy_config_path: str | None,
    ) -> None:
        self.concurrency = max(1, concurrency)
        self.timeout_seconds = max(1, timeout_seconds)
        self.retries = max(1, retries)
        self.rate_limiter = AsyncLimiter(max(1, requests_per_second), time_period=1)
        settings = get_settings()
        self.use_proxy = (
            (self.provider == "nse" and (getattr(settings, "use_nse_proxy", False) or os.environ.get("USE_NSE_PROXY", "false").lower() in ("true", "1", "yes")))
            or (self.provider == "bse" and (getattr(settings, "use_bse_proxy", False) or os.environ.get("USE_BSE_PROXY", "false").lower() in ("true", "1", "yes")))
        )
        self.proxy_pool: AsyncProxyPool | None = (
            load_proxy_pool(
                self.provider,
                config_path=proxy_config_path,
                default_concurrency=self.concurrency,
                redis_url=settings.redis_url,
            )
            if self.use_proxy
            else None
        )
        self._company_semaphore = asyncio.Semaphore(self.concurrency)
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(
            limit=max(12, self.concurrency * 4),
            limit_per_host=max(8, self.concurrency * 3),
            ttl_dns_cache=900,
            enable_cleanup_closed=True,
        )
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(
                total=self.timeout_seconds,
                connect=min(5, self.timeout_seconds),
                sock_read=self.timeout_seconds,
            ),
            headers=self.headers,
        )
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._session:
            await self._session.close()
        if self.proxy_pool:
            await self.proxy_pool.close()

    @property
    def headers(self) -> dict[str, str]:
        raise NotImplementedError

    async def _request_json(
        self,
        url: str,
        *,
        params: dict[str, Any],
        extra_headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        if not self._session:
            raise RuntimeError("Exchange client must be used as an async context manager")

        last_error: Exception | None = None
        for attempt in range(self.retries):
            delay: float | None = None
            failure_recorded = False
            try:
                if self.proxy_pool and self.use_proxy:
                    async with self.rate_limiter, self.proxy_pool.lease() as endpoint:
                        started = time.monotonic()
                        try:
                            async with self._session.get(
                                url,
                                params=params,
                                headers=extra_headers,
                                proxy=endpoint.url,
                            ) as response:
                                if response.status in {401, 403, 408, 425, 429} or response.status >= 500:
                                    await self.proxy_pool.failure(endpoint, f"http_{response.status}")
                                    failure_recorded = True
                                    last_error = aiohttp.ClientResponseError(
                                        response.request_info,
                                        response.history,
                                        status=response.status,
                                        message=f"{self.provider.upper()} request blocked or unavailable",
                                        headers=response.headers,
                                    )
                                    retry_after = response.headers.get("Retry-After", "")
                                    delay = float(retry_after) if retry_after.isdigit() else (0.35 * (2**attempt)) + random.random() * 0.25
                                elif response.status == 404:
                                    raise PermanentExchangeError(
                                        f"{self.provider.upper()} instrument was not found"
                                    )
                                else:
                                    response.raise_for_status()
                                    payload = await response.json(content_type=None)
                                    if not isinstance(payload, dict):
                                        raise ValueError(f"{self.provider.upper()} returned non-object JSON")
                                    await self.proxy_pool.success(
                                        endpoint,
                                        latency_ms=(time.monotonic() - started) * 1000,
                                    )
                                    return payload
                        except PermanentExchangeError:
                            raise
                        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
                            last_error = exc
                            if not failure_recorded:
                                await self.proxy_pool.failure(endpoint, type(exc).__name__)
                            delay = (0.35 * (2**attempt)) + random.random() * 0.25
                else:
                    async with self.rate_limiter:
                        try:
                            async with self._session.get(
                                url,
                                params=params,
                                headers=extra_headers,
                            ) as response:
                                if response.status in {401, 403, 408, 425, 429} or response.status >= 500:
                                    last_error = aiohttp.ClientResponseError(
                                        response.request_info,
                                        response.history,
                                        status=response.status,
                                        message=f"{self.provider.upper()} request blocked or unavailable",
                                        headers=response.headers,
                                    )
                                    retry_after = response.headers.get("Retry-After", "")
                                    delay = float(retry_after) if retry_after.isdigit() else (0.35 * (2**attempt)) + random.random() * 0.25
                                elif response.status == 404:
                                    raise PermanentExchangeError(
                                        f"{self.provider.upper()} instrument was not found"
                                    )
                                else:
                                    response.raise_for_status()
                                    payload = await response.json(content_type=None)
                                    if not isinstance(payload, dict):
                                        raise ValueError(f"{self.provider.upper()} returned non-object JSON")
                                    return payload
                        except PermanentExchangeError:
                            raise
                        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
                            last_error = exc
                            delay = (0.35 * (2**attempt)) + random.random() * 0.25
            except PermanentExchangeError:
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError, TimeoutError, ValueError) as exc:
                last_error = exc
                delay = (0.35 * (2**attempt)) + random.random() * 0.25

            if attempt + 1 < self.retries and delay is not None:
                await asyncio.sleep(delay)

        raise last_error or RuntimeError(f"{self.provider.upper()} exhausted retries")

    async def fetch(self, target: ExchangeTarget) -> ExchangeQuote:
        raise NotImplementedError

    async def fetch_many(
        self,
        targets: list[ExchangeTarget],
    ) -> tuple[list[ExchangeQuote], list[str], list[str]]:
        async def safe_fetch(target: ExchangeTarget) -> ExchangeQuote | Exception:
            try:
                return await self.fetch(target)
            except PermanentExchangeError as exc:
                logger.info("%s permanent miss for %s: %s", self.provider, target.key, exc)
                return exc
            except Exception as exc:
                logger.warning("%s fetch failed for %s: %s", self.provider, target.key, exc)
                return exc

        outcomes = await asyncio.gather(*(safe_fetch(target) for target in targets))
        completed: list[ExchangeQuote] = []
        failed: list[str] = []
        permanent: list[str] = []
        for target, outcome in zip(targets, outcomes):
            if isinstance(outcome, PermanentExchangeError):
                permanent.append(target.key)
            elif isinstance(outcome, Exception):
                failed.append(target.key)
            else:
                completed.append(outcome)
        return completed, failed, permanent


class NSEQuoteClient(_ExchangeHttpClient):
    provider: Provider = "nse"

    @property
    def headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nseindia.com/get-quotes/equity",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/140 Safari/537.36",
        }

    async def fetch(self, target: ExchangeTarget) -> ExchangeQuote:
        async with self._company_semaphore:
            metadata = await self._request_json(
                NSE_QUOTE_URL,
                params={"functionName": "getMetaData", "symbol": target.symbol},
                extra_headers={"Referer": f"https://www.nseindia.com/get-quotes/equity?symbol={target.symbol}"},
            )
            if metadata.get("isDelisted") is True:
                raise PermanentExchangeError(f"NSE symbol {target.symbol} is delisted")
            series_values = metadata.get("activeSeries") or []
            if not series_values:
                raise PermanentExchangeError(f"NSE returned no active series for {target.symbol}")
            series = target.series if target.series in series_values else str(series_values[0])
            symbol_data = await self._request_json(
                NSE_QUOTE_URL,
                params={
                    "functionName": "getSymbolData",
                    "symbol": target.symbol,
                    "series": series,
                    "marketType": metadata.get("marketType") or "N",
                },
                extra_headers={"Referer": f"https://www.nseindia.com/get-quotes/equity?symbol={target.symbol}"},
            )
            return parse_nse_quote(target, metadata, symbol_data)


class BSEQuoteClient(_ExchangeHttpClient):
    provider: Provider = "bse"

    @property
    def headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://www.bseindia.com",
            "Referer": "https://www.bseindia.com/",
            "Priority": "u=1, i",
            "Sec-CH-UA": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Linux"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        }

    async def fetch(self, target: ExchangeTarget) -> ExchangeQuote:
        async with self._company_semaphore:
            requests = (
                self._request_json(
                    f"{BSE_API_ROOT}/ComHeadernew/w",
                    params={"quotetype": "", "scripcode": target.symbol, "seriesid": ""},
                ),
                self._request_json(
                    f"{BSE_API_ROOT}/getScripHeaderData/w",
                    params={"Debtflag": "", "scripcode": target.symbol, "seriesid": ""},
                ),
                self._request_json(
                    f"{BSE_API_ROOT}/StockTrading/w",
                    params={"flag": "", "quotetype": "EQ", "scripcode": target.symbol},
                ),
            )
            header, price, trading = await asyncio.gather(*requests)
            return parse_bse_quote(target, header, price, trading)


def persist_exchange_quotes(results: list[ExchangeQuote]) -> ExchangeStats:
    stats = ExchangeStats(fetched=len(results))
    if not results:
        return stats

    rows = [
        {
            "company_id": item.company_id,
            "current_price": item.current_price,
            "high_price": item.high_price,
            "low_price": item.low_price,
            "market_cap": item.market_cap,
            "pe_ratio": item.pe_ratio,
            "roe": item.roe,
            "face_value": item.face_value,
            "data_source": f"{item.provider}_official",
            "market_data_updated_at": item.fetched_at,
        }
        for item in results
    ]
    company_rows = [
        {
            "id": item.company_id,
            **({"macro_economic_sector": item.macro_sector} if item.macro_sector else {}),
            **({"sector": item.sector} if item.sector else {}),
            **({"industry": item.industry} if item.industry else {}),
            **({"basic_industry": item.basic_industry} if item.basic_industry else {}),
        }
        for item in results
        if any((item.macro_sector, item.sector, item.industry, item.basic_industry))
    ]

    db = SessionLocalSync()
    try:
        statement = insert(KeyDetailsForCS).values(rows)
        statement = statement.on_conflict_do_update(
            index_elements=[KeyDetailsForCS.company_id],
            set_={
                "current_price": statement.excluded.current_price,
                "high_price": statement.excluded.high_price,
                "low_price": statement.excluded.low_price,
                "market_cap": func.coalesce(statement.excluded.market_cap, KeyDetailsForCS.market_cap),
                "pe_ratio": func.coalesce(statement.excluded.pe_ratio, KeyDetailsForCS.pe_ratio),
                "roe": func.coalesce(statement.excluded.roe, KeyDetailsForCS.roe),
                "face_value": func.coalesce(statement.excluded.face_value, KeyDetailsForCS.face_value),
                "data_source": statement.excluded.data_source,
                "market_data_updated_at": statement.excluded.market_data_updated_at,
            },
        )
        db.execute(statement)
        if company_rows:
            db.bulk_update_mappings(CompanyStock, company_rows)
        db.commit()
        stats.details_upserted = len(rows)
        stats.companies_updated = len(company_rows)
        return stats
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


async def refresh_exchange_targets_async(
    provider: Provider,
    targets: list[ExchangeTarget],
    *,
    total: int,
    offset: int,
    scanned_count: int,
    concurrency: int,
    timeout_seconds: int,
    retries: int,
    requests_per_second: int,
    proxy_config_path: str | None,
) -> dict[str, Any]:
    stats = ExchangeStats(requested=len(targets))
    if not targets:
        return {
            **stats.as_dict(),
            "provider": provider,
            "total": total,
            "offset": offset,
            "next_offset": 0 if total == 0 else offset,
            "failed_symbols": [],
            "permanent_failed_symbols": [],
            "proxy_health": [],
        }

    client_class = NSEQuoteClient if provider == "nse" else BSEQuoteClient
    async with client_class(
        concurrency=concurrency,
        timeout_seconds=timeout_seconds,
        retries=retries,
        requests_per_second=requests_per_second,
        proxy_config_path=proxy_config_path,
    ) as client:
        completed, failed, permanent = await client.fetch_many(targets)
        proxy_health = await client.proxy_pool.health_snapshot() if client.proxy_pool else []

    persisted = persist_exchange_quotes(completed)
    persisted.requested = len(targets)
    persisted.failed = len(failed)
    persisted.permanent_failed = len(permanent)
    return {
        **persisted.as_dict(),
        "provider": provider,
        "total": total,
        "offset": offset,
        "next_offset": 0 if offset + scanned_count >= total else offset + scanned_count,
        "failed_symbols": failed,
        "permanent_failed_symbols": permanent,
        "proxy_health": proxy_health,
    }


async def refresh_exchange_market_data_async(
    provider: Provider,
    *,
    limit: int | None,
    offset: int,
    concurrency: int,
    timeout_seconds: int,
    retries: int,
    requests_per_second: int,
    proxy_config_path: str | None,
    excluded_symbols: set[str] | None = None,
) -> dict[str, Any]:
    targets, total = load_exchange_targets(provider, limit=limit, offset=offset)
    scanned_count = len(targets)
    if excluded_symbols:
        targets = [target for target in targets if target.key not in excluded_symbols]
    return await refresh_exchange_targets_async(
        provider,
        targets,
        total=total,
        offset=offset,
        scanned_count=scanned_count,
        concurrency=concurrency,
        timeout_seconds=timeout_seconds,
        retries=retries,
        requests_per_second=requests_per_second,
        proxy_config_path=proxy_config_path,
    )


async def refresh_exchange_symbols_async(
    provider: Provider,
    keys: list[str],
    *,
    concurrency: int,
    timeout_seconds: int,
    retries: int,
    requests_per_second: int,
    proxy_config_path: str | None,
) -> dict[str, Any]:
    targets, resolved = load_exchange_targets_by_keys(provider, keys)
    result = await refresh_exchange_targets_async(
        provider,
        targets,
        total=resolved,
        offset=0,
        scanned_count=len(targets),
        concurrency=concurrency,
        timeout_seconds=timeout_seconds,
        retries=retries,
        requests_per_second=requests_per_second,
        proxy_config_path=proxy_config_path,
    )
    known = {target.key for target in targets}
    missing = [key for key in keys if key not in known]
    result["permanent_failed_symbols"].extend(missing)
    result["permanent_failed"] += len(missing)
    result["requested"] = len(keys)
    return result
