"""High-throughput market-data ingestion for the local screener database.

The network stage is asynchronous and bounded. Database work is deliberately
kept out of the request workers and is committed once per batch, so thousands
of stocks do not create thousands of sessions or commits.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Protocol
from urllib.parse import quote

import aiohttp
from aiolimiter import AsyncLimiter
from sqlalchemy import and_, func, or_, select
from sqlalchemy.dialects.postgresql import insert

from app.apis.models.stock_data import ChartDataset, CompanyStock, KeyDetailsForCS
from app.core.config import get_settings
from app.core.proxy_pool import AsyncProxyPool, load_proxy_pool
from app.db.postgres.sync_session import SessionLocalSync


logger = logging.getLogger("market_data_pipeline")

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
NSE_EQUITY_SERIES = {"EQ", "BE", "BZ", "SM", "ST"}


@dataclass(frozen=True, slots=True)
class StockTarget:
    company_id: int
    yahoo_symbol: str


@dataclass(slots=True)
class MarketDataResult:
    company_id: int
    yahoo_symbol: str
    current_price: float | None
    day_high: float | None
    day_low: float | None
    history: list[list[Any]]
    fetched_at: datetime


@dataclass(slots=True)
class PipelineStats:
    requested: int = 0
    fetched: int = 0
    failed: int = 0
    details_upserted: int = 0
    charts_inserted: int = 0
    charts_updated: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "requested": self.requested,
            "fetched": self.fetched,
            "failed": self.failed,
            "details_upserted": self.details_upserted,
            "charts_inserted": self.charts_inserted,
            "charts_updated": self.charts_updated,
        }


class PermanentProviderError(RuntimeError):
    """A symbol/provider combination that should not be retried immediately."""


class MarketDataProvider(Protocol):
    """Normalized provider contract used by the ingestion orchestration."""

    async def fetch(self, target: StockTarget, *, full_history: bool) -> MarketDataResult: ...

    async def fetch_many(
        self, targets: list[StockTarget], *, full_history: bool
    ) -> tuple[list[MarketDataResult], list[str], list[str]]: ...


def _chunks(items: list[Any], size: int) -> Iterable[list[Any]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _split_nse_symbol(symbol: str) -> tuple[str, str] | None:
    if "-" not in symbol:
        return None
    base, series = symbol.rsplit("-", 1)
    if series not in NSE_EQUITY_SERIES:
        return None
    return base.strip(), series


def market_target_predicate():
    """Restrict jobs to NSE equities and BSE six-digit equity scrip codes.

    The Angel BSE segment also contains debt and preference instruments whose
    symbols are not Yahoo equity symbols. Keeping those rows in the database is
    harmless, but they must not enter the equity quote pipeline.
    """

    return or_(
        CompanyStock.primary_exchange == "NSE",
        and_(
            CompanyStock.primary_exchange == "BSE",
            func.length(CompanyStock.bse_code) == 6,
            CompanyStock.bse_code >= "500000",
            CompanyStock.bse_code < "600000",
        ),
    )


def seed_company_universe(master_path: str | Path = "OpenAPIScripMaster.json") -> dict[str, int]:
    """Seed NSE/BSE cash-market instruments from the downloaded Angel master.

    NSE is preferred for dual-listed symbols. BSE-only symbols use the `.BO`
    Yahoo suffix. The operation is idempotent through the Yahoo-symbol key.
    """

    path = Path(master_path)
    with path.open("r", encoding="utf-8") as source:
        instruments = json.load(source)

    universe: dict[str, dict[str, Any]] = {}

    for item in instruments:
        if item.get("exch_seg") != "NSE" or item.get("instrumenttype"):
            continue
        parsed = _split_nse_symbol(str(item.get("symbol") or ""))
        if not parsed:
            continue
        symbol, series = parsed
        yahoo_symbol = f"{symbol}.NS"
        universe[symbol] = {
            "name": item.get("name") or symbol,
            "nse_symbol": symbol,
            "nse_code": str(item.get("token") or "") or None,
            "bse_code": None,
            "stock_format": series,
            "yahoo_symbol": yahoo_symbol,
            "primary_exchange": "NSE",
        }

    nse_count = len(universe)

    for item in instruments:
        if item.get("exch_seg") != "BSE" or item.get("instrumenttype"):
            continue
        try:
            bse_token = int(str(item.get("token") or ""))
        except ValueError:
            continue
        if not 500_000 <= bse_token < 600_000:
            continue
        symbol = str(item.get("symbol") or "").strip()
        if not symbol:
            continue
        bse_code = str(bse_token)
        if symbol in universe:
            universe[symbol]["bse_code"] = bse_code
            continue
        universe[symbol] = {
            "name": item.get("name") or symbol,
            "nse_symbol": symbol,
            "nse_code": None,
            "bse_code": bse_code,
            "stock_format": "EQ",
            "yahoo_symbol": f"{symbol}.BO",
            "primary_exchange": "BSE",
        }

    rows = list(universe.values())
    db = SessionLocalSync()
    try:
        for batch in _chunks(rows, 1_000):
            statement = insert(CompanyStock).values(batch)
            statement = statement.on_conflict_do_update(
                index_elements=[CompanyStock.yahoo_symbol],
                set_={
                    "name": statement.excluded.name,
                    "nse_symbol": statement.excluded.nse_symbol,
                    "nse_code": statement.excluded.nse_code,
                    "bse_code": statement.excluded.bse_code,
                    "stock_format": statement.excluded.stock_format,
                    "primary_exchange": statement.excluded.primary_exchange,
                },
            )
            db.execute(statement)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    return {
        "nse": nse_count,
        "bse_only": len(rows) - nse_count,
        "total": len(rows),
    }


def load_targets(*, limit: int | None = None, offset: int = 0) -> tuple[list[StockTarget], int]:
    db = SessionLocalSync()
    try:
        total = db.scalar(
            select(func.count(CompanyStock.id)).where(
                CompanyStock.yahoo_symbol.is_not(None),
                market_target_predicate(),
            )
        ) or 0
        base = (
            select(CompanyStock.id, CompanyStock.yahoo_symbol)
            .where(
                CompanyStock.yahoo_symbol.is_not(None),
                market_target_predicate(),
            )
            .order_by(CompanyStock.id)
        )
        statement = base.offset(max(offset, 0))
        if limit is not None:
            statement = statement.limit(max(limit, 0))
        rows = db.execute(statement).all()
        return [StockTarget(company_id=row.id, yahoo_symbol=row.yahoo_symbol) for row in rows], total
    finally:
        db.close()


def load_targets_by_symbols(symbols: list[str]) -> tuple[list[StockTarget], int]:
    """Resolve retry symbols in one query while preserving caller order."""

    if not symbols:
        return [], 0
    db = SessionLocalSync()
    try:
        rows = db.execute(
            select(CompanyStock.id, CompanyStock.yahoo_symbol).where(
                CompanyStock.yahoo_symbol.in_(symbols),
                market_target_predicate(),
            )
        ).all()
        by_symbol = {
            row.yahoo_symbol: StockTarget(
                company_id=row.id,
                yahoo_symbol=row.yahoo_symbol,
            )
            for row in rows
        }
        return [by_symbol[symbol] for symbol in symbols if symbol in by_symbol], len(rows)
    finally:
        db.close()


def parse_yahoo_chart(company_id: int, yahoo_symbol: str, payload: dict[str, Any]) -> MarketDataResult:
    chart = payload.get("chart") or {}
    if chart.get("error"):
        raise ValueError(str(chart["error"]))
    results = chart.get("result") or []
    if not results:
        raise ValueError("Yahoo returned no chart result")

    result = results[0]
    meta = result.get("meta") or {}
    timestamps = result.get("timestamp") or []
    quote_data = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    closes = quote_data.get("close") or []
    volumes = quote_data.get("volume") or []
    highs = quote_data.get("high") or []
    lows = quote_data.get("low") or []

    history: list[list[Any]] = []
    for index, timestamp in enumerate(timestamps):
        close = closes[index] if index < len(closes) else None
        if close is None:
            continue
        volume = volumes[index] if index < len(volumes) else None
        if float(close) <= 0:
            raise ValueError(f"Invalid non-positive price for {yahoo_symbol}")
        if volume is not None and int(volume) < 0:
            raise ValueError(f"Invalid negative volume for {yahoo_symbol}")
        history.append(
            [
                int(timestamp) * 1_000,
                round(float(close), 2),
                "",
                None,
                None,
                int(volume) if volume is not None else None,
            ]
        )

    def last_number(values: list[Any]) -> float | None:
        for value in reversed(values):
            if value is not None:
                return float(value)
        return None

    current = meta.get("regularMarketPrice")
    if current is None:
        current = last_number(closes)
    if current is None:
        raise PermanentProviderError(
            f"Yahoo returned no usable price for {yahoo_symbol}"
        )
    day_high = meta.get("regularMarketDayHigh")
    if day_high is None:
        day_high = last_number(highs)
    day_low = meta.get("regularMarketDayLow")
    if day_low is None:
        day_low = last_number(lows)

    if day_high is not None and day_low is not None and float(day_high) < float(day_low):
        raise ValueError(f"Invalid high/low range for {yahoo_symbol}")

    # Provider payloads occasionally contain duplicate timestamps; keep one
    # normalized, ordered candle so repeated ingestion remains deterministic.
    history = [value for _, value in sorted({value[0]: value for value in history}.items())]

    return MarketDataResult(
        company_id=company_id,
        yahoo_symbol=yahoo_symbol,
        current_price=float(current),
        day_high=float(day_high) if day_high is not None else None,
        day_low=float(day_low) if day_low is not None else None,
        history=history,
        fetched_at=datetime.now(timezone.utc),
    )


class YahooChartClient:
    def __init__(
        self,
        *,
        concurrency: int = 12,
        timeout_seconds: int = 15,
        retries: int = 3,
        requests_per_second: int = 10,
        proxy_config_path: str | None = None,
    ) -> None:
        self.concurrency = max(1, concurrency)
        self.timeout_seconds = max(1, timeout_seconds)
        self.retries = max(1, retries)
        self.rate_limiter = AsyncLimiter(max(1, requests_per_second), time_period=1)
        self.proxy_pool: AsyncProxyPool = load_proxy_pool(
            "yahoo",
            config_path=proxy_config_path,
            default_concurrency=self.concurrency,
            redis_url=get_settings().redis_url,
        )
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "YahooChartClient":
        connector = aiohttp.TCPConnector(
            limit=self.concurrency * 2,
            limit_per_host=self.concurrency,
            ttl_dns_cache=900,
            enable_cleanup_closed=True,
        )
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            },
        )
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._session:
            await self._session.close()
        await self.proxy_pool.close()

    async def fetch(self, target: StockTarget, *, full_history: bool) -> MarketDataResult:
        if not self._session:
            raise RuntimeError("YahooChartClient must be used as an async context manager")

        url = YAHOO_CHART_URL.format(symbol=quote(target.yahoo_symbol, safe=""))
        params = {
            "range": "max" if full_history else "5d",
            "interval": "1d",
            "events": "div,splits",
        }

        async with self._semaphore:
            for attempt in range(self.retries):
                retry_delay: float | None = None
                failure_recorded = False
                async with self.rate_limiter, self.proxy_pool.lease() as endpoint:
                    try:
                        started = time.monotonic()
                        async with self._session.get(
                            url,
                            params=params,
                            proxy=endpoint.url,
                        ) as response:
                            if response.status in {403, 429} or response.status >= 500:
                                await self.proxy_pool.failure(
                                    endpoint,
                                    error_type=f"http_{response.status}",
                                )
                                failure_recorded = True
                                retry_after = response.headers.get("Retry-After")
                                retry_delay = (
                                    float(retry_after)
                                    if retry_after and retry_after.isdigit()
                                    else (2**attempt) + random.random()
                                )
                                if attempt + 1 == self.retries:
                                    response.raise_for_status()
                            elif response.status == 404:
                                raise PermanentProviderError(
                                    f"Yahoo has no chart for {target.yahoo_symbol}"
                                )
                            else:
                                response.raise_for_status()
                                payload = await response.json(content_type=None)
                                result = parse_yahoo_chart(
                                    target.company_id,
                                    target.yahoo_symbol,
                                    payload,
                                )
                                await self.proxy_pool.success(
                                    endpoint,
                                    latency_ms=(time.monotonic() - started) * 1_000,
                                )
                                return result
                    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                        status = getattr(exc, "status", None)
                        if status != 404 and not failure_recorded:
                            await self.proxy_pool.failure(
                                endpoint,
                                error_type=type(exc).__name__,
                            )
                        if attempt + 1 == self.retries:
                            raise
                        retry_delay = (2**attempt) + random.random()
                    except ValueError as exc:
                        await self.proxy_pool.failure(
                            endpoint,
                            error_type=type(exc).__name__,
                        )
                        if attempt + 1 == self.retries:
                            raise
                        retry_delay = (2**attempt) + random.random()

                # Never hold a proxy lease while waiting for provider backoff.
                if retry_delay is not None:
                    await asyncio.sleep(retry_delay)

        raise RuntimeError("Yahoo request exhausted retries")

    async def fetch_many(
        self,
        targets: list[StockTarget],
        *,
        full_history: bool,
    ) -> tuple[list[MarketDataResult], list[str], list[str]]:
        async def fetch_safe(target: StockTarget) -> MarketDataResult | Exception:
            try:
                return await self.fetch(target, full_history=full_history)
            except PermanentProviderError as exc:
                logger.debug("Yahoo symbol quarantined: %s", exc)
                return exc
            except Exception as exc:
                logger.warning("Yahoo fetch failed for %s: %s", target.yahoo_symbol, exc)
                return exc

        outcomes = await asyncio.gather(*(fetch_safe(target) for target in targets))
        completed: list[MarketDataResult] = []
        failed: list[str] = []
        permanent_failed: list[str] = []
        for target, outcome in zip(targets, outcomes):
            if isinstance(outcome, PermanentProviderError):
                permanent_failed.append(target.yahoo_symbol)
            elif isinstance(outcome, Exception):
                failed.append(target.yahoo_symbol)
            else:
                completed.append(outcome)
        return completed, failed, permanent_failed


def persist_market_data(results: list[MarketDataResult], *, include_history: bool) -> PipelineStats:
    stats = PipelineStats(fetched=len(results))
    if not results:
        return stats

    details = [
        {
            "company_id": item.company_id,
            "current_price": item.current_price,
            "high_price": item.day_high,
            "low_price": item.day_low,
            "data_source": "yahoo_chart",
            "market_data_updated_at": item.fetched_at,
        }
        for item in results
        if item.current_price is not None
    ]

    db = SessionLocalSync()
    try:
        if details:
            statement = insert(KeyDetailsForCS).values(details)
            statement = statement.on_conflict_do_update(
                index_elements=[KeyDetailsForCS.company_id],
                set_={
                    "current_price": statement.excluded.current_price,
                    "high_price": statement.excluded.high_price,
                    "low_price": statement.excluded.low_price,
                    "data_source": statement.excluded.data_source,
                    "market_data_updated_at": statement.excluded.market_data_updated_at,
                },
            )
            db.execute(statement)
            stats.details_upserted = len(details)

        if include_history:
            histories = {item.company_id: item.history for item in results if item.history}
            if histories:
                existing = db.execute(
                    select(ChartDataset.id, ChartDataset.company_id).where(
                        ChartDataset.company_id.in_(histories),
                        ChartDataset.meta["days"].astext == "30Y",
                    )
                ).all()
                existing_by_company = {row.company_id: row.id for row in existing}
                updates = [
                    {"id": existing_by_company[company_id], "values": values}
                    for company_id, values in histories.items()
                    if company_id in existing_by_company
                ]
                inserts = [
                    ChartDataset(
                        company_id=company_id,
                        metric="Price",
                        label="Price on Yahoo Finance",
                        values=values,
                        meta={"days": "30Y", "source": "yahoo_chart"},
                    )
                    for company_id, values in histories.items()
                    if company_id not in existing_by_company
                ]
                if updates:
                    db.bulk_update_mappings(ChartDataset, updates)
                if inserts:
                    db.add_all(inserts)
                stats.charts_updated = len(updates)
                stats.charts_inserted = len(inserts)

        db.commit()
        return stats
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


async def refresh_yahoo_market_data_async(
    *,
    limit: int | None = 500,
    offset: int = 0,
    concurrency: int = 12,
    timeout_seconds: int = 15,
    retries: int = 3,
    requests_per_second: int = 10,
    full_history: bool = False,
    proxy_config_path: str | None = None,
    excluded_symbols: set[str] | None = None,
) -> dict[str, Any]:
    targets, total = load_targets(limit=limit, offset=offset)
    scanned_count = len(targets)
    if excluded_symbols:
        targets = [target for target in targets if target.yahoo_symbol not in excluded_symbols]
    return await _refresh_yahoo_targets_async(
        targets=targets,
        total=total,
        offset=offset,
        scanned_count=scanned_count,
        concurrency=concurrency,
        timeout_seconds=timeout_seconds,
        retries=retries,
        requests_per_second=requests_per_second,
        full_history=full_history,
        proxy_config_path=proxy_config_path,
    )


async def refresh_yahoo_symbols_async(
    symbols: list[str],
    *,
    concurrency: int = 12,
    timeout_seconds: int = 15,
    retries: int = 3,
    requests_per_second: int = 10,
    full_history: bool = False,
    proxy_config_path: str | None = None,
) -> dict[str, Any]:
    targets, resolved = load_targets_by_symbols(symbols)
    result = await _refresh_yahoo_targets_async(
        targets=targets,
        total=resolved,
        offset=0,
        scanned_count=len(targets),
        concurrency=concurrency,
        timeout_seconds=timeout_seconds,
        retries=retries,
        requests_per_second=requests_per_second,
        full_history=full_history,
        proxy_config_path=proxy_config_path,
    )
    missing = [symbol for symbol in symbols if symbol not in {item.yahoo_symbol for item in targets}]
    result["failed_symbols"].extend(missing)
    result["failed"] += len(missing)
    result["requested"] = len(symbols)
    return result


async def _refresh_yahoo_targets_async(
    *,
    targets: list[StockTarget],
    total: int,
    offset: int,
    scanned_count: int,
    concurrency: int,
    timeout_seconds: int,
    retries: int,
    requests_per_second: int,
    full_history: bool,
    proxy_config_path: str | None,
) -> dict[str, Any]:
    stats = PipelineStats(requested=len(targets))
    if not targets:
        return {
            **stats.as_dict(),
            "total": total,
            "offset": offset,
            "failed_symbols": [],
            "permanent_failed_symbols": [],
        }

    async with YahooChartClient(
        concurrency=concurrency,
        timeout_seconds=timeout_seconds,
        retries=retries,
        requests_per_second=requests_per_second,
        proxy_config_path=proxy_config_path,
    ) as client:
        completed, failed, permanent_failed = await client.fetch_many(
            targets,
            full_history=full_history,
        )
        proxy_health = await client.proxy_pool.health_snapshot()

    persisted = persist_market_data(completed, include_history=full_history)
    persisted.requested = len(targets)
    persisted.failed = len(failed)
    return {
        **persisted.as_dict(),
        "total": total,
        "offset": offset,
        "next_offset": 0 if offset + scanned_count >= total else offset + scanned_count,
        "failed_symbols": failed,
        "permanent_failed_symbols": permanent_failed,
        "proxy_health": proxy_health,
    }
