"""Seed and refresh the local market-data store."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.config import get_settings
from app.core.exchange_data_pipeline import refresh_exchange_market_data_async
from app.core.market_data_pipeline import (
    refresh_yahoo_market_data_async,
    seed_company_universe,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)

    seed = subcommands.add_parser("seed", help="Upsert the NSE/BSE cash-market universe")
    seed.add_argument("--master", default="OpenAPIScripMaster.json")

    for command in ("quotes", "yahoo-backfill"):
        refresh = subcommands.add_parser(command)
        refresh.add_argument("--limit", type=int, default=100)
        refresh.add_argument("--offset", type=int, default=0)
        refresh.add_argument("--concurrency", type=int)
        if command == "quotes":
            refresh.add_argument("--provider", choices=("nse", "bse"), default="nse")

    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.command == "seed":
        print(json.dumps(seed_company_universe(args.master), indent=2))
        return

    settings = get_settings()
    if args.command == "quotes":
        provider = args.provider
        result = asyncio.run(
            refresh_exchange_market_data_async(
                provider,
                limit=args.limit,
                offset=args.offset,
                concurrency=args.concurrency or (
                    settings.nse_concurrency if provider == "nse" else settings.bse_concurrency
                ),
                timeout_seconds=settings.exchange_timeout_seconds,
                retries=settings.exchange_retries,
                requests_per_second=(
                    settings.nse_requests_per_second
                    if provider == "nse"
                    else settings.bse_requests_per_second
                ),
                proxy_config_path=settings.screener_proxy_config,
            )
        )
    else:
        result = asyncio.run(
            refresh_yahoo_market_data_async(
                limit=args.limit,
                offset=args.offset,
                concurrency=args.concurrency or settings.yahoo_concurrency,
                timeout_seconds=settings.yahoo_timeout_seconds,
                retries=settings.yahoo_retries,
                requests_per_second=settings.yahoo_requests_per_second,
                full_history=True,
            )
        )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
