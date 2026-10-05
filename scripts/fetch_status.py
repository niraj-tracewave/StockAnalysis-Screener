"""Print current market-data coverage and retry state as JSON."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from sqlalchemy import text

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db.postgres.sync_session import engine
from app.db.redis.redis import redis_client


def main() -> None:
    with engine.connect() as connection:
        totals = connection.execute(
            text(
                """
                SELECT count(*) AS universe,
                       count(*) FILTER (WHERE k.current_price IS NOT NULL) AS fetched,
                       count(*) FILTER (WHERE k.current_price IS NULL) AS missing
                FROM company_stock s
                LEFT JOIN key_details_for_cs k ON k.company_id = s.id
                """
            )
        ).mappings().one()
        exchanges = connection.execute(
            text(
                """
                SELECT coalesce(s.primary_exchange, 'UNKNOWN') AS exchange,
                       count(*) AS universe,
                       count(*) FILTER (WHERE k.current_price IS NOT NULL) AS fetched
                FROM company_stock s
                LEFT JOIN key_details_for_cs k ON k.company_id = s.id
                GROUP BY s.primary_exchange
                ORDER BY s.primary_exchange
                """
            )
        ).mappings().all()
        sources = connection.execute(
            text(
                """
                SELECT coalesce(k.data_source, 'missing') AS data_source,
                       count(*) AS companies
                FROM company_stock s
                LEFT JOIN key_details_for_cs k ON k.company_id = s.id
                GROUP BY k.data_source
                ORDER BY k.data_source NULLS FIRST
                """
            )
        ).mappings().all()

    universe = int(totals["universe"])
    fetched = int(totals["fetched"])
    payload = {
        "universe": universe,
        "fetched": fetched,
        "missing": int(totals["missing"]),
        "coverage_percent": round((fetched / universe * 100) if universe else 0, 2),
        "by_exchange": [dict(row) for row in exchanges],
        "by_source": [dict(row) for row in sources],
        "providers": {
            provider: {
                "retry_pending": redis_client.scard(f"market-data:{provider}:retry"),
                "quarantined": redis_client.zcard(f"market-data:{provider}:quarantine"),
                "dead_letter": redis_client.scard(f"market-data:{provider}:dead-letter"),
                "next_offset": int(redis_client.get(f"market-data:{provider}:quote-offset") or 0),
            }
            for provider in ("nse", "bse")
        },
        "worker_log": "logs/services/market-worker.log",
    }
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
