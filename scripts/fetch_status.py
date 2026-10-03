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
                       count(k.id) AS fetched,
                       count(*) FILTER (WHERE k.id IS NULL) AS missing
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
                       count(k.id) AS fetched
                FROM company_stock s
                LEFT JOIN key_details_for_cs k ON k.company_id = s.id
                GROUP BY s.primary_exchange
                ORDER BY s.primary_exchange
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
        "retry_pending": redis_client.scard("market-data:yahoo:retry"),
        "quarantined_provider_unsupported": redis_client.zcard(
            "market-data:yahoo:quarantine"
        ),
        "dead_letter": redis_client.scard("market-data:yahoo:dead-letter"),
        "worker_log": "logs/services/market-worker.log",
    }
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
