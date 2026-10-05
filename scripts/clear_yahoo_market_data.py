"""Remove Yahoo-populated market fields without deleting financial details."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from sqlalchemy import text

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db.postgres.sync_session import engine
from app.db.redis.redis import redis_client


YAHOO_REDIS_KEYS = (
    "market-data:yahoo:quote:offset",
    "market-data:yahoo:retry",
    "market-data:yahoo:retry-attempts",
    "market-data:yahoo:dead-letter",
    "market-data:yahoo:quarantine",
    "market-data:yahoo:last-dispatch",
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="apply instead of preview")
    args = parser.parse_args()

    with engine.begin() as connection:
        affected = connection.execute(
            text("SELECT count(*) FROM key_details_for_cs WHERE data_source = 'yahoo_chart'")
        ).scalar_one()
        charts = connection.execute(
            text(
                """
                SELECT count(*) FROM chart_datasets
                WHERE meta->>'source' = 'yahoo_chart'
                   OR label = 'Price on Yahoo Finance'
                """
            )
        ).scalar_one()
        if args.apply:
            connection.execute(
                text(
                    """
                    UPDATE key_details_for_cs
                    SET current_price = NULL,
                        high_price = NULL,
                        low_price = NULL,
                        data_source = NULL,
                        market_data_updated_at = NULL
                    WHERE data_source = 'yahoo_chart'
                    """
                )
            )
            connection.execute(
                text(
                    """
                    DELETE FROM chart_datasets
                    WHERE meta->>'source' = 'yahoo_chart'
                       OR label = 'Price on Yahoo Finance'
                    """
                )
            )

    if args.apply:
        redis_client.delete(*YAHOO_REDIS_KEYS)
    mode = "cleared" if args.apply else "would_clear"
    print({"status": mode, "market_rows": int(affected), "chart_rows": int(charts)})


if __name__ == "__main__":
    main()
