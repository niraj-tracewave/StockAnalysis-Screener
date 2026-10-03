# StockAnalysis Screener

The high-throughput ingestion design, worker sizing, retry/resume behavior, and
provider-isolated proxy configuration are documented in
[`DATA_INGESTION_ARCHITECTURE.md`](DATA_INGESTION_ARCHITECTURE.md).

Current database completeness and the provider plan are in
[`DATA_COVERAGE_REPORT.md`](DATA_COVERAGE_REPORT.md) and
[`DATA_SOURCE_MATRIX.md`](DATA_SOURCE_MATRIX.md).

Local FastAPI service and high-throughput NSE/BSE/Yahoo market-data pipeline.

## Services

- FastAPI: `http://localhost:8001`
- Health: `http://localhost:8001/health`
- PostgreSQL database: `stock_screener`
- Redis broker: database 2; Celery results: database 3

## Initial setup

```bash
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
.venv/bin/alembic upgrade head
.venv/bin/python scripts/market_data.py seed
```

The seed command is idempotent. It reads `OpenAPIScripMaster.json`, prefers NSE
for dual-listed securities, and adds BSE-only securities with Yahoo `.BO`
symbols.

## Fast market-data commands

Refresh a bounded quote batch:

```bash
.venv/bin/python scripts/market_data.py quotes --limit 500 --concurrency 12
```

Backfill full daily price/volume history separately:

```bash
.venv/bin/python scripts/market_data.py backfill --limit 100 --offset 0 --concurrency 8
```

Continuous quote updates rotate through 500 securities every two minutes from
09:00 through 16:59 IST on weekdays. Legacy scraper schedules are disabled by
default; set `ENABLE_LEGACY_MARKET_JOBS=true` only when those slower jobs are
intentionally required.

## Start manually

```bash
./scripts/start_api.sh
./scripts/start_market_worker.sh
./scripts/start_market_beat.sh
```

## Start in background from the Desktop project

```bash
bash scripts/start_all_services.sh
bash scripts/status_market_services.sh
```

This uses detached macOS `screen` sessions named `stockscreener-api`,
`stockscreener-market-worker`, and `stockscreener-market-beat`. Logs are kept
under `logs/services/`; watch them with `tail -f logs/services/*.log`.

Proxy credentials, preflight and shared-health commands are documented in
[`PROXY_OPERATIONS.md`](PROXY_OPERATIONS.md).

To reload code or `.env` cleanly:

```bash
bash scripts/stop_all_services.sh
bash scripts/start_all_services.sh
```

## Start automatically on macOS login

macOS background services cannot read projects under `Desktop` unless the
background shell has been granted Files & Folders or Full Disk Access. Move the
repository to an unprotected development directory (for example `~/Projects`)
or grant that access before installing these services.

```bash
./scripts/install_launchd_services.sh
./scripts/status_market_services.sh
```

To remove the login services:

```bash
./scripts/uninstall_launchd_services.sh
```
