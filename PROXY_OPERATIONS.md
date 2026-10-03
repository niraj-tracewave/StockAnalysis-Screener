# Yahoo Proxy Operations

The Yahoo market-data pipeline supports eight environment-backed proxy slots.
Selection is latency/failure weighted. Health, cooldowns and active concurrency
leases are shared through Redis, so all eight Celery processes respect one
global limit per endpoint.

## Configure endpoints

Put provider credentials only in the ignored `.env` file. Do not put URLs with
credentials in `config/proxies.json`:

```env
YAHOO_PROXY_1_URL=http://username:password@host:port
YAHOO_PROXY_2_URL=http://username:password@host:port
YAHOO_PROXY_3_URL=
YAHOO_PROXY_4_URL=
YAHOO_PROXY_5_URL=
YAHOO_PROXY_6_URL=
YAHOO_PROXY_7_URL=
YAHOO_PROXY_8_URL=

PROXY_FAILURE_THRESHOLD=3
PROXY_COOLDOWN_SECONDS=60
PROXY_LEASE_TTL_SECONDS=45
PROXY_ACQUIRE_TIMEOUT_SECONDS=60
YAHOO_DIRECT_MAX_CONCURRENCY=4
```

Per-endpoint concurrency is configured in `config/proxies.json`. Start at four
to twelve concurrent requests per IP only when the proxy/data-provider terms
permit it; tune from measured latency and 429/timeout rates. Overall async HTTP
concurrency and rate are separately bounded by the market pipeline settings.

## Preflight and monitor

```bash
cd "/Users/milan/Desktop/TraceWave Latest Project/StockAnalysis-Screener"
.venv/bin/python scripts/check_proxy_health.py
.venv/bin/python scripts/proxy_status.py
```

Output redacts credentials. A failed endpoint is retried on a different healthy
endpoint, enters a shared exponential cooldown, and returns automatically after
cooldown. The current configuration permits direct fallback when no endpoint is
configured or all endpoint capacity is temporarily unavailable. Direct traffic
also uses a Redis global lease, so all workers together cannot burst from the
same Mac IP beyond `YAHOO_DIRECT_MAX_CONCURRENCY`.

After changing `.env`, reload the worker and scheduler (or all services):

```bash
bash scripts/stop_all_services.sh
bash scripts/start_all_services.sh
bash scripts/status_market_services.sh
tail -f logs/services/market-worker.log
```

Do not use scraped/free proxy lists; use a contracted static/sticky pool or
rotating gateway with documented limits.
