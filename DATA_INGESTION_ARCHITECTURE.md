# Market Data Ingestion Architecture

## Goal and compatibility boundary

The ingestion layer must refresh the full stock universe in under one hour while
keeping all existing API routes, response payloads, API-key handling, and
database columns unchanged. The design therefore changes only background task
coordination, provider access, retry state, and bulk persistence.

The system must also respect provider limits. Proxy rotation is supported only
for proxy endpoints owned or explicitly authorized by the operator. A provider
circuit breaker slows or pauses work after `429`, `403`, repeated `5xx`, or
transport failures instead of attempting to bypass a provider block.

## Runtime layout

```text
Celery Beat
   |
   +-- Yahoo quote coordinator (one logical run)
   |      |
   |      +-- 8 shard tasks x 250-500 stocks
   |             |
   |             +-- bounded async requests (default 12 per shard)
   |             +-- provider-specific proxy lease
   |             +-- one PostgreSQL upsert transaction per shard
   |             +-- failed symbols -> Redis retry set
   |
   +-- Yahoo retry coordinator
   |      +-- drains retry symbols into idempotent shard tasks
   |
   +-- Yahoo history backfill (separate low-priority/manual stream)
   |
   +-- NSE/BSE fundamentals (separate queues and conservative limits)
   |
   +-- Announcement project
          +-- independent NSE proxy pool and sticky HTTP session
          +-- independent BSE proxy pool and HTTP session
          +-- current database uniqueness constraints remain the final guard
```

The quote path is isolated from history and fundamentals so a slow full-history
or exchange call cannot delay current prices. The announcement application uses
its own proxy configuration file, Redis databases, workers, and provider pools;
it never shares an IP lease or cookies with the screener.

The stored instrument master is broader than the quote universe. Quote jobs use
only NSE equity series and BSE six-digit equity scrip codes in the 500000-599999
range. Debt, preference, and other exchange instruments remain stored but are
kept out of equity quote and retry queues.

## Throughput model

- Default: 8 Celery process workers.
- One coordinator reserves 8 non-overlapping shards of 250 stocks (2,000 stocks
  per wave) using an atomic Redis cursor.
- Each shard allows 12 in-flight Yahoo calls, for a maximum of 96 requests in
  flight per worker host. These values are configurable without code changes.
- A 13,679-stock universe is approximately seven waves. Even at several minutes
  per wave, retries included, this leaves substantial margin inside one hour.
- Database writes are PostgreSQL bulk upserts, never one commit per stock.

Increasing parallelism beyond the provider's healthy capacity can make the run
slower through throttling. The operational target is therefore sustained useful
throughput, not an unbounded connection count.

## Reliability and resume behavior

Every shard is idempotent. Successful data is committed immediately; failed
symbols are stored in a Redis set and retried independently, so a process crash
or one bad stock does not restart the whole universe. Celery uses late
acknowledgement, worker-loss rejection, one-message prefetch, and bounded task
time limits.

Redis keys provide control-plane state only:

- `market-data:yahoo:quote:offset` - next unreserved universe position.
- `market-data:yahoo:retry` - symbols awaiting retry.
- `market-data:yahoo:retry-attempts` - bounded retry counters.
- `market-data:yahoo:quarantine` - provider-unsupported symbols with expiry.
- `market-data:yahoo:last-dispatch` - coordinator metrics.

Permanent symbol errors (for example Yahoo `404`) enter a seven-day,
self-expiring quarantine instead of the transient retry loop. After expiry the
symbol is automatically re-probed, allowing newly supported listings to
recover without manual cleanup. Permanent errors do not reduce proxy health.
Transient `429`, `403`, `5xx`, timeout, and network errors reduce the selected
endpoint's score and place it in cooldown. Retry backoff includes jitter to
prevent all workers retrying at the same instant.

## Proxy isolation and health

Each project has an ignored local `config/proxies.json` and a committed example:

```json
{
  "yahoo": [
    {"url": "http://user:password@proxy-a.example:8080", "max_concurrency": 12}
  ],
  "nse": [],
  "bse": []
}
```

The screener reads `SCREENER_PROXY_CONFIG`; announcements read
`ANNOUNCEMENT_PROXY_CONFIG`. Each provider has a separate round-robin pool,
failure score, cooldown, and concurrency limit. Proxy credentials are redacted
from logs. An empty provider list means direct access, which keeps local setup
working before authorized proxy endpoints are supplied.

`aiolimiter` supplies a per-event-loop token bucket in addition to the
concurrency semaphore. This keeps throughput predictable when response latency
drops suddenly and prevents a healthy fast proxy from producing an accidental
request burst. Runtime proxy scoring and cooldown remain project-owned because
transport libraries do not understand NSE/BSE/Yahoo health semantics.

NSE's cookie-prime request and API request use the same sticky proxy and the
same `requests.Session`. BSE pages from one poll use the same endpoint. A failed
poll is retried with another healthy endpoint by the Celery task on the next
scheduled cycle; current task locks prevent duplicate inserts.

## Data streams

| Stream | Cadence | Shard size | Concurrency | Persistence |
|---|---:|---:|---:|---|
| Yahoo quotes | continuous market window | 250 x 8 | 12/shard | bulk upsert |
| Yahoo retry | every minute | up to 250 | 12 | bulk upsert |
| Yahoo history | manual/nightly | 100-250 | 4-8 | bulk insert/update |
| NSE/BSE fundamentals | separate queues | 200-500 | provider-limited | existing schema |
| NSE/BSE announcements | 10-second live dispatch | pages per poll | one sticky session/feed | bulk create |

## Operational guardrails

- Keep different worker queues for quotes, history, NSE, BSE, and announcements.
- Start at the defaults and inspect `429`/`403`, latency, failure ratio, retry
  depth, and database commit time before increasing concurrency.
- Never place proxy credentials in source control; use the ignored local file.
- Never scrape public/free proxy lists. Only configure endpoints the operator is
  allowed to use.
- Do not run legacy long-duration jobs on the fast quote queue.
- Database unique constraints and idempotent upserts remain mandatory because
  task delivery is at least once.

## Delivery phases

1. Add provider-aware proxy health modules and isolated configuration.
2. Replace the single Yahoo batch lock with atomic multi-shard dispatch.
3. Add retry/resume tasks and operational metrics.
4. Route announcement NSE/BSE sessions through their independent sticky pools.
5. Validate contracts, compile/tests, local services, and measured throughput.
