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
   +-- NSE quote coordinator -> 6 x 100-stock shards -> nse-quotes
   |
   +-- BSE quote coordinator -> 2 x 100-stock shards -> bse-quotes
   |
   +-- provider retry coordinators
   |      +-- drain idempotent Redis retry sets
   |
   +-- each shard
   |      +-- one shared async HTTP session
   |      +-- a proxy lease per request
   |      +-- bounded rate/concurrency and jittered retry
   |      +-- one PostgreSQL bulk upsert
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
- Coordinators reserve non-overlapping 100-stock shards with an atomic Redis
  cursor: six NSE shards and two BSE shards per minute.
- The direct NSE path is Redis-limited to eight requests across all processes;
  configured proxies add their own independently leased capacity.
- The live NSE validation processed a 100-stock shard in about 49 seconds and
  completed 3,514 valid quotes from a 3,540-row universe in a few minutes.
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

- `market-data:{nse|bse}:quote-offset` - next unreserved universe position.
- `market-data:{nse|bse}:retry` - symbols awaiting retry.
- `market-data:{nse|bse}:retry-attempts` - bounded retry counters.
- `market-data:{nse|bse}:quarantine` - unsupported symbols with expiry.
- `market-data:{nse|bse}:last-dispatch` - coordinator metrics.

Permanent symbol errors enter a one-day,
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
  "nse": {"endpoints": [{"url_env": "NSE_PROXY_1_URL", "max_concurrency": 4}]},
  "bse": {"endpoints": [{"url_env": "BSE_PROXY_1_URL", "max_concurrency": 4}]}
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

NSE does not need a slow homepage bootstrap: the current official quote endpoint
works directly with one reused `aiohttp` session. Every NSE/BSE request leases
capacity independently, so concurrent endpoint calls can rotate without
overloading one address. A failed request retries another healthy endpoint.

## Data streams

| Stream | Cadence | Shard size | Concurrency | Persistence |
|---|---:|---:|---:|---|
| NSE quotes | every minute in market window | 100 x 6 | 6/shard, global direct 8 | bulk upsert |
| BSE quotes | every minute in market window | 100 x 2 | 4/shard | bulk upsert |
| Exchange retry | every minute | up to 100/provider | provider-limited | bulk upsert |
| Yahoo history | manual compatibility only | 100 | manual | existing schema |
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
2. Use provider-specific atomic multi-shard dispatch.
3. Add retry/resume tasks and operational metrics.
4. Route announcement NSE/BSE sessions through their independent sticky pools.
5. Validate contracts, compile/tests, local services, and measured throughput.
