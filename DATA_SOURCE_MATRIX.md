# Data Source and Job Matrix

This plan fills existing tables only. It does not add fields or change API
responses. Coverage numbers come from `DATA_COVERAGE_REPORT.md` generated on
2026-10-02.

## Current gaps and acquisition plan

| Existing destination | Current coverage | Primary source | Fallback | Queue/cadence | Parallelism |
|---|---:|---|---|---|---:|
| `company_stock.website` | 0% | NSE/BSE company profile | Company filing | metadata/nightly | 4/provider |
| sector/industry fields | 0% | NSE/BSE security metadata | BSE profile for BSE-only | metadata/weekly | 4/provider |
| current/high/low price | live status command | Official NSE/BSE quote endpoints | Official exchange EOD files | quotes/every minute | 6 NSE + 2 BSE shards |
| market cap, PE, face value | 0% | NSE/BSE quote metadata | Filed results plus calculation | fundamentals/daily | 4/provider |
| book value, ROE, ROCE, dividend yield | 0% | Calculated from filed statements | Exchange summary when present | calculations/after filings | CPU workers |
| chart history | inspect with coverage report | Official NSE/BSE EOD historical data | none | history/nightly | file jobs |
| quarterly results | 0% | NSE/BSE financial-results XBRL | Company results filing | filings/hourly + backfill | 2-4/provider |
| P&L, balance sheet, cash flow, ratios | 0% | NSE/BSE XBRL | Annual report parsing | filings/nightly | 2-4/provider |
| shareholding | 0% | NSE/BSE Regulation 31 XBRL | Company filing | filings/daily | 2-4/provider |
| delivery data | 0% | Official NSE/BSE delivery/bhavcopy reports | none | exchange-files/EOD | one file job/exchange |
| market deals | 0% | Official bulk/block/short-selling reports | none | exchange-files/EOD | one file job/exchange |
| announcement industry | 10.5% | Resolve from stock metadata by symbol | NSE/BSE company profile | enrichment/after insert | database batch |

## Source priority

1. Exchange-published files and XBRL are authoritative for filings, ownership,
   delivery, and deals.
2. Exchange quote/company endpoints are preferred for security metadata.
3. Current prices come only from official exchange endpoints; missing values
   remain null until the exchange or an authorized proxy is available.
4. Ratios are calculated only when all required filing inputs are available.
5. Missing values remain null; the pipeline must not invent or guess them.

Official discovery pages:

- NSE financial results: <https://www.nseindia.com/companies-listing/corporate-filings-financial-results>
- NSE shareholding patterns: <https://www.nseindia.com/companies-listing/corporate-filings-shareholding-pattern>
- NSE XBRL formats: <https://www.nseindia.com/static/companies-listing/xbrl-information>
- NSE daily reports: <https://www.nseindia.com/all-reports>
- BSE financial results: <https://www.bseindia.com/corporates/comp_results.aspx>
- BSE shareholding patterns: <https://www.bseindia.com/corporates/shpdrPercnt.aspx>

## Runtime controls

- NSE: six 100-stock shards, six companies per shard, Redis-global direct limit
  eight, and four requests/second per process by default.
- BSE: two 100-stock shards, four companies per shard, three minimal endpoint
  calls per company, and provider-isolated proxy health.
- Transient failures: exponential backoff, jitter, proxy cooldown, and symbol
  retry queue.
- Permanent/mapping failures: bounded retries followed by Redis dead-letter set.
- Writes: idempotent PostgreSQL bulk upsert/insert, one transaction per shard.
- Each provider uses a different configured proxy pool. Empty pools mean direct
  access and are never silently replaced with public/free proxies.

## Operator commands

```bash
# Rebuild the stock coverage report
.venv/bin/python scripts/audit_data_coverage.py --output DATA_COVERAGE_REPORT.md

# Validate authorized screener proxies
.venv/bin/python scripts/check_proxy_health.py --config config/proxies.json

# Validate the announcement project's separate proxies
.venv/bin/python scripts/check_proxy_health.py \
  --config ../stock-announcement/config/proxies.json
```

Run the coverage audit after every complete backfill. The table with the lowest
company coverage determines the next backfill stream; quote refreshes remain on
their independent queue and must never wait for filing jobs.
