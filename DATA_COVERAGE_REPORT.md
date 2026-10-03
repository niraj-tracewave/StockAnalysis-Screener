# Stock Data Coverage Report

Generated: 2026-10-02T10:50:18+05:30

Stored security rows: **13,679**
Eligible NSE/BSE equity universe: **6,224**
Eligible companies with a key-details row: **5,598** (89.9%)

## Company fields

| Field | Present | Missing | Coverage |
|---|---:|---:|---:|
| name | 6,224 | 0 | 100.0% |
| website | 0 | 6,224 | 0.0% |
| bse_code | 5,422 | 802 | 87.1% |
| nse_symbol | 6,224 | 0 | 100.0% |
| nse_code | 3,540 | 2,684 | 56.9% |
| yahoo_symbol | 6,224 | 0 | 100.0% |
| primary_exchange | 6,224 | 0 | 100.0% |
| macro_economic_sector | 0 | 6,224 | 0.0% |
| sector | 0 | 6,224 | 0.0% |
| industry | 0 | 6,224 | 0.0% |
| stock_format | 6,224 | 0 | 100.0% |
| basic_industry | 0 | 6,224 | 0.0% |

## Key-detail fields

Coverage uses the eligible 6,000+ equity universe as the denominator, including companies with no key-details row.

| Field | Present | Missing | Coverage |
|---|---:|---:|---:|
| market_cap | 0 | 6,224 | 0.0% |
| current_price | 5,598 | 626 | 89.9% |
| high_price | 5,598 | 626 | 89.9% |
| low_price | 5,598 | 626 | 89.9% |
| pe_ratio | 0 | 6,224 | 0.0% |
| book_value | 0 | 6,224 | 0.0% |
| dividend_yield | 0 | 6,224 | 0.0% |
| roce | 0 | 6,224 | 0.0% |
| roe | 0 | 6,224 | 0.0% |
| face_value | 0 | 6,224 | 0.0% |
| data_source | 5,598 | 626 | 89.9% |
| market_data_updated_at | 5,598 | 626 | 89.9% |
| about | 0 | 6,224 | 0.0% |
| key_points | 0 | 6,224 | 0.0% |
| pros | 0 | 6,224 | 0.0% |
| cons | 0 | 6,224 | 0.0% |

## Related-table coverage

| Table | Rows | Companies present | Companies missing | Coverage |
|---|---:|---:|---:|---:|
| chart_datasets | 5 | 5 | 6,219 | 0.1% |
| stock_peer_datasets | 0 | 0 | 6,224 | 0.0% |
| quarterly_result_dateset | 0 | 0 | 6,224 | 0.0% |
| custom_format_quarterly_result_dateset | 0 | 0 | 6,224 | 0.0% |
| profit_loss_dataset | 0 | 0 | 6,224 | 0.0% |
| balance_sheet_dataset | 0 | 0 | 6,224 | 0.0% |
| cash_flow_dataset | 0 | 0 | 6,224 | 0.0% |
| ratios_dataset | 0 | 0 | 6,224 | 0.0% |
| share_holding_period | 0 | 0 | 6,224 | 0.0% |
| stock_delivery_datasets | 0 | 0 | 6,224 | 0.0% |
| market_deals | 0 | 0 | 6,224 | 0.0% |

## Chart periods

| Period | Rows | Companies |
|---|---:|---:|
| 30Y | 5 | 5 |

## Recommended authoritative sources

| Missing data group | Preferred source |
|---|---|
| website, sector, industry, basic industry | NSE/BSE security and company metadata; BSE fallback for BSE-only securities |
| market cap, PE, face value | NSE/BSE quote metadata; calculate market cap only when shares outstanding is reliable |
| book value, ROE, ROCE, dividend yield | Exchange-filed financial statements/XBRL, then calculate deterministically |
| quarterly results | NSE/BSE corporate financial-results filings and XBRL |
| profit/loss, balance sheet, cash flow, ratios | NSE/BSE annual and quarterly XBRL; company filings as fallback |
| shareholding | NSE/BSE Regulation 31 shareholding-pattern filings/XBRL |
| delivery data | Official NSE/BSE bhavcopy and delivery reports |
| bulk, block, short-selling deals | Official NSE/BSE daily reports |
| current/high/low/history | Yahoo chart for broad coverage; exchange quote/bhavcopy for validation |

## Priority rule

Populate identifiers and current prices first, then official filings, then calculated ratios. Do not infer a value when the filing is absent. Keep failed symbols in the retry/dead-letter workflow for inspection.
