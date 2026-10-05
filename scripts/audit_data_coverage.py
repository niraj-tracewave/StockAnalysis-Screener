"""Generate a database coverage report without changing application data."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import distinct, func, select

from app.apis.models.stock_data import (
    BalanceSheetDataset,
    CashFlowDataset,
    ChartDataset,
    CompanyStock,
    CustomFormatQuarterlyResultDateset,
    KeyDetailsForCS,
    MarketDeal,
    ProfitLossDataset,
    QuarterlyResultDateset,
    RatiosDataset,
    ShareHoldingPeriod,
    StockDeliveryDataset,
    StockPeerDataset,
)
from app.db.postgres.sync_session import SessionLocalSync
from app.core.market_data_pipeline import market_target_predicate


COMPANY_FIELDS = [
    "name",
    "website",
    "bse_code",
    "nse_symbol",
    "nse_code",
    "yahoo_symbol",
    "primary_exchange",
    "macro_economic_sector",
    "sector",
    "industry",
    "stock_format",
    "basic_industry",
]

DETAIL_FIELDS = [
    "market_cap",
    "current_price",
    "high_price",
    "low_price",
    "pe_ratio",
    "book_value",
    "dividend_yield",
    "roce",
    "roe",
    "face_value",
    "data_source",
    "market_data_updated_at",
    "about",
    "key_points",
    "pros",
    "cons",
]

CHILD_TABLES = [
    ("chart_datasets", ChartDataset),
    ("stock_peer_datasets", StockPeerDataset),
    ("quarterly_result_dateset", QuarterlyResultDateset),
    ("custom_format_quarterly_result_dateset", CustomFormatQuarterlyResultDateset),
    ("profit_loss_dataset", ProfitLossDataset),
    ("balance_sheet_dataset", BalanceSheetDataset),
    ("cash_flow_dataset", CashFlowDataset),
    ("ratios_dataset", RatiosDataset),
    ("share_holding_period", ShareHoldingPeriod),
    ("stock_delivery_datasets", StockDeliveryDataset),
    ("market_deals", MarketDeal),
]

SOURCE_GUIDE = [
    ("website, sector, industry, basic industry", "NSE/BSE security and company metadata; BSE fallback for BSE-only securities"),
    ("market cap, PE, face value", "NSE/BSE quote metadata; calculate market cap only when shares outstanding is reliable"),
    ("book value, ROE, ROCE, dividend yield", "Exchange-filed financial statements/XBRL, then calculate deterministically"),
    ("quarterly results", "NSE/BSE corporate financial-results filings and XBRL"),
    ("profit/loss, balance sheet, cash flow, ratios", "NSE/BSE annual and quarterly XBRL; company filings as fallback"),
    ("shareholding", "NSE/BSE Regulation 31 shareholding-pattern filings/XBRL"),
    ("delivery data", "Official NSE/BSE bhavcopy and delivery reports"),
    ("bulk, block, short-selling deals", "Official NSE/BSE daily reports"),
    ("current/high/low", "Official NSE/BSE quote endpoints; official exchange EOD files for validation"),
    ("history", "Official NSE/BSE EOD historical files"),
]


def _percent(count: int, total: int) -> str:
    return f"{(count / total * 100):.1f}%" if total else "0.0%"


def _column_coverage(
    db,
    model,
    fields: list[str],
    denominator: int,
    *,
    eligible_only: bool = False,
) -> list[tuple[str, int, int, str]]:
    rows = []
    for field in fields:
        column = getattr(model, field)
        statement = select(func.count(column)).select_from(model)
        if eligible_only and model is CompanyStock:
            statement = statement.where(market_target_predicate())
        elif eligible_only:
            statement = statement.join(
                CompanyStock,
                model.company_id == CompanyStock.id,
            ).where(market_target_predicate())
        present = int(db.scalar(statement) or 0)
        rows.append((field, present, max(denominator - present, 0), _percent(present, denominator)))
    return rows


def build_report() -> str:
    db = SessionLocalSync()
    try:
        company_total = int(db.scalar(select(func.count(CompanyStock.id))) or 0)
        eligible_total = int(
            db.scalar(
                select(func.count(CompanyStock.id)).where(market_target_predicate())
            )
            or 0
        )
        detail_total = int(
            db.scalar(
                select(func.count(KeyDetailsForCS.id))
                .join(CompanyStock, KeyDetailsForCS.company_id == CompanyStock.id)
                .where(market_target_predicate())
            )
            or 0
        )
        company_fields = _column_coverage(
            db,
            CompanyStock,
            COMPANY_FIELDS,
            eligible_total,
            eligible_only=True,
        )
        detail_fields = _column_coverage(
            db,
            KeyDetailsForCS,
            DETAIL_FIELDS,
            eligible_total,
            eligible_only=True,
        )

        child_rows = []
        for label, model in CHILD_TABLES:
            row_count = int(
                db.scalar(
                    select(func.count(model.id))
                    .join(CompanyStock, model.company_id == CompanyStock.id)
                    .where(market_target_predicate())
                )
                or 0
            )
            company_count = int(
                db.scalar(
                    select(func.count(distinct(model.company_id)))
                    .join(CompanyStock, model.company_id == CompanyStock.id)
                    .where(market_target_predicate())
                )
                or 0
            )
            child_rows.append(
                (label, row_count, company_count, max(eligible_total - company_count, 0), _percent(company_count, eligible_total))
            )

        chart_rows = db.execute(
            select(
                func.coalesce(ChartDataset.meta["days"].astext, "unknown"),
                func.count(ChartDataset.id),
                func.count(distinct(ChartDataset.company_id)),
            )
            .join(CompanyStock, ChartDataset.company_id == CompanyStock.id)
            .where(market_target_predicate())
            .group_by(func.coalesce(ChartDataset.meta["days"].astext, "unknown"))
            .order_by(func.count(ChartDataset.id).desc())
        ).all()
    finally:
        db.close()

    lines = [
        "# Stock Data Coverage Report",
        "",
        f"Generated: {datetime.now().astimezone().isoformat(timespec='seconds')}",
        "",
        f"Stored security rows: **{company_total:,}**",
        f"Eligible NSE/BSE equity universe: **{eligible_total:,}**",
        f"Eligible companies with a key-details row: **{detail_total:,}** ({_percent(detail_total, eligible_total)})",
        "",
        "## Company fields",
        "",
        "| Field | Present | Missing | Coverage |",
        "|---|---:|---:|---:|",
    ]
    lines.extend(f"| {name} | {present:,} | {missing:,} | {coverage} |" for name, present, missing, coverage in company_fields)
    lines.extend([
        "",
        "## Key-detail fields",
        "",
        "Coverage uses the eligible 6,000+ equity universe as the denominator, including companies with no key-details row.",
        "",
        "| Field | Present | Missing | Coverage |",
        "|---|---:|---:|---:|",
    ])
    lines.extend(f"| {name} | {present:,} | {missing:,} | {coverage} |" for name, present, missing, coverage in detail_fields)
    lines.extend([
        "",
        "## Related-table coverage",
        "",
        "| Table | Rows | Companies present | Companies missing | Coverage |",
        "|---|---:|---:|---:|---:|",
    ])
    lines.extend(
        f"| {name} | {rows:,} | {companies:,} | {missing:,} | {coverage} |"
        for name, rows, companies, missing, coverage in child_rows
    )
    lines.extend([
        "",
        "## Chart periods",
        "",
        "| Period | Rows | Companies |",
        "|---|---:|---:|",
    ])
    lines.extend(f"| {period} | {rows:,} | {companies:,} |" for period, rows, companies in chart_rows)
    lines.extend([
        "",
        "## Recommended authoritative sources",
        "",
        "| Missing data group | Preferred source |",
        "|---|---|",
    ])
    lines.extend(f"| {fields} | {source} |" for fields, source in SOURCE_GUIDE)
    lines.extend([
        "",
        "## Priority rule",
        "",
        "Populate identifiers and current prices first, then official filings, then calculated ratios. Do not infer a value when the filing is absent. Keep failed symbols in the retry/dead-letter workflow for inspection.",
        "",
    ])
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = build_report()
    if args.output:
        args.output.write_text(report, encoding="utf-8")
        print(args.output)
    else:
        print(report)


if __name__ == "__main__":
    main()
