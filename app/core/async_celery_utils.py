import asyncio
import csv
import json
import logging
import os
from datetime import datetime, date as bdate, timedelta
from decimal import Decimal, ROUND_HALF_UP
from itertools import islice
from zoneinfo import ZoneInfo
import yfinance as yf
import pytz
from bs4 import BeautifulSoup
from celery import group, shared_task
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import BDay
from sqlalchemy import select, or_, delete, and_, exists, desc, func, distinct, insert
from sqlalchemy.orm import selectinload

from app.apis.models.company import Company
from app.apis.models.stock_data import CompanyStock, KeyDetailsForCS, ChartDataset, QuarterlyResultDateset, \
    ResultFormatEnum, ShareHoldingPeriod, BalanceSheetDataset, ProfitLossDataset, CashFlowDataset, \
    CustomFormatQuarterlyResultDateset, StockDeliveryDataset, MarketDeal
from app.core.logging_config import setup_logging
from app.core.nse_search import fetch_nse_exact_symbol_data, fetch_bse_exact_symbol_data, fetch_nse_data, \
    fetch_bse_data, fetch_bse_exact_symbol_data_from_json
from app.core.utils import filter_exchange_data_from_file, fetch_symbols_from_covered_symbol_json, \
    load_processed_symbols, save_processed_symbol, fetch_integrated_filing_financials_data_from_nse, \
    convert_to_quarterly_format, fetch_symbols_from_covered_symbol_json_for_quarterly_result, \
    save_quarterly_result_processed_symbol, parse_financial_name, fetch_bse_integrated_filing_financials_data_from, \
    bse_convert_to_quarterly_format, update_nse_bse_scrip_code_load_processed_symbols, \
    update_nse_bse_scrip_code_save_processed_symbol, update_nse_bse_price_data_load_processed_symbols, \
    update_nse_bse_price_data_save_processed_symbol, decide_quarterly_format, bse_decide_quarterly_format, \
    update_nse_bse_newly_listed_stock_save_processed_symbol, fetch_newly_listed_stock_symbols_from_covered_symbol_json, \
    save_multiple_shareholding, save_multiple_dii_shareholding, save_multiple_fii_shareholding, \
    save_multiple_government_shareholding, save_multiple_public_shareholding, \
    fetch_symbols_from_covered_symbol_json_for_shareholder_result, update_nse_bse_shareholder_save_processed_symbol, \
    save_bse_multiple_shareholding, save_bse_multiple_dii_shareholding, save_bse_multiple_fii_shareholding, \
    save_bse_multiple_government_shareholding, save_bse_multiple_public_shareholding, to_year_month, \
    parse_balance_sheet, make_short_company_name, find_by_scripcode, parse_profit_loss, parse_cash_flow, \
    fetch_symbols_from_covered_symbol_json_for_balance_sheet_and_profit_loss_and_cash_flow, \
    update_nse_bse_balance_sheet_and_profit_loss_and_cash_flow_save_processed_symbol, fetch_dividend_values, \
    fetch_integrated_filing_financials_data_from_nse_for_book_value, \
    fetch_integrated_filing_financials_data_for_roce_from_nse, convert_existing_nse_to_screener, \
    fetch_integrated_filing_financials_data_type_from_nse, \
    fetch_integrated_filing_financials_data_from_bse_for_book_value, \
    fetch_integrated_filing_financials_data_for_roce_from_bse, \
    update_nse_bse_gross_deliverable_data_save_processed_symbol, \
    update_nse_bse_gross_deliverable_data_load_processed_symbols, \
    sync_update_nse_bse_gross_deliverable_data_save_processed_symbol, \
    update_nse_bse_gross_deliverable_count_load_processed_symbols, \
    update_nse_bse_gross_deliverable_list_load_processed_symbols
from app.db.postgres.sync_session import SessionLocalSync
from scripts.bse_fetch_shareholder_data import main_bse_fetch_shareholding_list, parse_bse_public_shareholder_table, \
    parse_bse_promoter_table, new_parse_bse_promoter_table, new_parse_bse_public_shareholder_table, \
    main_bse_cshp_fetch_shareholding_list
from scripts.bse_gross_deliverables import main_bse_fetch_gross_delivery_history, fetch_bse_security_position
from scripts.bse_stock_price_graph import new_main_fetch_stock_price_for_bse_graph
from scripts.fetch_balance_sheet_data import main_balance_sheet_html, main_find_company_json, \
    main_balance_sheet_standalone_html
from scripts.fetch_bse_integrated_filling_financials import main_bse_fetch_integrated_filing_financials
from scripts.fetch_daily_listed_stocks import main_newly_listed_stocks
from scripts.fetch_dividend_data_from_nse_bse import main_nse_corporate_action_call, main_bse_corporate_action_call
from scripts.fetch_integrated_filling_financials import main_fetch_integrated_filing_financials
from scripts.fetch_nse_block_deal import main_block_deals
from scripts.fetch_stock_volume_from_nse import main_fetch_volume_from_nse
from scripts.nse_fetch_shareholder_data import main_nse_fetch_shareholding_list, \
    main_nse_fetch_shareholding_data_using_api, main_nse_fetch_shareholding_data_using_api_for_book_value
from scripts.nse_gross_deliverables import main_nse_fetch_security_wise_historical_data, \
    fetch_sec_bhavdata_full_data
from scripts.nse_metadata_and_symboldata import fetch_nse_metadata, fetch_nse_symbol_data
from scripts.nse_newly_listed_stocks import main_nse_newly_listed_stocks
from scripts.nse_stock_price_graph import new_main_fetch_stock_price_for_graph
from scripts.nse_with_rotating_ip import main
from scripts.bse import main as main_bse

ist = pytz.timezone('Asia/Kolkata')

setup_logging()
special_logger = logging.getLogger("missing_symbols_logger")

async def fetch_and_store_company_data_from_top_50_async():
    """
    Background task to store NSE company data
    """
    db = SessionLocalSync()
    error_symbols = []
    try:
        file_path = 'OpenAPIScripMaster.json'
        all_filtered_data = filter_exchange_data_from_file(file_path)
        processed_symbols = []
        skipped_symbols = []
        def chunked(iterable, size):
            it = iter(iterable)
            while chunk := list(islice(it, size)):
                yield chunk

        existing_symbols = set()

        for chunk in chunked(all_filtered_data, 1000):
            stmt = select(CompanyStock.nse_symbol).where(
                CompanyStock.nse_symbol.in_(chunk)
            )
            result = db.execute(stmt)
            existing_symbols.update(row[0] for row in result)

        skipped_symbols_from_json = await fetch_symbols_from_covered_symbol_json()
        existing_symbols.update(skipped_symbols_from_json)
        missing_symbols = [s for s in all_filtered_data if s not in existing_symbols]

        special_logger.info(missing_symbols)
        special_logger.info("--------------------------------------------------------------------------------------")

        def chunk_list(data, size):
            for i in range(0, len(data), size):
                yield data[i:i + size]

        for chunk in chunk_list(missing_symbols[:100], 50):

            for symbol in chunk:
                try:
                    with db.begin_nested():
                        stmt = (
                            select(CompanyStock)
                            .where(
                                or_(
                                    CompanyStock.nse_symbol == symbol,
                                    CompanyStock.bse_code == symbol
                                )
                            )
                        )

                        result = db.execute(stmt)
                        company = result.scalars().first()
                        await asyncio.sleep(5)
                        if not company:
                            roe = current_price = high_price = isSuspended = low_price = pe_ratio = bse_code = nse_symbol = company_name = market_cap_cr = face_value = macro = sector = industry_info = basic_industry = None
                            nse_company_list = await fetch_nse_exact_symbol_data(symbol)
                            bse_company_list = await fetch_bse_exact_symbol_data(symbol)
                            print(bse_company_list, nse_company_list)
                            if nse_company_list and bse_company_list:
                                security_code = bse_company_list[0].get("bse_code")
                                nse_data = await main(symbol)
                                bse_data = await main_bse(security_code)
                                nse_symbol = nse_data.get('symbol')
                                company_name = nse_data.get('companyName')
                                header_data = bse_data.get('header')
                                symbol_data = nse_data.get('symbolData')
                                equity_response = symbol_data.get('equityResponse')[0]
                                nse_metadata = equity_response.get('metaData')
                                trade_info = equity_response.get('tradeInfo')
                                sec_info = equity_response.get('secInfo')
                                total_market_cap = trade_info.get('totalMarketCap')
                                if total_market_cap:
                                    market_cap_cr = round(total_market_cap / 1e7, 2)
                                else:
                                    market_cap_cr = None
                                current_price = trade_info.get('lastPrice')
                                face_value = trade_info.get('faceValue')
                                high_price = nse_metadata.get('dayHigh')
                                low_price = nse_metadata.get('dayLow')
                                pe_ratio = sec_info.get('pdSymbolPe')
                                roe = header_data.get('ROE')
                                macro = sec_info.get("macro")
                                sector = sec_info.get("sector")
                                industry_info = sec_info.get("industryInfo")
                                basic_industry = sec_info.get("basicIndustry")
                                isSuspended = sec_info.get("isSuspended")
                                bse_code = header_data.get("SecurityCode")
                                nse_code = nse_company_list[0].get("nse_code")
                                series = nse_metadata.get("series")
                                symbol_type = sec_info.get("classShare")
                                identifier = nse_metadata.get("identifier")
                            elif nse_company_list:
                                nse_data = await main(symbol)
                                nse_symbol = nse_data.get('symbol')
                                company_name = nse_data.get('companyName')
                                symbol_data = nse_data.get('symbolData')
                                equity_response = symbol_data.get('equityResponse')[0]
                                nse_metadata = equity_response.get('metaData')
                                trade_info = equity_response.get('tradeInfo')
                                sec_info = equity_response.get('secInfo')
                                total_market_cap = trade_info.get('totalMarketCap')
                                if total_market_cap:
                                    market_cap_cr = round(total_market_cap / 1e7, 2)
                                else:
                                    market_cap_cr = None
                                current_price = trade_info.get('lastPrice')
                                face_value = trade_info.get('faceValue')
                                high_price = nse_metadata.get('dayHigh')
                                low_price = nse_metadata.get('dayLow')
                                pe_ratio = sec_info.get('pdSymbolPe')
                                roe = None
                                bse_code = None
                                macro = sec_info.get("macro")
                                sector = sec_info.get("sector")
                                industry_info = sec_info.get("industryInfo")
                                basic_industry = sec_info.get("basicIndustry")
                                nse_code = nse_company_list[0].get("nse_code")
                                isSuspended = sec_info.get("isSuspended")
                                series = nse_metadata.get("series")
                                symbol_type = sec_info.get("classShare")
                                identifier = nse_metadata.get("identifier")
                            elif bse_company_list:
                                security_code = bse_company_list[0].get("bse_code")
                                bse_data = await main_bse(security_code)
                                header_data = bse_data.get('header')
                                script_header = bse_data.get('scriptHeader')
                                company_detail = script_header.get('Cmpname')
                                header = script_header.get('Header')
                                price_graph = bse_data.get('priceGraph')
                                stock_trading = bse_data.get('stockTrading')
                                company_name = company_detail.get('FullN')
                                total_market_cap = stock_trading.get('MktCapFull', None)
                                if total_market_cap:
                                    market_cap_cr = float(total_market_cap)
                                current_price = price_graph.get('CurrVal')
                                if current_price:
                                    current_price = float(current_price)
                                face_value = header_data.get('FaceVal')
                                if face_value:
                                    face_value = float(face_value)
                                high_price = header.get('High')
                                low_price = header.get('Low')
                                pe_ratio = header_data.get('PE')
                                roe = header_data.get('ROE')
                                bse_code = header_data.get("SecurityCode")
                                macro = header_data.get("Sector")
                                sector = header_data.get("IndustryNew")
                                industry_info = header_data.get("IGroup")
                                basic_industry = header_data.get("Industry")
                                nse_code = None
                            else:
                                skipped_symbols.append(symbol)
                                continue
                            if isSuspended == "Suspended":
                                skipped_symbols.append(symbol)
                                continue
                            company_stock = CompanyStock(
                                nse_symbol=symbol,
                                name=company_name,
                                nse_code=nse_code,
                                bse_code=bse_code,
                                macro_economic_sector=macro,
                                sector=sector,
                                industry=industry_info,
                                basic_industry=basic_industry
                            )
                            db.add(company_stock)
                            db.flush()

                            key_details = KeyDetailsForCS(
                                market_cap=market_cap_cr,
                                current_price=current_price,
                                pe_ratio=float(pe_ratio) if pe_ratio and pe_ratio != '-' else None,
                                face_value=face_value,
                                high_price=float(high_price),
                                low_price=float(low_price),
                                book_value=None,
                                dividend_yield=None,
                                roce=None,
                                roe=float(roe) if roe and roe != '-' else None,
                                company_id=company_stock.id
                            )
                            db.add(key_details)
                            db.flush()

                            fields = ["id"]

                            attrs = [getattr(CompanyStock, f) for f in fields]
                            # days_list = ["1W", "1D", "1M", "1Y", "5Y", "10Y", "15Y", "20Y", "25Y", "30Y"]
                            days_list = ["30Y"]
                            # days_list = ["1W", "1D", "1M"]
                            if nse_company_list and bse_company_list:
                                volume_data = await main_fetch_volume_from_nse(nse_code,
                                                                               f"{symbol}-{series}", symbol_type)
                                for days in days_list:
                                    company_name_with_dash = company_name.replace(" ", "-")
                                    nse_data = await new_main_fetch_stock_price_for_graph(days, identifier, symbol, company_name_with_dash)
                                    chart = nse_data.get('grapthData')
                                    if chart:
                                        volume_map = {item["time"]: item["volume"] for item in
                                                      volume_data.get("data", None)}
                                        updated_data = []
                                        for row in chart:
                                            time = row[0]
                                            volume = volume_map.get(time, None)
                                            updated_row = row + [volume]
                                            updated_data.append(updated_row)
                                        company_stock_chart_dataset_ops = ChartDataset(
                                                    metric="Price",
                                                    label="Price on NSE",
                                                    meta={"days": days},
                                                    company_id=company_stock.id,
                                                    values=updated_data,
                                                )
                                        db.add(company_stock_chart_dataset_ops)
                                        db.flush()
                            elif nse_company_list:
                                volume_data = await main_fetch_volume_from_nse(nse_code,
                                                                               f"{symbol}-{series}",
                                                                               symbol_type)
                                for days in days_list:
                                    company_name_with_dash = company_name.replace(" ", "-")
                                    nse_data = await new_main_fetch_stock_price_for_graph(days, identifier, symbol, company_name_with_dash)
                                    chart = nse_data.get('grapthData')
                                    if chart:
                                        volume_map = {item["time"]: item["volume"] for item in
                                                      volume_data.get("data", None)}
                                        updated_data = []
                                        for row in chart:
                                            time = row[0]
                                            volume = volume_map.get(time, None)
                                            updated_row = row + [volume]
                                            updated_data.append(updated_row)
                                        company_stock_chart_dataset_ops = ChartDataset(
                                            metric="Price",
                                            label="Price on NSE",
                                            meta={"days": days},
                                            company_id=company_stock.id,
                                            values=updated_data,
                                        )
                                        db.add(company_stock_chart_dataset_ops)
                                        db.flush()
                            elif bse_company_list:
                                # days_list = ["1M", "1Y", "5Y", "10Y"]
                                security_code = bse_company_list[0].get("bse_code")
                                days_list = ["30Y"]
                                for days in days_list:
                                    bse_data = await new_main_fetch_stock_price_for_bse_graph(security_code)
                                    script_header = bse_data.get('Data')
                                    if script_header:
                                        data_list = json.loads(script_header)
                                        result = []

                                        for item in data_list:
                                            ts_ms = int(
                                                datetime.strptime(item["dttm"],
                                                                  "%a %b %d %Y %H:%M:%S").timestamp() * 1000
                                            )
                                            price = float(item["vale1"])
                                            volume = int(item["vole"])
                                            result.append([ts_ms, price, "", None, None, volume])

                                        company_stock_chart_dataset_ops = ChartDataset(
                                            metric="Price",
                                            label="Price on BSE",
                                            meta={"days": days},
                                            company_id=company_stock.id,
                                            values=result,
                                        )
                                        db.add(company_stock_chart_dataset_ops)
                                        db.flush()

                            processed_symbols.append(symbol)

                except Exception as symbol_error:
                    # db.rollback()
                    error_symbols.append(symbol)
                    print(f"Error for symbol {symbol}: {symbol_error}")
                    continue

            db.commit()
            file_path = "covered_symbols.json"

            data = {
                "processed": [],
                "skipped": []
            }

            if os.path.exists(file_path):
                with open(file_path, "r") as f:
                    try:
                        data = json.load(f)
                    except:
                        pass

            data["processed"] = list(set(data.get("processed", []) + processed_symbols))
            data["skipped"] = list(set(data.get("skipped", []) + skipped_symbols))

            with open(file_path, "w") as f:
                json.dump(data, f, indent=2)

            print("Symbols appended to covered_symbols.json")
    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()
        file_path = "covered_symbols.json"

        data = {
            "processed": [],
            "skipped": [],
            "error": []
        }

        if os.path.exists(file_path):
            with open(file_path, "r") as f:
                try:
                    data = json.load(f)
                except:
                    pass

        data["processed"] = list(set(data.get("processed", [])))
        data["skipped"] = list(set(data.get("skipped", [])))
        data["error"] = list(set(data.get("error", [])+ error_symbols))

        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)

async def fetch_30y_stock_chart_data_async():
    db = SessionLocalSync()

    stmt = (
        select(CompanyStock)
        .options(selectinload(CompanyStock.details))
        .execution_options(yield_per=100)
    )

    result = db.execute(stmt)
    companies = result.scalars().all()

    for company in companies:
        try:
            print(f"\nProcessing company: {company.id} | {company.name}")

            chart_stmt = select(ChartDataset).where(
                ChartDataset.company_id == company.id,
                ChartDataset.meta["days"].astext.in_(["1W", "1M", "6M", "1Y", "5Y", "10Y", "15Y", "20Y", "25Y"])
            )
            result = db.execute(chart_stmt)
            charts = result.scalars().all()

            stmt = select(ChartDataset.id).where(
                ChartDataset.company_id == company.id,
                ChartDataset.meta["days"].astext.in_(["30Y"])
            )

            exists_30y = db.execute(stmt).first() is not None

            if charts:
                db.execute(
                    delete(ChartDataset).where(
                        ChartDataset.company_id == company.id
                    )
                )
                db.commit()

            if charts or not exists_30y:
                series = symbol_type = identifier = None

                try:
                    if company.nse_code:
                        nse_data = await main(company.nse_symbol)
                        symbol_data = nse_data.get('symbolData', {})
                        equity_response = symbol_data.get('equityResponse', [{}])[0]
                        nse_metadata = equity_response.get('metaData', {})
                        sec_info = equity_response.get('secInfo', {})

                        series = nse_metadata.get("series")
                        symbol_type = sec_info.get("classShare")
                        identifier = nse_metadata.get("identifier")

                except Exception as e:
                    print(f"NSE metadata failed for {company.nse_symbol}: {e}")
                    raise

                days_list = ["30Y"]

                if company.nse_code and company.bse_code:
                    try:
                        volume_data = await main_fetch_volume_from_nse(
                            company.nse_code,
                            f"{company.nse_symbol}-{series}",
                            symbol_type
                        )

                        for days in days_list:
                            company_name_with_dash = company.name.replace(" ", "-")

                            nse_data = await new_main_fetch_stock_price_for_graph(
                                days,
                                identifier,
                                company.nse_symbol,
                                company_name_with_dash
                            )

                            chart = nse_data.get('grapthData')

                            if chart:
                                volume_map = {
                                    item["time"]: item["volume"]
                                    for item in volume_data.get("data", [])
                                }

                                updated_data = []
                                for row in chart:
                                    time = row[0]
                                    volume = volume_map.get(time)
                                    updated_data.append(row + [volume])

                                dataset = ChartDataset(
                                    metric="Price",
                                    label="Price on NSE",
                                    meta={"days": days},
                                    company_id=company.id,
                                    values=updated_data,
                                )
                                db.add(dataset)

                    except Exception as e:
                        print(f"NSE chart fetch failed for {company.nse_symbol}: {e}")
                        raise

                elif company.nse_code:
                    try:
                        volume_data = await main_fetch_volume_from_nse(
                            company.nse_code,
                            f"{company.nse_symbol}-{series}",
                            symbol_type
                        )

                        for days in days_list:
                            company_name_with_dash = company.name.replace(" ", "-")

                            nse_data = await new_main_fetch_stock_price_for_graph(
                                days,
                                identifier,
                                company.nse_symbol,
                                company_name_with_dash
                            )

                            chart = nse_data.get('grapthData')

                            if chart:
                                volume_map = {
                                    item["time"]: item["volume"]
                                    for item in volume_data.get("data", [])
                                }

                                updated_data = []
                                for row in chart:
                                    time = row[0]
                                    volume = volume_map.get(time)
                                    updated_data.append(row + [volume])

                                dataset = ChartDataset(
                                    metric="Price",
                                    label="Price on NSE",
                                    meta={"days": days},
                                    company_id=company.id,
                                    values=updated_data,
                                )
                                db.add(dataset)

                    except Exception as e:
                        print(f"NSE chart fetch failed for {company.nse_symbol}: {e}")
                        raise

                elif company.bse_code:
                    try:
                        for days in days_list:
                            bse_data = await new_main_fetch_stock_price_for_bse_graph(
                                company.bse_code
                            )

                            script_header = bse_data.get('Data')
                            if script_header:
                                data_list = json.loads(script_header)
                                result_list = []

                                for item in data_list:
                                    ts_ms = int(
                                        datetime.strptime(
                                            item["dttm"],
                                            "%a %b %d %Y %H:%M:%S"
                                        ).timestamp() * 1000
                                    )
                                    price = float(item["vale1"])
                                    volume = int(item["vole"])

                                    result_list.append(
                                        [ts_ms, price, "", None, None, volume]
                                    )

                                dataset = ChartDataset(
                                    metric="Price",
                                    label="Price on BSE",
                                    meta={"days": days},
                                    company_id=company.id,
                                    values=result_list,
                                )
                                db.add(dataset)

                    except Exception as e:
                        print(f"BSE chart fetch failed for {company.bse_code}: {e}")
                        raise

                db.commit()

        except Exception as e:
            db.rollback()
            print(f"\nFAILED company: {company.id} | {company.name}")
            print("Error:", str(e))
            continue

    db.close()


# fetch stock chart data
async def fetch_and_update_30y_stock_chart_data_async():
    db = SessionLocalSync()

    stmt = (
        select(CompanyStock)
        .options(selectinload(CompanyStock.details))
        .execution_options(yield_per=100)
    )

    result = db.execute(stmt)
    companies = result.scalars().all()
    processed_symbols = await load_processed_symbols()
    new_process_symbol = []
    unprocessed_companies = [
        c for c in companies if c.nse_symbol not in processed_symbols
    ][:50]
    started_symbols = [c.nse_symbol for c in unprocessed_companies]
    await save_processed_symbol(started_symbols, "current_processed_symbols")
    for company in unprocessed_companies:
        try:
            print(f"\nProcessing company: {company.id} | {company.name}")

            stmt = select(ChartDataset).where(
                ChartDataset.company_id == company.id,
                ChartDataset.meta["days"].astext == "30Y"
            )

            exists_30y = db.scalar(stmt)


            if exists_30y:
                series = symbol_type = identifier = None

                try:
                    if company.nse_code:
                        nse_data = await main(company.nse_symbol)
                        symbol_data = nse_data.get('symbolData', {})
                        equity_response = symbol_data.get('equityResponse', [{}])[0]
                        nse_metadata = equity_response.get('metaData', {})
                        sec_info = equity_response.get('secInfo', {})

                        series = nse_metadata.get("series")
                        symbol_type = sec_info.get("classShare")
                        identifier = nse_metadata.get("identifier")

                except Exception as e:
                    print(f"NSE metadata failed for {company.nse_symbol}: {e}")
                    raise

                days_list = ["30Y"]

                if company.nse_code and company.bse_code:
                    try:
                        volume_data = await main_fetch_volume_from_nse(
                            company.nse_code,
                            f"{company.nse_symbol}-{series}",
                            symbol_type
                        )

                        for days in days_list:
                            company_name_with_dash = company.name.replace(" ", "-")

                            nse_data = await new_main_fetch_stock_price_for_graph(
                                days,
                                identifier,
                                company.nse_symbol,
                                company_name_with_dash
                            )

                            chart = nse_data.get('grapthData')

                            if chart:
                                volume_map = {
                                    item["time"]: item["volume"]
                                    for item in volume_data.get("data", [])
                                }

                                updated_data = []
                                for row in chart:
                                    time = row[0]
                                    volume = volume_map.get(time)
                                    updated_data.append(row + [volume])

                                exists_30y.values = updated_data

                    except Exception as e:
                        print(f"NSE chart fetch failed for {company.nse_symbol}: {e}")
                        raise

                elif company.nse_code:
                    try:
                        volume_data = await main_fetch_volume_from_nse(
                            company.nse_code,
                            f"{company.nse_symbol}-{series}",
                            symbol_type
                        )

                        for days in days_list:
                            company_name_with_dash = company.name.replace(" ", "-")

                            nse_data = await new_main_fetch_stock_price_for_graph(
                                days,
                                identifier,
                                company.nse_symbol,
                                company_name_with_dash
                            )

                            chart = nse_data.get('grapthData')

                            if chart:
                                volume_map = {
                                    item["time"]: item["volume"]
                                    for item in volume_data.get("data", [])
                                }

                                updated_data = []
                                for row in chart:
                                    time = row[0]
                                    volume = volume_map.get(time)
                                    updated_data.append(row + [volume])

                                exists_30y.values = updated_data

                    except Exception as e:
                        print(f"NSE chart fetch failed for {company.nse_symbol}: {e}")
                        raise

                elif company.bse_code:
                    try:
                        for days in days_list:
                            bse_data = await new_main_fetch_stock_price_for_bse_graph(
                                company.bse_code
                            )

                            script_header = bse_data.get('Data')
                            if script_header:
                                data_list = json.loads(script_header)
                                result_list = []

                                for item in data_list:
                                    ts_ms = int(
                                        datetime.strptime(
                                            item["dttm"],
                                            "%a %b %d %Y %H:%M:%S"
                                        ).timestamp() * 1000
                                    )
                                    price = float(item["vale1"])
                                    volume = int(item["vole"])

                                    result_list.append(
                                        [ts_ms, price, "", None, None, volume]
                                    )

                                exists_30y.values = result_list

                    except Exception as e:
                        print(f"BSE chart fetch failed for {company.bse_code}: {e}")
                        raise

                db.commit()
                new_process_symbol.append(company.nse_symbol)

        except Exception as e:
            db.rollback()
            print(f"\nFAILED company: {company.id} | {company.name}")
            print("Error:", str(e))
            continue

    db.close()
    await save_processed_symbol(new_process_symbol, "processed_symbols")

async def fetch_stock_quarterly_result_data_async():
    db = SessionLocalSync()
    error_symbols = []
    unsaved_symbols = []
    current_processed_symbols = []
    processed_symbols = []
    try:
        stmt = (
            select(CompanyStock)
            .outerjoin(
                QuarterlyResultDateset,
                CompanyStock.id == QuarterlyResultDateset.company_id
            )
            # .where(QuarterlyResultDateset.company_id.is_(None))
            .options(selectinload(CompanyStock.details))
            .execution_options(yield_per=100)
        )

        result = db.execute(stmt)
        companies = result.scalars().all()

        existing_symbols = set()

        skipped_symbols_from_json = await fetch_symbols_from_covered_symbol_json_for_quarterly_result()
        existing_symbols.update(skipped_symbols_from_json)
        missing_symbols = [
            c for c in companies if c.nse_symbol not in existing_symbols
        ]

        missing_symbols = missing_symbols[:100]

        current_processed_symbols = [c.nse_symbol for c in missing_symbols]

        await save_quarterly_result_processed_symbol(current_processed_symbols, "processing")

        def chunk_list(data, size):
            for i in range(0, len(data), size):
                yield data[i:i + size]

        for chunk in chunk_list(missing_symbols, 50):
            for company in chunk:
                try:
                    print(f"\nProcessing company: {company.id} | {company.name}")
                    with db.begin_nested():
                        db.execute(
                            delete(QuarterlyResultDateset)
                            .where(QuarterlyResultDateset.company_id == company.id)
                        )
                        nse_company_list = await fetch_nse_exact_symbol_data(company.nse_symbol)
                        bse_company_list = await fetch_bse_exact_symbol_data(company.nse_symbol)
                        if nse_company_list and bse_company_list:
                            integrated_filing_financials_list = await main_fetch_integrated_filing_financials(
                                company.nse_symbol, "equity")
                            quarterly_result = []
                            if integrated_filing_financials_list:
                                response_list = []
                                for integrated_filing_obj in integrated_filing_financials_list.get("data"):
                                    qe_date = integrated_filing_obj.get("qe_Date")
                                    consolidated = integrated_filing_obj.get("consolidated")
                                    ixbrl = integrated_filing_obj.get("ixbrl")
                                    formatted = None
                                    if qe_date:
                                        formatted = datetime.strptime(qe_date, "%d-%b-%Y").strftime("%b-%Y")
                                    if consolidated == "Consolidated":
                                        output, amount_type, format_type = await fetch_integrated_filing_financials_data_from_nse(ixbrl)
                                        output.append({
                                            "date": formatted or qe_date,
                                            "consolidated": consolidated,
                                            "amount_type": amount_type,
                                            "format": format_type,
                                        })
                                        response_list.append(output)
                                if response_list:
                                    quarterly_result = await decide_quarterly_format(response_list)
                            if quarterly_result:
                                company_stock = QuarterlyResultDateset(
                                    company_id=company.id,
                                    values=quarterly_result,
                                    result_format=ResultFormatEnum.consolidated
                                )
                                db.add(company_stock)
                                db.flush()
                                processed_symbols.append(company.nse_symbol)
                            else:
                                unsaved_symbols.append(company.nse_symbol)
                        elif bse_company_list:
                            integrated_filing_financials_list = await main_bse_fetch_integrated_filing_financials(
                                company.bse_code)
                            quarterly_result = []
                            if integrated_filing_financials_list:
                                response_list = []
                                for integrated_filing_obj in integrated_filing_financials_list.get("Table"):
                                    financial_name_obj = await parse_financial_name(integrated_filing_obj.get("Quarter_Name"))
                                    qe_date = f"{financial_name_obj.get('month')}-{financial_name_obj.get('year')}"
                                    consolidated = financial_name_obj.get("type")
                                    ixbrl = integrated_filing_obj.get("xbrlurl")
                                    if consolidated == "consolidated" and financial_name_obj.get("period") == "qtr":
                                        url = f"https://www.bseindia.com{ixbrl}"
                                        output, amount_type, format_type = await fetch_bse_integrated_filing_financials_data_from(url)
                                        output.append({
                                            "date": qe_date,
                                            "consolidated": consolidated,
                                            "amount_type": amount_type,
                                            "format": format_type
                                        })
                                        response_list.append(output)
                                if response_list:
                                    quarterly_result = await bse_decide_quarterly_format(response_list)
                            if quarterly_result:
                                company_stock = QuarterlyResultDateset(
                                    company_id=company.id,
                                    values=quarterly_result,
                                    result_format=ResultFormatEnum.consolidated
                                )
                                db.add(company_stock)
                                db.flush()
                                processed_symbols.append(company.nse_symbol)
                            else:
                                unsaved_symbols.append(company.nse_symbol)
                        elif nse_company_list:
                            integrated_filing_financials_list = await main_fetch_integrated_filing_financials(company.nse_symbol, "equity")
                            quarterly_result = []
                            if integrated_filing_financials_list:
                                response_list = []
                                for integrated_filing_obj in integrated_filing_financials_list.get("data"):
                                    qe_date = integrated_filing_obj.get("qe_Date")
                                    consolidated = integrated_filing_obj.get("consolidated")
                                    ixbrl = integrated_filing_obj.get("ixbrl")
                                    formatted = None
                                    if qe_date:
                                        formatted = datetime.strptime(qe_date, "%d-%b-%Y").strftime("%b-%Y")
                                    if consolidated == "Consolidated":
                                        output, amount_type, format_type = await fetch_integrated_filing_financials_data_from_nse(ixbrl)
                                        output.append({
                                            "date": formatted or qe_date,
                                            "consolidated": consolidated,
                                            "amount_type": amount_type,
                                            "format": format_type
                                        })
                                        response_list.append(output)
                                if response_list:
                                    quarterly_result = await decide_quarterly_format(response_list)
                            if quarterly_result:
                                company_stock = QuarterlyResultDateset(
                                    company_id=company.id,
                                    values=quarterly_result,
                                    result_format=ResultFormatEnum.consolidated
                                )
                                db.add(company_stock)
                                db.flush()
                                processed_symbols.append(company.nse_symbol)
                            else:
                                unsaved_symbols.append(company.nse_symbol)
                        else:
                            unsaved_symbols.append(company.nse_symbol)

                except Exception as e:
                    print("Error:", str(e))
                    print(f"\nFAILED company: {company.id} | {company.name}")
                    error_symbols.append(company.nse_symbol)
                    continue
        db.commit()
    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()
        await save_quarterly_result_processed_symbol(unsaved_symbols, "unsaved")
        await save_quarterly_result_processed_symbol(error_symbols, "error")
        await save_quarterly_result_processed_symbol(processed_symbols, "processed_symbols")
        await save_quarterly_result_processed_symbol(current_processed_symbols, "remove_processing")

async def update_nse_bse_scrip_code_async(limit: int | None = None):
    db = SessionLocalSync()
    current_run_symbols = set()
    try:
        from app.core import utils
        if not utils.ANGEL_NSE_MAP:
            utils.load_angel_map()

        stmt = select(CompanyStock).execution_options(yield_per=100)
        result = db.execute(stmt)
        processed_symbols = await update_nse_bse_scrip_code_load_processed_symbols()

        unprocessed_companies = []
        for c in result.scalars():
            if c.nse_symbol and c.nse_symbol not in processed_symbols:
                unprocessed_companies.append(c)
                if limit and len(unprocessed_companies) >= limit:
                    break

        if not unprocessed_companies:
            print("All symbols have already been processed for update_nse_bse_scrip_code.")
            return

        print(f"Total unprocessed companies to process for scrip code: {len(unprocessed_companies)}")

        semaphore = asyncio.Semaphore(15)

        async def resolve_codes_for_company(company):
            symbol = company.nse_symbol
            if not symbol:
                return company, None, None

            # 1. BSE code directly from in-memory Angel map
            bse_code = utils.ANGEL_NSE_MAP.get(symbol) or company.bse_code

            # 2. NSE code: Check in-memory Angel map first for standard series (-EQ, -BE, -BZ, -SM)
            nse_code = (
                utils.ANGEL_NSE_MAP.get(f"{symbol}-EQ")
                or utils.ANGEL_NSE_MAP.get(f"{symbol}-BE")
                or utils.ANGEL_NSE_MAP.get(f"{symbol}-BZ")
                or utils.ANGEL_NSE_MAP.get(f"{symbol}-SM")
            )

            # 3. If nse_code not found in memory, query NSE website
            if not nse_code:
                async with semaphore:
                    try:
                        nse_company_list = await fetch_nse_exact_symbol_data(symbol)
                        if nse_company_list:
                            nse_code = nse_company_list[0].get("nse_code")
                    except Exception as err:
                        print(f"Error fetching NSE data for {symbol}: {err}")

            nse_code = nse_code or company.nse_code
            return company, nse_code, bse_code

        STREAM_CHUNK = 100
        for i in range(0, len(unprocessed_companies), STREAM_CHUNK):
            company_slice = unprocessed_companies[i : i + STREAM_CHUNK]
            slice_symbols = [c.nse_symbol for c in company_slice]
            current_run_symbols.update(slice_symbols)
            await update_nse_bse_scrip_code_save_processed_symbol(
                slice_symbols, "current_processed_symbols"
            )

            tasks = [resolve_codes_for_company(comp) for comp in company_slice]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            chunk_success = []
            chunk_failed = []

            for comp, res in zip(company_slice, results):
                if isinstance(res, Exception) or not res:
                    chunk_failed.append(comp.nse_symbol)
                    continue
                _, nse_code, bse_code = res
                try:
                    comp.bse_code = bse_code
                    comp.nse_code = nse_code
                    chunk_success.append(comp.nse_symbol)
                except Exception as row_err:
                    print(f"Error updating company {comp.nse_symbol}: {row_err}")
                    chunk_failed.append(comp.nse_symbol)

            try:
                db.commit()
                if chunk_success:
                    current_run_symbols.difference_update(chunk_success)
                    await update_nse_bse_scrip_code_save_processed_symbol(
                        chunk_success, "processed_symbols"
                    )
            except Exception as commit_err:
                db.rollback()
                print(f"Error committing scrip code batch: {commit_err}")
                chunk_failed.extend(chunk_success)
                chunk_success = []

            if chunk_failed:
                current_run_symbols.difference_update(chunk_failed)
                await update_nse_bse_scrip_code_save_processed_symbol(
                    chunk_failed, "error"
                )

    except Exception as main_err:
        print(f"Error in update_nse_bse_scrip_code_async: {main_err}")
        try:
            if current_run_symbols:
                await update_nse_bse_scrip_code_save_processed_symbol(
                    list(current_run_symbols), "remove_processing"
                )
        except Exception:
            pass
        raise main_err
    finally:
        db.close()


async def update_nse_bse_stock_information_async():
    db = SessionLocalSync()
    try:
        stmt = (
                select(CompanyStock)
                .outerjoin(
                    KeyDetailsForCS,
                    CompanyStock.id == KeyDetailsForCS.company_id
                )
                .options(selectinload(CompanyStock.details))
                .execution_options(yield_per=100)
            )
        file_name = "update_nse_bse_price_data.json"
        error_symbols = []
        result = db.execute(stmt)
        # companies = result.scalars().all()
        processed_symbols = set(await update_nse_bse_price_data_load_processed_symbols(file_name))
        new_process_symbol = []
        # unprocessed_companies = [
        #     c for c in result.scalars() if c.nse_symbol not in processed_symbols
        # ][:30]
        unprocessed_companies = []
        for c in result.scalars():
            if c.nse_symbol not in processed_symbols:
                unprocessed_companies.append(c)
                if len(unprocessed_companies) == 30:
                    break
        started_symbols = [c.nse_symbol for c in unprocessed_companies]
        await update_nse_bse_price_data_save_processed_symbol(started_symbols, "current_processed_symbols", file_name)
        for company in unprocessed_companies:
            try:
                roe = company.details.roe
                current_price = company.details.current_price
                high_price = company.details.high_price
                low_price = company.details.low_price
                pe_ratio = company.details.pe_ratio
                face_value = company.details.face_value
                market_cap_cr = company.details.market_cap
                nse_company_list = await fetch_nse_exact_symbol_data(company.nse_symbol)
                bse_company_list = await fetch_bse_exact_symbol_data(company.nse_symbol)
                if nse_company_list and bse_company_list:
                    security_code = bse_company_list[0].get("bse_code")
                    nse_data = await main(company.nse_symbol)
                    bse_data = await main_bse(security_code)
                    header_data = bse_data.get('header')
                    symbol_data = nse_data.get('symbolData')
                    equity_response = symbol_data.get('equityResponse')[0]
                    nse_metadata = equity_response.get('metaData')
                    trade_info = equity_response.get('tradeInfo')
                    sec_info = equity_response.get('secInfo')
                    current_price = trade_info.get('lastPrice')
                    face_value = trade_info.get('faceValue')
                    high_price = nse_metadata.get('dayHigh')
                    low_price = nse_metadata.get('dayLow')
                    pe_ratio = sec_info.get('pdSymbolPe')
                    roe = header_data.get('ROE')
                    total_market_cap = trade_info.get('totalMarketCap')
                    if total_market_cap:
                        market_cap_cr = round(total_market_cap / 1e7, 2)
                    else:
                        market_cap_cr = None
                elif nse_company_list:
                    nse_data = await main(company.nse_symbol)
                    symbol_data = nse_data.get('symbolData')
                    equity_response = symbol_data.get('equityResponse')[0]
                    nse_metadata = equity_response.get('metaData')
                    trade_info = equity_response.get('tradeInfo')
                    sec_info = equity_response.get('secInfo')
                    current_price = trade_info.get('lastPrice')
                    face_value = trade_info.get('faceValue')
                    high_price = nse_metadata.get('dayHigh')
                    low_price = nse_metadata.get('dayLow')
                    pe_ratio = sec_info.get('pdSymbolPe')
                    total_market_cap = trade_info.get('totalMarketCap')
                    if total_market_cap:
                        market_cap_cr = round(total_market_cap / 1e7, 2)
                    else:
                        market_cap_cr = None
                elif bse_company_list:
                    security_code = bse_company_list[0].get("bse_code")
                    bse_data = await main_bse(security_code)
                    header_data = bse_data.get('header')
                    script_header = bse_data.get('scriptHeader')
                    header = script_header.get('Header')
                    price_graph = bse_data.get('priceGraph')
                    current_price = price_graph.get('CurrVal') or header.get('LTP')
                    if current_price:
                        current_price = float(current_price)
                    face_value = header_data.get('FaceVal')
                    if face_value:
                        face_value = float(face_value)
                    high_price = header.get('High')
                    low_price = header.get('Low')
                    pe_ratio = header_data.get('PE')
                    roe = header_data.get('ROE')
                    stock_trading = bse_data.get('stockTrading')
                    total_market_cap = stock_trading.get('MktCapFull', None)
                    if total_market_cap:
                        market_cap_cr = float(str(total_market_cap).replace(",", ""))
                company.details.roe = float(roe) if roe and roe != '-' else None
                company.details.current_price = current_price
                company.details.high_price = high_price
                company.details.low_price = low_price
                company.details.pe_ratio = float(pe_ratio) if pe_ratio and pe_ratio != '-' else None
                company.details.face_value = face_value
                company.details.market_cap = market_cap_cr
                db.commit()
                new_process_symbol.append(company.nse_symbol)

            except Exception as symbol_error:
                db.rollback()
                error_symbols.append(company.nse_symbol)
                print(f"Error for symbol {company.nse_symbol}: {symbol_error}")
                continue

        await update_nse_bse_price_data_save_processed_symbol(new_process_symbol, "processed_symbols", file_name)
        await update_nse_bse_price_data_save_processed_symbol(error_symbols, "error", file_name)
    finally:
        db.close()


async def fetch_and_store_newly_listed_company_data_from_nse_bse_async():
    """
    Background task to store NSE/BSE newly listed company data (optimized for high speed).
    """
    db = SessionLocalSync()
    error_symbols = []
    processed_symbols = []
    file_name = "nse_bse_newly_listed_stocks"

    def safe_float(val):
        if val is None or val == "" or val == "-":
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    try:
        # Fetch NSE and BSE newly listed lists concurrently
        nse_task = main_nse_newly_listed_stocks()
        bse_task = main_newly_listed_stocks()
        nse_res, bse_res = await asyncio.gather(nse_task, bse_task, return_exceptions=True)
        nse_newly_listed_stocks = nse_res if not isinstance(nse_res, Exception) and nse_res else {}
        newly_listed_stocks = bse_res if not isinstance(bse_res, Exception) and bse_res else {}

        nse_newly_listed_stocks_symbol = [
            item.get('symbol')
            for item in nse_newly_listed_stocks.get('data', [])
            if item.get('symbol')
        ]
        newly_listed_stocks_symbol = [
            item.get('symbol')
            for item in newly_listed_stocks.get('data', {}).get("results", [])
            if item.get('symbol')
        ]

        missing_symbols = list(set(nse_newly_listed_stocks_symbol + newly_listed_stocks_symbol))

        existing_symbols = await fetch_newly_listed_stock_symbols_from_covered_symbol_json(file_name)
        current_processed_symbols = [s for s in missing_symbols if s and s not in existing_symbols]
        await update_nse_bse_newly_listed_stock_save_processed_symbol(
            current_processed_symbols, "current_processed_symbols", file_name
        )
        if not current_processed_symbols:
            return

        # Bulk check existing symbols in DB upfront to skip already inserted ones instantly
        candidates = current_processed_symbols[:100]
        existing_in_db_records = db.execute(
            select(CompanyStock.nse_symbol, CompanyStock.bse_code).where(
                or_(
                    CompanyStock.nse_symbol.in_(candidates),
                    CompanyStock.bse_code.in_(candidates),
                )
            )
        ).all()

        db_existing_set = set()
        for nse_sym, bse_c in existing_in_db_records:
            if nse_sym:
                db_existing_set.add(nse_sym.strip().upper())
            if bse_c:
                db_existing_set.add(str(bse_c).strip().upper())

        symbols_to_fetch = []
        already_in_db = []
        for s in candidates:
            if s.strip().upper() in db_existing_set:
                already_in_db.append(s)
                processed_symbols.append(s)
            else:
                symbols_to_fetch.append(s)

        if already_in_db:
            await update_nse_bse_newly_listed_stock_save_processed_symbol(
                already_in_db, "processed_symbols", file_name
            )

        # Concurrency limiter to fetch multiple symbols fast without hitting rate limits
        semaphore = asyncio.Semaphore(10)

        async def fetch_single_newly_listed_symbol(symbol):
            async with semaphore:
                try:
                    # Search NSE and BSE exact symbol data in parallel
                    nse_list_task = fetch_nse_exact_symbol_data(symbol)
                    bse_list_task = fetch_bse_exact_symbol_data(symbol)
                    nse_company_list, bse_company_list = await asyncio.gather(
                        nse_list_task, bse_list_task, return_exceptions=True
                    )
                    if isinstance(nse_company_list, Exception):
                        nse_company_list = None
                    if isinstance(bse_company_list, Exception):
                        bse_company_list = None

                    if nse_company_list and bse_company_list:
                        security_code = bse_company_list[0].get("bse_code")
                        nse_data, bse_data = await asyncio.gather(
                            main(symbol),
                            main_bse(security_code),
                            return_exceptions=True
                        )
                        if isinstance(nse_data, Exception) or not nse_data:
                            return symbol, None, "NSE data fetch failed"
                        if isinstance(bse_data, Exception) or not bse_data:
                            bse_data = {}

                        nse_symbol = nse_data.get('symbol') or symbol
                        company_name = nse_data.get('companyName')
                        header_data = bse_data.get('header') or {}
                        symbol_data = nse_data.get('symbolData') or {}
                        eq_resp = symbol_data.get('equityResponse') or [{}]
                        equity_response = eq_resp[0] if eq_resp else {}
                        nse_metadata = equity_response.get('metaData') or {}
                        trade_info = equity_response.get('tradeInfo') or {}
                        sec_info = equity_response.get('secInfo') or {}
                        total_market_cap = trade_info.get('totalMarketCap')
                        market_cap_cr = round(total_market_cap / 1e7, 2) if total_market_cap else None
                        current_price = trade_info.get('lastPrice')
                        face_value = trade_info.get('faceValue')
                        high_price = nse_metadata.get('dayHigh')
                        low_price = nse_metadata.get('dayLow')
                        pe_ratio = sec_info.get('pdSymbolPe')
                        roe = header_data.get('ROE')
                        macro = sec_info.get("macro")
                        sector = sec_info.get("sector")
                        industry_info = sec_info.get("industryInfo")
                        basic_industry = sec_info.get("basicIndustry")
                        bse_code = security_code
                        nse_code = nse_company_list[0].get("nse_code")
                        series = nse_metadata.get("series")
                        symbol_type = sec_info.get("classShare")
                        identifier = nse_metadata.get("identifier")

                        # Fetch volume & 30Y graph commented out
                        # days = "30Y"
                        # company_name_with_dash = (company_name or symbol).replace(" ", "-")
                        # vol_task = main_fetch_volume_from_nse(
                        #     nse_code, f"{symbol}-{series}", symbol_type
                        # )
                        # graph_task = new_main_fetch_stock_price_for_graph(
                        #     days, identifier, symbol, company_name_with_dash
                        # )
                        # vol_res, graph_res = await asyncio.gather(
                        #     vol_task, graph_task, return_exceptions=True
                        # )

                        # chart_values = []
                        # if not isinstance(graph_res, Exception) and graph_res:
                        #     chart = graph_res.get('grapthData') or []
                        #     vol_data = vol_res.get("data") if (not isinstance(vol_res, Exception) and vol_res) else []
                        #     volume_map = {
                        #         item["time"]: item["volume"]
                        #         for item in vol_data
                        #         if "time" in item and "volume" in item
                        #     } if vol_data else {}
                        #     for row in chart:
                        #         time_val = row[0]
                        #         volume_val = volume_map.get(time_val, None)
                        #         chart_values.append(row + [volume_val])

                        payload = {
                            "exchange": "BOTH",
                            "symbol": symbol,
                            "company_name": company_name,
                            "nse_code": nse_code,
                            "bse_code": bse_code,
                            "macro": macro,
                            "sector": sector,
                            "industry_info": industry_info,
                            "basic_industry": basic_industry,
                            "market_cap_cr": market_cap_cr,
                            "current_price": safe_float(current_price),
                            "pe_ratio": safe_float(pe_ratio),
                            "face_value": safe_float(face_value),
                            "high_price": safe_float(high_price),
                            "low_price": safe_float(low_price),
                            "roe": safe_float(roe),
                            "chart_values": [],
                            "chart_label": "Price on NSE",
                        }
                        return symbol, payload, None

                    elif nse_company_list:
                        nse_data = await main(symbol)
                        if not nse_data:
                            return symbol, None, "NSE data empty"

                        nse_symbol = nse_data.get('symbol') or symbol
                        company_name = nse_data.get('companyName')
                        symbol_data = nse_data.get('symbolData') or {}
                        eq_resp = symbol_data.get('equityResponse') or [{}]
                        equity_response = eq_resp[0] if eq_resp else {}
                        nse_metadata = equity_response.get('metaData') or {}
                        trade_info = equity_response.get('tradeInfo') or {}
                        sec_info = equity_response.get('secInfo') or {}
                        total_market_cap = trade_info.get('totalMarketCap')
                        market_cap_cr = round(total_market_cap / 1e7, 2) if total_market_cap else None
                        current_price = trade_info.get('lastPrice')
                        face_value = trade_info.get('faceValue')
                        high_price = nse_metadata.get('dayHigh')
                        low_price = nse_metadata.get('dayLow')
                        pe_ratio = sec_info.get('pdSymbolPe')
                        roe = None
                        bse_code = None
                        macro = sec_info.get("macro")
                        sector = sec_info.get("sector")
                        industry_info = sec_info.get("industryInfo")
                        basic_industry = sec_info.get("basicIndustry")
                        nse_code = nse_company_list[0].get("nse_code")
                        series = nse_metadata.get("series")
                        symbol_type = sec_info.get("classShare")
                        identifier = nse_metadata.get("identifier")

                        # days = "30Y"
                        # company_name_with_dash = (company_name or symbol).replace(" ", "-")
                        # vol_task = main_fetch_volume_from_nse(
                        #     nse_code, f"{symbol}-{series}", symbol_type
                        # )
                        # graph_task = new_main_fetch_stock_price_for_graph(
                        #     days, identifier, symbol, company_name_with_dash
                        # )
                        # vol_res, graph_res = await asyncio.gather(
                        #     vol_task, graph_task, return_exceptions=True
                        # )

                        # chart_values = []
                        # if not isinstance(graph_res, Exception) and graph_res:
                        #     chart = graph_res.get('grapthData') or []
                        #     vol_data = vol_res.get("data") if (not isinstance(vol_res, Exception) and vol_res) else []
                        #     volume_map = {
                        #         item["time"]: item["volume"]
                        #         for item in vol_data
                        #         if "time" in item and "volume" in item
                        #     } if vol_data else {}
                        #     for row in chart:
                        #         time_val = row[0]
                        #         volume_val = volume_map.get(time_val, None)
                        #         chart_values.append(row + [volume_val])

                        payload = {
                            "exchange": "NSE",
                            "symbol": symbol,
                            "company_name": company_name,
                            "nse_code": nse_code,
                            "bse_code": bse_code,
                            "macro": macro,
                            "sector": sector,
                            "industry_info": industry_info,
                            "basic_industry": basic_industry,
                            "market_cap_cr": market_cap_cr,
                            "current_price": safe_float(current_price),
                            "pe_ratio": safe_float(pe_ratio),
                            "face_value": safe_float(face_value),
                            "high_price": safe_float(high_price),
                            "low_price": safe_float(low_price),
                            "roe": None,
                            "chart_values": [],
                            "chart_label": "Price on NSE",
                        }
                        return symbol, payload, None

                    elif bse_company_list:
                        security_code = bse_company_list[0].get("bse_code")
                        bse_data = await main_bse(security_code)
                        # graph_task = new_main_fetch_stock_price_for_bse_graph(security_code)
                        # bse_data, bse_graph = await asyncio.gather(
                        #     bse_task, graph_task, return_exceptions=True
                        # )
                        if isinstance(bse_data, Exception) or not bse_data:
                            return symbol, None, "BSE data empty"

                        header_data = bse_data.get('header') or {}
                        script_header = bse_data.get('scriptHeader') or {}
                        company_detail = script_header.get('Cmpname') or {}
                        header = script_header.get('Header') or {}
                        price_graph = bse_data.get('priceGraph') or {}
                        stock_trading = bse_data.get('stockTrading') or {}
                        company_name = company_detail.get('FullN')
                        total_market_cap = stock_trading.get('MktCapFull', None)
                        market_cap_cr = float(total_market_cap) if total_market_cap else None
                        current_price = price_graph.get('CurrVal')
                        current_price = float(current_price) if current_price else None
                        face_value = header_data.get('FaceVal')
                        face_value = float(face_value) if face_value else None
                        high_price = header.get('High')
                        low_price = header.get('Low')
                        pe_ratio = header_data.get('PE')
                        roe = header_data.get('ROE')
                        bse_code = security_code
                        macro = header_data.get("Sector")
                        sector = header_data.get("IndustryNew")
                        industry_info = header_data.get("IGroup")
                        basic_industry = header_data.get("Industry")
                        nse_code = None

                        # chart_values = []
                        # if not isinstance(bse_graph, Exception) and bse_graph:
                        #     script_hdr = bse_graph.get('Data')
                        #     if script_hdr:
                        #         data_list = json.loads(script_hdr)
                        #         for item in data_list:
                        #             ts_ms = int(
                        #                 datetime.strptime(
                        #                     item["dttm"], "%a %b %d %Y %H:%M:%S"
                        #                 ).timestamp() * 1000
                        #             )
                        #             price = float(item["vale1"])
                        #             volume = int(item["vole"])
                        #             chart_values.append([ts_ms, price, "", None, None, volume])

                        payload = {
                            "exchange": "BSE",
                            "symbol": symbol,
                            "company_name": company_name,
                            "nse_code": nse_code,
                            "bse_code": bse_code,
                            "macro": macro,
                            "sector": sector,
                            "industry_info": industry_info,
                            "basic_industry": basic_industry,
                            "market_cap_cr": market_cap_cr,
                            "current_price": safe_float(current_price),
                            "pe_ratio": safe_float(pe_ratio),
                            "face_value": safe_float(face_value),
                            "high_price": safe_float(high_price),
                            "low_price": safe_float(low_price),
                            "roe": safe_float(roe),
                            "chart_values": [],
                            "chart_label": "Price on BSE",
                        }
                        return symbol, payload, None
                    else:
                        return symbol, None, "Symbol not found on NSE or BSE"
                except Exception as ex:
                    return symbol, None, str(ex)

        # Process in chunks of 50
        def chunk_list(data, size):
            for i in range(0, len(data), size):
                yield data[i:i + size]

        for chunk in chunk_list(symbols_to_fetch, 50):
            tasks = [fetch_single_newly_listed_symbol(symbol) for symbol in chunk]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            chunk_processed = []
            chunk_errors = []

            for item in results:
                if isinstance(item, Exception) or not item:
                    continue
                symbol, payload, err = item
                if not payload:
                    chunk_errors.append(symbol)
                    error_symbols.append(symbol)
                    print(f"Error for symbol {symbol}: {err}")
                    continue

                try:
                    with db.begin_nested():
                        company_stock = CompanyStock(
                            nse_symbol=symbol,
                            name=payload["company_name"],
                            nse_code=payload["nse_code"],
                            bse_code=payload["bse_code"],
                            macro_economic_sector=payload["macro"],
                            sector=payload["sector"],
                            industry=payload["industry_info"],
                            basic_industry=payload["basic_industry"]
                        )
                        db.add(company_stock)
                        db.flush()

                        key_details = KeyDetailsForCS(
                            market_cap=payload["market_cap_cr"],
                            current_price=payload["current_price"],
                            pe_ratio=payload["pe_ratio"],
                            face_value=payload["face_value"],
                            high_price=payload["high_price"],
                            low_price=payload["low_price"],
                            book_value=None,
                            dividend_yield=None,
                            roce=None,
                            roe=payload["roe"],
                            company_id=company_stock.id
                        )
                        db.add(key_details)
                        db.flush()

                        # if payload.get("chart_values"):
                        #     company_stock_chart_dataset_ops = ChartDataset(
                        #         metric="Price",
                        #         label=payload["chart_label"],
                        #         meta={"days": "30Y"},
                        #         company_id=company_stock.id,
                        #         values=payload["chart_values"],
                        #     )
                        #     db.add(company_stock_chart_dataset_ops)
                        #     db.flush()

                        chunk_processed.append(symbol)
                        processed_symbols.append(symbol)
                except Exception as symbol_db_error:
                    chunk_errors.append(symbol)
                    error_symbols.append(symbol)
                    print(f"DB Error for symbol {symbol}: {symbol_db_error}")

            db.commit()
            # print("Symbols batch committed to database")

            if chunk_processed:
                await update_nse_bse_newly_listed_stock_save_processed_symbol(
                    chunk_processed, "processed_symbols", file_name
                )
            if chunk_errors:
                await update_nse_bse_newly_listed_stock_save_processed_symbol(
                    chunk_errors, "error", file_name
                )

    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()
        await update_nse_bse_newly_listed_stock_save_processed_symbol(
            processed_symbols, "processed_symbols", file_name
        )
        await update_nse_bse_newly_listed_stock_save_processed_symbol(
            error_symbols, "error", file_name
        )
        try:
            from app.db.redis.redis import redis_client_1
            redis_client_1.delete(f"{file_name}:current_processed_symbols")
        except Exception:
            pass


async def fetch_and_update_stock_shareholding_pattern_data_async():
    db = SessionLocalSync()
    error_symbols = []
    unsaved_symbols = []
    processed_symbols = []
    file_name = "nse_bse_stocks_share_holder_data"
    try:
        stmt = (
            select(CompanyStock)
            .outerjoin(
                ShareHoldingPeriod,
                CompanyStock.id == ShareHoldingPeriod.company_id
            )
            .distinct(CompanyStock.id)
            .options(selectinload(CompanyStock.details))
            .execution_options(yield_per=100)
        )

        result = db.execute(stmt)
        companies = result.scalars().all()

        existing_symbols = set()

        skipped_symbols_from_json = await fetch_symbols_from_covered_symbol_json_for_shareholder_result(file_name)
        existing_symbols.update(skipped_symbols_from_json)
        missing_symbols = [
            c for c in companies if c.nse_symbol not in existing_symbols
        ]

        missing_symbols = missing_symbols[:50]

        current_processed_symbols = [c.nse_symbol for c in missing_symbols]
        await update_nse_bse_shareholder_save_processed_symbol(current_processed_symbols, "processing", file_name)

        def chunk_list(data, size):
            for i in range(0, len(data), size):
                yield data[i:i + size]

        for chunk in chunk_list(missing_symbols, 50):
            for company in chunk:
                try:
                    print(f"\nProcessing company: {company.id} | {company.name}")
                    with db.begin_nested():
                        nse_company_list = await fetch_nse_exact_symbol_data(company.nse_symbol)
                        bse_company_list = await fetch_bse_exact_symbol_data(company.nse_symbol)
                        if nse_company_list and bse_company_list:
                            shareholding_list = await main_nse_fetch_shareholding_list(company.nse_symbol, "equities")
                            if shareholding_list:
                                shareholding_data_list = []
                                for shareholding_obj in shareholding_list:
                                    row = {
                                        "date": shareholding_obj.get("date"),
                                        "remarksWeb": shareholding_obj.get("remarksWeb"),
                                        "revisionRemark": shareholding_obj.get("revisionRemark"),
                                        "revisionDate": shareholding_obj.get("revisionDate"),
                                    }
                                    shareholding_all_data = await main_nse_fetch_shareholding_data_using_api(
                                        id="popup1",
                                        symbol=shareholding_obj.get("symbol"),
                                        name=shareholding_obj.get("name"),
                                        rec_id=shareholding_obj.get("recordId"),
                                        row=row)
                                    if shareholding_all_data.get("type") == "data":
                                        shareholding_all_data.update({"date": shareholding_obj.get("date")})
                                        shareholding_data_list.append(shareholding_all_data)
                                    elif shareholding_all_data.get("type") == "redirect":
                                        pass
                                await save_multiple_shareholding(db, company.id, shareholding_data_list)
                                await save_multiple_dii_shareholding(db, company.id, shareholding_data_list)
                                await save_multiple_fii_shareholding(db, company.id, shareholding_data_list)
                                await save_multiple_government_shareholding(db, company.id, shareholding_data_list)
                                await save_multiple_public_shareholding(db, company.id, shareholding_data_list)
                            else:
                                unsaved_symbols.append(company.nse_symbol)
                                continue

                        elif bse_company_list:
                            shareholding_list = await main_bse_fetch_shareholding_list(company.bse_code)
                            cutoff = await to_year_month("Mar 2023")
                            shareholding_list = [
                                item for item in shareholding_list.get("Table", [])
                                if await to_year_month(item["qtr"]) >= cutoff
                            ]
                            unique_data = {}
                            for item in shareholding_list:
                                qtr = item["qtr"]

                                if (
                                        qtr not in unique_data
                                        or item.get("status") != "Revised"
                                ):
                                    unique_data[qtr] = item
                            shareholding_list = list(unique_data.values())
                            if shareholding_list:
                                shareholding_data_list = []
                                for shareholding_obj in shareholding_list:
                                    if shareholding_obj.get("status") == "New":
                                        navigateurl_promoter = f"Corp_shpPromoterNGroup_ng/w?SCRIPCODE={company.bse_code}&QtrCode={shareholding_obj.get('qtrid')}"
                                        navigateurl_publicshareholder = f"Corp_shpSec_SHPPubShold_ng/w?SCRIPCODE={company.bse_code}&QtrCode={shareholding_obj.get('qtrid')}"
                                        promoter_data = await new_parse_bse_promoter_table(navigateurl_promoter)
                                        public_shareholder_data = await new_parse_bse_public_shareholder_table(navigateurl_publicshareholder)
                                        promoter_data.update(public_shareholder_data)
                                        promoter_data.update({"date": shareholding_obj.get("qtr")})

                                        shareholding_data_list.append(promoter_data)
                                await save_bse_multiple_shareholding(db, company.id, shareholding_data_list)
                                await save_bse_multiple_dii_shareholding(db, company.id, shareholding_data_list)
                                await save_bse_multiple_fii_shareholding(db, company.id, shareholding_data_list)
                                await save_bse_multiple_government_shareholding(db, company.id, shareholding_data_list)
                                await save_bse_multiple_public_shareholding(db, company.id, shareholding_data_list)
                            else:
                                unsaved_symbols.append(company.nse_symbol)
                                continue
                        elif nse_company_list:
                            shareholding_list = await main_nse_fetch_shareholding_list(company.nse_symbol, "sme")
                            if shareholding_list:
                                shareholding_data_list = []
                                for shareholding_obj in shareholding_list:
                                    row = {
                                        "date": shareholding_obj.get("date"),
                                        "remarksWeb": shareholding_obj.get("remarksWeb"),
                                        "revisionRemark": shareholding_obj.get("revisionRemark"),
                                        "revisionDate": shareholding_obj.get("revisionDate"),
                                    }
                                    shareholding_all_data = await main_nse_fetch_shareholding_data_using_api(
                                        id="popup1",
                                        symbol=shareholding_obj.get("symbol"),
                                        name=shareholding_obj.get("name"),
                                        rec_id=shareholding_obj.get("recordId"),
                                        row=row)
                                    if shareholding_all_data.get("type") == "data":
                                        shareholding_all_data.update({"date": shareholding_obj.get("date")})
                                        shareholding_data_list.append(shareholding_all_data)
                                    elif shareholding_all_data.get("type") == "redirect":
                                        pass
                                await save_multiple_shareholding(db, company.id, shareholding_data_list)
                                await save_multiple_dii_shareholding(db, company.id, shareholding_data_list)
                                await save_multiple_fii_shareholding(db, company.id, shareholding_data_list)
                                await save_multiple_government_shareholding(db, company.id, shareholding_data_list)
                                await save_multiple_public_shareholding(db, company.id, shareholding_data_list)
                            else:
                                unsaved_symbols.append(company.nse_symbol)
                                continue
                        else:
                            unsaved_symbols.append(company.nse_symbol)
                            continue
                        processed_symbols.append(company.nse_symbol)

                except Exception as e:
                    print("Error:", str(e))
                    print(f"\nFAILED company: {company.id} | {company.name}")
                    error_symbols.append(company.nse_symbol)
                    continue
        db.commit()
    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()
        await update_nse_bse_shareholder_save_processed_symbol(unsaved_symbols, "data_not_available", file_name)
        await update_nse_bse_shareholder_save_processed_symbol(error_symbols, "error", file_name)
        await update_nse_bse_shareholder_save_processed_symbol(processed_symbols, "processed_symbols", file_name)

async def fetch_and_update_stock_balance_sheet_profit_loss_cash_flow_consolidated_data_async():
    db = SessionLocalSync()
    error_symbols = []
    unsaved_symbols = []
    processed_symbols = []
    file_name = "nse_bse_stocks_balance_sheet_and_profit_loss_and_cash_flow_data"
    try:
        pl_consolidated = (
            select(ProfitLossDataset.id)
            .where(ProfitLossDataset.company_id == CompanyStock.id)
            .where(ProfitLossDataset.values["type"].astext == "Consolidated")
            .correlate(CompanyStock)
        )

        bs_consolidated = (
            select(BalanceSheetDataset.id)
            .where(BalanceSheetDataset.company_id == CompanyStock.id)
            .where(BalanceSheetDataset.values["type"].astext == "Consolidated")
            .correlate(CompanyStock)
        )

        cf_consolidated = (
            select(CashFlowDataset.id)
            .where(CashFlowDataset.company_id == CompanyStock.id)
            .where(CashFlowDataset.values["type"].astext == "Consolidated")
            .correlate(CompanyStock)
        )

        stmt = (
            select(CompanyStock)
            .where(
                ~and_(
                    exists(pl_consolidated),
                    exists(bs_consolidated),
                    exists(cf_consolidated),
                )
            )
            .options(
                selectinload(CompanyStock.details),
                selectinload(CompanyStock.profit_loss),
                selectinload(CompanyStock.balance_sheet),
                selectinload(CompanyStock.cash_flow),
            )
            .execution_options(yield_per=100)
        )

        result = db.execute(stmt)
        companies = result.scalars().all()

        existing_symbols = set()

        skipped_symbols_from_json = await fetch_symbols_from_covered_symbol_json_for_balance_sheet_and_profit_loss_and_cash_flow(file_name)
        existing_symbols.update(skipped_symbols_from_json)
        missing_symbols = [
            c for c in companies if c.nse_symbol not in existing_symbols
        ]

        missing_symbols = missing_symbols[:5]

        current_processed_symbols = [c.nse_symbol for c in missing_symbols]

        await update_nse_bse_balance_sheet_and_profit_loss_and_cash_flow_save_processed_symbol(current_processed_symbols, "processing", file_name)

        def chunk_list(data, size):
            for i in range(0, len(data), size):
                yield data[i:i + size]

        def get_consolidated(dataset_list):
            for d in dataset_list:
                values = json.loads(d.values) if isinstance(d.values, str) else d.values
                if values.get("type") == "Consolidated":
                    d._parsed_values = values
                    return d
            return None

        for chunk in chunk_list(missing_symbols, 1):
            for company in chunk:
                try:
                    print(f"\nProcessing company: {company.id} | {company.name}")
                    with db.begin_nested():
                        nse_company_list = await fetch_nse_exact_symbol_data(company.nse_symbol)
                        bse_company_list = await fetch_bse_exact_symbol_data(company.nse_symbol)
                        profit_loss = get_consolidated(company.profit_loss)
                        balance_sheet = get_consolidated(company.balance_sheet)
                        cash_flow = get_consolidated(company.cash_flow)
                        if nse_company_list and bse_company_list:
                            html_data = await main_balance_sheet_html(company.nse_symbol)
                            soup = BeautifulSoup(html_data, "html.parser")
                            if not balance_sheet:
                                balance_sheet_data = await parse_balance_sheet(soup)
                                balance_sheet_data.update({"type": "Consolidated"})
                            if not profit_loss:
                                profit_loss_data = await parse_profit_loss(soup)
                                profit_loss_data.update({"type": "Consolidated"})
                            if not cash_flow:
                                cash_flow_data = await parse_cash_flow(soup)
                                cash_flow_data.update({"type": "Consolidated"})
                        elif nse_company_list:
                            html_data = await main_balance_sheet_html(company.nse_symbol)
                            soup = BeautifulSoup(html_data, "html.parser")
                            if not balance_sheet:
                                balance_sheet_data = await parse_balance_sheet(soup)
                                balance_sheet_data.update({"type": "Consolidated"})
                            if not profit_loss:
                                profit_loss_data = await parse_profit_loss(soup)
                                profit_loss_data.update({"type": "Consolidated"})
                            if not cash_flow:
                                cash_flow_data = await parse_cash_flow(soup)
                                cash_flow_data.update({"type": "Consolidated"})
                        elif bse_company_list:
                            c_name = await make_short_company_name(company.name)
                            c_list = await main_find_company_json(c_name)
                            exact_company = await find_by_scripcode(c_list, company.bse_code)
                            if exact_company:
                                html_data = await main_balance_sheet_html(f"SCRIP-{exact_company.get('FINCODE')}")
                                soup = BeautifulSoup(html_data, "html.parser")
                                if not balance_sheet:
                                    balance_sheet_data = await parse_balance_sheet(soup)
                                    balance_sheet_data.update({"type": "Consolidated"})
                                if not profit_loss:
                                    profit_loss_data = await parse_profit_loss(soup)
                                    profit_loss_data.update({"type": "Consolidated"})
                                if not cash_flow:
                                    cash_flow_data = await parse_cash_flow(soup)
                                    cash_flow_data.update({"type": "Consolidated"})
                            else:
                                unsaved_symbols.append(company.nse_symbol)
                                continue
                        else:
                            unsaved_symbols.append(company.nse_symbol)
                            continue

                        if not balance_sheet:
                            balance_sheet_dataset_ops = BalanceSheetDataset(
                                company_id=company.id,
                                values=balance_sheet_data,
                            )
                            db.add(balance_sheet_dataset_ops)
                            db.flush()

                        if not profit_loss:
                            profit_loss_dataset_ops = ProfitLossDataset(
                                company_id=company.id,
                                values=profit_loss_data,
                            )
                            db.add(profit_loss_dataset_ops)
                            db.flush()

                        if not cash_flow:
                            profit_loss_dataset_ops = CashFlowDataset(
                                company_id=company.id,
                                values=cash_flow_data,
                            )
                            db.add(profit_loss_dataset_ops)
                            db.flush()

                        processed_symbols.append(company.nse_symbol)


                except Exception as e:
                    print("Error:", str(e))
                    print(f"\nFAILED company: {company.id} | {company.name}")
                    error_symbols.append(company.nse_symbol)
                    continue
        db.commit()
    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()
        await update_nse_bse_shareholder_save_processed_symbol(unsaved_symbols, "data_not_available", file_name)
        await update_nse_bse_shareholder_save_processed_symbol(error_symbols, "error", file_name)
        await update_nse_bse_shareholder_save_processed_symbol(processed_symbols, "processed_symbols", file_name)


async def fetch_and_update_stock_balance_sheet_profit_loss_cash_flow_standalone_data_async():
    db = SessionLocalSync()
    error_symbols = []
    unsaved_symbols = []
    processed_symbols = []
    file_name = "nse_bse_stocks_balance_sheet_and_profit_loss_and_cash_flow_data_standalone"
    try:
        pl_standalone = (
            select(ProfitLossDataset.id)
            .where(ProfitLossDataset.company_id == CompanyStock.id)
            .where(ProfitLossDataset.values["type"].astext == "Standalone")
            .correlate(CompanyStock)
        )

        bs_standalone = (
            select(BalanceSheetDataset.id)
            .where(BalanceSheetDataset.company_id == CompanyStock.id)
            .where(BalanceSheetDataset.values["type"].astext == "Standalone")
            .correlate(CompanyStock)
        )

        cf_standalone = (
            select(CashFlowDataset.id)
            .where(CashFlowDataset.company_id == CompanyStock.id)
            .where(CashFlowDataset.values["type"].astext == "Standalone")
            .correlate(CompanyStock)
        )

        stmt = (
            select(CompanyStock)
            .where(
                ~and_(
                    exists(pl_standalone),
                    exists(bs_standalone),
                    exists(cf_standalone),
                )
            )
            .options(
                selectinload(CompanyStock.details),
                selectinload(CompanyStock.profit_loss),
                selectinload(CompanyStock.balance_sheet),
                selectinload(CompanyStock.cash_flow),
            )
            .execution_options(yield_per=100)
        )

        result = db.execute(stmt)
        companies = result.scalars().all()

        existing_symbols = set()

        skipped_symbols_from_json = await fetch_symbols_from_covered_symbol_json_for_balance_sheet_and_profit_loss_and_cash_flow(file_name)
        existing_symbols.update(skipped_symbols_from_json)
        missing_symbols = [
            c for c in companies if c.nse_symbol not in existing_symbols
        ]

        missing_symbols = missing_symbols[:2]

        current_processed_symbols = [c.nse_symbol for c in missing_symbols]

        await update_nse_bse_balance_sheet_and_profit_loss_and_cash_flow_save_processed_symbol(current_processed_symbols, "processing", file_name)

        def chunk_list(data, size):
            for i in range(0, len(data), size):
                yield data[i:i + size]

        def get_consolidated(dataset_list):
            for d in dataset_list:
                values = json.loads(d.values) if isinstance(d.values, str) else d.values
                if values.get("type") == "Standalone":
                    d._parsed_values = values
                    return d
            return None

        for chunk in chunk_list(missing_symbols, 1):
            for company in chunk:
                try:
                    print(f"\nProcessing company: {company.id} | {company.name}")
                    with db.begin_nested():
                        nse_company_list = await fetch_nse_exact_symbol_data(company.nse_symbol)
                        bse_company_list = await fetch_bse_exact_symbol_data(company.nse_symbol)
                        profit_loss = get_consolidated(company.profit_loss)
                        balance_sheet = get_consolidated(company.balance_sheet)
                        cash_flow = get_consolidated(company.cash_flow)
                        if nse_company_list and bse_company_list:
                            html_data = await main_balance_sheet_standalone_html(company.nse_symbol)
                            soup = BeautifulSoup(html_data, "html.parser")
                            if not balance_sheet:
                                balance_sheet_data = await parse_balance_sheet(soup)
                                balance_sheet_data.update({"type": "Standalone"})
                            if not profit_loss:
                                profit_loss_data = await parse_profit_loss(soup)
                                profit_loss_data.update({"type": "Standalone"})
                            if not cash_flow:
                                cash_flow_data = await parse_cash_flow(soup)
                                cash_flow_data.update({"type": "Standalone"})
                        elif nse_company_list:
                            html_data = await main_balance_sheet_standalone_html(company.nse_symbol)
                            soup = BeautifulSoup(html_data, "html.parser")
                            if not balance_sheet:
                                balance_sheet_data = await parse_balance_sheet(soup)
                                balance_sheet_data.update({"type": "Standalone"})
                            if not profit_loss:
                                profit_loss_data = await parse_profit_loss(soup)
                                profit_loss_data.update({"type": "Standalone"})
                            if not cash_flow:
                                cash_flow_data = await parse_cash_flow(soup)
                                cash_flow_data.update({"type": "Standalone"})
                        elif bse_company_list:
                            c_name = await make_short_company_name(company.name)
                            c_list = await main_find_company_json(c_name)
                            exact_company = await find_by_scripcode(c_list, company.bse_code)
                            if exact_company:
                                html_data = await main_balance_sheet_standalone_html(f"SCRIP-{exact_company.get('FINCODE')}")
                                soup = BeautifulSoup(html_data, "html.parser")
                                if not balance_sheet:
                                    balance_sheet_data = await parse_balance_sheet(soup)
                                    balance_sheet_data.update({"type": "Standalone"})
                                if not profit_loss:
                                    profit_loss_data = await parse_profit_loss(soup)
                                    profit_loss_data.update({"type": "Standalone"})
                                if not cash_flow:
                                    cash_flow_data = await parse_cash_flow(soup)
                                    cash_flow_data.update({"type": "Standalone"})
                            else:
                                unsaved_symbols.append(company.nse_symbol)
                                continue
                        else:
                            unsaved_symbols.append(company.nse_symbol)
                            continue

                        if not balance_sheet:
                            balance_sheet_dataset_ops = BalanceSheetDataset(
                                company_id=company.id,
                                values=balance_sheet_data,
                            )
                            db.add(balance_sheet_dataset_ops)
                            db.flush()

                        if not profit_loss:
                            profit_loss_dataset_ops = ProfitLossDataset(
                                company_id=company.id,
                                values=profit_loss_data,
                            )
                            db.add(profit_loss_dataset_ops)
                            db.flush()

                        if not cash_flow:
                            profit_loss_dataset_ops = CashFlowDataset(
                                company_id=company.id,
                                values=cash_flow_data,
                            )
                            db.add(profit_loss_dataset_ops)
                            db.flush()

                        processed_symbols.append(company.nse_symbol)


                except Exception as e:
                    print("Error:", str(e))
                    print(f"\nFAILED company: {company.id} | {company.name}")
                    error_symbols.append(company.nse_symbol)
                    continue
        db.commit()
    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()
        await update_nse_bse_shareholder_save_processed_symbol(unsaved_symbols, "data_not_available", file_name)
        await update_nse_bse_shareholder_save_processed_symbol(error_symbols, "error", file_name)
        await update_nse_bse_shareholder_save_processed_symbol(processed_symbols, "processed_symbols", file_name)


async def fetch_calculate_and_update_stock_dividend_data_async():
    db = SessionLocalSync()
    error_symbols = []
    unsaved_symbols = []
    processed_symbols = []
    file_name = "nse_bse_stocks_dividend"
    try:
        stmt = (
            select(CompanyStock)
            .outerjoin(
                KeyDetailsForCS,
                CompanyStock.id == KeyDetailsForCS.company_id
            )
            .options(selectinload(CompanyStock.details))
            .execution_options(yield_per=100)
        )

        result = db.execute(stmt)
        companies = result.scalars().all()

        existing_symbols = set()

        skipped_symbols_from_json = await fetch_symbols_from_covered_symbol_json_for_balance_sheet_and_profit_loss_and_cash_flow(file_name)
        existing_symbols.update(skipped_symbols_from_json)
        missing_symbols = [
            c for c in companies if c.nse_symbol not in existing_symbols
        ]

        missing_symbols = missing_symbols[:5]

        current_processed_symbols = [c.nse_symbol for c in missing_symbols]
        await update_nse_bse_balance_sheet_and_profit_loss_and_cash_flow_save_processed_symbol(current_processed_symbols, "processing", file_name)

        def chunk_list(data, size):
            for i in range(0, len(data), size):
                yield data[i:i + size]

        def to_percentage(val):
            return (Decimal(str(val)) * 100).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        for chunk in chunk_list(missing_symbols, 1):
            for company in chunk:
                try:
                    print(f"\nProcessing company: {company.id} | {company.name}")
                    dividend_yield = None
                    savepoint = db.begin_nested()
                    try:
                        nse_company_list = await fetch_nse_exact_symbol_data(company.nse_symbol)
                        bse_company_list = await fetch_bse_exact_symbol_data(company.nse_symbol)
                        if nse_company_list and bse_company_list:
                            nse_data = await main(company.nse_symbol)
                            symbol_data = nse_data.get('symbolData')
                            equity_response = symbol_data.get('equityResponse')[0]
                            metaData = equity_response.get('metaData', {})
                            tradeInfo = equity_response.get('tradeInfo', {})
                            close_price = metaData.get("closePrice") or tradeInfo.get("lastPrice")
                            company_name = nse_company_list[0].get("company_name").replace(' ', '-')
                            c_date = datetime.now(ist).date()
                            one_year_ago = c_date.replace(year=c_date.year - 1)
                            corporate_action_list = await main_nse_corporate_action_call(company.nse_symbol, company_name)
                            filtered_corporate_action_list = [
                               item for item in corporate_action_list
                               if item['exDate'] and item['exDate'] != '-'
                                  and one_year_ago <= ist.localize(
                                   datetime.strptime(item['exDate'], '%d-%b-%Y')).date() <= c_date
                            ]
                            df_corporate_action = await fetch_dividend_values(filtered_corporate_action_list)
                            if not df_corporate_action.empty:
                                total_dividend = df_corporate_action['Amount (Rs)'].sum()
                                dividend_yield = to_percentage((total_dividend / close_price))
                        elif nse_company_list:
                            nse_data = await main(company.nse_symbol)
                            symbol_data = nse_data.get('symbolData')
                            equity_response = symbol_data.get('equityResponse')[0]
                            metaData = equity_response.get('metaData', {})
                            tradeInfo = equity_response.get('tradeInfo', {})
                            close_price = metaData.get("closePrice") or tradeInfo.get("lastPrice")
                            company_name = nse_company_list[0].get("company_name").replace(' ', '-')
                            c_date = datetime.now(ist).date()
                            one_year_ago = c_date.replace(year=c_date.year - 1)
                            corporate_action_list = await main_nse_corporate_action_call(company.nse_symbol,
                                                                                         company_name)
                            filtered_corporate_action_list = [
                                item for item in corporate_action_list
                                if item['exDate'] and item['exDate'] != '-'
                                   and one_year_ago <= ist.localize(
                                    datetime.strptime(item['exDate'], '%d-%b-%Y')).date() <= c_date
                            ]
                            df_corporate_action = await fetch_dividend_values(filtered_corporate_action_list)
                            if not df_corporate_action.empty:
                                total_dividend = df_corporate_action['Amount (Rs)'].sum()
                                dividend_yield = to_percentage((total_dividend / close_price))
                        elif bse_company_list:
                            security_code = bse_company_list[0].get("bse_code")
                            bse_data = await main_bse(security_code)
                            price_graph = bse_data.get('priceGraph')
                            current_price = price_graph.get('CurrVal')
                            if current_price:
                                current_price = float(current_price)
                            c_date = datetime.now(ist).date()
                            one_year_ago = c_date.replace(year=c_date.year - 1)
                            corporate_action_list = await main_bse_corporate_action_call(company.bse_code)
                            filtered_corporate_action_list = [
                                item for item in corporate_action_list
                                if item['Ex_date'] and item['Ex_date'] != '-'
                                   and one_year_ago <= ist.localize(
                                    datetime.strptime(item['Ex_date'], '%d %b %Y')).date() <= c_date
                            ]
                            df_corporate_action = await fetch_dividend_values(filtered_corporate_action_list)
                            if not df_corporate_action.empty:
                                total_dividend = df_corporate_action['Amount (Rs)'].sum()
                                dividend_yield = to_percentage((total_dividend / current_price))
                        else:
                            unsaved_symbols.append(company.nse_symbol)
                            continue
                        company.details.dividend_yield = dividend_yield
                        processed_symbols.append(company.nse_symbol)
                    except Exception as e:
                        savepoint.rollback()
                        raise

                except Exception as e:
                    print("Error:", str(e))
                    print(f"\nFAILED company: {company.id} | {company.name}")
                    error_symbols.append(company.nse_symbol)
                    continue
        db.commit()
    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()
        await update_nse_bse_shareholder_save_processed_symbol(unsaved_symbols, "data_not_available", file_name)
        await update_nse_bse_shareholder_save_processed_symbol(error_symbols, "error", file_name)
        await update_nse_bse_shareholder_save_processed_symbol(processed_symbols, "processed_symbols", file_name)


async def fetch_calculate_and_update_stock_book_value_data_async():
    db = SessionLocalSync()
    error_symbols = []
    unsaved_symbols = []
    processed_symbols = []
    file_name = "nse_bse_stocks_book_value"
    try:
        stmt = (
            select(CompanyStock)
            .outerjoin(
                KeyDetailsForCS,
                CompanyStock.id == KeyDetailsForCS.company_id
            )
            .options(selectinload(CompanyStock.details))
            .execution_options(yield_per=100)
        )

        result = db.execute(stmt)
        companies = result.scalars().all()

        existing_symbols = set()

        skipped_symbols_from_json = await fetch_symbols_from_covered_symbol_json_for_balance_sheet_and_profit_loss_and_cash_flow(file_name)
        existing_symbols.update(skipped_symbols_from_json)
        missing_symbols = [
            c for c in companies if c.nse_symbol not in existing_symbols
        ]

        missing_symbols = missing_symbols[:5]

        current_processed_symbols = [c.nse_symbol for c in missing_symbols]
        await update_nse_bse_balance_sheet_and_profit_loss_and_cash_flow_save_processed_symbol(current_processed_symbols, "processing", file_name)

        def chunk_list(data, size):
            for i in range(0, len(data), size):
                yield data[i:i + size]


        def to_decimal(val):
            try:
                return Decimal(str(val or "0").replace(",", ""))
            except Exception:
                return Decimal("0")

        def find_by_date(data, target_date):
            return next((item for item in data if item["date"] == target_date), None)

        for chunk in chunk_list(missing_symbols, 1):
            for company in chunk:
                try:
                    print(f"\nProcessing company: {company.id} | {company.name}")
                    savepoint = db.begin_nested()
                    try:
                        nse_company_list = await fetch_nse_exact_symbol_data(company.nse_symbol)
                        bse_company_list = await fetch_bse_exact_symbol_data(company.nse_symbol)
                        book_value = None
                        if nse_company_list and bse_company_list:
                            integrated_filing_financials_list = await main_fetch_integrated_filing_financials(
                                company.nse_symbol, "equity")
                            total_equity = 0
                            col_iv = 0
                            output_obj = {}
                            if integrated_filing_financials_list:
                                consolidated_list = []
                                standalone_list = []
                                for integrated_filing_obj in integrated_filing_financials_list.get("data"):
                                    consolidated = integrated_filing_obj.get("consolidated")
                                    type_sub = integrated_filing_obj.get("type_Sub")
                                    if type_sub == "Revision":
                                        continue
                                    if consolidated == "Consolidated":
                                        consolidated_list.append(integrated_filing_obj)
                                    elif consolidated == "Standalone":
                                        standalone_list.append(integrated_filing_obj)
                                if consolidated_list:
                                    for i in consolidated_list:
                                        ixbrl = i.get("ixbrl")
                                        qe_Date = i.get("qe_Date")
                                        output, amount_type, format_type = await fetch_integrated_filing_financials_data_from_nse_for_book_value(
                                                ixbrl)
                                        if output:
                                            output['date'] = qe_Date
                                            output['amount_type'] = amount_type
                                            output['format_type'] = format_type
                                            output_obj = output
                                            break
                                if not output_obj:
                                    for i in standalone_list:
                                        ixbrl = i.get("ixbrl")
                                        qe_Date = i.get("qe_Date")
                                        output, amount_type, format_type = await fetch_integrated_filing_financials_data_from_nse_for_book_value(
                                                ixbrl)
                                        if output:
                                            output['date'] = qe_Date
                                            output['amount_type'] = amount_type
                                            output['format_type'] = format_type
                                            output_obj = output
                                            break

                                if not output_obj:
                                    unsaved_symbols.append(company.nse_symbol)
                                    continue

                                if output_obj.get("format_type") == "LI":
                                    share_capital = to_decimal(output_obj.get("Share capital"))
                                    reserves_and_surplus = to_decimal(output_obj.get("Reserves and surplus"))
                                    total_equity = share_capital + reserves_and_surplus
                                elif output_obj.get("format_type") == "INDAS":
                                    total_equity = to_decimal(output_obj.get("Total equity attributable to owners of parent"))
                                elif output_obj.get("format_type") == "BANKING":
                                    capital = to_decimal(output_obj.get("Capital"))
                                    reserves_and_surplus = to_decimal(output_obj.get("Reserves and surplus"))
                                    total_equity = capital + reserves_and_surplus
                                elif output_obj.get("format_type") == "NBFC":
                                    total_equity = to_decimal(output_obj.get("Total equity attributable to owners of parent"))
                                elif output_obj.get("format_type") == "GI":
                                    share_capital = to_decimal(output_obj.get("Share capital"))
                                    reserves_and_surplus = to_decimal(output_obj.get("Reserves and surplus"))
                                    shareholder_fund = to_decimal(output_obj.get("(b)"))
                                    total_equity = share_capital + reserves_and_surplus + shareholder_fund
                                if output_obj.get("amount_type") == "Lakhs":
                                    total_equity = total_equity * 100000

                                if total_equity:
                                    shareholding_list = await main_nse_fetch_shareholding_list(company.nse_symbol,
                                                                                               "equities")
                                    shareholding_obj = find_by_date(shareholding_list, output_obj.get("date"))
                                    if shareholding_obj:
                                        row = {
                                            "date": shareholding_obj.get("date"),
                                            "remarksWeb": shareholding_obj.get("remarksWeb"),
                                            "revisionRemark": shareholding_obj.get("revisionRemark"),
                                            "revisionDate": shareholding_obj.get("revisionDate"),
                                        }
                                        shareholding_all_data = await main_nse_fetch_shareholding_data_using_api_for_book_value(
                                            id="popup1",
                                            symbol=shareholding_obj.get("symbol"),
                                            name=shareholding_obj.get("name"),
                                            rec_id=shareholding_obj.get("recordId"),
                                            row=row)
                                        if shareholding_all_data.get("type") == "data":
                                            total_row = next(
                                                item for item in shareholding_all_data['result']['data']['summary']
                                                if item['COL_II'] == 'Total'
                                            )
                                            col_iv = total_row['COL_VII']
                                            col_iv = int(col_iv)
                                    else:
                                        unsaved_symbols.append(company.nse_symbol)
                                        continue

                                    if col_iv:
                                        book_value = total_equity / col_iv
                                        book_value = round(book_value, 2)
                            else:
                                unsaved_symbols.append(company.nse_symbol)
                                continue
                        elif nse_company_list:
                            integrated_filing_financials_list = await main_fetch_integrated_filing_financials(
                                company.nse_symbol, "equity")
                            total_equity = 0
                            col_iv = 0
                            output_obj = {}
                            if integrated_filing_financials_list:
                                consolidated_list = []
                                standalone_list = []
                                for integrated_filing_obj in integrated_filing_financials_list.get("data"):
                                    consolidated = integrated_filing_obj.get("consolidated")
                                    type_sub = integrated_filing_obj.get("type_Sub")
                                    if type_sub == "Revision":
                                        continue
                                    if consolidated == "Consolidated":
                                        consolidated_list.append(integrated_filing_obj)
                                    elif consolidated == "Standalone":
                                        standalone_list.append(integrated_filing_obj)
                                if consolidated_list:
                                    for i in consolidated_list:
                                        ixbrl = i.get("ixbrl")
                                        qe_Date = i.get("qe_Date")
                                        output, amount_type, format_type = await fetch_integrated_filing_financials_data_from_nse_for_book_value(
                                            ixbrl)
                                        if output:
                                            output['date'] = qe_Date
                                            output['amount_type'] = amount_type
                                            output['format_type'] = format_type
                                            output_obj = output
                                            break
                                if not output_obj:
                                    for i in standalone_list:
                                        ixbrl = i.get("ixbrl")
                                        qe_Date = i.get("qe_Date")
                                        output, amount_type, format_type = await fetch_integrated_filing_financials_data_from_nse_for_book_value(
                                            ixbrl)
                                        if output:
                                            output['date'] = qe_Date
                                            output['amount_type'] = amount_type
                                            output['format_type'] = format_type
                                            output_obj = output
                                            break

                                if not output_obj:
                                    unsaved_symbols.append(company.nse_symbol)
                                    continue

                                if output_obj.get("format_type") == "LI":
                                    share_capital = to_decimal(output_obj.get("Share capital"))
                                    reserves_and_surplus = to_decimal(output_obj.get("Reserves and surplus"))
                                    total_equity = share_capital + reserves_and_surplus
                                elif output_obj.get("format_type") == "INDAS":
                                    total_equity = to_decimal(
                                        output_obj.get("Total equity attributable to owners of parent"))
                                elif output_obj.get("format_type") == "BANKING":
                                    capital = to_decimal(output_obj.get("Capital"))
                                    reserves_and_surplus = to_decimal(output_obj.get("Reserves and surplus"))
                                    total_equity = capital + reserves_and_surplus
                                elif output_obj.get("format_type") == "NBFC":
                                    total_equity = to_decimal(
                                        output_obj.get("Total equity attributable to owners of parent"))
                                elif output_obj.get("format_type") == "GI":
                                    share_capital = to_decimal(output_obj.get("Share capital"))
                                    reserves_and_surplus = to_decimal(output_obj.get("Reserves and surplus"))
                                    shareholder_fund = to_decimal(output_obj.get("(b)"))
                                    total_equity = share_capital + reserves_and_surplus + shareholder_fund
                                if output_obj.get("amount_type") == "Lakhs":
                                    total_equity = total_equity * 100000

                                if total_equity:
                                    shareholding_list = await main_nse_fetch_shareholding_list(company.nse_symbol,
                                                                                               "equities")
                                    shareholding_obj = find_by_date(shareholding_list, output_obj.get("date"))
                                    if shareholding_obj:
                                        row = {
                                            "date": shareholding_obj.get("date"),
                                            "remarksWeb": shareholding_obj.get("remarksWeb"),
                                            "revisionRemark": shareholding_obj.get("revisionRemark"),
                                            "revisionDate": shareholding_obj.get("revisionDate"),
                                        }
                                        shareholding_all_data = await main_nse_fetch_shareholding_data_using_api_for_book_value(
                                            id="popup1",
                                            symbol=shareholding_obj.get("symbol"),
                                            name=shareholding_obj.get("name"),
                                            rec_id=shareholding_obj.get("recordId"),
                                            row=row)
                                        if shareholding_all_data.get("type") == "data":
                                            total_row = next(
                                                item for item in shareholding_all_data['result']['data']['summary']
                                                if item['COL_II'] == 'Total'
                                            )
                                            col_iv = total_row['COL_VII']
                                            col_iv = int(col_iv)
                                    else:
                                        unsaved_symbols.append(company.nse_symbol)
                                        continue

                                    if col_iv:
                                        book_value = total_equity / col_iv
                                        book_value = round(book_value, 2)
                            else:
                                unsaved_symbols.append(company.nse_symbol)
                                continue
                        elif bse_company_list:
                            integrated_filing_financials_list = await main_bse_fetch_integrated_filing_financials(
                                company.bse_code)
                            total_equity = 0
                            col_iv = 0
                            output_obj = {}
                            if integrated_filing_financials_list:
                                consolidated_list = []
                                standalone_list = []
                                for integrated_filing_obj in integrated_filing_financials_list.get("Table"):
                                    financial_name_obj = await parse_financial_name(
                                        integrated_filing_obj.get("Quarter_Name"))
                                    consolidated = financial_name_obj.get("type")
                                    if consolidated == "standalone" and financial_name_obj.get("period") == "qtr":
                                        standalone_list.append(integrated_filing_obj)
                                    if consolidated == "consolidated" and financial_name_obj.get("period") == "qtr":
                                        consolidated_list.append(integrated_filing_obj)
                                if consolidated_list:
                                    for i in consolidated_list:
                                        ixbrl = i.get("xbrlurl")
                                        ixbrl = f"https://www.bseindia.com{ixbrl}"
                                        financial_name_obj = await parse_financial_name(
                                            i.get("Quarter_Name"))
                                        qe_date = f"{financial_name_obj.get('month')}-{financial_name_obj.get('year')}"
                                        output, amount_type, format_type = await fetch_integrated_filing_financials_data_from_bse_for_book_value(
                                            ixbrl)
                                        if output:
                                            output['date'] = qe_date
                                            output['amount_type'] = amount_type
                                            output['format_type'] = format_type
                                            output['Qtrid'] = i.get("Qtrid")
                                            output_obj = output
                                            break
                                if not output_obj:
                                    for st in standalone_list:
                                        ixbrl = st.get("xbrlurl")
                                        ixbrl = f"https://www.bseindia.com{ixbrl}"
                                        financial_name_obj = await parse_financial_name(
                                            st.get("Quarter_Name"))
                                        qe_date = f"{financial_name_obj.get('month')}-{financial_name_obj.get('year')}"
                                        output, amount_type, format_type = await fetch_integrated_filing_financials_data_from_bse_for_book_value(
                                            ixbrl)
                                        if output:
                                            output['date'] = qe_date
                                            output['amount_type'] = amount_type
                                            output['format_type'] = format_type
                                            output['Qtrid'] = st.get("Qtrid")
                                            output_obj = output
                                            break

                                if not output_obj:
                                    unsaved_symbols.append(company.nse_symbol)
                                    continue

                                if output_obj.get("format_type") == "INDAS":
                                    total_equity = to_decimal(
                                        output_obj.get("Total equity attributable to owners of parent"))
                                elif output_obj.get("format_type") == "BANKING":
                                    capital = to_decimal(output_obj.get("Capital"))
                                    reserves_and_surplus = to_decimal(output_obj.get("Reserves and surplus"))
                                    total_equity = capital + reserves_and_surplus
                                elif output_obj.get("format_type") == "NBFC":
                                    total_equity = to_decimal(
                                        output_obj.get("Total equity attributable to owners of parent"))
                                elif output_obj.get("format_type") == "General Insurance":
                                    share_capital = to_decimal(output_obj.get("Share capital"))
                                    reserves_and_surplus = to_decimal(output_obj.get("Reserves and surplus"))
                                    shareholder_fund = to_decimal(output_obj.get("Shareholders funds"))
                                    total_equity = share_capital + reserves_and_surplus + shareholder_fund
                                elif output_obj.get("format_type") == "Life Insurance":
                                    share_capital = to_decimal(output_obj.get("Share capital"))
                                    reserves_and_surplus = to_decimal(output_obj.get("Reserves and surplus"))
                                    total_equity = share_capital + reserves_and_surplus

                                if output_obj.get("amount_type") == "Lakhs":
                                    total_equity = total_equity * 100000
                                elif output_obj.get("amount_type") == "Crores":
                                    total_equity = total_equity * 10000000

                                if total_equity:
                                    shareholding_obj = await main_bse_cshp_fetch_shareholding_list(output_obj.get("Qtrid"),
                                                                                               company.bse_code)
                                    if shareholding_obj:
                                        table1 = shareholding_obj.get("Table1")
                                        grand_total = next(
                                            (item for item in table1 if item.get("Fld_ShortName") == "Grand Total"),
                                            None
                                        )
                                        col_iv = grand_total.get("Fld_TotalNoOfShares")
                                    else:
                                        unsaved_symbols.append(company.nse_symbol)
                                        continue

                                    if col_iv:
                                        book_value = total_equity / col_iv
                                        book_value = round(book_value, 2)
                            else:
                                unsaved_symbols.append(company.nse_symbol)
                                continue

                        else:
                            unsaved_symbols.append(company.nse_symbol)
                            continue
                        company.details.book_value = book_value
                        processed_symbols.append(company.nse_symbol)
                    except Exception as e:
                        savepoint.rollback()
                        raise

                except Exception as e:
                    print("Error:", str(e))
                    print(f"\nFAILED company: {company.id} | {company.name}")
                    error_symbols.append(company.nse_symbol)
                    continue
        db.commit()
    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()
        await update_nse_bse_shareholder_save_processed_symbol(unsaved_symbols, "data_not_available", file_name)
        await update_nse_bse_shareholder_save_processed_symbol(error_symbols, "error", file_name)
        await update_nse_bse_shareholder_save_processed_symbol(processed_symbols, "processed_symbols", file_name)


async def fetch_calculate_and_update_stock_roce_data_async():
    db = SessionLocalSync()
    error_symbols = []
    unsaved_symbols = []
    processed_symbols = []
    file_name = "nse_bse_stocks_roce"
    try:
        stmt = (
            select(CompanyStock)
            .outerjoin(
                KeyDetailsForCS,
                CompanyStock.id == KeyDetailsForCS.company_id
            )
            .options(selectinload(CompanyStock.details))
            .execution_options(yield_per=100)
        )

        result = db.execute(stmt)
        companies = result.scalars().all()

        existing_symbols = set()

        skipped_symbols_from_json = await fetch_symbols_from_covered_symbol_json_for_balance_sheet_and_profit_loss_and_cash_flow(file_name)
        existing_symbols.update(skipped_symbols_from_json)
        missing_symbols = [
            c for c in companies if c.nse_symbol not in existing_symbols
        ]

        missing_symbols = missing_symbols[:5]

        current_processed_symbols = [c.nse_symbol for c in missing_symbols]
        await update_nse_bse_balance_sheet_and_profit_loss_and_cash_flow_save_processed_symbol(current_processed_symbols, "processing", file_name)

        def chunk_list(data, size):
            for i in range(0, len(data), size):
                yield data[i:i + size]


        def to_decimal(val):
            try:
                return Decimal(str(val or "0").replace(",", ""))
            except Exception:
                return Decimal("0")

        def find_by_date(data, target_date):
            return next((item for item in data if item["date"] == target_date), None)

        for chunk in chunk_list(missing_symbols, 1):
            for company in chunk:
                try:
                    print(f"\nProcessing company: {company.id} | {company.name}")
                    savepoint = db.begin_nested()
                    try:
                        nse_company_list = await fetch_nse_exact_symbol_data(company.nse_symbol)
                        bse_company_list = await fetch_bse_exact_symbol_data(company.nse_symbol)
                        roce = None
                        if nse_company_list and bse_company_list:
                            integrated_filing_financials_list = await main_fetch_integrated_filing_financials(
                                company.nse_symbol, "equity")
                            output_obj = []
                            output_financial_data_list = []
                            ebit = 0
                            if integrated_filing_financials_list:
                                consolidated_list = []
                                standalone_list = []
                                for integrated_filing_obj in integrated_filing_financials_list.get("data"):
                                    consolidated = integrated_filing_obj.get("consolidated")
                                    type_sub = integrated_filing_obj.get("type_Sub")
                                    qe_date = integrated_filing_obj.get("qe_Date")
                                    if type_sub == "Revision":
                                        continue
                                    if consolidated == "Consolidated" and "MAR" in qe_date:
                                        consolidated_list.append(integrated_filing_obj)
                                    elif consolidated == "Standalone" and "MAR" in qe_date:
                                        standalone_list.append(integrated_filing_obj)
                                if consolidated_list and len(consolidated_list) == 2:
                                    for i in consolidated_list:
                                        ixbrl = i.get("ixbrl")
                                        qe_Date = i.get("qe_Date")
                                        output, amount_type, format_type = await fetch_integrated_filing_financials_data_from_nse_for_book_value(
                                                ixbrl)
                                        output['date'] = qe_Date
                                        output['amount_type'] = amount_type
                                        output['format_type'] = format_type

                                        output_f_data, amount_type, format_type = await fetch_integrated_filing_financials_data_for_roce_from_nse(
                                            ixbrl)
                                        output_obj.append(output)
                                        output_f_data['date'] = qe_Date
                                        output_f_data['amount_type'] = amount_type
                                        output_f_data['format_type'] = format_type
                                        output_financial_data_list.append(output_f_data)

                                if not output_obj or not output_financial_data_list:
                                    output_obj = []
                                    output_financial_data_list = []
                                    if standalone_list and len(standalone_list) == 2:
                                        for i in standalone_list:
                                            ixbrl = i.get("ixbrl")
                                            qe_Date = i.get("qe_Date")
                                            output, amount_type, format_type = await fetch_integrated_filing_financials_data_from_nse_for_book_value(
                                                    ixbrl)
                                            output['date'] = qe_Date
                                            output['amount_type'] = amount_type
                                            output['format_type'] = format_type
                                            output_f_data, amount_type, format_type = await fetch_integrated_filing_financials_data_for_roce_from_nse(
                                                ixbrl)
                                            output_obj.append(output)
                                            output_f_data['date'] = qe_Date
                                            output_f_data['amount_type'] = amount_type
                                            output_f_data['format_type'] = format_type
                                            output_financial_data_list.append(output_f_data)

                                if not output_obj or not output_financial_data_list:
                                    unsaved_symbols.append(company.nse_symbol)
                                    continue
                                ist = pytz.timezone("Asia/Kolkata")
                                current_year = datetime.now(ist).year
                                for output_financial_data_obj in output_financial_data_list:
                                    if f"MAR-{current_year}" in output_financial_data_obj.get("date"):
                                        if output_financial_data_obj.get("format_type") == "INDAS":
                                            ebit = output_financial_data_obj.get("Finance costs") + output_financial_data_obj.get("Total profit before tax")
                                            if output_financial_data_obj.get("amount_type") == "Crores":
                                                ebit = ebit / 100000
                                        elif output_financial_data_obj.get("format_type") == "BANKING":
                                            ebit = output_financial_data_obj.get(
                                                "Total profit (loss) from ordinary activities before tax") + output_financial_data_obj.get(
                                                "Interest expenses")
                                            if output_financial_data_obj.get("amount_type") == "Crores":
                                                ebit = ebit / 100000
                                        elif output_financial_data_obj.get("format_type") == "NBFC":
                                            ebit = output_financial_data_obj.get(
                                                "Total profit before tax") + output_financial_data_obj.get(
                                                "Finance costs")
                                            if output_financial_data_obj.get("amount_type") == "Crores":
                                                ebit = ebit / 100000
                                        elif output_financial_data_obj.get("format_type") == "GI":
                                            ebit = output_financial_data_obj.get(
                                                "Profit / Loss before extraordinary items")
                                            if output_financial_data_obj.get("amount_type") == "Crores":
                                                ebit = ebit / 100000
                                        elif output_financial_data_obj.get("format_type") == "LI":
                                            ebit = output_financial_data_obj.get(
                                                "Profit/ (loss) before tax")
                                            if output_financial_data_obj.get("amount_type") == "Crores":
                                                ebit = ebit / 100000
                                total_capital_employed = 0
                                for out in output_obj:
                                    if out.get("format_type") == "INDAS":
                                        equity_share_capital = to_decimal(out.get("Equity share capital"))
                                        other_equity = to_decimal(out.get("Other equity"))
                                        borrowing_current = to_decimal(out.get("Borrowings, current"))
                                        borrowing_non_current = to_decimal(out.get("Borrowings, non-current"))
                                        total = equity_share_capital + other_equity + borrowing_current + borrowing_non_current
                                        if out.get("amount_type") == "Crores":
                                            total = total / 100000
                                        total_capital_employed += total
                                    elif out.get("format_type") == "BANKING":
                                        equity_share_capital = to_decimal(out.get("Total Assets"))
                                        total = equity_share_capital
                                        if out.get("amount_type") == "Crores":
                                            total = total / 100000
                                        total_capital_employed += total
                                    elif out.get("format_type") == "NBFC":
                                        equity_share_capital = to_decimal(out.get("Equity share capital"))
                                        other_equity = to_decimal(out.get("Other equity"))
                                        debt_securities = to_decimal(out.get("Debt Securities"))
                                        borrowing = to_decimal(out.get("Borrowings (Other than Debt Securities)"))
                                        deposits = to_decimal(out.get("Deposits"))
                                        subordinated_liabilities = to_decimal(out.get("Subordinated Liabilities"))
                                        total = equity_share_capital + other_equity + debt_securities + borrowing + deposits + subordinated_liabilities
                                        if out.get("amount_type") == "Crores":
                                            total = total / 100000
                                        total_capital_employed += total
                                    elif out.get("format_type") == "GI":
                                        equity_share_capital = to_decimal(out.get("Share capital"))
                                        other_equity = to_decimal(out.get("Reserves and surplus"))
                                        borrowings = to_decimal(out.get("Borrowings"))
                                        total = equity_share_capital + other_equity + borrowings
                                        if out.get("amount_type") == "Crores":
                                            total = total / 100000
                                        total_capital_employed += total
                                    elif out.get("format_type") == "LI":
                                        equity_share_capital = to_decimal(out.get("Share capital"))
                                        other_equity = to_decimal(out.get("Reserves and surplus"))
                                        share_application_money = to_decimal(out.get("Share application money received pending allotment of shares"))
                                        credit_fair_value = to_decimal(out.get("Credit (Debit) Fair value change account"))
                                        borrowings = to_decimal(out.get("Borrowings"))
                                        funds_for_appropriations = to_decimal(out.get("Funds for Future Appropriations"))
                                        total = equity_share_capital + other_equity + share_application_money + credit_fair_value + borrowings + funds_for_appropriations
                                        if out.get("amount_type") == "Crores":
                                            total = total / 100000
                                        total_capital_employed += total
                                if total_capital_employed and ebit:
                                    avg_capital_employed = float(total_capital_employed) / 2
                                    roce = (float(ebit) / avg_capital_employed) * 100
                                    roce = round(roce, 2)

                            else:
                                unsaved_symbols.append(company.nse_symbol)
                                continue
                        elif nse_company_list:
                            integrated_filing_financials_list = await main_fetch_integrated_filing_financials(
                                company.nse_symbol, "equity")
                            output_obj = []
                            output_financial_data_list = []
                            ebit = 0
                            if integrated_filing_financials_list:
                                consolidated_list = []
                                standalone_list = []
                                for integrated_filing_obj in integrated_filing_financials_list.get("data"):
                                    consolidated = integrated_filing_obj.get("consolidated")
                                    type_sub = integrated_filing_obj.get("type_Sub")
                                    qe_date = integrated_filing_obj.get("qe_Date")
                                    if type_sub == "Revision":
                                        continue
                                    if consolidated == "Consolidated" and "MAR" in qe_date:
                                        consolidated_list.append(integrated_filing_obj)
                                    elif consolidated == "Standalone" and "MAR" in qe_date:
                                        standalone_list.append(integrated_filing_obj)
                                if consolidated_list and len(consolidated_list) == 2:
                                    for i in consolidated_list:
                                        ixbrl = i.get("ixbrl")
                                        qe_Date = i.get("qe_Date")
                                        output, amount_type, format_type = await fetch_integrated_filing_financials_data_from_nse_for_book_value(
                                            ixbrl)
                                        output['date'] = qe_Date
                                        output['amount_type'] = amount_type
                                        output['format_type'] = format_type

                                        output_f_data, amount_type, format_type = await fetch_integrated_filing_financials_data_for_roce_from_nse(
                                            ixbrl)
                                        output_obj.append(output)
                                        output_f_data['date'] = qe_Date
                                        output_f_data['amount_type'] = amount_type
                                        output_f_data['format_type'] = format_type
                                        output_financial_data_list.append(output_f_data)

                                if not output_obj or not output_financial_data_list:
                                    output_obj = []
                                    output_financial_data_list = []
                                    if standalone_list and len(standalone_list) == 2:
                                        for i in standalone_list:
                                            ixbrl = i.get("ixbrl")
                                            qe_Date = i.get("qe_Date")
                                            output, amount_type, format_type = await fetch_integrated_filing_financials_data_from_nse_for_book_value(
                                                ixbrl)
                                            output['date'] = qe_Date
                                            output['amount_type'] = amount_type
                                            output['format_type'] = format_type
                                            output_f_data, amount_type, format_type = await fetch_integrated_filing_financials_data_for_roce_from_nse(
                                                ixbrl)
                                            output_obj.append(output)
                                            output_f_data['date'] = qe_Date
                                            output_f_data['amount_type'] = amount_type
                                            output_f_data['format_type'] = format_type
                                            output_financial_data_list.append(output_f_data)

                                if not output_obj or not output_financial_data_list:
                                    unsaved_symbols.append(company.nse_symbol)
                                    continue
                                ist = pytz.timezone("Asia/Kolkata")
                                current_year = datetime.now(ist).year
                                for output_financial_data_obj in output_financial_data_list:
                                    if f"MAR-{current_year}" in output_financial_data_obj.get("date"):
                                        if output_financial_data_obj.get("format_type") == "INDAS":
                                            ebit = output_financial_data_obj.get(
                                                "Finance costs") + output_financial_data_obj.get(
                                                "Total profit before tax")
                                            if output_financial_data_obj.get("amount_type") == "Crores":
                                                ebit = ebit / 100000
                                        elif output_financial_data_obj.get("format_type") == "BANKING":
                                            ebit = output_financial_data_obj.get(
                                                "Total profit (loss) from ordinary activities before tax") + output_financial_data_obj.get(
                                                "Interest expenses")
                                            if output_financial_data_obj.get("amount_type") == "Crores":
                                                ebit = ebit / 100000
                                        elif output_financial_data_obj.get("format_type") == "NBFC":
                                            ebit = output_financial_data_obj.get(
                                                "Total profit before tax") + output_financial_data_obj.get(
                                                "Finance costs")
                                            if output_financial_data_obj.get("amount_type") == "Crores":
                                                ebit = ebit / 100000
                                        elif output_financial_data_obj.get("format_type") == "GI":
                                            ebit = output_financial_data_obj.get(
                                                "Profit / Loss before extraordinary items")
                                            if output_financial_data_obj.get("amount_type") == "Crores":
                                                ebit = ebit / 100000
                                        elif output_financial_data_obj.get("format_type") == "LI":
                                            ebit = output_financial_data_obj.get(
                                                "Profit/ (loss) before tax")
                                            if output_financial_data_obj.get("amount_type") == "Crores":
                                                ebit = ebit / 100000
                                total_capital_employed = 0
                                for out in output_obj:
                                    if out.get("format_type") == "INDAS":
                                        equity_share_capital = to_decimal(out.get("Equity share capital"))
                                        other_equity = to_decimal(out.get("Other equity"))
                                        borrowing_current = to_decimal(out.get("Borrowings, current"))
                                        borrowing_non_current = to_decimal(out.get("Borrowings, non-current"))
                                        total = equity_share_capital + other_equity + borrowing_current + borrowing_non_current
                                        if out.get("amount_type") == "Crores":
                                            total = total / 100000
                                        total_capital_employed += total
                                    elif out.get("format_type") == "BANKING":
                                        equity_share_capital = to_decimal(out.get("Total Assets"))
                                        total = equity_share_capital
                                        if out.get("amount_type") == "Crores":
                                            total = total / 100000
                                        total_capital_employed += total
                                    elif out.get("format_type") == "NBFC":
                                        equity_share_capital = to_decimal(out.get("Equity share capital"))
                                        other_equity = to_decimal(out.get("Other equity"))
                                        debt_securities = to_decimal(out.get("Debt Securities"))
                                        borrowing = to_decimal(out.get("Borrowings (Other than Debt Securities)"))
                                        deposits = to_decimal(out.get("Deposits"))
                                        subordinated_liabilities = to_decimal(out.get("Subordinated Liabilities"))
                                        total = equity_share_capital + other_equity + debt_securities + borrowing + deposits + subordinated_liabilities
                                        if out.get("amount_type") == "Crores":
                                            total = total / 100000
                                        total_capital_employed += total
                                    elif out.get("format_type") == "GI":
                                        equity_share_capital = to_decimal(out.get("Share capital"))
                                        other_equity = to_decimal(out.get("Reserves and surplus"))
                                        borrowings = to_decimal(out.get("Borrowings"))
                                        total = equity_share_capital + other_equity + borrowings
                                        if out.get("amount_type") == "Crores":
                                            total = total / 100000
                                        total_capital_employed += total
                                    elif out.get("format_type") == "LI":
                                        equity_share_capital = to_decimal(out.get("Share capital"))
                                        other_equity = to_decimal(out.get("Reserves and surplus"))
                                        share_application_money = to_decimal(
                                            out.get("Share application money received pending allotment of shares"))
                                        credit_fair_value = to_decimal(
                                            out.get("Credit (Debit) Fair value change account"))
                                        borrowings = to_decimal(out.get("Borrowings"))
                                        funds_for_appropriations = to_decimal(
                                            out.get("Funds for Future Appropriations"))
                                        total = equity_share_capital + other_equity + share_application_money + credit_fair_value + borrowings + funds_for_appropriations
                                        if out.get("amount_type") == "Crores":
                                            total = total / 100000
                                        total_capital_employed += total
                                if total_capital_employed and ebit:
                                    avg_capital_employed = float(total_capital_employed) / 2
                                    roce = (float(ebit) / avg_capital_employed) * 100
                                    roce = round(roce, 2)
                            else:
                                unsaved_symbols.append(company.nse_symbol)
                                continue
                        elif bse_company_list:
                            integrated_filing_financials_list = await main_bse_fetch_integrated_filing_financials(
                                company.bse_code)
                            output_obj = []
                            output_financial_data_list = []
                            ebit = 0
                            if integrated_filing_financials_list:
                                consolidated_list = []
                                standalone_list = []
                                for integrated_filing_obj in integrated_filing_financials_list.get("Table"):
                                    type_sub = integrated_filing_obj.get("status")
                                    if type_sub == "Revision":
                                        continue

                                    financial_name_obj = await parse_financial_name(
                                        integrated_filing_obj.get("Quarter_Name"))
                                    consolidated = financial_name_obj.get("type")
                                    if type_sub == "Revision":
                                        continue
                                    if consolidated == "standalone" and financial_name_obj.get("period") == "qtr" and financial_name_obj.get("month") == "Mar":
                                        standalone_list.append(integrated_filing_obj)
                                    if consolidated == "consolidated" and financial_name_obj.get("period") == "qtr" and financial_name_obj.get("month") == "Mar":
                                        consolidated_list.append(integrated_filing_obj)
                                if consolidated_list and len(consolidated_list) == 2:
                                    for i in consolidated_list:
                                        ixbrl = i.get("xbrlurl")
                                        if ".xml" in ixbrl:
                                            continue
                                        ixbrl = f"https://www.bseindia.com{ixbrl}"
                                        financial_name_obj = await parse_financial_name(
                                            i.get("Quarter_Name"))
                                        qe_date = f"{financial_name_obj.get('month')}-{financial_name_obj.get('year')}"
                                        output, amount_type, format_type = await fetch_integrated_filing_financials_data_from_bse_for_book_value(
                                            ixbrl)
                                        output['date'] = qe_date
                                        output['amount_type'] = amount_type
                                        output['format_type'] = format_type
                                        output['Qtrid'] = i.get("Qtrid")

                                        output_f_data, amount_type, format_type = await fetch_integrated_filing_financials_data_for_roce_from_bse(ixbrl)
                                        output_obj.append(output)
                                        output_f_data['date'] = qe_date
                                        output_f_data['amount_type'] = amount_type
                                        output_f_data['format_type'] = format_type
                                        output_f_data['Qtrid'] = i.get("Qtrid")
                                        output_financial_data_list.append(output_f_data)
                                if not output_obj or not output_financial_data_list:
                                    output_obj = []
                                    output_financial_data_list = []
                                    if standalone_list and (len(standalone_list) == 2 or len(standalone_list) == 3):
                                        for st in standalone_list:
                                            ixbrl = st.get("xbrlurl")
                                            if ".xml" in ixbrl:
                                                continue
                                            ixbrl = f"https://www.bseindia.com{ixbrl}"
                                            financial_name_obj = await parse_financial_name(
                                                st.get("Quarter_Name"))
                                            qe_date = f"{financial_name_obj.get('month')}-{financial_name_obj.get('year')}"
                                            output, amount_type, format_type = await fetch_integrated_filing_financials_data_from_bse_for_book_value(
                                                ixbrl)
                                            output['date'] = qe_date
                                            output['amount_type'] = amount_type
                                            output['format_type'] = format_type
                                            output['Qtrid'] = st.get("Qtrid")

                                            output_f_data, amount_type, format_type = await fetch_integrated_filing_financials_data_for_roce_from_bse(
                                                ixbrl)
                                            output_obj.append(output)
                                            output_f_data['date'] = qe_date
                                            output_f_data['amount_type'] = amount_type
                                            output_f_data['format_type'] = format_type
                                            output_f_data['Qtrid'] = st.get("Qtrid")
                                            output_financial_data_list.append(output_f_data)
                                if not output_obj or not output_financial_data_list:
                                    unsaved_symbols.append(company.nse_symbol)
                                    continue
                                ist = pytz.timezone("Asia/Kolkata")
                                current_year = datetime.now(ist).year
                                for output_financial_data_obj in output_financial_data_list:
                                    if f"Mar-{current_year}" in output_financial_data_obj.get("date"):
                                        if output_financial_data_obj.get("format_type") == "INDAS":
                                            ebit = output_financial_data_obj.get(
                                                "Finance costs") + output_financial_data_obj.get(
                                                "Total profit before tax")
                                            if output_financial_data_obj.get("amount_type") == "Crores":
                                                ebit = ebit / 100000
                                        elif output_financial_data_obj.get("format_type") == "BANKING":
                                            ebit = output_financial_data_obj.get(
                                                "Total profit (loss) from ordinary activities before tax") + output_financial_data_obj.get(
                                                "Interest expended")
                                            if output_financial_data_obj.get("amount_type") == "Crores":
                                                ebit = ebit / 100000
                                        elif output_financial_data_obj.get("format_type") == "NBFC":
                                            ebit = output_financial_data_obj.get(
                                                "Total profit before tax") + output_financial_data_obj.get(
                                                "Finance costs")
                                            if output_financial_data_obj.get("amount_type") == "Crores":
                                                ebit = ebit / 100000
                                        elif output_financial_data_obj.get("format_type") == "General Insurance":
                                            ebit = output_financial_data_obj.get(
                                                "Profit / Loss before extraordinary items")
                                            if output_financial_data_obj.get("amount_type") == "Crores":
                                                ebit = ebit / 100000
                                        elif output_financial_data_obj.get("format_type") == "Life Insurance":
                                            ebit = output_financial_data_obj.get(
                                                "Profit/ (loss) before tax")
                                            if output_financial_data_obj.get("amount_type") == "Crores":
                                                ebit = ebit / 100000
                                total_capital_employed = 0
                                for out in output_obj:
                                    if out.get("format_type") == "INDAS":
                                        equity_share_capital = to_decimal(out.get("Equity share capital"))
                                        other_equity = to_decimal(out.get("Other equity"))
                                        borrowing_current = to_decimal(out.get("Borrowings, current"))
                                        borrowing_non_current = to_decimal(out.get("Borrowings, non-current"))
                                        total = equity_share_capital + other_equity + borrowing_current + borrowing_non_current
                                        if out.get("amount_type") == "Crores":
                                            total = total / 100000
                                        total_capital_employed += total
                                    elif out.get("format_type") == "BANKING":
                                        equity_share_capital = to_decimal(out.get("Total Assets"))
                                        total = equity_share_capital
                                        if out.get("amount_type") == "Crores":
                                            total = total / 100000
                                        total_capital_employed += total
                                    elif out.get("format_type") == "NBFC":
                                        equity_share_capital = to_decimal(out.get("Equity share capital"))
                                        other_equity = to_decimal(out.get("Other equity"))
                                        debt_securities = to_decimal(out.get("Debt Securities"))
                                        borrowing = to_decimal(out.get("Borrowings (Other than Debt Securities)"))
                                        deposits = to_decimal(out.get("Deposits"))
                                        subordinated_liabilities = to_decimal(out.get("Subordinated Liabilities"))
                                        total = equity_share_capital + other_equity + debt_securities + borrowing + deposits + subordinated_liabilities
                                        if out.get("amount_type") == "Crores":
                                            total = total / 100000
                                        total_capital_employed += total
                                    elif out.get("format_type") == "General Insurance":
                                        equity_share_capital = to_decimal(out.get("Share capital"))
                                        other_equity = to_decimal(out.get("Reserves and surplus"))
                                        borrowings = to_decimal(out.get("Borrowings"))
                                        total = equity_share_capital + other_equity + borrowings
                                        if out.get("amount_type") == "Crores":
                                            total = total / 100000
                                        total_capital_employed += total
                                    elif out.get("format_type") == "Life Insurance":
                                        equity_share_capital = to_decimal(out.get("Share capital"))
                                        other_equity = to_decimal(out.get("Reserves and surplus"))
                                        share_application_money = to_decimal(
                                            out.get("Share application money received pending allotment of shares"))
                                        credit_fair_value = to_decimal(
                                            out.get("Credit (Debit) Fair value change account"))
                                        borrowings = to_decimal(out.get("Borrowings"))
                                        funds_for_appropriations = to_decimal(
                                            out.get("Funds for Future Appropriations"))
                                        total = equity_share_capital + other_equity + share_application_money + credit_fair_value + borrowings + funds_for_appropriations
                                        if out.get("amount_type") == "Crores":
                                            total = total / 100000
                                        total_capital_employed += total
                                if total_capital_employed and ebit:
                                    avg_capital_employed = float(total_capital_employed) / 2
                                    roce = (float(ebit) / avg_capital_employed) * 100
                                    roce = round(roce, 2)
                        else:
                            unsaved_symbols.append(company.nse_symbol)
                            continue
                        company.details.roce = roce
                        processed_symbols.append(company.nse_symbol)
                    except Exception as e:
                        savepoint.rollback()
                        raise

                except Exception as e:
                    print("Error:", str(e))
                    print(f"\nFAILED company: {company.id} | {company.name}")
                    error_symbols.append(company.nse_symbol)
                    continue
        db.commit()
    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()
        await update_nse_bse_shareholder_save_processed_symbol(unsaved_symbols, "data_not_available", file_name)
        await update_nse_bse_shareholder_save_processed_symbol(error_symbols, "error", file_name)
        await update_nse_bse_shareholder_save_processed_symbol(processed_symbols, "processed_symbols", file_name)


async def convert_stock_quarterly_result_data_async():
    db = SessionLocalSync()
    error_symbols = []
    unsaved_symbols = []
    current_processed_symbols = []
    processed_symbols = []
    file_name = 'convert_stock_quarterly_result'
    try:
        stmt = (
            select(CompanyStock)
            .outerjoin(
                QuarterlyResultDateset,
                CompanyStock.id == QuarterlyResultDateset.company_id
            )
            .options(selectinload(CompanyStock.details))
            .execution_options(yield_per=100)
        )

        result = db.execute(stmt)
        companies = result.scalars().all()

        existing_symbols = set()

        skipped_symbols_from_json = await fetch_symbols_from_covered_symbol_json_for_balance_sheet_and_profit_loss_and_cash_flow(
            file_name)
        existing_symbols.update(skipped_symbols_from_json)
        missing_symbols = [
            c for c in companies if c.nse_symbol not in existing_symbols
        ]

        missing_symbols = missing_symbols[:50]

        current_processed_symbols = [c.nse_symbol for c in missing_symbols]
        await update_nse_bse_balance_sheet_and_profit_loss_and_cash_flow_save_processed_symbol(
            current_processed_symbols, "processing", file_name)

        def chunk_list(data, size):
            for i in range(0, len(data), size):
                yield data[i:i + size]

        for chunk in chunk_list(missing_symbols, 50):
            for company in chunk:
                try:
                    print(f"\nProcessing company: {company.id} | {company.name}")
                    with db.begin_nested():
                        existing_dataset = db.execute(
                            select(QuarterlyResultDateset)
                            .where(
                                QuarterlyResultDateset.company_id == company.id,
                                QuarterlyResultDateset.result_format == "consolidated"
                            )
                        ).scalar_one_or_none()

                        if not existing_dataset:
                            unsaved_symbols.append(company.nse_symbol)
                            continue

                        nse_company_list = await fetch_nse_exact_symbol_data(company.nse_symbol)
                        bse_company_list = await fetch_bse_exact_symbol_data(company.nse_symbol)
                        if nse_company_list and bse_company_list:
                            integrated_filing_financials_list = await main_fetch_integrated_filing_financials(
                                company.nse_symbol, "equity")
                            format_type = None
                            if integrated_filing_financials_list:
                                for integrated_filing_obj in integrated_filing_financials_list.get("data"):
                                    consolidated = integrated_filing_obj.get("consolidated")
                                    ixbrl = integrated_filing_obj.get("ixbrl")
                                    if consolidated == "Consolidated":
                                        format_type = await fetch_integrated_filing_financials_data_type_from_nse(
                                            ixbrl)
                                        if format_type:
                                            break
                            old_values = existing_dataset.values
                            converted = await convert_existing_nse_to_screener(old_values, format_type)
                            if not converted:
                                converted = {
                                    "rows": [],
                                    "headers": [],
                                    "format_type": []
                                }
                            custom_quarterly_result = CustomFormatQuarterlyResultDateset(
                                company_id=company.id,
                                values=converted,
                                result_format=ResultFormatEnum.consolidated
                            )
                            db.add(custom_quarterly_result)
                            db.flush()
                            company.stock_format = format_type
                            processed_symbols.append(company.nse_symbol)
                        elif bse_company_list:
                            unsaved_symbols.append(company.nse_symbol)
                        elif nse_company_list:
                            integrated_filing_financials_list = await main_fetch_integrated_filing_financials(
                                company.nse_symbol, "equity")
                            format_type = None
                            if integrated_filing_financials_list:
                                for integrated_filing_obj in integrated_filing_financials_list.get("data"):
                                    consolidated = integrated_filing_obj.get("consolidated")
                                    ixbrl = integrated_filing_obj.get("ixbrl")
                                    if consolidated == "Consolidated":
                                        format_type = await fetch_integrated_filing_financials_data_type_from_nse(
                                            ixbrl)
                                        if format_type:
                                            break
                            old_values = existing_dataset.values
                            converted = await convert_existing_nse_to_screener(old_values, format_type)
                            if not converted:
                                converted = {
                                    "rows": [],
                                    "headers": [],
                                    "format_type": []
                                }
                            custom_quarterly_result = CustomFormatQuarterlyResultDateset(
                                company_id=company.id,
                                values=converted,
                                result_format=ResultFormatEnum.consolidated
                            )
                            db.add(custom_quarterly_result)
                            db.flush()
                            company.stock_format = format_type
                            db.commit()
                            processed_symbols.append(company.nse_symbol)
                        else:
                            unsaved_symbols.append(company.nse_symbol)

                except Exception as e:
                    print("Error:", str(e))
                    print(f"\nFAILED company: {company.id} | {company.name}")
                    error_symbols.append(company.nse_symbol)
                    continue
        db.commit()
    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()
        await update_nse_bse_shareholder_save_processed_symbol(unsaved_symbols, "data_not_available", file_name)
        await update_nse_bse_shareholder_save_processed_symbol(error_symbols, "error", file_name)
        await update_nse_bse_shareholder_save_processed_symbol(processed_symbols, "processed_symbols", file_name)

async def fetch_stock_quarterly_result_standalone_data_async():
    db = SessionLocalSync()
    error_symbols = []
    unsaved_symbols = []
    current_processed_symbols = []
    processed_symbols = []
    file_name = 'stock_standalone_quarterly_result.json'
    try:
        stmt = (
            select(CompanyStock)
            .outerjoin(
                QuarterlyResultDateset,
                CompanyStock.id == QuarterlyResultDateset.company_id
            )
            .options(selectinload(CompanyStock.details))
            .execution_options(yield_per=100)
        )

        result = db.execute(stmt)
        companies = result.scalars().all()

        existing_symbols = set()
        skipped_symbols_from_json = await fetch_symbols_from_covered_symbol_json_for_balance_sheet_and_profit_loss_and_cash_flow(
            file_name)
        existing_symbols.update(skipped_symbols_from_json)
        missing_symbols = [
            c for c in companies if c.nse_symbol not in existing_symbols
        ]
        missing_symbols = missing_symbols[:30]

        current_processed_symbols = [c.nse_symbol for c in missing_symbols]
        await update_nse_bse_balance_sheet_and_profit_loss_and_cash_flow_save_processed_symbol(
            current_processed_symbols, "processing", file_name)

        def chunk_list(data, size):
            for i in range(0, len(data), size):
                yield data[i:i + size]

        for chunk in chunk_list(missing_symbols, 30):
            for company in chunk:
                try:
                    print(f"\nProcessing company: {company.id} | {company.name}")
                    with db.begin_nested():
                        db.execute(
                            delete(QuarterlyResultDateset)
                            .where(QuarterlyResultDateset.company_id == company.id)
                        )
                        nse_company_list = await fetch_nse_exact_symbol_data(company.nse_symbol)
                        bse_company_list = await fetch_bse_exact_symbol_data(company.nse_symbol)
                        if nse_company_list and bse_company_list:
                            integrated_filing_financials_list = await main_fetch_integrated_filing_financials(
                                company.nse_symbol, "equity")
                            quarterly_result = []
                            if integrated_filing_financials_list:
                                response_list = []
                                for integrated_filing_obj in integrated_filing_financials_list.get("data"):
                                    qe_date = integrated_filing_obj.get("qe_Date")
                                    consolidated = integrated_filing_obj.get("consolidated")
                                    ixbrl = integrated_filing_obj.get("ixbrl")
                                    formatted = None
                                    if qe_date:
                                        formatted = datetime.strptime(qe_date, "%d-%b-%Y").strftime("%b-%Y")
                                    if consolidated == "Standalone":
                                        output, amount_type, format_type = await fetch_integrated_filing_financials_data_from_nse(ixbrl)
                                        output.append({
                                            "date": formatted or qe_date,
                                            "consolidated": consolidated,
                                            "amount_type": amount_type,
                                            "format": format_type,
                                        })
                                        response_list.append(output)
                                if response_list:
                                    quarterly_result = await decide_quarterly_format(response_list)
                            if quarterly_result:
                                company_stock = QuarterlyResultDateset(
                                    company_id=company.id,
                                    values=quarterly_result,
                                    result_format=ResultFormatEnum.standalone
                                )
                                db.add(company_stock)
                                db.flush()
                                processed_symbols.append(company.nse_symbol)
                            else:
                                unsaved_symbols.append(company.nse_symbol)
                        elif bse_company_list:
                            integrated_filing_financials_list = await main_bse_fetch_integrated_filing_financials(
                                company.bse_code)
                            quarterly_result = []
                            if integrated_filing_financials_list:
                                response_list = []
                                for integrated_filing_obj in integrated_filing_financials_list.get("Table"):
                                    financial_name_obj = await parse_financial_name(integrated_filing_obj.get("Quarter_Name"))
                                    qe_date = f"{financial_name_obj.get('month')}-{financial_name_obj.get('year')}"
                                    consolidated = financial_name_obj.get("type")
                                    ixbrl = integrated_filing_obj.get("xbrlurl")
                                    if consolidated == "standalone" and financial_name_obj.get("period") == "qtr":
                                        url = f"https://www.bseindia.com{ixbrl}"
                                        output, amount_type, format_type = await fetch_bse_integrated_filing_financials_data_from(url)
                                        output.append({
                                            "date": qe_date,
                                            "consolidated": consolidated,
                                            "amount_type": amount_type,
                                            "format": format_type
                                        })
                                        response_list.append(output)
                                if response_list:
                                    quarterly_result = await bse_decide_quarterly_format(response_list)
                            if quarterly_result:
                                company_stock = QuarterlyResultDateset(
                                    company_id=company.id,
                                    values=quarterly_result,
                                    result_format=ResultFormatEnum.standalone
                                )
                                db.add(company_stock)
                                db.flush()
                                processed_symbols.append(company.nse_symbol)
                            else:
                                unsaved_symbols.append(company.nse_symbol)
                        elif nse_company_list:
                            integrated_filing_financials_list = await main_fetch_integrated_filing_financials(company.nse_symbol, "equity")
                            quarterly_result = []
                            if integrated_filing_financials_list:
                                response_list = []
                                for integrated_filing_obj in integrated_filing_financials_list.get("data"):
                                    qe_date = integrated_filing_obj.get("qe_Date")
                                    consolidated = integrated_filing_obj.get("consolidated")
                                    ixbrl = integrated_filing_obj.get("ixbrl")
                                    formatted = None
                                    if qe_date:
                                        formatted = datetime.strptime(qe_date, "%d-%b-%Y").strftime("%b-%Y")
                                    if consolidated == "Standalone":
                                        output, amount_type, format_type = await fetch_integrated_filing_financials_data_from_nse(ixbrl)
                                        output.append({
                                            "date": formatted or qe_date,
                                            "consolidated": consolidated,
                                            "amount_type": amount_type,
                                            "format": format_type
                                        })
                                        response_list.append(output)
                                if response_list:
                                    quarterly_result = await decide_quarterly_format(response_list)
                            if quarterly_result:
                                company_stock = QuarterlyResultDateset(
                                    company_id=company.id,
                                    values=quarterly_result,
                                    result_format=ResultFormatEnum.standalone
                                )
                                db.add(company_stock)
                                db.flush()
                                processed_symbols.append(company.nse_symbol)
                            else:
                                unsaved_symbols.append(company.nse_symbol)
                        else:
                            unsaved_symbols.append(company.nse_symbol)

                except Exception as e:
                    print("Error:", str(e))
                    print(f"\nFAILED company: {company.id} | {company.name}")
                    error_symbols.append(company.nse_symbol)
                    continue
        db.commit()
    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()
        await save_quarterly_result_processed_symbol(unsaved_symbols, "unsaved")
        await save_quarterly_result_processed_symbol(error_symbols, "error")
        await save_quarterly_result_processed_symbol(processed_symbols, "processed_symbols")
        await save_quarterly_result_processed_symbol(current_processed_symbols, "remove_processing")


# Gross delivery

def bulk_insert_stock_delivery(
    session,
    company_id: int,
    delivery_data: list,
    platform : str
):
    records = []

    delivery_data.sort(key=lambda x: x["dt_tm"])

    for i, item in enumerate(delivery_data):
        rolling_avg_volume = None
        rolling_delivery_percent = None
        insight = None

        if i >= 4:
            window = delivery_data[i - 4:i + 1]

            total_traded = sum(x["No_Of_Shares"] for x in window)
            total_delivery = sum(x["Delivery_Qty"] for x in window)

            rolling_avg_volume = total_traded / len(window)
            rolling_delivery_percent = (
                round((total_delivery / total_traded) * 100, 2)
                if total_traded
                else None
            )

            if rolling_delivery_percent is not None:
                current_delivery_percent = item.get("Perc_Del_Qty", 0)
                diff = round(
                    current_delivery_percent - rolling_delivery_percent,
                    2,
                )

                if diff >= 12:
                    insight = "Jump in delivery"
                elif diff >= 5:
                    insight = "Rising delivery"
                elif diff <= -12:
                    insight = "Drop in delivery"
                elif diff <= -6:
                    insight = "Falling delivery"
                else:
                    insight = "-"

        records.append(
            StockDeliveryDataset(
                company_id=company_id,
                trading_date=datetime.fromisoformat(item["dt_tm"]),
                combined_traded_volume=item.get("No_Of_Shares"),
                combined_delivery_volume=item.get("Delivery_Qty"),
                combined_delivery_percent=item.get("Perc_Del_Qty"),
                insight=insight,
                combined_rolling_week_avg_volume=rolling_avg_volume,
                rolling_week_delivery_percent=rolling_delivery_percent,
                platform=platform
            )
        )

    session.add_all(records)


def bulk_nse_insert_stock_delivery(
    session,
    company_id: int,
    delivery_data: list,
    platform: str,
):
    records = []

    # Oldest -> Newest
    delivery_data.sort(
        key=lambda x: datetime.strptime(
            x["mTIMESTAMP"],
            "%d-%b-%Y",
        )
    )

    for i, item in enumerate(delivery_data):
        rolling_avg_volume = None
        rolling_delivery_percent = None
        insight = None

        if i >= 4:
            window = delivery_data[i - 4:i + 1]

            total_traded = sum(
                x["CH_TOT_TRADED_QTY"] for x in window
            )

            total_delivery = sum(
                x["COP_DELIV_QTY"] for x in window
            )

            rolling_avg_volume = round(
                total_traded / len(window),
                2,
            )

            rolling_delivery_percent = (
                round(
                    (total_delivery / total_traded) * 100,
                    2,
                )
                if total_traded
                else None
            )

            if rolling_delivery_percent is not None:
                current_delivery_percent = item["COP_DELIV_PERC"]

                diff = round(
                    current_delivery_percent
                    - rolling_delivery_percent,
                    2,
                )

                if diff >= 12:
                    insight = "Jump in delivery"
                elif diff >= 5:
                    insight = "Rising delivery"
                elif diff <= -12:
                    insight = "Drop in delivery"
                elif diff <= -6:
                    insight = "Falling delivery"
                else:
                    insight = "-"

        previous_close = item.get("CH_PREVIOUS_CLS_PRICE")
        close_price = item.get("CH_CLOSING_PRICE")

        price_change = None
        if previous_close:
            price_change = round(
                ((close_price - previous_close) / previous_close)
                * 100,
                2,
            )

        records.append(
            StockDeliveryDataset(
                company_id=company_id,
                trading_date=datetime.strptime(
                    item["mTIMESTAMP"],
                    "%d-%b-%Y",
                ),

                combined_traded_volume=item["CH_TOT_TRADED_QTY"],
                combined_delivery_volume=item["COP_DELIV_QTY"],
                combined_delivery_percent=item["COP_DELIV_PERC"],

                price_change_percent=price_change,

                insight=insight,
                combined_rolling_week_avg_volume=rolling_avg_volume,
                rolling_week_delivery_percent=rolling_delivery_percent,

                platform=platform,
            )
        )

    session.add_all(records)

def parse_number(value):
    """
    Converts values like:
    2,365.60 -> 2365.60
    30,36,839 -> 3036839
    """
    if value is None or value == "":
        return None

    if isinstance(value, (int, float)):
        return value

    value = str(value).replace(",", "").strip()
    if value in ("", "-", "None", "null", "N/A", "NA"):
        return None

    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return None


async def parse_nse_delivery_csv(csv_file_path):
    data = []

    with open(csv_file_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row = {k.strip(): v for k, v in row.items()}

            trade_date = datetime.strptime(
                row["Date"], "%d-%b-%Y"
            )

            data.append(
                {
                    "CH_SYMBOL": row["Symbol"],
                    "CH_SERIES": row["Series"],
                    "mTIMESTAMP": row["Date"],

                    "CH_PREVIOUS_CLS_PRICE": parse_number(row["Prev Close"]),
                    "CH_OPENING_PRICE": parse_number(row["Open Price"]),
                    "CH_TRADE_HIGH_PRICE": parse_number(row["High Price"]),
                    "CH_TRADE_LOW_PRICE": parse_number(row["Low Price"]),
                    "CH_LAST_TRADED_PRICE": parse_number(row["Last Price"]),
                    "CH_CLOSING_PRICE": parse_number(row["Close Price"]),
                    "VWAP": parse_number(row["Average Price"]),
                    "CH_TOT_TRADED_QTY": parse_number(row["Total Traded Quantity"]),
                    "CH_TOTAL_TRADES": parse_number(row["No. of Trades"]),

                    "CH_TIMESTAMP": (
                        trade_date.strftime("%Y-%m-%d")
                        + "T18:30:00.000Z"
                    ),

                    "COP_DELIV_QTY": parse_number(row["Deliverable Qty"]),
                    "COP_DELIV_PERC": parse_number(row["% Dly Qt to Traded Qty"]),
                }
            )

    return {"data": data}

async def fetch_gross_deliverables_nse_bse_stock_information_async():
    db = SessionLocalSync()
    try:
        stmt = (
                select(CompanyStock)
                .outerjoin(
                    KeyDetailsForCS,
                    CompanyStock.id == KeyDetailsForCS.company_id
                )
                .options(selectinload(CompanyStock.details))
            )
        file_name = "update_nse_bse_gross_deliverables_data.json"
        error_symbols = []
        result = db.execute(stmt)
        processed_symbols = set(await update_nse_bse_gross_deliverable_data_load_processed_symbols(file_name))
        new_process_symbol = []
        unprocessed_companies = []
        for c in result.scalars():
            if c.nse_symbol not in processed_symbols:
                unprocessed_companies.append(c)
                if len(unprocessed_companies) == 50:
                    break
        started_symbols = [c.nse_symbol for c in unprocessed_companies]
        await update_nse_bse_gross_deliverable_data_save_processed_symbol(started_symbols, "current_processed_symbols", file_name)
        for company in unprocessed_companies:
            try:
                nse_company_list, bse_company_list = await asyncio.gather(
                    fetch_nse_exact_symbol_data(company.nse_symbol),
                    fetch_bse_exact_symbol_data(company.nse_symbol),
                )

                if nse_company_list:
                    symbol = nse_company_list[0].get("symbol")
                    series = nse_company_list[0].get("series")
                    c_date  = datetime.now(ZoneInfo("Asia/Kolkata"))
                    previous_date_5years = c_date - relativedelta(years=5)
                    from_date = previous_date_5years.strftime("%d-%m-%Y")
                    to_date = c_date.strftime("%d-%m-%Y")
                    gross_delivery_path = await main_nse_fetch_security_wise_historical_data(symbol, from_date, to_date, series)
                    gross_delivery_data = await parse_nse_delivery_csv(gross_delivery_path)
                    bulk_nse_insert_stock_delivery(db, company.id, gross_delivery_data.get("data"), "NSE")

                if bse_company_list:
                    bse_code = bse_company_list[0].get("bse_code")
                    c_date = datetime.now(ZoneInfo("Asia/Kolkata"))
                    previous_date_5years = c_date - relativedelta(years=5)
                    from_date = previous_date_5years.strftime("%d/%m/%Y")
                    to_date = c_date.strftime("%d/%m/%Y")
                    gross_delivery_data = await main_bse_fetch_gross_delivery_history(bse_code, from_date, to_date)
                    bulk_insert_stock_delivery(db, company.id, gross_delivery_data.get("Table"), "BSE")
                print("------------------------------------------------------------------------------------")

                db.commit()
                new_process_symbol.append(company.nse_symbol)
                if os.path.exists(gross_delivery_path):
                    os.remove(gross_delivery_path)

            except Exception as symbol_error:
                db.rollback()
                error_symbols.append(company.nse_symbol)
                print(f"Error for symbol {company.nse_symbol}: {symbol_error}")
                if os.path.exists(gross_delivery_path):
                    os.remove(gross_delivery_path)
                continue

        await update_nse_bse_gross_deliverable_data_save_processed_symbol(new_process_symbol, "processed_symbols", file_name)
        await update_nse_bse_gross_deliverable_data_save_processed_symbol(error_symbols, "error", file_name)
    finally:
        db.close()



def single_nse_insert_stock_delivery(
    session,
    company_id: int,
    delivery_data: dict,
    platform: str,
):
    raw_date = delivery_data.get("mTIMESTAMP")
    if isinstance(raw_date, datetime):
        trading_date = raw_date
    elif isinstance(raw_date, bdate):
        trading_date = datetime.combine(raw_date, datetime.min.time())
    elif isinstance(raw_date, str):
        trading_date = None
        for fmt in ("%d-%b-%Y %H:%M:%S", "%d-%b-%Y", "%d-%m-%Y %H:%M:%S", "%d-%m-%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d%m%Y"):
            try:
                trading_date = datetime.strptime(raw_date.strip(), fmt)
                break
            except ValueError:
                pass
        if trading_date is None:
            trading_date = datetime.now(ZoneInfo("Asia/Kolkata"))
    else:
        trading_date = datetime.now(ZoneInfo("Asia/Kolkata"))

    previous_records = (
        session.execute(
            select(StockDeliveryDataset)
            .where(
                StockDeliveryDataset.company_id == company_id,
                StockDeliveryDataset.platform == platform,
                func.date(StockDeliveryDataset.trading_date) < trading_date.date(),
            )
            .order_by(desc(StockDeliveryDataset.trading_date))
            .limit(4)
        )
        .scalars()
        .all()
    )

    # Oldest -> Newest
    previous_records.reverse()

    window = [
        {
            "CH_TOT_TRADED_QTY": r.combined_traded_volume,
            "COP_DELIV_QTY": r.combined_delivery_volume,
            "COP_DELIV_PERC": r.combined_delivery_percent,
        }
        for r in previous_records
    ]

    # Add current day's data
    window.append({
        "CH_TOT_TRADED_QTY": parse_number(delivery_data.get("CH_TOT_TRADED_QTY")),
        "COP_DELIV_QTY": parse_number(delivery_data.get("COP_DELIV_QTY")),
        "COP_DELIV_PERC": parse_number(delivery_data.get("COP_DELIV_PERC")),
    })

    rolling_avg_volume = None
    rolling_delivery_percent = None
    insight = None

    record = session.execute(
        select(StockDeliveryDataset).where(
            StockDeliveryDataset.company_id == company_id,
            StockDeliveryDataset.platform == platform,
            func.date(StockDeliveryDataset.trading_date) == trading_date.date(),
        )
    ).scalar_one_or_none()

    current_deliv_perc = parse_number(delivery_data.get("COP_DELIV_PERC"))

    if len(window) == 5:
        total_traded = sum(float(x["CH_TOT_TRADED_QTY"]) for x in window if x.get("CH_TOT_TRADED_QTY") is not None)
        total_delivery = sum(float(x["COP_DELIV_QTY"]) for x in window if x.get("COP_DELIV_QTY") is not None)

        rolling_avg_volume = round(total_traded / 5, 2)

        rolling_delivery_percent = (
            round((total_delivery / total_traded) * 100, 2)
            if total_traded
            else None
        )

        if rolling_delivery_percent is not None and current_deliv_perc is not None:
            diff = round(
                current_deliv_perc - rolling_delivery_percent,
                2,
            )

            if diff >= 12:
                insight = "Jump in delivery"
            elif diff >= 5:
                insight = "Rising delivery"
            elif diff <= -12:
                insight = "Drop in delivery"
            elif diff <= -6:
                insight = "Falling delivery"
            else:
                insight = "-"

    previous_close = parse_number(delivery_data.get("CH_PREVIOUS_CLS_PRICE"))
    close_price = parse_number(delivery_data.get("CH_CLOSING_PRICE"))
    price_change = None
    if previous_close and close_price:
        price_change = round(
            ((close_price - previous_close) / previous_close) * 100,
            2,
        )

    tot_traded_qty = parse_number(delivery_data.get("CH_TOT_TRADED_QTY"))
    deliv_qty = parse_number(delivery_data.get("COP_DELIV_QTY"))

    if record:
        record.combined_traded_volume = tot_traded_qty
        record.combined_delivery_volume = deliv_qty
        record.combined_delivery_percent = current_deliv_perc
        record.price_change_percent = price_change
        record.insight = insight
        record.combined_rolling_week_avg_volume = rolling_avg_volume
        record.rolling_week_delivery_percent = rolling_delivery_percent
    else:
        record = StockDeliveryDataset(
            company_id=company_id,
            trading_date=trading_date,
            combined_traded_volume=tot_traded_qty,
            combined_delivery_volume=deliv_qty,
            combined_delivery_percent=current_deliv_perc,
            price_change_percent=price_change,
            insight=insight,
            combined_rolling_week_avg_volume=rolling_avg_volume,
            rolling_week_delivery_percent=rolling_delivery_percent,
            platform=platform,
        )
        session.add(record)


def single_bse_insert_stock_delivery(
    session,
    company_id: int,
    delivery_data: dict,
    platform: str,
):
    trading_date = datetime.strptime(
        delivery_data["dt_tm"], "%Y-%m-%d %H:%M:%S"
    )

    previous_records = (
        session.execute(
            select(StockDeliveryDataset)
            .where(
                StockDeliveryDataset.company_id == company_id,
                StockDeliveryDataset.platform == platform,
                func.date(StockDeliveryDataset.trading_date) < trading_date.date(),
            )
            .order_by(desc(StockDeliveryDataset.trading_date))
            .limit(4)
        )
        .scalars()
        .all()
    )

    previous_records.reverse()

    window = [
        {
            "No_Of_Shares": r.combined_traded_volume,
            "Delivery_Qty": r.combined_delivery_volume,
            "Perc_Del_Qty": r.combined_delivery_percent,
        }
        for r in previous_records
    ]

    window.append(delivery_data)

    rolling_avg_volume = None
    rolling_delivery_percent = None
    insight = None

    record = session.execute(
        select(StockDeliveryDataset).where(
            StockDeliveryDataset.company_id == company_id,
            StockDeliveryDataset.platform == platform,
            func.date(StockDeliveryDataset.trading_date) == trading_date.date(),
        )
    ).scalar_one_or_none()

    if len(window) == 5:
        total_traded = sum(float(x["No_Of_Shares"]) for x in window)
        total_delivery = sum(float(x["Delivery_Qty"]) for x in window)

        rolling_avg_volume = round(total_traded / 5, 2)

        rolling_delivery_percent = (
            round((total_delivery / total_traded) * 100, 2)
            if total_traded
            else None
        )

        diff = round(
            float(delivery_data["Perc_Del_Qty"]) - rolling_delivery_percent,
            2,
        )

        if diff >= 12:
            insight = "Jump in delivery"
        elif diff >= 5:
            insight = "Rising delivery"
        elif diff <= -12:
            insight = "Drop in delivery"
        elif diff <= -6:
            insight = "Falling delivery"
        else:
            insight = "-"

    if record:
        record.combined_traded_volume = delivery_data["No_Of_Shares"]
        record.combined_delivery_volume = delivery_data["Delivery_Qty"]
        record.combined_delivery_percent = delivery_data["Perc_Del_Qty"]
        record.price_change_percent = None
        record.insight = insight
        record.combined_rolling_week_avg_volume = rolling_avg_volume
        record.rolling_week_delivery_percent = rolling_delivery_percent
    else:
        record = StockDeliveryDataset(
            company_id=company_id,
            trading_date=trading_date,
            combined_traded_volume=delivery_data["No_Of_Shares"],
            combined_delivery_volume=delivery_data["Delivery_Qty"],
            combined_delivery_percent=delivery_data["Perc_Del_Qty"],
            price_change_percent=None,
            insight=insight,
            combined_rolling_week_avg_volume=rolling_avg_volume,
            rolling_week_delivery_percent=rolling_delivery_percent,
            platform=platform,
        )
        session.add(record)


GROUP_SIZE = 20

@shared_task(bind=True)
def process_delivery_batch(self, batch, skip_symbols, file_name):

    db = SessionLocalSync()

    failed = []
    new_process_symbol = []
    try:
        for item in batch:
            try:
                if item.get("nse"):
                    single_nse_insert_stock_delivery(
                        db,
                        item["company_id"],
                        item["nse"],
                        "NSE",
                    )
                if item.get("bse"):
                    single_bse_insert_stock_delivery(
                        db,
                        item["company_id"],
                        item["bse"],
                        "BSE",
                    )
                db.commit()

            except Exception as e:
                db.rollback()
                failed.append(item["symbol"])
            finally:
                new_process_symbol.append(item["symbol"])

    finally:
        sync_update_nse_bse_gross_deliverable_data_save_processed_symbol(new_process_symbol, "processed_symbols", file_name)
        failed.extend(skip_symbols)
        sync_update_nse_bse_gross_deliverable_data_save_processed_symbol(failed, "error", file_name)
        count_stmt = (
            select(func.count(distinct(CompanyStock.id)))
            .outerjoin(
                KeyDetailsForCS,
                CompanyStock.id == KeyDetailsForCS.company_id
            )
        )

        total_count = db.execute(count_stmt).scalar_one()
        total_count_from_file = update_nse_bse_gross_deliverable_count_load_processed_symbols(file_name)
        if total_count == total_count_from_file:
            update_nse_bse_gross_deliverable_list_load_processed_symbols(file_name)
        db.close()


async def fetch_current_day_gross_deliverables_nse_stock_information_async(date_str: str | None = None):
    """
    Fetch current day gross deliverable stock information from NSE bhavdata CSV:
    https://nsearchives.nseindia.com//products/content/sec_bhavdata_full_{DDMMYYYY}.csv
    where date_str is dynamic and defaults to the current day in IST (DDMMYYYY).
    """
    if not date_str:
        now_ist = datetime.now(ZoneInfo("Asia/Kolkata"))
        date_str = now_ist.strftime("%d%m%Y")

    bhavdata_map = await fetch_sec_bhavdata_full_data(date_str=date_str)
    if not bhavdata_map:
        print(f"No bhavdata found for date {date_str}. File may not be published yet or today is a market holiday.")
        return

    db = SessionLocalSync()
    file_name = "update_daily_nse_gross_deliverables_data.json"
    try:
        stmt = (
            select(CompanyStock)
            .outerjoin(
                KeyDetailsForCS,
                CompanyStock.id == KeyDetailsForCS.company_id
            )
            .options(selectinload(CompanyStock.details))
        )
        error_symbols = []
        result = db.execute(stmt)
        processed_symbols = set(await update_nse_bse_gross_deliverable_data_load_processed_symbols(file_name))
        new_process_symbol = []
        unprocessed_companies = []
        payloads = []
        skip_symbols = []
        for c in result.scalars():
            if c.nse_symbol not in processed_symbols:
                unprocessed_companies.append(c)
                if len(unprocessed_companies) == 100:
                    break

        if not unprocessed_companies:
            print("All companies are already processed.")
            return

        started_symbols = [c.nse_symbol for c in unprocessed_companies]
        await update_nse_bse_gross_deliverable_data_save_processed_symbol(started_symbols, "current_processed_symbols", file_name)

        for company in unprocessed_companies:
            try:
                row = bhavdata_map.get(company.nse_symbol)
                if not row:
                    skip_symbols.append(company.nse_symbol)
                    continue

                trade_date_str = row.get("DATE1")
                priceVolumeDeliverable = {
                    "CH_SYMBOL": row.get("SYMBOL"),
                    "CH_SERIES": row.get("SERIES"),
                    "mTIMESTAMP": trade_date_str,
                    "CH_PREVIOUS_CLS_PRICE": parse_number(row.get("PREV_CLOSE")),
                    "CH_OPENING_PRICE": parse_number(row.get("OPEN_PRICE")),
                    "CH_TRADE_HIGH_PRICE": parse_number(row.get("HIGH_PRICE")),
                    "CH_TRADE_LOW_PRICE": parse_number(row.get("LOW_PRICE")),
                    "CH_LAST_TRADED_PRICE": parse_number(row.get("LAST_PRICE")),
                    "CH_CLOSING_PRICE": parse_number(row.get("CLOSE_PRICE")),
                    "VWAP": parse_number(row.get("AVG_PRICE")),
                    "CH_TOT_TRADED_QTY": parse_number(row.get("TTL_TRD_QNTY")),
                    "CH_TOT_TRADED_VAL": parse_number(row.get("TURNOVER_LACS")),
                    "CH_TOTAL_TRADES": parse_number(row.get("NO_OF_TRADES")),
                    "CH_TIMESTAMP": None,
                    "COP_DELIV_QTY": parse_number(row.get("DELIV_QTY")),
                    "COP_DELIV_PERC": parse_number(row.get("DELIV_PER")),
                }

                payloads.append({
                    "company_id": company.id,
                    "symbol": company.nse_symbol,
                    "nse": priceVolumeDeliverable,
                })
                new_process_symbol.append(company.nse_symbol)

                if len(payloads) == GROUP_SIZE:
                    group(
                        process_delivery_batch.s(payloads.copy(), skip_symbols.copy(), file_name),
                    ).apply_async()
                    payloads.clear()
                    skip_symbols.clear()

            except Exception as symbol_error:
                error_symbols.append(company.nse_symbol)
                print(f"Error for symbol {company.nse_symbol}: {symbol_error}")
                continue

        if payloads or skip_symbols:
            group(
                process_delivery_batch.s(
                    payloads.copy(), skip_symbols.copy(),
                    file_name
                ),
            ).apply_async()
            payloads.clear()
            skip_symbols.clear()

    finally:
        db.close()


@shared_task(bind=True)
def nse_process_delivery_batch(self, batch, skip_symbols, file_name):

    db = SessionLocalSync()

    failed = []
    new_process_symbol = []
    try:
        for item in batch:
            try:
                if item.get("nse"):
                    single_nse_insert_stock_delivery(
                        db,
                        item["company_id"],
                        item["nse"],
                        "NSE",
                    )
                if item.get("bse"):
                    single_bse_insert_stock_delivery(
                        db,
                        item["company_id"],
                        item["bse"],
                        "BSE",
                    )
                db.commit()
                new_process_symbol.append(item["symbol"])

            except Exception as e:
                db.rollback()
                failed.append(item["symbol"])

    finally:
        sync_update_nse_bse_gross_deliverable_data_save_processed_symbol(new_process_symbol, "processed_symbols", file_name)
        failed.extend(skip_symbols)
        sync_update_nse_bse_gross_deliverable_data_save_processed_symbol(failed, "error", file_name)
        count_stmt = (
            select(func.count(distinct(CompanyStock.id)))
            .outerjoin(
                KeyDetailsForCS,
                CompanyStock.id == KeyDetailsForCS.company_id
            )
        )

        total_count = db.execute(count_stmt).scalar_one()
        total_count_from_file = update_nse_bse_gross_deliverable_count_load_processed_symbols(file_name)
        if total_count == total_count_from_file:
            update_nse_bse_gross_deliverable_list_load_processed_symbols(file_name)
        db.close()

async def fetch_current_day_gross_deliverables_bse_stock_information_async():
    db = SessionLocalSync()
    try:
        stmt = (
            select(CompanyStock)
            .outerjoin(
                KeyDetailsForCS,
                CompanyStock.id == KeyDetailsForCS.company_id
            )
            .options(selectinload(CompanyStock.details))
        )
        now_ist = datetime.now(ZoneInfo("Asia/Kolkata"))
        date_str = now_ist.strftime("%Y%m%d")
        redis_target = f"redis:bse_gross_deliverables:{date_str}"

        error_symbols = []
        result = db.execute(stmt)
        processed_symbols = set(await update_nse_bse_gross_deliverable_data_load_processed_symbols(redis_target))
        new_process_symbol = []
        unprocessed_companies = []
        payloads = []
        skip_symbols = []
        for c in result.scalars():
            if c.nse_symbol not in processed_symbols:
                unprocessed_companies.append(c)
                if len(unprocessed_companies) == 100:
                    break

        if not unprocessed_companies:
            print(f"All BSE symbols have already been processed for date {date_str}.")
            return

        started_symbols = [c.nse_symbol for c in unprocessed_companies]
        await update_nse_bse_gross_deliverable_data_save_processed_symbol(started_symbols, "current_processed_symbols", redis_target)
        for company in unprocessed_companies:
            try:
                bse_company_list = await fetch_bse_exact_symbol_data(company.nse_symbol)
                if not bse_company_list:
                    skip_symbols.append(company.nse_symbol)

                if bse_company_list:
                    bse_code = bse_company_list[0].get("bse_code")
                    c_name = bse_company_list[0].get("company_name")
                    security_position = await fetch_bse_security_position(bse_code)
                    security_position = json.loads(security_position)
                    dt = datetime.strptime(security_position.get("TradeDate"), "%d %b %Y |%H:%M")

                    formatted_tradedate = dt.strftime("%Y-%m-%d %H:%M:%S")
                    bsePriceVolumeDeliverable = {
                        "dt_tm": formatted_tradedate,
                        "Scrip_cd": bse_code,
                        "LONG_NAME": c_name,
                        "Delivery_Qty": parse_number(security_position.get("DeliverableQty")),
                        "Delivery_Val": None,
                        "No_Of_Shares": parse_number(security_position.get("QtyTraded")),
                        "Turnover": None,
                        "Perc_Del_Qty": parse_number(security_position.get("PcDQ_TQ")),
                    }

                    payloads.append({
                        "company_id": company.id,
                        "symbol": company.nse_symbol,
                        "bse": bsePriceVolumeDeliverable,
                    })
                    new_process_symbol.append(company.nse_symbol)

                    if len(payloads) == GROUP_SIZE:
                        group(
                            nse_process_delivery_batch.s(payloads.copy(), skip_symbols.copy(), redis_target),
                        ).apply_async()
                        payloads.clear()
                        skip_symbols.clear()

            except Exception as symbol_error:
                db.rollback()
                error_symbols.append(company.nse_symbol)
                print(f"Error for symbol {company.nse_symbol}: {symbol_error}")
                continue

        if payloads or skip_symbols:
            group(
                nse_process_delivery_batch.s(
                    payloads.copy(), skip_symbols.copy(),
                    redis_target
                ),
            ).apply_async()
            payloads.clear()
            skip_symbols.clear()

    finally:
        db.close()



async def hourly_fetch_current_day_gross_deliverables_nse_stock_information_async():
    """
    Fetch current day gross deliverable stock information hourly for all NSE stocks (~6000).
    Supports both:
    1. During Market Hours: High-concurrency live quote fetching via GetQuoteApi with session pooling.
    2. After 6 PM: Instant Bhavdata processing if already published.
    """
    import aiohttp
    from app.db.redis.redis import redis_client_1
    from scripts.nse_metadata_and_symboldata import NSE_SYMBOL_DATA_HEADERS, NSE_SYMBOL_DATA_URL

    started_symbols = []
    new_process_symbol = []
    redis_prefix = ""
    db = SessionLocalSync()
    try:
        stmt = (
            select(CompanyStock)
            .outerjoin(
                KeyDetailsForCS,
                CompanyStock.id == KeyDetailsForCS.company_id
            )
            .options(selectinload(CompanyStock.details))
        )
        now_ist = datetime.now(ZoneInfo("Asia/Kolkata"))
        date_str = now_ist.strftime("%Y%m%d")
        redis_target = f"redis:hourly_nse_gross_deliverables:{date_str}"
        redis_prefix = redis_target[6:] if redis_target.startswith("redis:") else redis_target

        # Clear any failed/error symbols from Redis so they can be retried in this hourly cycle
        try:
            redis_client_1.delete(f"{redis_prefix}:error")
        except Exception as e:
            print(f"Error clearing redis error set: {e}")

        result = db.execute(stmt)
        processed_symbols = set(await update_nse_bse_gross_deliverable_data_load_processed_symbols(redis_target))
        new_process_symbol = []
        unprocessed_companies = []
        payloads = []
        skip_symbols = []
        for c in result.scalars():
            if c.nse_symbol and c.nse_symbol not in processed_symbols:
                unprocessed_companies.append(c)

        if not unprocessed_companies:
            print(f"All NSE symbols have already been processed for date {date_str}.")
            return

        started_symbols = [c.nse_symbol for c in unprocessed_companies]
        await update_nse_bse_gross_deliverable_data_save_processed_symbol(
            started_symbols, "current_processed_symbols", redis_target
        )

        BATCH_SIZE = 50

        # Step 1: Check if Bhavdata CSV is already available (e.g. after 6 PM)
        bhavdata_date_str = now_ist.strftime("%d%m%Y")
        bhavdata_map = await fetch_sec_bhavdata_full_data(date_str=bhavdata_date_str)

        if bhavdata_map:
            print(f"Bhavdata available for {bhavdata_date_str}. Processing {len(unprocessed_companies)} companies via bhavdata.")
            bhav_failed = []
            for company in unprocessed_companies:
                row = bhavdata_map.get(company.nse_symbol)
                if not row:
                    bhav_failed.append(company.nse_symbol)
                    continue

                trade_date_str = row.get("DATE1")
                priceVolumeDeliverable = {
                    "CH_SYMBOL": row.get("SYMBOL"),
                    "CH_SERIES": row.get("SERIES"),
                    "mTIMESTAMP": trade_date_str,
                    "CH_PREVIOUS_CLS_PRICE": parse_number(row.get("PREV_CLOSE")),
                    "CH_OPENING_PRICE": parse_number(row.get("OPEN_PRICE")),
                    "CH_TRADE_HIGH_PRICE": parse_number(row.get("HIGH_PRICE")),
                    "CH_TRADE_LOW_PRICE": parse_number(row.get("LOW_PRICE")),
                    "CH_LAST_TRADED_PRICE": parse_number(row.get("LAST_PRICE")),
                    "CH_CLOSING_PRICE": parse_number(row.get("CLOSE_PRICE")),
                    "VWAP": parse_number(row.get("AVG_PRICE")),
                    "CH_TOT_TRADED_QTY": parse_number(row.get("TTL_TRD_QNTY")),
                    "CH_TOT_TRADED_VAL": parse_number(row.get("TURNOVER_LACS")),
                    "CH_TOTAL_TRADES": None,
                    "CH_TIMESTAMP": None,
                    "COP_DELIV_QTY": parse_number(row.get("DELIV_QTY")),
                    "COP_DELIV_PERC": parse_number(row.get("DELIV_PER")),
                }

                payloads.append({
                    "company_id": company.id,
                    "symbol": company.nse_symbol,
                    "nse": priceVolumeDeliverable,
                })
                new_process_symbol.append(company.nse_symbol)

                if len(payloads) >= BATCH_SIZE:
                    group(
                        nse_process_delivery_batch.s(payloads.copy(), [], redis_target),
                    ).apply_async()
                    payloads.clear()

            if payloads:
                group(
                    nse_process_delivery_batch.s(
                        payloads.copy(), [],
                        redis_target
                    ),
                ).apply_async()
                payloads.clear()

            if bhav_failed:
                try:
                    redis_client_1.srem(f"{redis_prefix}:current_processed_symbols", *bhav_failed)
                    redis_client_1.srem(f"{redis_prefix}:error", *bhav_failed)
                except Exception:
                    pass

            return

        # Step 2: During market hours, Bhavdata is not published yet.
        # Fetch live market data via NSE GetQuoteApi concurrently with session pooling
        print(f"Bhavdata not published yet for {bhavdata_date_str} (Market Hours). Fetching live data concurrently for {len(unprocessed_companies)} companies.")

        headers = {
            **NSE_SYMBOL_DATA_HEADERS,
            "authority": "www.nseindia.com",
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "user-agent": (
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/140.0.0.0 Safari/537.36"
            ),
        }

        semaphore = asyncio.Semaphore(20)
        connector = aiohttp.TCPConnector(limit=50, limit_per_host=30, ttl_dns_cache=300, ssl=False)
        timeout = aiohttp.ClientTimeout(total=15, connect=5)
        async with aiohttp.ClientSession(headers=headers, connector=connector, timeout=timeout) as session:
            # Initial handshake to establish valid cookies on nseindia.com
            try:
                async with session.get("https://www.nseindia.com", headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as _:
                    pass
            except Exception as e:
                pass

            # Load cached symbol metadata (exact_symbol, series, market_type) from Redis (DB 10)
            redis_symbol_meta_key = "nse:symbol_meta"
            try:
                raw_cached_meta = redis_client_1.hgetall(redis_symbol_meta_key) or {}
                cached_meta_map = {
                    sym: json.loads(val) for sym, val in raw_cached_meta.items() if val
                }
            except Exception as e:
                print(f"Error loading cached NSE symbol metadata from Redis: {e}")
                cached_meta_map = {}

            async def fetch_single_company_live(comp):
                symbol = comp.nse_symbol
                async with semaphore:
                    try:
                        meta = cached_meta_map.get(symbol)
                        exact_symbol = meta.get("symbol", symbol) if meta else symbol
                        series = meta.get("series", "EQ") if meta else "EQ"
                        market_type = meta.get("market_type", "N") if meta else "N"

                        # 1. If not cached in Redis yet, fetch exact symbol data and metadata first
                        if not meta:
                            nse_company_list = await fetch_nse_exact_symbol_data(symbol)
                            if not nse_company_list:
                                return comp, None

                            exact_symbol = nse_company_list[0].get("symbol") or symbol
                            c_name = "-".join(nse_company_list[0].get("company_name", "").split())
                            series = nse_company_list[0].get("series") or "EQ"
                            metadata = await fetch_nse_metadata(exact_symbol, c_name)
                            market_type = metadata.get("marketType") or "N"

                            # Cache in Redis so all future hourly runs use it directly with 0 extra calls
                            try:
                                redis_client_1.hset(
                                    redis_symbol_meta_key,
                                    symbol,
                                    json.dumps({
                                        "symbol": exact_symbol,
                                        "series": series,
                                        "market_type": market_type,
                                    })
                                )
                                cached_meta_map[symbol] = {
                                    "symbol": exact_symbol,
                                    "series": series,
                                    "market_type": market_type,
                                }
                            except Exception as save_err:
                                print(f"Error saving symbol meta to Redis for {symbol}: {save_err}")

                        # 2. Fetch live quote using exact dynamic market_type and series
                        url = f"{NSE_SYMBOL_DATA_URL}?functionName=getSymbolData&marketType={market_type}&series={series}&symbol={exact_symbol}"
                        req_headers = {
                            **headers,
                            "path": f"/api/NextApi/apiClient/GetQuoteApi?functionName=getSymbolData&marketType={market_type}&series={series}&symbol={exact_symbol}",
                            "referer": f"https://www.nseindia.com/get-quotes/equity?symbol={exact_symbol}",
                        }
                        async with session.get(url, headers=req_headers) as response:
                            if response.status == 200:
                                data = await response.json()
                                eq_resp = data.get("equityResponse", [])
                                if eq_resp:
                                    trade_info = eq_resp[0].get("tradeInfo") or {}
                                    meta_data = eq_resp[0].get("metaData") or {}
                                    return comp, {
                                        "CH_SYMBOL": exact_symbol,
                                        "CH_SERIES": series,
                                        "mTIMESTAMP": trade_info.get("secwisedelposdate", None),
                                        "CH_PREVIOUS_CLS_PRICE": meta_data.get("previousClose"),
                                        "CH_OPENING_PRICE": meta_data.get("open"),
                                        "CH_TRADE_HIGH_PRICE": meta_data.get("dayHigh"),
                                        "CH_TRADE_LOW_PRICE": meta_data.get("dayLow"),
                                        "CH_LAST_TRADED_PRICE": meta_data.get("lastPrice"),
                                        "CH_CLOSING_PRICE": trade_info.get("closePrice"),
                                        "VWAP": meta_data.get("averagePrice"),
                                        "CH_TOT_TRADED_QTY": trade_info.get("quantitytraded"),
                                        "CH_TOT_TRADED_VAL": trade_info.get("totalTradedValue"),
                                        "CH_TOTAL_TRADES": None,
                                        "CH_TIMESTAMP": None,
                                        "COP_DELIV_QTY": trade_info.get("deliveryquantity"),
                                        "COP_DELIV_PERC": trade_info.get("deliveryToTradedQuantity"),
                                    }

                        # 3. Fallback: If cached request failed (e.g. series reclassified), refresh metadata
                        if meta:
                            nse_company_list = await fetch_nse_exact_symbol_data(symbol)
                            if nse_company_list:
                                exact_symbol = nse_company_list[0].get("symbol") or symbol
                                c_name = "-".join(nse_company_list[0].get("company_name", "").split())
                                series = nse_company_list[0].get("series") or "EQ"
                                metadata = await fetch_nse_metadata(exact_symbol, c_name)
                                market_type = metadata.get("marketType") or "N"

                                # Update Redis cache
                                try:
                                    redis_client_1.hset(
                                        redis_symbol_meta_key,
                                        symbol,
                                        json.dumps({
                                            "symbol": exact_symbol,
                                            "series": series,
                                            "market_type": market_type,
                                        })
                                    )
                                    cached_meta_map[symbol] = {
                                        "symbol": exact_symbol,
                                        "series": series,
                                        "market_type": market_type,
                                    }
                                except Exception:
                                    pass

                                symbol_data = await fetch_nse_symbol_data(exact_symbol, market_type, series)
                                eq_resp = symbol_data.get("equityResponse", [])
                                if eq_resp:
                                    trade_info = eq_resp[0].get("tradeInfo") or {}
                                    meta_data = eq_resp[0].get("metaData") or {}
                                    return comp, {
                                        "CH_SYMBOL": exact_symbol,
                                        "CH_SERIES": series,
                                        "mTIMESTAMP": trade_info.get("secwisedelposdate", None),
                                        "CH_PREVIOUS_CLS_PRICE": meta_data.get("previousClose"),
                                        "CH_OPENING_PRICE": meta_data.get("open"),
                                        "CH_TRADE_HIGH_PRICE": meta_data.get("dayHigh"),
                                        "CH_TRADE_LOW_PRICE": meta_data.get("dayLow"),
                                        "CH_LAST_TRADED_PRICE": meta_data.get("lastPrice"),
                                        "CH_CLOSING_PRICE": trade_info.get("closePrice"),
                                        "VWAP": meta_data.get("averagePrice"),
                                        "CH_TOT_TRADED_QTY": trade_info.get("quantitytraded"),
                                        "CH_TOT_TRADED_VAL": trade_info.get("totalTradedValue"),
                                        "CH_TOTAL_TRADES": None,
                                        "CH_TIMESTAMP": None,
                                        "COP_DELIV_QTY": trade_info.get("deliveryquantity"),
                                        "COP_DELIV_PERC": trade_info.get("deliveryToTradedQuantity"),
                                    }

                        return comp, None
                    except Exception as err:
                        print(f"Error fetching live data for {symbol}: {err}")
                        return comp, None

            # Process in concurrent chunks of 100 to stream to Celery batches
            STREAM_CHUNK = 100
            for i in range(0, len(unprocessed_companies), STREAM_CHUNK):
                company_slice = unprocessed_companies[i : i + STREAM_CHUNK]
                tasks = [fetch_single_company_live(comp) for comp in company_slice]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                chunk_failed_symbols = []
                for comp, res in zip(company_slice, results):
                    if isinstance(res, Exception) or not res:
                        chunk_failed_symbols.append(comp.nse_symbol)
                        continue
                    company, deliverable = res
                    if deliverable:
                        payloads.append({
                            "company_id": company.id,
                            "symbol": company.nse_symbol,
                            "nse": deliverable,
                        })
                        new_process_symbol.append(company.nse_symbol)
                    else:
                        chunk_failed_symbols.append(company.nse_symbol)

                # Remove failed / error symbols from Redis immediately so they don't remain stuck
                if chunk_failed_symbols:
                    try:
                        redis_client_1.srem(f"{redis_prefix}:current_processed_symbols", *chunk_failed_symbols)
                        redis_client_1.srem(f"{redis_prefix}:error", *chunk_failed_symbols)
                    except Exception as redis_err:
                        print(f"Error removing failed symbols from Redis: {redis_err}")

                if len(payloads) >= BATCH_SIZE:
                    group(
                        nse_process_delivery_batch.s(payloads.copy(), [], redis_target),
                    ).apply_async()
                    payloads.clear()

        # Send any remaining payloads to Celery
        if payloads:
            group(
                nse_process_delivery_batch.s(
                    payloads.copy(), [],
                    redis_target
                ),
            ).apply_async()
            payloads.clear()

    finally:
        # Cleanup: Remove any symbols from Redis current_processed_symbols and error that were not sent in batches
        if started_symbols and redis_prefix:
            remaining_symbols = set(started_symbols) - set(new_process_symbol)
            if remaining_symbols:
                try:
                    redis_client_1.srem(f"{redis_prefix}:current_processed_symbols", *remaining_symbols)
                    redis_client_1.srem(f"{redis_prefix}:error", *remaining_symbols)
                except Exception as e:
                    print(f"Error cleaning up remaining symbols from Redis: {e}")
        db.close()

# fetch stock chart data
CHART_GROUP_SIZE = 50

def _fetch_yahoo_stock_sync(ticker: str) -> tuple[dict, list]:
    """Fetch info and max 1d history for a ticker using yfinance synchronously."""
    yf_ticker = yf.Ticker(ticker)

    # 1. Fundamentals / info
    stock_data = {}
    try:
        info = yf_ticker.info
        if isinstance(info, dict):
            stock_data = info
    except Exception:
        stock_data = {}

    # Safeguard: retrieve last price from fast_info if missing in info
    if "currentPrice" not in stock_data and "regularMarketPrice" not in stock_data:
        try:
            fast_info = getattr(yf_ticker, "fast_info", None)
            if fast_info:
                last_price = getattr(fast_info, "last_price", None)
                if last_price:
                    stock_data["currentPrice"] = last_price
        except Exception:
            pass

    # 2. Historical chart data (max daily)
    chart_data = []
    try:
        hist = yf_ticker.history(
            period="max",
            interval="1d",
            auto_adjust=False
        )
        if hist is not None and not hist.empty:
            timestamps = (hist.index.astype("int64") // 10**6).tolist()
            closes = hist["Close"].tolist()
            volumes = hist["Volume"].tolist()
            for ts, c, v in zip(timestamps, closes, volumes):
                if c is not None and c == c and float(c) > 0:
                    chart_data.append([
                        int(ts),
                        round(float(c), 2),
                        "",
                        None,
                        None,
                        int(v) if v is not None and v == v else 0,
                    ])
    except Exception:
        chart_data = []

    return stock_data, chart_data


@shared_task(bind=True)
def process_chart_data_delivery_batch(self, chart_data_history, skip_symbols, redis_target):
    db = SessionLocalSync()

    failed = []
    new_process_symbol = []
    try:
        if chart_data_history:
            company_ids = [item["company_id"] for item in chart_data_history if item.get("company_id")]

            # Bulk fetch existing ChartDataset
            existing_charts = {
                c.company_id: c
                for c in db.scalars(
                    select(ChartDataset).where(
                        ChartDataset.company_id.in_(company_ids),
                        ChartDataset.meta["days"].astext == "30Y"
                    )
                )
            }

            # Bulk fetch existing KeyDetails
            existing_details = {
                kd.company_id: kd
                for kd in db.scalars(
                    select(KeyDetailsForCS).where(
                        KeyDetailsForCS.company_id.in_(company_ids)
                    )
                )
            }

            for item in chart_data_history:
                try:
                    cid = item.get("company_id")
                    if not cid:
                        continue

                    exchange = item.get("exchange") or ("BSE" if item.get("suffix") == "BO" else "NSE")
                    label = f"Price on {exchange}"

                    # Update or insert ChartDataset
                    chart = existing_charts.get(cid)
                    if chart:
                        if item.get("chart_data"):
                            chart.values = item.get("chart_data")
                    elif item.get("chart_data"):
                        db.add(ChartDataset(
                            company_id=cid,
                            metric="Price",
                            label=label,
                            meta={"days": "30Y"},
                            values=item.get("chart_data"),
                        ))

                    # Update or insert KeyDetailsForCS
                    kd = existing_details.get(cid)
                    if kd:
                        kd.current_price = item.get("current_price")
                        kd.pe_ratio = item.get("pe_ratio")
                        kd.book_value = item.get("book_value")
                        kd.dividend_yield = item.get("dividend_yield")
                        kd.roe = item.get("roe")
                        kd.about = item.get("about")
                    else:
                        db.add(KeyDetailsForCS(
                            company_id=cid,
                            current_price=item.get("current_price"),
                            pe_ratio=item.get("pe_ratio"),
                            book_value=item.get("book_value"),
                            dividend_yield=item.get("dividend_yield"),
                            roe=item.get("roe"),
                            about=item.get("about"),
                        ))

                    new_process_symbol.append(item["symbol"])
                except Exception as e:
                    failed.append(item.get("symbol"))

            db.commit()

    except Exception as e:
        db.rollback()
        print(f"Error in process_chart_data_delivery_batch: {e}")
        failed.extend(new_process_symbol)
        new_process_symbol = []
    finally:
        sync_update_nse_bse_gross_deliverable_data_save_processed_symbol(new_process_symbol, "processed_symbols", redis_target)
        failed.extend(skip_symbols)
        if failed:
            sync_update_nse_bse_gross_deliverable_data_save_processed_symbol(failed, "error", redis_target)

        db.close()


async def fetch_and_update_basic_and_30y_stock_chart_data_async(limit: int | None = None):
    db = SessionLocalSync()
    now_ist = datetime.now(ZoneInfo("Asia/Kolkata"))
    date_str = now_ist.strftime("%Y%m%d")
    redis_target = f"redis:daily_basic_and_30y_stock_chart_data:{date_str}"
    redis_prefix = redis_target[6:] if redis_target.startswith("redis:") else redis_target
    redis_suffix_key = "cache:stock_exchange_suffix"

    current_run_symbols = set()
    try:
        from app.db.redis.redis import redis_client_1

        # Clear any stale current_processed_symbols left over from an interrupted or errored previous run
        try:
            redis_client_1.delete(f"{redis_prefix}:current_processed_symbols")
        except Exception as e:
            pass

        try:
            cached_suffix_map = redis_client_1.hgetall(redis_suffix_key) or {}
            # Purge any previously cached "SKIP" entries so they get re-evaluated properly
            bad_skips = [sym for sym, val in cached_suffix_map.items() if val == "SKIP"]
            if bad_skips:
                redis_client_1.hdel(redis_suffix_key, *bad_skips)
                for sym in bad_skips:
                    cached_suffix_map.pop(sym, None)
        except Exception as e:
            print(f"Error loading cached exchange suffix from Redis: {e}")
            cached_suffix_map = {}

        stmt = (
            select(CompanyStock)
            .outerjoin(
                KeyDetailsForCS,
                CompanyStock.id == KeyDetailsForCS.company_id
            )
            .options(selectinload(CompanyStock.details))
        )
        result = db.execute(stmt)
        processed_symbols = set(await update_nse_bse_gross_deliverable_data_load_processed_symbols(redis_target))

        unprocessed_companies = []
        for c in result.scalars():
            if c.nse_symbol and c.nse_symbol not in processed_symbols:
                unprocessed_companies.append(c)
                if limit and len(unprocessed_companies) >= limit:
                    break

        if not unprocessed_companies:
            print(f"All symbols have already been processed for {redis_target}.")
            return

        print(f"Total unprocessed companies to process: {len(unprocessed_companies)}")

        async def resolve_company_suffix(company) -> str:
            """Check Redis cache, DB yahoo_symbol, or NSE/BSE to get suffix."""
            symbol = company.nse_symbol
            if not symbol:
                return "SKIP"

            # 1. Check Redis cache
            cached = cached_suffix_map.get(symbol)
            if cached and cached in ("NS", "BO"):
                return cached

            # 2. Check CompanyStock yahoo_symbol from database
            if company.yahoo_symbol:
                ysym = company.yahoo_symbol.strip().upper()
                if ysym.endswith(".NS"):
                    suffix = "NS"
                elif ysym.endswith(".BO"):
                    suffix = "BO"
                else:
                    suffix = "NS"
                try:
                    redis_client_1.hset(redis_suffix_key, symbol, suffix)
                except Exception:
                    pass
                cached_suffix_map[symbol] = suffix
                return suffix

            # 3. Query NSE/BSE exact symbol data
            try:
                nse_company_list, bse_company_list = await asyncio.gather(
                    fetch_nse_exact_symbol_data(symbol),
                    fetch_bse_exact_symbol_data(symbol),
                )
                if nse_company_list and bse_company_list:
                    suffix = "NS"
                elif bse_company_list:
                    suffix = "BO"
                elif nse_company_list:
                    suffix = "NS"
                else:
                    suffix = "NS" if company.nse_symbol else ("BO" if company.bse_code else "SKIP")
            except Exception as exc:
                # If network error occurs during NSE/BSE lookup, fallback to exchange symbol rather than failing with SKIP
                suffix = "NS" if company.nse_symbol else ("BO" if company.bse_code else "SKIP")

            if suffix != "SKIP":
                try:
                    redis_client_1.hset(redis_suffix_key, symbol, suffix)
                except Exception as redis_exc:
                    print(f"Error storing suffix in Redis for {symbol}: {redis_exc}")
                cached_suffix_map[symbol] = suffix

            return suffix

        # Concurrency limiter (safe rate limit for Yahoo Finance)
        semaphore = asyncio.Semaphore(15)

        async def fetch_single_company(company):
            symbol = company.nse_symbol
            if not symbol:
                return company, None

            suffix = await resolve_company_suffix(company)
            if suffix == "SKIP":
                return company, None

            ticker = f"{symbol}.{suffix}"

            async with semaphore:
                try:
                    stock_data, chart_data = await asyncio.wait_for(
                        asyncio.to_thread(_fetch_yahoo_stock_sync, ticker),
                        timeout=15.0
                    )
                    if not stock_data and not chart_data:
                        return company, None

                    current_price = stock_data.get("currentPrice") or stock_data.get("regularMarketPrice")
                    if (current_price is None or current_price == 0) and chart_data:
                        current_price = chart_data[-1][1]

                    payload = {
                        "company_id": company.id,
                        "symbol": symbol,
                        "exchange": "BSE" if suffix == "BO" else "NSE",
                        "suffix": suffix,
                        "chart_data": chart_data,
                        "about": stock_data.get("longBusinessSummary"),
                        "book_value": round(stock_data.get("bookValue") or 0, 2),
                        "dividend_yield": round(stock_data.get("dividendYield") or 0, 2),
                        "pe_ratio": round(stock_data.get("trailingPE") or 0, 2),
                        "roe": round((stock_data.get("returnOnEquity") or 0) * 100, 2),
                        "current_price": round(current_price or 0, 2),
                    }
                    return company, payload
                except Exception as err:
                    print(f"Error fetching Yahoo data for {symbol} ({ticker}): {err}")
                    return company, None

        # Process in streaming chunks of 100
        STREAM_CHUNK = 100
        payloads = []
        skip_symbols = []

        for i in range(0, len(unprocessed_companies), STREAM_CHUNK):
            company_slice = unprocessed_companies[i : i + STREAM_CHUNK]
            slice_symbols = [c.nse_symbol for c in company_slice]
            current_run_symbols.update(slice_symbols)
            await update_nse_bse_gross_deliverable_data_save_processed_symbol(
                slice_symbols, "current_processed_symbols", redis_target
            )

            tasks = [
                fetch_single_company(comp)
                for comp in company_slice
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            chunk_failed = []
            for comp, res in zip(company_slice, results):
                if isinstance(res, Exception) or not res or res[1] is None:
                    skip_symbols.append(comp.nse_symbol)
                    chunk_failed.append(comp.nse_symbol)
                else:
                    _, payload = res
                    payloads.append(payload)

            # Immediately remove failed symbols from current_processed_symbols in Redis
            # and record them into error set so they never remain stuck
            if chunk_failed:
                try:
                    current_run_symbols.difference_update(chunk_failed)
                    redis_client_1.srem(f"{redis_prefix}:current_processed_symbols", *chunk_failed)
                    sync_update_nse_bse_gross_deliverable_data_save_processed_symbol(
                        chunk_failed, "error", redis_target
                    )
                except Exception as redis_err:
                    print(f"Error cleaning up failed symbols from Redis: {redis_err}")

            if len(payloads) >= CHART_GROUP_SIZE:
                group(
                    process_chart_data_delivery_batch.s(payloads.copy(), skip_symbols.copy(), redis_target),
                ).apply_async()
                payloads.clear()
                skip_symbols.clear()

        # Flush any remaining payloads
        if payloads or skip_symbols:
            group(
                process_chart_data_delivery_batch.s(
                    payloads.copy(), skip_symbols.copy(),
                    redis_target
                ),
            ).apply_async()
            payloads.clear()
            skip_symbols.clear()

    except Exception as main_err:
        print(f"Error in fetch_and_update_basic_and_30y_stock_chart_data_async: {main_err}")
        # If an error occurs in main function, immediately remove all symbols added to current_processed_symbols during this run
        try:
            from app.db.redis.redis import redis_client_1
            if current_run_symbols:
                redis_client_1.srem(f"{redis_prefix}:current_processed_symbols", *current_run_symbols)
            else:
                redis_client_1.delete(f"{redis_prefix}:current_processed_symbols")
        except Exception as redis_clean_err:
            print(f"Error cleaning up current_processed_symbols on main error: {redis_clean_err}")
        raise main_err
    finally:
        db.close()


#block deal

BULK_BLOCK_DEAL_GROUP_SIZE = 500

@shared_task(bind=True)
def process_past_block_deal_batch(self, block_deals):
    db = SessionLocalSync()

    try:
        market_deals = []
        seen_in_batch = set()

        for item in block_deals:
            try:
                company_id = item.get("company_id")

                if not company_id:
                    continue

                date_raw = (
                    item.get("Date ", "")
                    or item.get("Date", "")
                    or ""
                ).strip()

                if not date_raw:
                    continue

                trade_date = datetime.strptime(
                    date_raw,
                    "%d-%b-%Y"
                ).date()

                symbol = (
                    item.get("Symbol ", "")
                    or item.get("Symbol", "")
                    or ""
                ).strip()

                client_name = (
                    item.get("Client Name ", "")
                    or item.get("Client Name", "")
                    or ""
                ).strip()

                buy_sell = (
                    item.get("Buy / Sell ", "")
                    or item.get("Buy / Sell", "")
                    or ""
                ).strip().upper()

                quantity_raw = (
                    item.get("Quantity Traded ", "")
                    or item.get("Quantity Traded", "")
                    or ""
                )

                quantity = int(
                    str(quantity_raw)
                    .replace(",", "")
                    .strip()
                ) if quantity_raw and str(quantity_raw).replace(",", "").strip().lstrip("-").isdigit() else None

                price_raw = (
                    item.get(
                        "Trade Price / Wght. Avg. Price ",
                        ""
                    )
                    or item.get("Trade Price / Wght. Avg. Price", "")
                    or ""
                )

                try:
                    price = float(
                        str(price_raw)
                        .replace(",", "")
                        .strip()
                    ) if price_raw else None
                except (ValueError, TypeError):
                    price = None

                value = None

                # Check duplicate in current batch
                deal_signature = (
                    company_id,
                    "BLOCK",
                    trade_date,
                    symbol,
                    client_name,
                    buy_sell,
                    quantity,
                    price,
                    "NSE",
                )
                if deal_signature in seen_in_batch:
                    continue
                seen_in_batch.add(deal_signature)

                # Check duplicate in database
                existing_deal = db.execute(
                    select(MarketDeal.id)
                    .where(
                        MarketDeal.company_id == company_id,
                        MarketDeal.deal_type == "BLOCK",
                        MarketDeal.trade_date == trade_date,
                        MarketDeal.symbol == symbol,
                        MarketDeal.client_name == client_name,
                        MarketDeal.buy_sell == buy_sell,
                        MarketDeal.quantity == quantity,
                        MarketDeal.price == price,
                        MarketDeal.exchange == "NSE",
                    )
                    .limit(1)
                ).scalar_one_or_none()

                if existing_deal:
                    continue

                market_deals.append({
                    "company_id": company_id,
                    "deal_type": "BLOCK",
                    "trade_date": trade_date,
                    "symbol": symbol,
                    "client_name": client_name,
                    "buy_sell": buy_sell,
                    "quantity": quantity,
                    "price": price,
                    "value": value,
                    "exchange": "NSE",
                })

            except Exception as e:
                print(
                    f"Failed block deal: {item} | Error: {e}"
                )

        if market_deals:
            stmt = insert(MarketDeal)
            db.execute(stmt, market_deals)
            db.commit()

        print(
            f"Inserted {len(market_deals)} block deals"
        )

        return {
            "inserted": len(market_deals)
        }

    except Exception:
        db.rollback()
        raise

    finally:
        db.close()

async def fetch_past_block_deal_async():
    from app.db.redis.redis import redis_client_1

    redis_prefix = "fetch_past_block_deal"
    proc_key = f"{redis_prefix}:processed_symbols"
    current_key = f"{redis_prefix}:current_processed_symbols"
    err_key = f"{redis_prefix}:error"

    db = SessionLocalSync()
    started_symbols = []
    try:
        companies = (
            db.query(CompanyStock.id, CompanyStock.nse_symbol)
            .filter(CompanyStock.nse_symbol.isnot(None))
            .all()
        )

        company_map = {
            symbol.strip().upper(): company_id
            for company_id, symbol in companies
            if symbol
        }

        started_symbols = list(company_map.keys())

        # Store currently processing symbols in Redis without expiration
        if started_symbols:
            for i in range(0, len(started_symbols), 1000):
                redis_client_1.sadd(current_key, *started_symbols[i:i + 1000])

        today = bdate.today()
        date_ranges = []

        for i in range(10):
            if i == 0:
                from_date = today - relativedelta(years=1)
                to_date = today
            else:
                from_date = today - relativedelta(years=i + 1)
                to_date = today - relativedelta(years=i) - timedelta(days=1)

            date_ranges.append({
                "from": from_date.strftime("%d-%m-%Y"),
                "to": to_date.strftime("%d-%m-%Y"),
            })

        oldest_year = today.year - 10

        date_ranges.append({
            "from": bdate(oldest_year, 1, 1).strftime("%d-%m-%Y"),
            "to": (
                    today - relativedelta(years=10) - timedelta(days=1)
            ).strftime("%d-%m-%Y"),
        })

        all_block_deals = []
        error_symbols = set()
        deal_symbols = set()

        for c_date in date_ranges:
            block_deals = await main_block_deals({
                "optionType": "block_deals",
                "from": c_date.get("from"),
                "to": c_date.get("to"),
                "csv": "true",
            })
            if not block_deals:
                continue

            # -----------------------------------------
            # 4. Add company_id to every record
            # -----------------------------------------
            for deal in block_deals:
                try:
                    symbol = (
                            deal.get("Symbol")
                            or deal.get("Symbol ")
                            or ""
                    ).strip().upper()

                    if not symbol:
                        continue

                    company_id = company_map.get(symbol)

                    if not company_id:
                        # Ignore symbols that are not in the database
                        continue

                    deal["company_id"] = company_id
                    deal_symbols.add(symbol)
                    all_block_deals.append(deal)

                except Exception as deal_err:
                    print(f"Error processing deal {deal}: {deal_err}")
                    if symbol and symbol in company_map:
                        error_symbols.add(symbol)

        # Update any database error symbols in Redis without expiration
        if error_symbols:
            err_list = list(error_symbols)
            for i in range(0, len(err_list), 1000):
                chunk = err_list[i:i + 1000]
                redis_client_1.sadd(err_key, *chunk)
                redis_client_1.srem(current_key, *chunk)

        if all_block_deals:
            for i in range(0, len(all_block_deals), BULK_BLOCK_DEAL_GROUP_SIZE):
                batch = all_block_deals[i:i + BULK_BLOCK_DEAL_GROUP_SIZE]
                batch_symbols = {
                    (d.get("Symbol") or d.get("Symbol ") or "").strip().upper()
                    for d in batch
                    if (d.get("Symbol") or d.get("Symbol "))
                }
                batch_symbols.discard("")

                print(
                    f"Batch {i // CHART_GROUP_SIZE + 1}: "
                    f"{len(batch)} records"
                )

                try:
                    process_past_block_deal_batch.delay(
                        batch
                    )
                except Exception as batch_err:
                    print(f"Error dispatching batch to Celery: {batch_err}")
                    failed_db_symbols = {s for s in batch_symbols if s in company_map}
                    if failed_db_symbols:
                        error_symbols.update(failed_db_symbols)
                        b_list = list(failed_db_symbols)
                        for b_i in range(0, len(b_list), 1000):
                            chunk = b_list[b_i:b_i + 1000]
                            redis_client_1.sadd(err_key, *chunk)
                            redis_client_1.srem(current_key, *chunk)

        # Store successful symbols in Redis without expiration
        success_symbols = (set(started_symbols) | deal_symbols) - error_symbols
        if success_symbols:
            success_list = list(success_symbols)
            for i in range(0, len(success_list), 1000):
                chunk = success_list[i:i + 1000]
                redis_client_1.sadd(proc_key, *chunk)
                redis_client_1.srem(current_key, *chunk)

        redis_client_1.delete(current_key)

    except Exception as e:
        try:
            if started_symbols:
                for i in range(0, len(started_symbols), 1000):
                    redis_client_1.srem(current_key, *started_symbols[i:i + 1000])
            redis_client_1.delete(current_key)
        except Exception as redis_err:
            print(f"Error removing current_processed_symbols from Redis: {redis_err}")
        raise e

    finally:
        db.close()


@shared_task(bind=True)
def process_daily_block_deal_batch(self, block_deals):
    db = SessionLocalSync()

    try:
        market_deals = []

        for item in block_deals:
            try:
                company_id = item.get("company_id")

                if not company_id:
                    continue

                trade_date = datetime.strptime(
                    item.get("Date ", "").strip(),
                    "%d-%b-%Y"
                ).date()

                symbol = (
                    item.get("Symbol ", "") or ""
                ).strip()

                client_name = (
                    item.get("Client Name ", "") or ""
                ).strip()

                buy_sell = (
                    item.get("Buy / Sell ", "") or ""
                ).strip().upper()

                quantity_raw = (
                    item.get("Quantity Traded ", "") or ""
                )

                quantity = (
                    int(
                        str(quantity_raw)
                        .replace(",", "")
                        .strip()
                    )
                    if quantity_raw
                    else None
                )

                price_raw = (
                    item.get(
                        "Trade Price / Wght. Avg. Price ",
                        ""
                    ) or ""
                )

                price = (
                    float(
                        str(price_raw)
                        .replace(",", "")
                        .strip()
                    )
                    if price_raw
                    else None
                )

                # Check duplicate
                existing_deal = db.execute(
                    select(MarketDeal.id)
                    .where(
                        MarketDeal.company_id == company_id,
                        MarketDeal.deal_type == "BLOCK",
                        MarketDeal.trade_date == trade_date,
                        MarketDeal.symbol == symbol,
                        MarketDeal.client_name == client_name,
                        MarketDeal.buy_sell == buy_sell,
                        MarketDeal.quantity == quantity,
                        MarketDeal.price == price,
                        MarketDeal.exchange == "NSE",
                    )
                    .limit(1)
                ).scalar_one_or_none()

                if existing_deal:
                    continue

                market_deals.append({
                    "company_id": company_id,
                    "deal_type": "BLOCK",
                    "trade_date": trade_date,
                    "symbol": symbol,
                    "client_name": client_name,
                    "buy_sell": buy_sell,
                    "quantity": quantity,
                    "price": price,
                    "value": None,
                    "exchange": "NSE",
                })

            except Exception as e:
                print(
                    f"Failed block deal: {item} | Error: {e}"
                )

        if market_deals:
            db.execute(
                insert(MarketDeal),
                market_deals
            )
            db.commit()

        print(
            f"Inserted {len(market_deals)} new block deals"
        )

        return {
            "inserted": len(market_deals)
        }

    except Exception:
        db.rollback()
        raise

    finally:
        db.close()

async def fetch_daily_block_deal_async():
    db = SessionLocalSync()
    try:
        companies = (
            db.query(CompanyStock.id, CompanyStock.nse_symbol)
            .filter(CompanyStock.nse_symbol.isnot(None))
            .all()
        )

        company_map = {
            symbol.strip().upper(): company_id
            for company_id, symbol in companies
            if symbol
        }

        today = bdate.today().strftime("%d-%m-%Y")
        one_day_behind = (bdate.today() - BDay(1)).strftime("%d-%m-%Y")

        all_block_deals = []
        block_deals = await main_block_deals({
            "optionType": "block_deals",
            "from": one_day_behind,
            "to": today,
            "csv": "true",
        })

        if block_deals:
            for deal in block_deals:
                symbol = (
                        deal.get("Symbol")
                        or deal.get("Symbol ")
                        or ""
                ).strip().upper()

                company_id = company_map.get(symbol)

                deal["company_id"] = company_id

                all_block_deals.append(deal)


            if all_block_deals:
                for i in range(0, len(all_block_deals), BULK_BLOCK_DEAL_GROUP_SIZE):
                    batch = all_block_deals[i:i + BULK_BLOCK_DEAL_GROUP_SIZE]

                    print(
                        f"Batch {i // CHART_GROUP_SIZE + 1}: "
                        f"{len(batch)} records"
                    )

                    process_daily_block_deal_batch.delay(
                        batch
                    )

    finally:
        db.close()

#bulk deal

@shared_task(bind=True)
def process_past_bulk_deal_batch(self, block_deals):
    db = SessionLocalSync()

    try:
        market_deals = []
        seen_in_batch = set()

        for item in block_deals:
            try:
                company_id = item.get("company_id")

                if not company_id:
                    continue

                date_raw = (
                    item.get("Date ", "")
                    or item.get("Date", "")
                    or ""
                ).strip()

                if not date_raw:
                    continue

                trade_date = datetime.strptime(
                    date_raw,
                    "%d-%b-%Y"
                ).date()

                symbol = (
                    item.get("Symbol ", "")
                    or item.get("Symbol", "")
                    or ""
                ).strip()

                client_name = (
                    item.get("Client Name ", "")
                    or item.get("Client Name", "")
                    or ""
                ).strip()

                buy_sell = (
                    item.get("Buy / Sell ", "")
                    or item.get("Buy / Sell", "")
                    or ""
                ).strip().upper()

                quantity_raw = (
                    item.get("Quantity Traded ", "")
                    or item.get("Quantity Traded", "")
                    or ""
                )

                quantity = int(
                    str(quantity_raw)
                    .replace(",", "")
                    .strip()
                ) if quantity_raw and str(quantity_raw).replace(",", "").strip().lstrip("-").isdigit() else None

                price_raw = (
                    item.get(
                        "Trade Price / Wght. Avg. Price ",
                        ""
                    )
                    or item.get("Trade Price / Wght. Avg. Price", "")
                    or ""
                )

                try:
                    price = float(
                        str(price_raw)
                        .replace(",", "")
                        .strip()
                    ) if price_raw else None
                except (ValueError, TypeError):
                    price = None

                value = None

                # Check duplicate in current batch
                deal_signature = (
                    company_id,
                    "BULK",
                    trade_date,
                    symbol,
                    client_name,
                    buy_sell,
                    quantity,
                    price,
                    "NSE",
                )
                if deal_signature in seen_in_batch:
                    continue
                seen_in_batch.add(deal_signature)

                # Check duplicate in database
                existing_deal = db.execute(
                    select(MarketDeal.id)
                    .where(
                        MarketDeal.company_id == company_id,
                        MarketDeal.deal_type == "BULK",
                        MarketDeal.trade_date == trade_date,
                        MarketDeal.symbol == symbol,
                        MarketDeal.client_name == client_name,
                        MarketDeal.buy_sell == buy_sell,
                        MarketDeal.quantity == quantity,
                        MarketDeal.price == price,
                        MarketDeal.exchange == "NSE",
                    )
                    .limit(1)
                ).scalar_one_or_none()

                if existing_deal:
                    continue

                market_deals.append({
                    "company_id": company_id,
                    "deal_type": "BULK",
                    "trade_date": trade_date,
                    "symbol": symbol,
                    "client_name": client_name,
                    "buy_sell": buy_sell,
                    "quantity": quantity,
                    "price": price,
                    "value": value,
                    "exchange": "NSE",
                })

            except Exception as e:
                print(
                    f"Failed block deal: {item} | Error: {e}"
                )

        if market_deals:
            stmt = insert(MarketDeal)
            db.execute(stmt, market_deals)
            db.commit()

        print(
            f"Inserted {len(market_deals)} block deals"
        )

        return {
            "inserted": len(market_deals)
        }

    except Exception:
        db.rollback()
        raise

    finally:
        db.close()

async def fetch_past_bulk_deal_async():
    from app.db.redis.redis import redis_client_1

    redis_prefix = "fetch_past_bulk_deal"
    proc_key = f"{redis_prefix}:processed_symbols"
    current_key = f"{redis_prefix}:current_processed_symbols"
    err_key = f"{redis_prefix}:error"

    db = SessionLocalSync()
    started_symbols = []
    try:
        companies = (
            db.query(CompanyStock.id, CompanyStock.nse_symbol)
            .filter(CompanyStock.nse_symbol.isnot(None))
            .all()
        )

        company_map = {
            symbol.strip().upper(): company_id
            for company_id, symbol in companies
            if symbol
        }

        started_symbols = list(company_map.keys())

        # Store currently processing symbols in Redis without expiration
        if started_symbols:
            for i in range(0, len(started_symbols), 1000):
                redis_client_1.sadd(current_key, *started_symbols[i:i + 1000])

        today = bdate.today()
        date_ranges = []

        for i in range(10):
            if i == 0:
                from_date = today - relativedelta(years=1)
                to_date = today
            else:
                from_date = today - relativedelta(years=i + 1)
                to_date = today - relativedelta(years=i) - timedelta(days=1)

            date_ranges.append({
                "from": from_date.strftime("%d-%m-%Y"),
                "to": to_date.strftime("%d-%m-%Y"),
            })

        oldest_year = today.year - 10

        date_ranges.append({
            "from": bdate(oldest_year, 1, 1).strftime("%d-%m-%Y"),
            "to": (
                    today - relativedelta(years=10) - timedelta(days=1)
            ).strftime("%d-%m-%Y"),
        })

        all_block_deals = []
        error_symbols = set()
        deal_symbols = set()

        for c_date in date_ranges:
            block_deals = await main_block_deals({
                "optionType": "bulk_deals",
                "from": c_date.get("from"),
                "to": c_date.get("to"),
                "csv": "true",
            })
            if not block_deals:
                continue

            # -----------------------------------------
            # 4. Add company_id to every record
            # -----------------------------------------
            for deal in block_deals:
                try:
                    symbol = (
                            deal.get("Symbol")
                            or deal.get("Symbol ")
                            or ""
                    ).strip().upper()

                    if not symbol:
                        continue

                    company_id = company_map.get(symbol)

                    if not company_id:
                        # Ignore symbols not in database
                        continue

                    deal["company_id"] = company_id
                    deal_symbols.add(symbol)
                    all_block_deals.append(deal)

                except Exception as deal_err:
                    print(f"Error processing deal {deal}: {deal_err}")
                    if symbol and symbol in company_map:
                        error_symbols.add(symbol)

        # Update any database error symbols in Redis without expiration
        if error_symbols:
            err_list = list(error_symbols)
            for i in range(0, len(err_list), 1000):
                chunk = err_list[i:i + 1000]
                redis_client_1.sadd(err_key, *chunk)
                redis_client_1.srem(current_key, *chunk)

        if all_block_deals:
            for i in range(0, len(all_block_deals), BULK_BLOCK_DEAL_GROUP_SIZE):
                batch = all_block_deals[i:i + BULK_BLOCK_DEAL_GROUP_SIZE]
                batch_symbols = {
                    (d.get("Symbol") or d.get("Symbol ") or "").strip().upper()
                    for d in batch
                    if (d.get("Symbol") or d.get("Symbol "))
                }
                batch_symbols.discard("")

                print(
                    f"Batch {i // CHART_GROUP_SIZE + 1}: "
                    f"{len(batch)} records"
                )

                try:
                    process_past_bulk_deal_batch.delay(
                        batch
                    )
                except Exception as batch_err:
                    print(f"Error dispatching batch to Celery: {batch_err}")
                    failed_db_symbols = {s for s in batch_symbols if s in company_map}
                    if failed_db_symbols:
                        error_symbols.update(failed_db_symbols)
                        b_list = list(failed_db_symbols)
                        for b_i in range(0, len(b_list), 1000):
                            chunk = b_list[b_i:b_i + 1000]
                            redis_client_1.sadd(err_key, *chunk)
                            redis_client_1.srem(current_key, *chunk)

        # Store successful symbols in Redis without expiration
        success_symbols = (set(started_symbols) | deal_symbols) - error_symbols
        if success_symbols:
            success_list = list(success_symbols)
            for i in range(0, len(success_list), 1000):
                chunk = success_list[i:i + 1000]
                redis_client_1.sadd(proc_key, *chunk)
                redis_client_1.srem(current_key, *chunk)

        # Remove any remaining from current_processed_symbols
        redis_client_1.delete(current_key)

    except Exception as e:
        try:
            if started_symbols:
                for i in range(0, len(started_symbols), 1000):
                    redis_client_1.srem(current_key, *started_symbols[i:i + 1000])
            redis_client_1.delete(current_key)
        except Exception as redis_err:
            print(f"Error removing current_processed_symbols from Redis: {redis_err}")
        raise e

    finally:
        db.close()


@shared_task(bind=True)
def process_daily_bulk_deal_batch(self, block_deals):
    db = SessionLocalSync()

    try:
        market_deals = []

        for item in block_deals:
            try:
                company_id = item.get("company_id")

                if not company_id:
                    continue

                trade_date = datetime.strptime(
                    item.get("Date ", "").strip(),
                    "%d-%b-%Y"
                ).date()

                symbol = (
                    item.get("Symbol ", "") or ""
                ).strip()

                client_name = (
                    item.get("Client Name ", "") or ""
                ).strip()

                buy_sell = (
                    item.get("Buy / Sell ", "") or ""
                ).strip().upper()

                quantity_raw = (
                    item.get("Quantity Traded ", "") or ""
                )

                quantity = (
                    int(
                        str(quantity_raw)
                        .replace(",", "")
                        .strip()
                    )
                    if quantity_raw
                    else None
                )

                price_raw = (
                    item.get(
                        "Trade Price / Wght. Avg. Price ",
                        ""
                    ) or ""
                )

                price = (
                    float(
                        str(price_raw)
                        .replace(",", "")
                        .strip()
                    )
                    if price_raw
                    else None
                )

                # Check duplicate
                existing_deal = db.execute(
                    select(MarketDeal.id)
                    .where(
                        MarketDeal.company_id == company_id,
                        MarketDeal.deal_type == "BULK",
                        MarketDeal.trade_date == trade_date,
                        MarketDeal.symbol == symbol,
                        MarketDeal.client_name == client_name,
                        MarketDeal.buy_sell == buy_sell,
                        MarketDeal.quantity == quantity,
                        MarketDeal.price == price,
                        MarketDeal.exchange == "NSE",
                    )
                    .limit(1)
                ).scalar_one_or_none()

                if existing_deal:
                    continue

                market_deals.append({
                    "company_id": company_id,
                    "deal_type": "BULK",
                    "trade_date": trade_date,
                    "symbol": symbol,
                    "client_name": client_name,
                    "buy_sell": buy_sell,
                    "quantity": quantity,
                    "price": price,
                    "value": None,
                    "exchange": "NSE",
                })

            except Exception as e:
                print(
                    f"Failed block deal: {item} | Error: {e}"
                )

        if market_deals:
            db.execute(
                insert(MarketDeal),
                market_deals
            )
            db.commit()

        print(
            f"Inserted {len(market_deals)} new block deals"
        )

        return {
            "inserted": len(market_deals)
        }

    except Exception:
        db.rollback()
        raise

    finally:
        db.close()

async def fetch_daily_bulk_deal_async():
    db = SessionLocalSync()
    try:
        companies = (
            db.query(CompanyStock.id, CompanyStock.nse_symbol)
            .filter(CompanyStock.nse_symbol.isnot(None))
            .all()
        )

        company_map = {
            symbol.strip().upper(): company_id
            for company_id, symbol in companies
            if symbol
        }

        today = bdate.today().strftime("%d-%m-%Y")
        one_day_behind = (bdate.today() - BDay(1)).strftime("%d-%m-%Y")

        all_block_deals = []
        block_deals = await main_block_deals({
            "optionType": "bulk_deals",
            "from": one_day_behind,
            "to": today,
            "csv": "true",
        })

        if block_deals:
            for deal in block_deals:
                symbol = (
                        deal.get("Symbol")
                        or deal.get("Symbol ")
                        or ""
                ).strip().upper()

                company_id = company_map.get(symbol)

                deal["company_id"] = company_id

                all_block_deals.append(deal)


            if all_block_deals:
                for i in range(0, len(all_block_deals), BULK_BLOCK_DEAL_GROUP_SIZE):
                    batch = all_block_deals[i:i + BULK_BLOCK_DEAL_GROUP_SIZE]

                    print(
                        f"Batch {i // CHART_GROUP_SIZE + 1}: "
                        f"{len(batch)} records"
                    )

                    process_daily_bulk_deal_batch.delay(
                        batch
                    )

    finally:
        db.close()


#short shelling
@shared_task(bind=True)
def process_past_short_selling_batch(self, block_deals):
    db = SessionLocalSync()

    try:
        market_deals = []
        seen_in_batch = set()

        for item in block_deals:
            try:
                company_id = item.get("company_id")

                if not company_id:
                    continue

                date_str = (
                    item.get("Date ", "")
                    or item.get("Date", "")
                    or ""
                ).strip()

                if not date_str:
                    continue

                trade_date = datetime.strptime(
                    date_str,
                    "%d-%b-%Y"
                ).date()

                symbol = (
                    item.get("Symbol ", "")
                    or item.get("Symbol", "")
                    or ""
                ).strip()

                client_name = (
                    item.get("Security Name ", "")
                    or item.get("Security Name", "")
                    or item.get("Client Name ", "")
                    or item.get("Client Name", "")
                    or ""
                ).strip()

                buy_sell = None

                quantity_raw = (
                    item.get("Quantity ", "")
                    or item.get("Quantity", "")
                    or ""
                )

                quantity = int(
                    str(quantity_raw)
                    .replace(",", "")
                    .strip()
                ) if quantity_raw else None

                price = None
                value = None

                # Check duplicate within the current batch
                deal_key = (company_id, trade_date, symbol, client_name, quantity)
                if deal_key in seen_in_batch:
                    continue
                seen_in_batch.add(deal_key)

                # Check duplicate in database
                existing_deal = db.execute(
                    select(MarketDeal.id)
                    .where(
                        MarketDeal.company_id == company_id,
                        MarketDeal.deal_type == "SHORT_SELLING",
                        MarketDeal.trade_date == trade_date,
                        MarketDeal.symbol == symbol,
                        MarketDeal.client_name == client_name,
                        MarketDeal.quantity == quantity,
                        MarketDeal.exchange == "NSE",
                    )
                    .limit(1)
                ).scalar_one_or_none()

                if existing_deal:
                    continue

                market_deals.append({
                    "company_id": company_id,
                    "deal_type": "SHORT_SELLING",
                    "trade_date": trade_date,
                    "symbol": symbol,
                    "client_name": client_name,
                    "buy_sell": buy_sell,
                    "quantity": quantity,
                    "price": price,
                    "value": value,
                    "exchange": "NSE",
                })

            except Exception as e:
                print(
                    f"Failed short selling deal: {item} | Error: {e}"
                )

        if market_deals:
            stmt = insert(MarketDeal)
            db.execute(stmt, market_deals)
            db.commit()

        print(
            f"Inserted {len(market_deals)} new short selling deals"
        )

        return {
            "inserted": len(market_deals)
        }

    except Exception:
        db.rollback()
        raise

    finally:
        db.close()

async def fetch_past_short_selling_async():
    from app.db.redis.redis import redis_client_1

    redis_prefix = "fetch_past_short_selling"
    proc_key = f"{redis_prefix}:processed_symbols"
    current_key = f"{redis_prefix}:current_processed_symbols"
    err_key = f"{redis_prefix}:error"

    db = SessionLocalSync()
    started_symbols = []
    try:
        companies = (
            db.query(CompanyStock.id, CompanyStock.nse_symbol)
            .filter(CompanyStock.nse_symbol.isnot(None))
            .all()
        )

        company_map = {
            symbol.strip().upper(): company_id
            for company_id, symbol in companies
            if symbol
        }

        started_symbols = list(company_map.keys())

        # Store currently processing symbols in Redis without expiration
        if started_symbols:
            for i in range(0, len(started_symbols), 1000):
                redis_client_1.sadd(current_key, *started_symbols[i:i + 1000])

        today = bdate.today()
        date_ranges = []

        for i in range(10):
            if i == 0:
                from_date = today - relativedelta(years=1)
                to_date = today
            else:
                from_date = today - relativedelta(years=i + 1)
                to_date = today - relativedelta(years=i) - timedelta(days=1)

            date_ranges.append({
                "from": from_date.strftime("%d-%m-%Y"),
                "to": to_date.strftime("%d-%m-%Y"),
            })

        oldest_year = today.year - 10

        date_ranges.append({
            "from": bdate(oldest_year, 1, 1).strftime("%d-%m-%Y"),
            "to": (
                    today - relativedelta(years=10) - timedelta(days=1)
            ).strftime("%d-%m-%Y"),
        })

        all_short_sellings = []
        error_symbols = set()
        deal_symbols = set()

        for c_date in date_ranges:
            short_sellings = await main_block_deals({
                "optionType": "short_selling",
                "from": c_date.get("from"),
                "to": c_date.get("to"),
                "csv": "true",
            })
            if not short_sellings:
                continue

            # -----------------------------------------
            # 4. Add company_id to every record
            # -----------------------------------------
            for deal in short_sellings:
                try:
                    symbol = (
                            deal.get("Symbol")
                            or deal.get("Symbol ")
                            or ""
                    ).strip().upper()

                    if not symbol:
                        continue

                    company_id = company_map.get(symbol)

                    if not company_id:
                        # Ignore symbols not in database
                        continue

                    deal["company_id"] = company_id
                    deal_symbols.add(symbol)
                    all_short_sellings.append(deal)

                except Exception as deal_err:
                    print(f"Error processing deal {deal}: {deal_err}")
                    if symbol and symbol in company_map:
                        error_symbols.add(symbol)

        # Update any database error symbols in Redis without expiration
        if error_symbols:
            err_list = list(error_symbols)
            for i in range(0, len(err_list), 1000):
                chunk = err_list[i:i + 1000]
                redis_client_1.sadd(err_key, *chunk)
                redis_client_1.srem(current_key, *chunk)

        if all_short_sellings:
            for i in range(0, len(all_short_sellings), BULK_BLOCK_DEAL_GROUP_SIZE):
                batch = all_short_sellings[i:i + BULK_BLOCK_DEAL_GROUP_SIZE]
                batch_symbols = {
                    (d.get("Symbol") or d.get("Symbol ") or "").strip().upper()
                    for d in batch
                    if (d.get("Symbol") or d.get("Symbol "))
                }
                batch_symbols.discard("")

                print(
                    f"Batch {i // CHART_GROUP_SIZE + 1}: "
                    f"{len(batch)} records"
                )

                try:
                    process_past_short_selling_batch.delay(
                        batch
                    )
                except Exception as batch_err:
                    print(f"Error dispatching batch to Celery: {batch_err}")
                    failed_db_symbols = {s for s in batch_symbols if s in company_map}
                    if failed_db_symbols:
                        error_symbols.update(failed_db_symbols)
                        b_list = list(failed_db_symbols)
                        for b_i in range(0, len(b_list), 1000):
                            chunk = b_list[b_i:b_i + 1000]
                            redis_client_1.sadd(err_key, *chunk)
                            redis_client_1.srem(current_key, *chunk)

        # Store successful symbols in Redis without expiration
        success_symbols = (set(started_symbols) | deal_symbols) - error_symbols
        if success_symbols:
            success_list = list(success_symbols)
            for i in range(0, len(success_list), 1000):
                chunk = success_list[i:i + 1000]
                redis_client_1.sadd(proc_key, *chunk)
                redis_client_1.srem(current_key, *chunk)

        # Remove any remaining from current_processed_symbols
        redis_client_1.delete(current_key)

    except Exception as e:
        try:
            if started_symbols:
                for i in range(0, len(started_symbols), 1000):
                    redis_client_1.srem(current_key, *started_symbols[i:i + 1000])
            redis_client_1.delete(current_key)
        except Exception as redis_err:
            print(f"Error removing current_processed_symbols from Redis: {redis_err}")
        raise e

    finally:
        db.close()


@shared_task(bind=True)
def process_daily_short_selling_batch(self, block_deals):
    db = SessionLocalSync()

    try:
        market_deals = []

        for item in block_deals:
            try:
                company_id = item.get("company_id")

                if not company_id:
                    continue

                trade_date = datetime.strptime(
                    item.get("Date ", "").strip(),
                    "%d-%b-%Y"
                ).date()

                symbol = (
                    item.get("Symbol ", "") or ""
                ).strip()

                client_name = (
                    item.get("Security Name ", "") or ""
                ).strip()

                buy_sell = None

                quantity_raw = (
                    item.get("Quantity ", "") or ""
                )

                quantity = (
                    int(
                        str(quantity_raw)
                        .replace(",", "")
                        .strip()
                    )
                    if quantity_raw
                    else None
                )

                price = None
                value = None

                # Check duplicate
                existing_deal = db.execute(
                    select(MarketDeal.id)
                    .where(
                        MarketDeal.company_id == company_id,
                        MarketDeal.deal_type == "SHORT_SELLING",
                        MarketDeal.trade_date == trade_date,
                        MarketDeal.symbol == symbol,
                        MarketDeal.client_name == client_name,
                        MarketDeal.buy_sell == buy_sell,
                        MarketDeal.quantity == quantity,
                        MarketDeal.price == price,
                        MarketDeal.exchange == "NSE",
                    )
                    .limit(1)
                ).scalar_one_or_none()

                if existing_deal:
                    continue

                market_deals.append({
                    "company_id": company_id,
                    "deal_type": "SHORT_SELLING",
                    "trade_date": trade_date,
                    "symbol": symbol,
                    "client_name": client_name,
                    "buy_sell": buy_sell,
                    "quantity": quantity,
                    "price": price,
                    "value": value,
                    "exchange": "NSE",
                })

            except Exception as e:
                print(
                    f"Failed block deal: {item} | Error: {e}"
                )

        if market_deals:
            db.execute(
                insert(MarketDeal),
                market_deals
            )
            db.commit()

        print(
            f"Inserted {len(market_deals)} new block deals"
        )

        return {
            "inserted": len(market_deals)
        }

    except Exception:
        db.rollback()
        raise

    finally:
        db.close()

async def fetch_daily_short_selling_async():
    db = SessionLocalSync()
    try:
        companies = (
            db.query(CompanyStock.id, CompanyStock.nse_symbol)
            .filter(CompanyStock.nse_symbol.isnot(None))
            .all()
        )

        company_map = {
            symbol.strip().upper(): company_id
            for company_id, symbol in companies
            if symbol
        }

        today = bdate.today().strftime("%d-%m-%Y")
        one_day_behind = (bdate.today() - BDay(1)).strftime("%d-%m-%Y")

        all_short_sellings = []
        block_deals = await main_block_deals({
            "optionType": "short_selling",
            "from": one_day_behind,
            "to": today,
            "csv": "true",
        })

        if block_deals:
            for deal in block_deals:
                symbol = (
                        deal.get("Symbol")
                        or deal.get("Symbol ")
                        or ""
                ).strip().upper()

                company_id = company_map.get(symbol)

                deal["company_id"] = company_id

                all_short_sellings.append(deal)


            if all_short_sellings:
                for i in range(0, len(all_short_sellings), BULK_BLOCK_DEAL_GROUP_SIZE):
                    batch = all_short_sellings[i:i + BULK_BLOCK_DEAL_GROUP_SIZE]

                    print(
                        f"Batch {i // CHART_GROUP_SIZE + 1}: "
                        f"{len(batch)} records"
                    )

                    process_daily_short_selling_batch.delay(
                        batch
                    )

    finally:
        db.close()
