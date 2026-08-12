import asyncio
import csv
import json
import logging
import os
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from itertools import islice
from zoneinfo import ZoneInfo

import pytz
from bs4 import BeautifulSoup
from celery import group, shared_task
from dateutil.relativedelta import relativedelta
from sqlalchemy import select, or_, delete, and_, exists, desc, func, distinct
from sqlalchemy.orm import selectinload

from app.apis.models.company import Company
from app.apis.models.stock_data import CompanyStock, KeyDetailsForCS, ChartDataset, QuarterlyResultDateset, \
    ResultFormatEnum, ShareHoldingPeriod, BalanceSheetDataset, ProfitLossDataset, CashFlowDataset, \
    CustomFormatQuarterlyResultDateset, StockDeliveryDataset
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
from scripts.fetch_stock_volume_from_nse import main_fetch_volume_from_nse
from scripts.nse_fetch_shareholder_data import main_nse_fetch_shareholding_list, \
    main_nse_fetch_shareholding_data_using_api, main_nse_fetch_shareholding_data_using_api_for_book_value
from scripts.nse_gross_deliverables import main_nse_fetch_security_wise_historical_data
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
                                    qe_date = f"{financial_name_obj.get("month")}-{financial_name_obj.get("year")}"
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

async def update_nse_bse_scrip_code_async():
    db = SessionLocalSync()
    stmt = (
        select(CompanyStock)
        .execution_options(yield_per=100)
    )

    result = db.execute(stmt)
    # companies = result.scalars().all()
    processed_symbols = await update_nse_bse_scrip_code_load_processed_symbols()
    new_process_symbol = []
    # unprocessed_companies = [
    #     c for c in companies if c.nse_symbol not in processed_symbols
    # ][:25]
    unprocessed_companies = []
    for c in result.scalars():
        if c.nse_symbol not in processed_symbols:
            unprocessed_companies.append(c)
            if len(unprocessed_companies) == 25:
                break
    started_symbols = [c.nse_symbol for c in unprocessed_companies]
    await update_nse_bse_scrip_code_save_processed_symbol(started_symbols, "current_processed_symbols")
    for company in unprocessed_companies:
        try:
            print(f"\nProcessing company: {company.id} | {company.name}")
            nse_company_list = await fetch_nse_exact_symbol_data(company.nse_symbol)
            bse_security_code = await fetch_bse_exact_symbol_data_from_json(company.nse_symbol)

            nse_code = company.nse_code
            bse_code = company.bse_code
            if nse_company_list and bse_security_code:
                platform = "NSE, BSE"
                nse_code = nse_company_list[0].get("nse_code") if nse_company_list else None
                bse_code =bse_security_code if bse_security_code else None
            elif nse_company_list:
                platform = "NSE"
                nse_code = nse_company_list[0].get("nse_code") if nse_company_list else None
            elif bse_security_code:
                platform = "BSE"
                bse_code = bse_security_code if bse_security_code else None

            company.bse_code = bse_code
            company.nse_code = nse_code

            db.commit()
            new_process_symbol.append(company.nse_symbol)

        except Exception as e:
            db.rollback()
            print(f"\nFAILED company: {company.id} | {company.name}")
            print("Error:", str(e))
            continue
    db.close()
    await update_nse_bse_scrip_code_save_processed_symbol(new_process_symbol, "processed_symbols")


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
    Background task to store NSE company data
    """
    db = SessionLocalSync()
    error_symbols = []
    processed_symbols = []
    file_name = "nse_bse_newly_listed_stocks"
    try:
        nse_newly_listed_stocks = await main_nse_newly_listed_stocks()
        nse_newly_listed_stocks_symbol = [item.get('symbol') for item in nse_newly_listed_stocks.get('data', [])]

        newly_listed_stocks = await main_newly_listed_stocks()
        newly_listed_stocks_symbol = [item.get('symbol') for item in newly_listed_stocks.get('data', {}).get("results", [])]

        missing_symbols  = list(set(nse_newly_listed_stocks_symbol + newly_listed_stocks_symbol))

        existing_symbols = await fetch_newly_listed_stock_symbols_from_covered_symbol_json(file_name)
        current_processed_symbols = [s for s in missing_symbols if s not in existing_symbols]
        await update_nse_bse_newly_listed_stock_save_processed_symbol(current_processed_symbols, "current_processed_symbols", file_name)

        def chunk_list(data, size):
            for i in range(0, len(data), size):
                yield data[i:i + size]

        for chunk in chunk_list(current_processed_symbols[:100], 50):

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
                        await asyncio.sleep(2)
                        if not company:
                            roe = current_price = high_price = isSuspended = low_price = pe_ratio = bse_code = nse_symbol = company_name = market_cap_cr = face_value = macro = sector = industry_info = basic_industry = None
                            nse_company_list = await fetch_nse_exact_symbol_data(symbol)
                            bse_company_list = await fetch_bse_exact_symbol_data(symbol)
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
                                bse_code = security_code
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
                                bse_code = security_code
                                macro = header_data.get("Sector")
                                sector = header_data.get("IndustryNew")
                                industry_info = header_data.get("IGroup")
                                basic_industry = header_data.get("Industry")
                                nse_code = None
                            else:
                                error_symbols.append(symbol)
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

                            days_list = ["30Y"]
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
                    error_symbols.append(symbol)
                    print(f"Error for symbol {symbol}: {symbol_error}")
                    continue

            db.commit()
            print("Symbols appended to covered_symbols.json")
    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()
        await update_nse_bse_newly_listed_stock_save_processed_symbol(processed_symbols,
                                                                      "processed_symbols", file_name)
        await update_nse_bse_newly_listed_stock_save_processed_symbol(error_symbols,
                                                                      "error", file_name)


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
                                        navigateurl_promoter = f"Corp_shpPromoterNGroup_ng/w?SCRIPCODE={company.bse_code}&QtrCode={shareholding_obj.get("qtrid")}"
                                        navigateurl_publicshareholder = f"Corp_shpSec_SHPPubShold_ng/w?SCRIPCODE={company.bse_code}&QtrCode={shareholding_obj.get("qtrid")}"
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
                                html_data = await main_balance_sheet_html(f"SCRIP-{exact_company.get("FINCODE")}")
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
                                html_data = await main_balance_sheet_standalone_html(f"SCRIP-{exact_company.get("FINCODE")}")
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
                                        qe_date = f"{financial_name_obj.get("month")}-{financial_name_obj.get("year")}"
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
                                        qe_date = f"{financial_name_obj.get("month")}-{financial_name_obj.get("year")}"
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
                                        qe_date = f"{financial_name_obj.get("month")}-{financial_name_obj.get("year")}"
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
                                            qe_date = f"{financial_name_obj.get("month")}-{financial_name_obj.get("year")}"
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
                                    qe_date = f"{financial_name_obj.get("month")}-{financial_name_obj.get("year")}"
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

    value = value.replace(",", "").strip()

    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


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

    trading_date = datetime.strptime(
        delivery_data["mTIMESTAMP"],  "%d-%b-%Y %H:%M:%S",
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
        total_traded = sum(x["CH_TOT_TRADED_QTY"] for x in window)
        total_delivery = sum(x["COP_DELIV_QTY"] for x in window)

        rolling_avg_volume = round(total_traded / 5, 2)

        rolling_delivery_percent = (
            round((total_delivery / total_traded) * 100, 2)
            if total_traded
            else None
        )

        diff = round(
            delivery_data["COP_DELIV_PERC"] - rolling_delivery_percent,
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
        record.combined_traded_volume = delivery_data["CH_TOT_TRADED_QTY"]
        record.combined_delivery_volume = delivery_data["COP_DELIV_QTY"]
        record.combined_delivery_percent = delivery_data["COP_DELIV_PERC"]
        record.price_change_percent = None
        record.insight = insight
        record.combined_rolling_week_avg_volume = rolling_avg_volume
        record.rolling_week_delivery_percent = rolling_delivery_percent
    else:
        record = StockDeliveryDataset(
            company_id=company_id,
            trading_date=delivery_data['mTIMESTAMP'],
            combined_traded_volume=delivery_data["CH_TOT_TRADED_QTY"],
            combined_delivery_volume=delivery_data["COP_DELIV_QTY"],
            combined_delivery_percent=delivery_data["COP_DELIV_PERC"],
            price_change_percent=None,
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


GROUP_SIZE = 15

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


async def fetch_current_day_gross_deliverables_nse_stock_information_async():
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
        file_name = "update_daily_nse_gross_deliverables_data.json"
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
        started_symbols = [c.nse_symbol for c in unprocessed_companies]
        await update_nse_bse_gross_deliverable_data_save_processed_symbol(started_symbols, "current_processed_symbols", file_name)
        for company in unprocessed_companies:
            try:
                nse_company_list = await fetch_nse_exact_symbol_data(company.nse_symbol)
                if not nse_company_list:
                    skip_symbols.append(company.nse_symbol)

                if nse_company_list:
                    symbol = nse_company_list[0].get("symbol")
                    c_name = "-".join(nse_company_list[0].get("company_name").split())
                    series = nse_company_list[0].get("series")
                    metadata = await fetch_nse_metadata(symbol, c_name)
                    marketType = metadata.get("marketType")
                    symbol_data = await fetch_nse_symbol_data(symbol, marketType, series)
                    equityResponse = symbol_data.get("equityResponse", [])
                    tradeInfo = equityResponse[0].get("tradeInfo") if equityResponse else {}
                    equityResponseMetaData = equityResponse[0].get("metaData") if equityResponse else {}
                    priceVolumeDeliverable = {
                        "CH_SYMBOL": symbol,
                        "CH_SERIES": series,
                        "mTIMESTAMP": tradeInfo.get("secwisedelposdate", None),
                        "CH_PREVIOUS_CLS_PRICE": equityResponseMetaData.get("previousClose"),
                        "CH_OPENING_PRICE": equityResponseMetaData.get("open"),
                        "CH_TRADE_HIGH_PRICE": equityResponseMetaData.get("dayHigh"),
                        "CH_TRADE_LOW_PRICE": equityResponseMetaData.get("dayLow"),
                        "CH_LAST_TRADED_PRICE": equityResponseMetaData.get("lastPrice"),
                        "CH_CLOSING_PRICE": tradeInfo.get("closePrice"),
                        "VWAP": equityResponseMetaData.get("averagePrice"),
                        "CH_TOT_TRADED_QTY": tradeInfo.get("quantitytraded"),
                        "CH_TOT_TRADED_VAL": tradeInfo.get("totalTradedValue"),
                        "CH_TOTAL_TRADES": None,
                        "CH_TIMESTAMP": None,
                        "COP_DELIV_QTY": tradeInfo.get("deliveryquantity"),
                        "COP_DELIV_PERC": tradeInfo.get("deliveryToTradedQuantity")
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
                db.rollback()
                error_symbols.append(company.nse_symbol)
                print(f"Error for symbol {company.nse_symbol}: {symbol_error}")
                continue

        if payloads:
            group(
                process_delivery_batch.s(
                    payloads.copy(),skip_symbols.copy(),
                    file_name
                ),
            ).apply_async()
            payloads.clear()
            skip_symbols.clear()

    finally:
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
        file_name = "update_daily_bse_gross_deliverables_data.json"
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
        started_symbols = [c.nse_symbol for c in unprocessed_companies]
        await update_nse_bse_gross_deliverable_data_save_processed_symbol(started_symbols, "current_processed_symbols", file_name)
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
                            process_delivery_batch.s(payloads.copy(), skip_symbols.copy(), file_name),
                        ).apply_async()
                        payloads.clear()
                        skip_symbols.clear()

            except Exception as symbol_error:
                db.rollback()
                error_symbols.append(company.nse_symbol)
                print(f"Error for symbol {company.nse_symbol}: {symbol_error}")
                continue

        if payloads:
            group(
                process_delivery_batch.s(
                    payloads.copy(),skip_symbols.copy(),
                    file_name
                ),
            ).apply_async()
            payloads.clear()
            skip_symbols.clear()

    finally:
        db.close()