import asyncio
import json
import logging
import os
from datetime import datetime
from itertools import islice

from sqlalchemy import select, or_, delete
from sqlalchemy.orm import selectinload

from app.apis.models.stock_data import CompanyStock, KeyDetailsForCS, ChartDataset, QuarterlyResultDateset, \
    ResultFormatEnum
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
    update_nse_bse_newly_listed_stock_save_processed_symbol, fetch_newly_listed_stock_symbols_from_covered_symbol_json
from app.db.postgres.sync_session import SessionLocalSync
from scripts.bse_stock_price_graph import new_main_fetch_stock_price_for_bse_graph
from scripts.fetch_bse_integrated_filling_financials import main_bse_fetch_integrated_filing_financials
from scripts.fetch_daily_listed_stocks import main_newly_listed_stocks
from scripts.fetch_integrated_filling_financials import main_fetch_integrated_filing_financials
from scripts.fetch_stock_volume_from_nse import main_fetch_volume_from_nse
from scripts.nse_newly_listed_stocks import main_nse_newly_listed_stocks
from scripts.nse_stock_price_graph import new_main_fetch_stock_price_for_graph
from scripts.nse_with_rotating_ip import main
from scripts.bse import main as main_bse


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
    ][:400]
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
                    market_cap_cr = float(total_market_cap)
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


async def fetch_and_store_newly_listed_company_data_from_nse_bse_async():
    """
    Background task to store NSE company data
    """
    db = SessionLocalSync()
    error_symbols = []
    processed_symbols = []
    file_name = "nse_bse_newly_listed_stocks.json"
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
