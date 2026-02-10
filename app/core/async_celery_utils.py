import asyncio
import json
from datetime import datetime
from itertools import islice

from sqlalchemy import select, or_

from app.apis.models.stock_data import CompanyStock, KeyDetailsForCS, ChartDataset
from app.core.nse_search import fetch_nse_exact_symbol_data, fetch_bse_exact_symbol_data
from app.core.utils import filter_exchange_data_from_file
from app.db.postgres.sync_session import SessionLocalSync
from scripts.bse_stock_price_graph import main_fetch_stock_price_for_bse_graph
from scripts.nse_stock_price_graph import main_fetch_stock_price_for_graph
from scripts.nse_with_rotating_ip import main
from scripts.bse import main as main_bse



async def fetch_and_store_company_data_from_top_50_async():
    """
    Background task to store NSE company data
    """
    db = SessionLocalSync()
    try:
        file_path = 'OpenAPIScripMaster.json'
        all_filtered_data = filter_exchange_data_from_file(file_path)

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

        missing_symbols = [s for s in all_filtered_data if s not in existing_symbols]

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
                                continue
                            if isSuspended == "Suspended":
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
                            days_list = ["1W", "1D", "1M", "1Y", "5Y", "10Y", "15Y", "20Y", "25Y", "30Y"]
                            # days_list = ["1W", "1D", "1M"]
                            if nse_company_list and bse_company_list:
                                security_code = bse_company_list[0].get("bse_code")
                                for days in days_list:
                                    nse_data = await main_fetch_stock_price_for_graph(symbol, days)
                                    chart = nse_data.get('chart')
                                    # company_stock_chart_dataset_ops = BaseDBOperations(db, ChartDataset)
                                    company_stock_chart_dataset_ops = ChartDataset(
                                        metric="Price",
                                        label="Price on NSE",
                                        meta={"days": days},
                                        company_id=company_stock.id,
                                        values=chart.get("grapthData"),
                                    )
                                    db.add(company_stock_chart_dataset_ops)
                                    db.flush()
                            elif nse_company_list:
                                for days in days_list:
                                    nse_data = await main_fetch_stock_price_for_graph(symbol, days)
                                    chart = nse_data.get('chart')
                                    company_stock_chart_dataset_ops = ChartDataset(
                                        metric="Price",
                                        label="Price on NSE",
                                        meta={"days": days},
                                        company_id=company_stock.id,
                                        values=chart.get("grapthData"),
                                    )
                                    db.add(company_stock_chart_dataset_ops)
                                    db.flush()
                            elif bse_company_list:
                                # days_list = ["1M", "1Y", "5Y", "10Y"]
                                security_code = bse_company_list[0].get("bse_code")
                                days_list = ["1W", "1Y", "5Y"]
                                for days in days_list:
                                    bse_data = await main_fetch_stock_price_for_bse_graph(security_code, days)
                                    script_header = bse_data.get('scriptHeader')
                                    data_list = json.loads(script_header.get("Data"))
                                    result = []

                                    for item in data_list:
                                        ts_ms = int(
                                            datetime.strptime(item["dttm"], "%a %b %d %Y %H:%M:%S").timestamp() * 1000
                                        )
                                        price = float(item["vale1"])
                                        result.append([ts_ms, price])

                                        company_stock_chart_dataset_ops = ChartDataset(
                                            metric="Price",
                                            label="Price on BSE",
                                            meta={"days": days},
                                            company_id=company_stock.id,
                                            values=result,
                                        )
                                        db.add(company_stock_chart_dataset_ops)
                                        db.flush()

                except Exception as symbol_error:
                    # db.rollback()
                    print(f"Error for symbol {symbol}: {symbol_error}")
                    continue

            db.commit()
    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()