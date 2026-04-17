import asyncio
import json
import os
from datetime import datetime, timedelta, timezone

from dateutil.relativedelta import relativedelta
from fastapi import Depends
from fastapi.responses import FileResponse
from sqlalchemy import select, func, or_
from sqlalchemy.orm import Session, selectinload

from app.apis.deps import get_db, get_external_db
from app.apis.models.follow_unfollow_external_db_model import FollowUnfollowExternal
from app.apis.models.stock_data import CompanyStock, KeyDetailsForCS, ChartDataset, ShareHoldingPeriod, \
    QuarterlyResultDateset
from app.apis.v1.schemas.stock_data import SearchCompanyStockSchema, QuarterlyResultSchema, UpdateStockPriceSchema
from app.core.constants import quarterly_result, profit_loss, balance_sheet, cash_flow, ratios, share_holding_pattern, \
    YEAR_OR_MONTY_TO_DAYS_MAP
from app.core.custom_error_response import CustomValidationError
from app.core.custom_response import CustomJSONResponse
from app.core.nse_search import fetch_bse_exact_symbol_data, fetch_nse_exact_symbol_data
from app.core.utils import parse_qtr, parse_period_to_date, fetch_top_50_company_from_nse, \
    fetch_json_from_angle_one, fetch_integrated_filing_financials_data_from_nse, convert_to_quarterly_format
from app.db.postgres.base import BaseDBOperations
from app.tasks.tasks import fetch_and_store_company_data_from_top_50
from scripts.bse_fetch_share_holder_link_of_stock import main_fetch_stock_share_holder_pattern_urls
from scripts.bse_shareholder_pattern import main_fetch_stock_share_holder_pattern
from scripts.fetch_integrated_filling_financials import main_fetch_integrated_filing_financials
from scripts.fetch_stock_volume_from_nse import main_fetch_volume_from_nse
from scripts.nse import main
from scripts.bse import main as main_bse
from scripts.nse_stock_price_graph import main_fetch_stock_price_for_graph, new_main_fetch_stock_price_for_graph
from scripts.bse_stock_price_graph import main_fetch_stock_price_for_bse_graph, new_main_fetch_stock_price_for_bse_graph


class CompanyStockFetchService:

    @staticmethod
    async def company_search(search_request: SearchCompanyStockSchema, db: Session = Depends(get_db)):
        symbol = search_request.symbol
        scrip = search_request.scrip
        stmt = (
            select(CompanyStock)
            .options(selectinload(CompanyStock.details))
            .options(selectinload(CompanyStock.charts))
            .where(
                or_(
                    CompanyStock.nse_symbol == symbol,
                    CompanyStock.bse_code == symbol
                )
            )
        )

        result = await db.execute(stmt)
        company = result.scalars().first()

        if company:
            raise CustomValidationError(
                {"error": [f"{symbol} already exist"]}, 200
            )
        roe = current_price = high_price = low_price = pe_ratio = bse_code = nse_symbol = company_name = market_cap_cr = face_value = macro = sector = industry_info = basic_industry = None
        nse_company_list = await fetch_nse_exact_symbol_data(search_request.symbol)
        bse_company_list = await fetch_bse_exact_symbol_data(search_request.symbol)
        if nse_company_list and bse_company_list:
            bse_code = bse_company_list[0].get("bse_code")
            nse_data = await main(symbol)
            bse_data = await main_bse(bse_code)
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
        elif bse_company_list:
            security_code = bse_company_list[0].get("security_code")
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
        company_stock_ops = BaseDBOperations(db, CompanyStock)
        company_stock_data_db = await company_stock_ops.create({'nse_symbol': symbol, 'name': company_name, 'nse_code': nse_code,
                                                                "bse_code": bse_code, "macro_economic_sector": macro,
                                                                "sector": sector, "industry": industry_info, "basic_industry": basic_industry})

        key_company_stock_detail_ops = BaseDBOperations(db, KeyDetailsForCS)
        await key_company_stock_detail_ops.create({'market_cap': market_cap_cr, 'current_price': current_price,
                                        "pe_ratio": float(pe_ratio) if pe_ratio and pe_ratio != '-' else None, "face_value": face_value, "high_price": float(high_price),
                                        "low_price": float(low_price), "book_value": None, "dividend_yield": None,
                                        "roce": None, "roe": float(roe) if roe and roe != '-' else None, "company_id": company_stock_data_db.id})

        return CustomJSONResponse.custom_response(
            message="Company stock data fetched successfully.",
            data={"data": []}
        )

    @staticmethod
    async def stock_price_chart(search_request: SearchCompanyStockSchema, db: Session = Depends(get_db)):
        symbol = search_request.symbol
        scrip = search_request.scrip

        company_stock_ops = BaseDBOperations(db, CompanyStock)
        fields = ["id"]

        attrs = [getattr(CompanyStock, f) for f in fields]
        # days_list = ["1W", "1D", "1M", "1Y", "5Y", "10Y", "15Y", "20Y", "25Y", "30Y"]
        days_list = ["1W", "1D", "1M"]
        if scrip and symbol:
            company_stock_data_db = await company_stock_ops.retrieve_selected_columns(
                attrs, nse_symbol=symbol, bse_code=scrip
            )
            for days in days_list:
                nse_data = await main_fetch_stock_price_for_graph(symbol, days)
                chart = nse_data.get('chart')
                company_stock_chart_dataset_ops = BaseDBOperations(db, ChartDataset)
                company_stock_chart_dataset_db = await company_stock_chart_dataset_ops.create(
                    {'metric': "Price", 'label': "Price on NSE", "meta": {"days": days}, "values": chart.get("grapthData"), "company_id": company_stock_data_db.id})
        elif symbol:
            company_stock_data_db = await company_stock_ops.retrieve_selected_columns(
                attrs, nse_symbol=symbol
            )
            for days in days_list:
                nse_data = await main_fetch_stock_price_for_graph(symbol, days)
                chart = nse_data.get('chart')
                company_stock_chart_dataset_ops = BaseDBOperations(db, ChartDataset)
                company_stock_chart_dataset_db = await company_stock_chart_dataset_ops.create(
                    {'metric': "Price", 'label': "Price on NSE", "meta": {"days": days}, "values": chart.get("grapthData"), "company_id": company_stock_data_db.id})
        elif scrip:
            # days_list = ["1M", "1Y", "5Y", "10Y"]
            days_list = ["1W", "1Y", "5Y"]
            company_stock_data_db = await company_stock_ops.retrieve_selected_columns(
                attrs, bse_code=scrip
            )
            for days in days_list:
                bse_data = await main_fetch_stock_price_for_bse_graph(scrip, days)
                script_header = bse_data.get('scriptHeader')
                data_list = json.loads(script_header.get("Data"))
                result = []

                for item in data_list:
                    ts_ms = int(
                        datetime.strptime(item["dttm"], "%a %b %d %Y %H:%M:%S").timestamp() * 1000
                    )
                    price = float(item["vale1"])
                    result.append([ts_ms, price])

                company_stock_chart_dataset_ops = BaseDBOperations(db, ChartDataset)
                company_stock_chart_dataset_db = await company_stock_chart_dataset_ops.create(
                    {'metric': "Price", 'label': "Price on BSE", "meta": {"days": days}, "values": result,
                     "company_id": company_stock_data_db.id})

        return CustomJSONResponse.custom_response(
            message="Company stock price data fetched successfully.",
            data={"data": []}
        )

    @staticmethod
    async def fetch_company_peer_data(search_request: SearchCompanyStockSchema, db: Session = Depends(get_db)):
        symbol = search_request.symbol
        scrip = search_request.scrip
        if scrip and symbol:
            pass
        elif symbol:
            pass
        elif scrip:
            pass

        return CustomJSONResponse.custom_response(
            message="Company stock peer data fetched successfully.",
            data={"data": []}
        )

    @staticmethod
    async def fetch_company_quarterly_result(search_request: SearchCompanyStockSchema, db: Session = Depends(get_db)):
        symbol = search_request.symbol
        scrip = search_request.scrip

        stmt = (
            select(CompanyStock)
            .options(selectinload(CompanyStock.details))
            .options(selectinload(CompanyStock.charts))
            .where(
                or_(
                    CompanyStock.nse_symbol == symbol,
                    CompanyStock.bse_code == symbol
                )
            )
        )

        result = await db.execute(stmt)
        company = result.scalars().first()

        if company is None:
            raise CustomValidationError(
                validations={"error": [f"Company not found for symbol : {symbol}"]}, status_code=400
            )

        if scrip and symbol:
            pass
        elif symbol:
            integrated_filing_financials_list = await main_fetch_integrated_filing_financials(symbol, "equity")
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
                        output = await fetch_integrated_filing_financials_data_from_nse(ixbrl)
                        output.append({
                            "date": formatted or qe_date,
                            "consolidated": consolidated
                        })
                        response_list.append(output)
                if response_list:
                    quarterly_result = await convert_to_quarterly_format(response_list)
            if quarterly_result:
                company_quarterly_result_ops = BaseDBOperations(db, QuarterlyResultDateset)
                await company_quarterly_result_ops.create(
                    {'company_id': company.id, 'values': quarterly_result })
        elif scrip:
            pass

        return CustomJSONResponse.custom_response(
            message="Company stock quarterly result data fetched successfully.",
            data={"data": []}
        )

    @staticmethod
    async def fetch_company_profit_loss(search_request: SearchCompanyStockSchema, db: Session = Depends(get_db)):
        symbol = search_request.symbol
        scrip = search_request.scrip
        if scrip and symbol:
            pass
        elif symbol:
            pass
        elif scrip:
            pass

        return CustomJSONResponse.custom_response(
            message="Company stock profit loss data fetched successfully.",
            data={"data": []}
        )

    @staticmethod
    async def fetch_company_balance_sheet(search_request: SearchCompanyStockSchema, db: Session = Depends(get_db)):
        symbol = search_request.symbol
        scrip = search_request.scrip
        if scrip and symbol:
            pass
        elif symbol:
            pass
        elif scrip:
            pass

        return CustomJSONResponse.custom_response(
            message="Company stock balance sheet data fetched successfully.",
            data={"data": []}
        )

    @staticmethod
    async def fetch_company_cash_flows(search_request: SearchCompanyStockSchema, db: Session = Depends(get_db)):
        symbol = search_request.symbol
        scrip = search_request.scrip
        if scrip and symbol:
            pass
        elif symbol:
            pass
        elif scrip:
            pass

        return CustomJSONResponse.custom_response(
            message="Company stock cash flow data fetched successfully.",
            data={"data": []}
        )

    @staticmethod
    async def fetch_company_ratios(search_request: SearchCompanyStockSchema, db: Session = Depends(get_db)):
        symbol = search_request.symbol
        scrip = search_request.scrip
        if scrip and symbol:
            pass
        elif symbol:
            pass
        elif scrip:
            pass

        return CustomJSONResponse.custom_response(
            message="Company stock ratio data fetched successfully.",
            data={"data": []}
        )

    @staticmethod
    async def fetch_company_shareholding_pattern(search_request: SearchCompanyStockSchema, db: Session = Depends(get_db)):
        symbol = search_request.symbol
        scrip = search_request.scrip
        shareholding_pattern = {}
        if scrip and symbol:
            nse_data = await main_fetch_stock_share_holder_pattern(scrip)
            share_holder = nse_data.get("share_holder").get("Table")[:15]
            for i in share_holder:
                qtrid = i.get("qtrid")
                qtr = i.get("qtr")
                result  = await parse_qtr(qtr)
                navigateurl = f"corporates/shpPromoterNGroup.aspx?scripcd={scrip}&qtrid={qtrid}&QtrName={result}"
                dd = await main_fetch_stock_share_holder_pattern_urls(navigateurl)
                shareholding_pattern[result] = dd
                # print(dd)
            # print(shareholding_pattern)
            # ddd = await build_response(dd)
            for period_str, promoters in shareholding_pattern.items():
                # print(period_str, promoters)
                period_date = await parse_period_to_date(period_str)
                print(period_date)
                company_stock_ops = BaseDBOperations(db, CompanyStock)
                fields = ["id"]

                attrs = [getattr(CompanyStock, f) for f in fields]
                company_stock_data_db = await company_stock_ops.retrieve_selected_columns(
                    attrs, nse_symbol=symbol, bse_code=scrip
                )
                stmt = select(ShareHoldingPeriod).where(
                    ShareHoldingPeriod.company_id == company_stock_data_db.id,
                    ShareHoldingPeriod.period_date == period_date,
                    ShareHoldingPeriod.period_type == "quarterly"
                )

                result = await db.execute(stmt)
                period = result.scalar_one_or_none()
                print(period)
        elif symbol:
            pass
        elif scrip:
            pass

        return CustomJSONResponse.custom_response(
            message="Company stock shareholding pattern data fetched successfully.",
            data={"data": []}
        )

    @staticmethod
    async def fetch_listed_companies(page: int, page_size: int, db: Session = Depends(get_db)):
        offset = (page - 1) * page_size

        total_stmt = select(func.count(CompanyStock.id))
        total_result = await db.execute(total_stmt)
        total = total_result.scalar()

        stmt = (
            select(CompanyStock)
            .options(selectinload(CompanyStock.details))
            .options(selectinload(CompanyStock.charts))
            .limit(page_size)
            .offset(offset)
            .order_by(CompanyStock.id)
        )

        result = await db.execute(stmt)
        companies = result.scalars().all()

        response = []

        for company in companies:
            details = company.details

            one_month_charts = [
                {
                    "metric": chart.metric,
                    "label": chart.label,
                    "values": chart.values,
                    "meta": chart.meta,
                }
                for chart in company.charts
                if chart.meta and chart.meta.get("days") == "1M"
            ]

            response.append({
                "id": company.id,
                "name": company.name,
                "website": company.website,
                "bse_code": company.bse_code,
                "nse_code": company.nse_code,
                "nse_symbol": company.nse_symbol,
                "macro_economic_sector": company.macro_economic_sector,
                "sector": company.sector,
                "industry": company.industry,
                "basic_industry": company.basic_industry,
                "key_details": {
                    "market_cap": details.market_cap if details else None,
                    "current_price": details.current_price if details else None,
                    "high_price": details.high_price if details else None,
                    "low_price": details.low_price if details else None,
                    "pe_ratio": details.pe_ratio if details else None,
                    "book_value": details.book_value if details else None,
                    "dividend_yield": details.dividend_yield if details else None,
                    "roce": details.roce if details else None,
                    "roe": details.roe if details else None,
                    "face_value": details.face_value if details else None,
                    "about": details.about if details else None,
                    "key_points": details.key_points if details else None,
                    "pros": details.pros if details else None,
                    "cons": details.cons if details else None,
                } if details else None,
                "chart": one_month_charts,
                "quarterly_result": quarterly_result,
                "profit_loss": profit_loss,
                "balance_sheet": balance_sheet,
                "cash_flow": cash_flow,
                "ratios": ratios,
                "share_holding_pattern": share_holding_pattern,
                "use_own_stock_socket": True
            })

        return CustomJSONResponse.custom_response(
            message="Listed Company list fetched successfully.",
            data={
            "page": page,
            "page_size": page_size,
            "total_records": total,
            "total_pages": (total + page_size - 1) // page_size,
            "data": response
        }
        )

    @staticmethod
    async def fetch_listed_company_detail(
            symbol,scrip, current_user: int | None,
            db: Session = Depends(get_db), external_db: Session = Depends(get_external_db),
    ):
        try:

            stmt = (
                select(CompanyStock)
                .options(selectinload(CompanyStock.details))
                .options(selectinload(CompanyStock.charts))
                .options(selectinload(CompanyStock.quarterly_result))
                .where(
                    or_(
                        CompanyStock.nse_symbol == symbol,
                        CompanyStock.bse_code == symbol
                    )
                )
            )

            result = await db.execute(stmt)
            company = result.scalars().first()

            if not company:
                roe = series = symbol_type = identifier = current_price = high_price = low_price = pe_ratio = bse_code = nse_symbol = company_name = market_cap_cr = face_value = macro = sector = industry_info = basic_industry = None
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
                company_stock_ops = BaseDBOperations(db, CompanyStock)
                company_stock_data_db = await company_stock_ops.create(
                    {'nse_symbol': symbol, 'name': company_name, 'nse_code': nse_code,
                     "bse_code": bse_code, "macro_economic_sector": macro,
                     "sector": sector, "industry": industry_info, "basic_industry": basic_industry})

                key_company_stock_detail_ops = BaseDBOperations(db, KeyDetailsForCS)
                await key_company_stock_detail_ops.create({'market_cap': market_cap_cr, 'current_price': current_price,
                                                           "pe_ratio": float(
                                                               pe_ratio) if pe_ratio and pe_ratio != '-' else None,
                                                           "face_value": face_value, "high_price": float(high_price),
                                                           "low_price": float(low_price), "book_value": None,
                                                           "dividend_yield": None,
                                                           "roce": None, "roe": float(roe) if roe and roe != '-' else None,
                                                           "company_id": company_stock_data_db.id})

                company_stock_ops = BaseDBOperations(db, CompanyStock)
                fields = ["id"]

                attrs = [getattr(CompanyStock, f) for f in fields]
                days_list = ["30Y"]
                if nse_company_list and bse_company_list:
                    security_code = bse_company_list[0].get("bse_code")
                    company_stock_data_db = await company_stock_ops.retrieve_selected_columns(
                        attrs, nse_symbol=symbol, bse_code=security_code
                    )
                    volume_data = await main_fetch_volume_from_nse(company_stock_data_db.nse_code, f"{symbol}-{series}", symbol_type)
                    for days in days_list:
                        company_name_with_dash = company_name.replace(" ", "-")
                        nse_data = await new_main_fetch_stock_price_for_graph(days, identifier, symbol, company_name_with_dash)
                        chart = nse_data.get('grapthData')
                        if chart:
                            volume_map = {item["time"]: item["volume"] for item in volume_data.get("data", None)}
                            updated_data = []
                            for row in chart:
                                time = row[0]
                                volume = volume_map.get(time, None)
                                updated_row = row + [volume]
                                updated_data.append(updated_row)
                            company_stock_chart_dataset_ops = BaseDBOperations(db, ChartDataset)
                            company_stock_chart_dataset_db = await company_stock_chart_dataset_ops.create(
                                {'metric': "Price", 'label': "Price on NSE", "meta": {"days": days},
                                 "values": updated_data, "company_id": company_stock_data_db.id})
                elif nse_company_list:
                    company_stock_data_db = await company_stock_ops.retrieve_selected_columns(
                        attrs, nse_symbol=symbol
                    )
                    volume_data = await main_fetch_volume_from_nse(company_stock_data_db.nse_code, f"{symbol}-{series}",
                                                                   symbol_type)
                    for days in days_list:
                        company_name_with_dash = company_name.replace(" ", "-")
                        nse_data = await new_main_fetch_stock_price_for_graph(days, identifier, symbol, company_name_with_dash)
                        chart = nse_data.get('grapthData')
                        if chart:
                            volume_map = {item["time"]: item["volume"] for item in volume_data.get("data", None)}
                            updated_data = []
                            for row in chart:
                                time = row[0]
                                volume = volume_map.get(time, None)
                                updated_row = row + [volume]
                                updated_data.append(updated_row)
                            company_stock_chart_dataset_ops = BaseDBOperations(db, ChartDataset)
                            company_stock_chart_dataset_db = await company_stock_chart_dataset_ops.create(
                                {'metric': "Price", 'label': "Price on NSE", "meta": {"days": days},
                                 "values": updated_data, "company_id": company_stock_data_db.id})
                elif bse_company_list:
                    security_code = bse_company_list[0].get("bse_code")
                    days_list = ["30Y"]
                    company_stock_data_db = await company_stock_ops.retrieve_selected_columns(
                        attrs, bse_code=security_code
                    )
                    for days in days_list:
                        bse_data = await new_main_fetch_stock_price_for_bse_graph(security_code)
                        script_header = bse_data.get('Data')
                        if script_header:
                            data_list = json.loads(script_header)
                            result = []

                            for item in data_list:
                                ts_ms = int(
                                    datetime.strptime(item["dttm"], "%a %b %d %Y %H:%M:%S").timestamp() * 1000
                                )
                                price = float(item["vale1"])
                                volume = int(item["vole"])
                                result.append([ts_ms, price, "", None, None, volume])

                            company_stock_chart_dataset_ops = BaseDBOperations(db, ChartDataset)
                            company_stock_chart_dataset_db = await company_stock_chart_dataset_ops.create(
                                {'metric': "Price", 'label': "Price on BSE", "meta": {"days": days}, "values": result,
                                 "company_id": company_stock_data_db.id})

            result = await db.execute(stmt)
            company = result.scalars().first()
            details = company.details

            is_following = False
            if current_user:
                follow_stmt = select(FollowUnfollowExternal.id).where(
                    FollowUnfollowExternal.user_id == current_user,
                    FollowUnfollowExternal.symbol == symbol
                )

                follow_result = await external_db.execute(follow_stmt)
                is_following = follow_result.scalar() is not None

            one_month_charts = []
            for chart in company.charts:
                if chart.meta and chart.meta.get("days") == "30Y":
                    values = chart.values
                    filtered_values = values[0:30]

                    one_month_charts.append({
                        "metric": chart.metric,
                        "label": chart.label,
                        "values": filtered_values,
                        "meta": {**chart.meta, "days": "1M"}
                    })
            quarterly_result_r = {}
            for item in company.quarterly_result:
                quarterly_result_r = QuarterlyResultSchema.model_validate(item).model_dump()
                if quarterly_result_r:
                    quarterly_result_r = quarterly_result_r.get("values")
            response = {
                "id": company.id,
                "name": company.name,
                "website": company.website,
                "bse_code": company.bse_code,
                "nse_code": company.nse_code,
                "nse_symbol": company.nse_symbol,
                "macro_economic_sector": company.macro_economic_sector,
                "sector": company.sector,
                "industry": company.industry,
                "basic_industry": company.basic_industry,

                "key_details": {
                    "market_cap": details.market_cap if details else None,
                    "current_price": details.current_price if details else None,
                    "high_price": details.high_price if details else None,
                    "low_price": details.low_price if details else None,
                    "pe_ratio": details.pe_ratio if details else None,
                    "book_value": details.book_value if details else None,
                    "dividend_yield": details.dividend_yield if details else None,
                    "roce": details.roce if details else None,
                    "roe": details.roe if details else None,
                    "face_value": details.face_value if details else None,
                    "about": details.about if details else None,
                    "key_points": details.key_points if details else None,
                    "pros": details.pros if details else None,
                    "cons": details.cons if details else None,
                } if details else None,

                "chart": one_month_charts,
                "quarterly_result": quarterly_result_r or quarterly_result,
                "profit_loss": profit_loss,
                "balance_sheet": balance_sheet,
                "cash_flow": cash_flow,
                "ratios": ratios,
                "share_holding_pattern": share_holding_pattern,
                "use_own_stock_socket": True,
                "is_following": is_following,
            }

            return CustomJSONResponse.custom_response(
                message="Company detail fetched successfully",
                data=response
            )
        except Exception as e:
            raise CustomValidationError(
                {"error": [str(e)]}, 200
            )

    @staticmethod
    async def fetch_and_store_top_50_company_data(db: Session = Depends(get_db)):
        try:
            symbol_list = await fetch_top_50_company_from_nse()
            for symbol in symbol_list[20:35]:
                stmt = (
                    select(CompanyStock)
                    .where(
                        or_(
                            CompanyStock.nse_symbol == symbol,
                            CompanyStock.bse_code == symbol
                        )
                    )
                )

                result = await db.execute(stmt)
                company = result.scalars().first()
                await asyncio.sleep(300)
                if not company:
                    roe = current_price = high_price = low_price = pe_ratio = bse_code = nse_symbol = company_name = market_cap_cr = face_value = macro = sector = industry_info = basic_industry = None
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
                    company_stock_ops = BaseDBOperations(db, CompanyStock)
                    company_stock_data_db = await company_stock_ops.create(
                        {'nse_symbol': symbol, 'name': company_name, 'nse_code': nse_code,
                         "bse_code": bse_code, "macro_economic_sector": macro,
                         "sector": sector, "industry": industry_info, "basic_industry": basic_industry})

                    key_company_stock_detail_ops = BaseDBOperations(db, KeyDetailsForCS)
                    await key_company_stock_detail_ops.create({'market_cap': market_cap_cr, 'current_price': current_price,
                                                               "pe_ratio": float(
                                                                   pe_ratio) if pe_ratio and pe_ratio != '-' else None,
                                                               "face_value": face_value, "high_price": float(high_price),
                                                               "low_price": float(low_price), "book_value": None,
                                                               "dividend_yield": None,
                                                               "roce": None, "roe": float(roe) if roe and roe != '-' else None,
                                                               "company_id": company_stock_data_db.id})

                    company_stock_ops = BaseDBOperations(db, CompanyStock)
                    fields = ["id"]

                    attrs = [getattr(CompanyStock, f) for f in fields]
                    days_list = ["1W", "1D", "1M", "1Y", "5Y", "10Y", "15Y", "20Y", "25Y", "30Y"]
                    # days_list = ["1W", "1D", "1M"]
                    if nse_company_list and bse_company_list:
                        security_code = bse_company_list[0].get("bse_code")
                        company_stock_data_db = await company_stock_ops.retrieve_selected_columns(
                            attrs, nse_symbol=symbol, bse_code=security_code
                        )
                        for days in days_list:
                            nse_data = await main_fetch_stock_price_for_graph(symbol, days)
                            chart = nse_data.get('chart')
                            company_stock_chart_dataset_ops = BaseDBOperations(db, ChartDataset)
                            company_stock_chart_dataset_db = await company_stock_chart_dataset_ops.create(
                                {'metric': "Price", 'label': "Price on NSE", "meta": {"days": days},
                                 "values": chart.get("grapthData"), "company_id": company_stock_data_db.id})
                    elif nse_company_list:
                        company_stock_data_db = await company_stock_ops.retrieve_selected_columns(
                            attrs, nse_symbol=symbol
                        )
                        for days in days_list:
                            nse_data = await main_fetch_stock_price_for_graph(symbol, days)
                            chart = nse_data.get('chart')
                            company_stock_chart_dataset_ops = BaseDBOperations(db, ChartDataset)
                            company_stock_chart_dataset_db = await company_stock_chart_dataset_ops.create(
                                {'metric': "Price", 'label': "Price on NSE", "meta": {"days": days},
                                 "values": chart.get("grapthData"), "company_id": company_stock_data_db.id})
                    elif bse_company_list:
                        # days_list = ["1M", "1Y", "5Y", "10Y"]
                        security_code = bse_company_list[0].get("bse_code")
                        days_list = ["1W", "1Y", "5Y"]
                        company_stock_data_db = await company_stock_ops.retrieve_selected_columns(
                            attrs, bse_code=security_code
                        )
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

                            company_stock_chart_dataset_ops = BaseDBOperations(db, ChartDataset)
                            company_stock_chart_dataset_db = await company_stock_chart_dataset_ops.create(
                                {'metric': "Price", 'label': "Price on BSE", "meta": {"days": days}, "values": result,
                                 "company_id": company_stock_data_db.id})


            return CustomJSONResponse.custom_response(
                message="Company detail fetched successfully",
                data=symbol_list
            )
        except Exception as e:
            raise CustomValidationError(
                {"error": [str(e)]}, 200
            )

    @staticmethod
    async def fetch_and_get_scrip_code_from_angle_one():
        await fetch_json_from_angle_one()
        return CustomJSONResponse.custom_response(
            message="Listed Company list fetched successfully.",
            data={
            }
        )

    @staticmethod
    async def download_scrip_master_file():
        file_path = os.path.join(os.getcwd(), "OpenAPIScripMaster.json")
        return FileResponse(file_path, media_type="application/json", filename="OpenAPIScripMaster.json", headers={
        "Content-Disposition": "attachment; filename=OpenAPIScripMaster.json"
    })

    @staticmethod
    async def fetch_and_get_scrip_code_from_json_file():
        fetch_and_store_company_data_from_top_50.delay()
        return CustomJSONResponse.custom_response(
            message="Listed Company list fetched successfully.",
            data={
            }
        )

    @staticmethod
    async def fetch_listed_company_chart_data(
            symbol, days, scrip,
            db: Session = Depends(get_db),
    ):
        try:

            stmt = (
                select(CompanyStock)
                .options(selectinload(CompanyStock.details))
                .where(
                    or_(
                        CompanyStock.nse_symbol == symbol,
                        CompanyStock.bse_code == symbol
                    )
                )
            )

            result = await db.execute(stmt)
            company = result.scalars().first()
            chart_data = []
            if company is None:
                raise CustomValidationError(
                    validations={"error": [f"Company not found for symbol : {symbol}"]}, status_code=400
                )
            if company:
                chart_stmt = select(ChartDataset).where(
                    ChartDataset.company_id == company.id,
                    ChartDataset.meta["days"].astext == "30Y"
                )

                chart_result = await db.execute(chart_stmt)
                charts = chart_result.scalars().all()
                chart_data = []
                for chart in charts:
                    if chart.meta and chart.meta.get("days") == "30Y":
                        values = chart.values
                        day_count = YEAR_OR_MONTY_TO_DAYS_MAP.get(days)
                        now = datetime.now()
                        yesterday = now - timedelta(days=1)
                        yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

                        if "Y" in days:
                            start_date = yesterday - relativedelta(years=day_count)
                        elif "W" in days:
                            start_date = yesterday - timedelta(weeks=day_count)
                        elif "M" in days:
                            start_date = yesterday - relativedelta(months=day_count)
                        dt = datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0, tzinfo=timezone.utc)
                        timestamp_ms = int(dt.timestamp() * 1000)
                        filtered_values = [
                            v for v in values
                            if v[0] > timestamp_ms
                        ]

                        chart_data.append({
                            "metric": chart.metric,
                            "label": chart.label,
                            "values": filtered_values,
                            "meta": {**chart.meta, "days": days}
                        })
                if not chart_data:
                    series = symbol_type = identifier = None
                    if company.nse_code and company.bse_code:
                        nse_data = await main(symbol)
                        symbol_data = nse_data.get('symbolData')
                        equity_response = symbol_data.get('equityResponse')[0]
                        nse_metadata = equity_response.get('metaData')
                        sec_info = equity_response.get('secInfo')
                        series = nse_metadata.get("series")
                        symbol_type = sec_info.get("classShare")
                        identifier = nse_metadata.get("identifier")
                    elif company.nse_code:
                        nse_data = await main(symbol)
                        symbol_data = nse_data.get('symbolData')
                        equity_response = symbol_data.get('equityResponse')[0]
                        nse_metadata = equity_response.get('metaData')
                        sec_info = equity_response.get('secInfo')
                        series = nse_metadata.get("series")
                        symbol_type = sec_info.get("classShare")
                        identifier = nse_metadata.get("identifier")
                    company_stock_ops = BaseDBOperations(db, CompanyStock)
                    fields = ["id"]

                    attrs = [getattr(CompanyStock, f) for f in fields]
                    days_list = ["30Y"]
                    if company.nse_code and company.bse_code:
                        company_stock_data_db = await company_stock_ops.retrieve_selected_columns(
                            attrs, nse_symbol=symbol, bse_code=company.bse_code
                        )
                        volume_data = await main_fetch_volume_from_nse(company_stock_data_db.nse_code,
                                                                       f"{symbol}-{series}", symbol_type)
                        for days in days_list:
                            company_name_with_dash = company.name.replace(" ", "-")
                            nse_data = await new_main_fetch_stock_price_for_graph(days, identifier, symbol, company_name_with_dash)
                            chart = nse_data.get('grapthData')
                            if chart:
                                volume_map = {item["time"]: item["volume"] for item in volume_data.get("data", None)}
                                updated_data = []
                                for row in chart:
                                    time = row[0]
                                    volume = volume_map.get(time, None)
                                    updated_row = row + [volume]
                                    updated_data.append(updated_row)
                                company_stock_chart_dataset_ops = BaseDBOperations(db, ChartDataset)
                                company_stock_chart_dataset_db = await company_stock_chart_dataset_ops.create(
                                    {'metric': "Price", 'label': "Price on NSE", "meta": {"days": days},
                                     "values": updated_data, "company_id": company_stock_data_db.id})
                    elif company.nse_code:
                        company_stock_data_db = await company_stock_ops.retrieve_selected_columns(
                            attrs, nse_symbol=symbol
                        )
                        volume_data = await main_fetch_volume_from_nse(company_stock_data_db.nse_code,
                                                                       f"{symbol}-{series}",
                                                                       symbol_type)
                        for days in days_list:
                            company_name_with_dash = company.name.replace(" ", "-")
                            nse_data = await new_main_fetch_stock_price_for_graph(days, identifier, symbol, company_name_with_dash)
                            chart = nse_data.get('grapthData')
                            if chart:
                                volume_map = {item["time"]: item["volume"] for item in volume_data.get("data", None)}
                                updated_data = []
                                for row in chart:
                                    time = row[0]
                                    volume = volume_map.get(time, None)
                                    updated_row = row + [volume]
                                    updated_data.append(updated_row)
                                company_stock_chart_dataset_ops = BaseDBOperations(db, ChartDataset)
                                company_stock_chart_dataset_db = await company_stock_chart_dataset_ops.create(
                                    {'metric': "Price", 'label': "Price on NSE", "meta": {"days": days},
                                     "values": updated_data, "company_id": company_stock_data_db.id})
                    elif company.bse_code:
                        days_list = ["30Y"]
                        company_stock_data_db = await company_stock_ops.retrieve_selected_columns(
                            attrs, bse_code=company.bse_code
                        )
                        for days in days_list:
                            bse_data = await new_main_fetch_stock_price_for_bse_graph(company.bse_code)
                            script_header = bse_data.get('Data')
                            if script_header:
                                data_list = json.loads(script_header)
                                result = []

                                for item in data_list:
                                    ts_ms = int(
                                        datetime.strptime(item["dttm"], "%a %b %d %Y %H:%M:%S").timestamp() * 1000
                                    )
                                    price = float(item["vale1"])
                                    volume = int(item["vole"])
                                    result.append([ts_ms, price, "", None, None, volume])

                                company_stock_chart_dataset_ops = BaseDBOperations(db, ChartDataset)
                                company_stock_chart_dataset_db = await company_stock_chart_dataset_ops.create(
                                    {'metric': "Price", 'label': "Price on BSE", "meta": {"days": days},
                                     "values": result,
                                     "company_id": company_stock_data_db.id})

                    chart_stmt = select(ChartDataset).where(
                        ChartDataset.company_id == company.id,
                        ChartDataset.meta["days"].astext == "30Y"
                    )

                    chart_result = await db.execute(chart_stmt)
                    charts = chart_result.scalars().all()
                    chart_data = []
                    for chart in charts:
                        if chart.meta and chart.meta.get("days") == "30Y":
                            values = chart.values
                            day_count = YEAR_OR_MONTY_TO_DAYS_MAP.get(days)
                            filtered_values = values[0:day_count]

                            chart_data.append({
                                "metric": chart.metric,
                                "label": chart.label,
                                "values": filtered_values,
                                "meta": {**chart.meta, "days": days}
                            })
            return CustomJSONResponse.custom_response(
                message="Company chart data fetched successfully",
                data=chart_data
            )
        except CustomValidationError:
            raise

        except Exception as e:
            raise CustomValidationError(
                {"error": [str(e)]}, 200
            )

    @staticmethod
    async def update_stock_price(
            request: UpdateStockPriceSchema,
            db: Session = Depends(get_db),
    ):
        try:

            stmt = (
                select(CompanyStock)
                .options(selectinload(CompanyStock.details))
                .where(
                    or_(
                        CompanyStock.nse_symbol == request.symbol
                    )
                )
            )

            result = await db.execute(stmt)
            company = result.scalars().first()
            chart_data = []
            if company is None:
                raise CustomValidationError(
                    validations={"error": [f"Company not found for symbol : {request.symbol}"]}, status_code=400
                )
            if company:
                company.details.current_price = request.price
                await db.commit()

                await db.refresh(company.details)


            return CustomJSONResponse.custom_response(
                message="Company stock price updated successfully",
                data=chart_data
            )
        except CustomValidationError:
            raise

        except Exception as e:
            raise CustomValidationError(
                {"error": [str(e)]}, 200
            )