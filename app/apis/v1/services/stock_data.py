import json
from datetime import datetime

from fastapi import Depends
from sqlalchemy import select, func, or_
from sqlalchemy.orm import Session, selectinload

from app.apis.deps import get_db
from app.apis.models.stock_data import CompanyStock, KeyDetailsForCS, ChartDataset, ShareHoldingPeriod
from app.apis.v1.schemas.stock_data import SearchCompanyStockSchema
from app.core.constants import quarterly_result, profit_loss, balance_sheet, cash_flow, ratios, share_holding_pattern
from app.core.custom_response import CustomJSONResponse
from app.core.utils import parse_qtr, parse_period_to_date, fetch_nse_scrip_code
from app.db.postgres.base import BaseDBOperations
from scripts.bse_fetch_share_holder_link_of_stock import main_fetch_stock_share_holder_pattern_urls
from scripts.bse_shareholder_pattern import main_fetch_stock_share_holder_pattern
from scripts.nse import main
from scripts.bse import main as main_bse
from scripts.nse_stock_price_graph import main_fetch_stock_price_for_graph
from scripts.bse_stock_price_graph import main_fetch_stock_price_for_bse_graph


class CompanyStockFetchService:

    @staticmethod
    async def company_search(search_request: SearchCompanyStockSchema, db: Session = Depends(get_db)):
        symbol = search_request.symbol
        scrip = search_request.scrip
        roe = current_price = high_price = low_price = pe_ratio = bse_code = nse_symbol = company_name = market_cap_cr = face_value = macro = sector = industry_info = basic_industry = None
        if scrip and symbol:
            nse_data = await main(symbol)
            bse_data = await main_bse(scrip)
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
            nse_code = fetch_nse_scrip_code(nse_symbol, "NSE, BSE")
        elif symbol:
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
            nse_code = fetch_nse_scrip_code(nse_symbol, "NSE, BSE")
        elif scrip:
            bse_data = await main_bse(search_request.scrip)
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
            macro = header_data.get("IndustryNew")
            sector = header_data.get("sector")
            industry_info = header_data.get("IGroup")
            basic_industry = header_data.get("Industry")
            nse_code = None
        company_stock_ops = BaseDBOperations(db, CompanyStock)
        company_stock_data_db = await company_stock_ops.create({'nse_symbol': nse_symbol, 'name': company_name, 'nse_code': nse_code,
                                                                "bse_code": bse_code, "macro_economic_sector": macro,
                                                                "sector": sector, "industry": industry_info, "basic_industry": basic_industry})

        key_company_stock_detail_ops = BaseDBOperations(db, KeyDetailsForCS)
        await key_company_stock_detail_ops.create({'market_cap': market_cap_cr, 'current_price': current_price,
                                        "pe_ratio": float(pe_ratio) if pe_ratio and pe_ratio != '-' else None, "face_value": face_value, "high_price": float(high_price),
                                        "low_price": float(low_price), "book_value": None, "dividend_yield": None,
                                        "roce": None, "roe": float(roe) if roe and roe != '-' else None, "company_id": company_stock_data_db.id})

        return CustomJSONResponse(
            success=True,
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

        return CustomJSONResponse(
            success=True,
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

        return CustomJSONResponse(
            success=True,
            message="Company stock peer data fetched successfully.",
            data={"data": []}
        )

    @staticmethod
    async def fetch_company_quarterly_result(search_request: SearchCompanyStockSchema, db: Session = Depends(get_db)):
        symbol = search_request.symbol
        scrip = search_request.scrip
        if scrip and symbol:
            pass
        elif symbol:
            pass
        elif scrip:
            pass

        return CustomJSONResponse(
            success=True,
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

        return CustomJSONResponse(
            success=True,
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

        return CustomJSONResponse(
            success=True,
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

        return CustomJSONResponse(
            success=True,
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

        return CustomJSONResponse(
            success=True,
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

        return CustomJSONResponse(
            success=True,
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

        return CustomJSONResponse(
            success=True,
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
            symbol,
            db: Session = Depends(get_db)
    ):

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

        if not company:
            return CustomJSONResponse(
                success=False,
                message="Company not found",
                data=None
            )

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
            "quarterly_result": quarterly_result,
            "profit_loss": profit_loss,
            "balance_sheet": balance_sheet,
            "cash_flow": cash_flow,
            "ratios": ratios,
            "share_holding_pattern": share_holding_pattern,
            "use_own_stock_socket": True
        }

        return CustomJSONResponse(
            success=True,
            message="Company detail fetched successfully",
            data=response
        )