from fastapi import Depends
from sqlalchemy.orm import Session

from app.apis.deps import get_db
from app.apis.models.stock_data import CompanyStock, KeyDetailsForCS
from app.apis.v1.schemas.stock_data import SearchCompanyStockSchema
from app.core.custom_response import CustomJSONResponse
from app.db.postgres.base import BaseDBOperations
from scripts.nse import main
from scripts.bse import main as main_bse


class CompanyStockFetchService:

    @staticmethod
    async def company_search(search_request: SearchCompanyStockSchema, db: Session = Depends(get_db)):
        symbol = search_request.symbol
        scrip = search_request.scrip
        roe = current_price = high_price = low_price = pe_ratio = bse_code = nse_symbol = company_name = market_cap_cr = face_value = None
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
            bse_code = header_data.get("SecurityCode")
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
        company_stock_ops = BaseDBOperations(db, CompanyStock)
        company_stock_data_db = await company_stock_ops.create({'nse_symbol': nse_symbol, 'name': company_name, "bse_code": bse_code})

        key_company_stock_detail_ops = BaseDBOperations(db, KeyDetailsForCS)
        await key_company_stock_detail_ops.create({'market_cap': market_cap_cr, 'current_price': current_price,
                                        "pe_ratio": float(pe_ratio), "face_value": face_value, "high_price": float(high_price),
                                        "low_price": float(low_price), "book_value": None, "dividend_yield": None,
                                        "roce": None, "roe": float(roe) if roe else None, "company_id": company_stock_data_db.id})

        return CustomJSONResponse(
            success=True,
            message="Company stock data fetched successfully.",
            data={"data": []}
        )