from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from starlette import status

from app.apis.deps import get_db, get_external_db
from app.apis.v1.schemas.stock_data import SearchCompanyStockSchema
from app.apis.v1.services.stock_data import CompanyStockFetchService

router = APIRouter()
@router.post("/fetch-company-data", status_code=status.HTTP_200_OK)
async def search_company(request: SearchCompanyStockSchema, db: Session = Depends(get_db)):
    return await CompanyStockFetchService.company_search(request, db)

@router.post("/fetch-company-stock-price-graph", status_code=status.HTTP_200_OK)
async def search_company(request: SearchCompanyStockSchema, db: Session = Depends(get_db)):
    return await CompanyStockFetchService.stock_price_chart(request, db)

@router.post("/fetch-company-peer-data", status_code=status.HTTP_200_OK)
async def search_company(request: SearchCompanyStockSchema, db: Session = Depends(get_db)):
    return await CompanyStockFetchService.fetch_company_peer_data(request, db)

@router.post("/fetch-company-quarterly-result", status_code=status.HTTP_200_OK)
async def search_company(request: SearchCompanyStockSchema, db: Session = Depends(get_db)):
    return await CompanyStockFetchService.fetch_company_quarterly_result(request, db)


@router.post("/fetch-company-profit-loss", status_code=status.HTTP_200_OK)
async def search_company(request: SearchCompanyStockSchema, db: Session = Depends(get_db)):
    return await CompanyStockFetchService.fetch_company_profit_loss(request, db)

@router.post("/fetch-company-balance-sheet", status_code=status.HTTP_200_OK)
async def search_company(request: SearchCompanyStockSchema, db: Session = Depends(get_db)):
    return await CompanyStockFetchService.fetch_company_balance_sheet(request, db)

@router.post("/fetch-company-cash-flows", status_code=status.HTTP_200_OK)
async def search_company(request: SearchCompanyStockSchema, db: Session = Depends(get_db)):
    return await CompanyStockFetchService.fetch_company_cash_flows(request, db)

@router.post("/fetch-company-ratio", status_code=status.HTTP_200_OK)
async def search_company(request: SearchCompanyStockSchema, db: Session = Depends(get_db)):
    return await CompanyStockFetchService.fetch_company_ratios(request, db)

@router.post("/fetch-company-shareholding-pattern", status_code=status.HTTP_200_OK)
async def search_company(request: SearchCompanyStockSchema, db: Session = Depends(get_db)):
    return await CompanyStockFetchService.fetch_company_shareholding_pattern(request, db)

@router.get("/list-of-listed-companies", status_code=status.HTTP_200_OK)
async def search_company( page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=500),db: Session = Depends(get_db)):
    return await CompanyStockFetchService.fetch_listed_companies(page, page_size, db)

@router.get("/list-of-listed-company-detail/{symbol}/", status_code=status.HTTP_200_OK)
async def search_company( symbol: str, scrip: str | None = None, db: Session = Depends(get_db), external_db: Session = Depends(get_external_db)):
    return await CompanyStockFetchService.fetch_listed_company_detail(symbol, scrip, db, external_db)

@router.get("/fetch-top-50-nse-data", status_code=status.HTTP_200_OK)
async def search_company(db: Session = Depends(get_db)):
    return await CompanyStockFetchService.fetch_and_store_top_50_company_data(db)

@router.get("/fetch-scrip-code", status_code=status.HTTP_200_OK)
async def search_company():
    return await CompanyStockFetchService.fetch_and_get_scrip_code_from_angle_one()

@router.get("/fetch-stock-data-in-background", status_code=status.HTTP_200_OK)
async def search_company():
    return await CompanyStockFetchService.fetch_and_get_scrip_code_from_json_file()