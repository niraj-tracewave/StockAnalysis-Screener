from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from starlette import status

from app.apis.deps import get_db
from app.apis.v1.schemas.stock_data import SearchCompanyStockSchema
from app.apis.v1.services.stock_data import CompanyStockFetchService

router = APIRouter()
@router.post("/fetch-company-data", status_code=status.HTTP_200_OK)
async def search_company(request: SearchCompanyStockSchema, db: Session = Depends(get_db)):
    return await CompanyStockFetchService.company_search(request, db)