from fastapi import Depends
from sqlalchemy.orm import Session

from app.apis.deps import get_db
from app.apis.v1.schemas.stock_data import SearchCompanyStockSchema
from app.core.custom_response import CustomJSONResponse


class CompanyStockFetchService:

    @staticmethod
    async def company_search(search_request: SearchCompanyStockSchema, db: Session = Depends(get_db)):
        return CustomJSONResponse(
            success=True,
            message="Company stock data fetched successfully.",
            data={"data": []}
        )