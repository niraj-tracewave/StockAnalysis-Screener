from fastapi.encoders import jsonable_encoder
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apis.models.company import Company
from app.apis.v1.schemas.follow_unfollow_company import SearchCompanySchema
from app.core.custom_response import CustomJSONResponse
from app.core.jwt_authentication import JWTBearer
from app.core.nse_search import fetch_nse_data, fetch_bse_data
from app.tasks.tasks import store_company_data

jwt_handler = JWTBearer()


class FollowUnfollowCompanyService:

    @staticmethod
    async def company_search(search_request: SearchCompanySchema, db: Session):
        try:
            nse_company_list = []
            bse_company_list = []
            stmt = (
                select(Company)
                .where(
                    (Company.symbol.ilike(f"{search_request.search}%")) |
                    (Company.company_name.ilike(f"{search_request.search}%"))
                )
                .limit(10)
            )
            result = await db.execute(stmt)
            data = result.scalars().all()
            company_data = [
                jsonable_encoder({
                    "id": obj.id,
                    "company_name": obj.company_name,
                    "symbol": obj.symbol,
                    "platform": obj.platform,
                    "is_active": obj.is_active,
                    "url": obj.url,
                    "created_at": obj.created_at,
                    "nse_code": obj.nse_code,
                    "bse_code": obj.bse_code,
                })
                for obj in data
            ]
            if not data:
                nse_company_list = await fetch_nse_data(search_request.search)
                bse_company_list = await fetch_bse_data(search_request.search)
                store_company_data.delay(nse_company_list, bse_company_list)
                company_data = nse_company_list + bse_company_list
            return CustomJSONResponse(
                success=True,
                message="Company list fetched successfully.",
                data={"data": company_data}
            )
        except Exception as e:
            return CustomJSONResponse(
                success=False,
                message=str(e),
                data={"data": []},
                status_code=500
            )