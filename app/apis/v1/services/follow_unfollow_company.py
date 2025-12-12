from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apis.deps import get_db
from app.apis.models.company import Company
from app.apis.v1.schemas.follow_unfollow_company import SearchCompanySchema
from app.core.custom_response import CustomJSONResponse
from app.core.jwt_authentication import JWTBearer
from app.core.nse_search import fetch_nse_data

jwt_handler = JWTBearer()


class FollowUnfollowCompanyService:

    @staticmethod
    async def company_search(search_request: SearchCompanySchema, db: Session):
        nse_company_list = await fetch_nse_data(search_request.search)
        # stmt = (
        #     select(Company)
        #     .where(
        #         (Company.symbol.ilike(f"{search_request.search}%")) |
        #         (Company.company_name.ilike(f"{search_request.search}%"))
        #     )
        #     .limit(20)
        # )
        # result = await db.execute(stmt)
        # print(result)
        # data = result.scalars().all()
        # print(data)
        return CustomJSONResponse(
            success=True,
            message="Otp sent successfully11.",
            data={"nse_company_list": nse_company_list, "data": "data"}
        )