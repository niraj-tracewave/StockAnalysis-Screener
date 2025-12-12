from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from starlette import status

from app.apis.deps import get_db
from app.apis.v1.services.follow_unfollow_company import SearchCompanySchema, FollowUnfollowCompanyService

router = APIRouter()

@router.post("/search-company", status_code=status.HTTP_200_OK)
async def search_company(request: SearchCompanySchema, db: Session = Depends(get_db)):
    return await FollowUnfollowCompanyService.company_search(request, db)