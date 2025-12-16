from fastapi import Depends
from sqlalchemy.orm import Session

from app.apis.deps import get_db as db, get_db
from app.apis.models.company import Company
from app.core.celery_app import celery_app


@celery_app.task(
    name="store_company_data",
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 5},
)
def store_company_data(nse_list: list, bse_list: list):
    """
    Background task to store NSE & BSE company data
    """
    from sqlalchemy.dialects.postgresql import insert
    db =  Depends(get_db)
    print(db)
    try:
        companies = []

        for item in nse_list:
            companies.append({
                "company_name": item["company_name"],
                "symbol": item["symbol"],
                "platform": "NSE",
            })

        for item in bse_list:
            companies.append({
                "company_name": item["company_name"],
                "symbol": item["symbol"],
                "platform": "BSE",
            })

        print(companies, "cccccc")


        # stmt = insert(Company).values(companies)
        # stmt = stmt.on_conflict_do_nothing(
        #     index_elements=["symbol", "platform"]
        # )
        #
        # db.execute(stmt)
        # db.commit()

    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
