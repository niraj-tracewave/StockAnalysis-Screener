from app.db.postgres.sync_session import SessionLocalSync
from app.apis.models.company import Company
from sqlalchemy.dialects.postgresql import insert

from app.core.celery_app import celery_app


@celery_app.task(
    name="store_company_data",
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 5},
)
def store_company_data(nse_list : list, bse_list : list):
    """
    Background task to store NSE & BSE company data
    """
    db = SessionLocalSync()
    try:
        companies = []

        for item in nse_list:
            companies.append({
                "company_name": item["company_name"],
                "symbol": item["symbol"],
                "platform": "NSE",
                "url": item["url"],
                "nse_code": item["nse_code"],
                "bse_code": None
            })

        for item in bse_list:
            companies.append({
                "company_name": item["company_name"],
                "symbol": item["symbol"],
                "platform": "BSE",
                "url": item["url"],
                "bse_code": item["security_code"],
                "nse_code": None
            })

        stmt = insert(Company).values(companies)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=["symbol", "platform"]
        )

        db.execute(stmt)
        db.commit()

    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
