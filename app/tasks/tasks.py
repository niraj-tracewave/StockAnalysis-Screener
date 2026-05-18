import asyncio
import uuid

from sqlalchemy import select

from app.apis.models.stock_data import CompanyStock
from app.core.async_celery_utils import fetch_and_store_company_data_from_top_50_async, \
    fetch_30y_stock_chart_data_async, fetch_and_update_30y_stock_chart_data_async, \
    fetch_stock_quarterly_result_data_async, fetch_and_update_stock_shareholding_pattern_data_async, \
    update_nse_bse_scrip_code_async, update_nse_bse_stock_information_async, \
    fetch_and_store_newly_listed_company_data_from_nse_bse_async, \
    fetch_and_update_stock_shareholding_pattern_data_async, \
    fetch_and_update_stock_balance_sheet_profit_loss_cash_flow_consolidated_data_async, \
    fetch_and_update_stock_balance_sheet_profit_loss_cash_flow_standalone_data_async, \
    fetch_calculate_and_update_stock_dividend_data_async, fetch_calculate_and_update_stock_book_value_data_async, \
    fetch_calculate_and_update_stock_roce_data_async
from app.core.utils import run_async_task
from app.db.postgres.sync_session import SessionLocalSync
from app.db.redis.redis import redis_client
from app.apis.models.company import Company
from sqlalchemy.dialects.postgresql import insert
from app.core.celery_app import celery_app


TASK_LOCK_TTL_SECONDS = 11000


def run_async_task(coro):
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)
    finally:
        loop.close()
        asyncio.set_event_loop(None)


def run_locked_async_task(lock_name, coro):
    lock_key = f"celery-lock:{lock_name}"
    lock_value = str(uuid.uuid4())
    acquired = redis_client.set(lock_key, lock_value, nx=True, ex=TASK_LOCK_TTL_SECONDS)
    if not acquired:
        print(f"Skipping {lock_name}: previous run is still active")
        return None

    try:
        return run_async_task(coro)
    finally:
        if redis_client.get(lock_key) == lock_value:
            redis_client.delete(lock_key)


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
                "bse_code": item["bse_code"],
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

@celery_app.task(
    name="update_stock_scrip_code",
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 5},
)
def update_stock_scrip_code(nse_code : int, bse_code : int, search : str):
    """
    Background task to store NSE & BSE stock scrip code
    """
    db = SessionLocalSync()
    try:

        stmt = (
            select(CompanyStock)
            .where(
                (CompanyStock.nse_symbol == search)
            )
        )

        result = db.execute(stmt)
        data = result.scalars().first()
        if data:
            data.nse_code = nse_code
            data.bse_code = bse_code
            db.commit()

    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


@celery_app.task(
    name="fetch_and_store_company_data_from_top_50",
)
def fetch_and_store_company_data_from_top_50():
    """
    Background task to store NSE company data
    """
    from app.core.utils import load_angel_map
    load_angel_map()
    # asyncio.run(fetch_and_store_company_data_from_top_50_async())
    run_locked_async_task("fetch_and_store_company_data_from_top_50", fetch_and_store_company_data_from_top_50_async())


@celery_app.task(
    name="fetch_30y_stock_chart_data",
)
def fetch_30y_stock_chart_data():
    """
    Background task to store NSE company data
    """
    # asyncio.run(fetch_30y_stock_chart_data_async())
    run_locked_async_task("fetch_30y_stock_chart_data", fetch_30y_stock_chart_data_async())


@celery_app.task(
    name="fetch_and_update_30y_stock_chart_data",
)
def fetch_and_update_30y_stock_chart_data():
    """
    Background task to store NSE company data
    """
    # asyncio.run(fetch_and_update_30y_stock_chart_data_async())
    run_locked_async_task("fetch_and_update_30y_stock_chart_data", fetch_and_update_30y_stock_chart_data_async())

@celery_app.task(
    name="fetch_quarterly_result_data",
)
def fetch_quarterly_result_data():
    """
    Background task to store NSE company data
    """
    run_locked_async_task("fetch_quarterly_result_data", fetch_stock_quarterly_result_data_async())

@celery_app.task(
    name="update_nse_bse_scrip_code",
)
def update_nse_bse_scrip_code():
    """
    Background task to store NSE company data
    """
    from app.core.utils import load_angel_map
    load_angel_map()
    run_locked_async_task("update_nse_bse_scrip_code", update_nse_bse_scrip_code_async())


@celery_app.task(
    name="update_nse_bse_stock_information",
)
def update_nse_bse_stock_information():
    """
    Background task to store NSE company data
    """
    run_locked_async_task("update_nse_bse_stock_information", update_nse_bse_stock_information_async())

@celery_app.task(
    name="fetch_and_store_newly_company_data_from_nse_bse",
)
def fetch_and_store_newly_company_data_from_nse_bse():
    """
    Background task to store NSE company data
    """
    from app.core.utils import load_angel_map
    load_angel_map()
    run_locked_async_task(
        "fetch_and_store_newly_company_data_from_nse_bse",
        fetch_and_store_newly_listed_company_data_from_nse_bse_async()
    )

@celery_app.task(
    name="fetch_and_update_stock_shareholding_pattern_data",
)
def fetch_and_update_stock_shareholding_pattern_data():
    """
    Background task to store NSE company data
    """
    from app.core.utils import load_angel_map
    load_angel_map()
    # loop = asyncio.new_event_loop()
    # asyncio.set_event_loop(loop)
    # loop.run_until_complete(fetch_and_update_stock_shareholding_pattern_data_async())
    # loop.close()
    run_async_task(fetch_and_update_stock_shareholding_pattern_data_async())


@celery_app.task(
    name="fetch_and_update_stock_balance_sheet_consolidated_data",
)
def fetch_and_update_stock_balance_sheet_consolidated_data():
    """
    Background task to store NSE company data
    """
    run_async_task(fetch_and_update_stock_balance_sheet_profit_loss_cash_flow_consolidated_data_async())

@celery_app.task(
    name="fetch_and_update_stock_balance_sheet_standalone_data",
)
def fetch_and_update_stock_balance_sheet_standalone_data():
    """
    Background task to store NSE company data
    """
    run_async_task(fetch_and_update_stock_balance_sheet_profit_loss_cash_flow_standalone_data_async())

@celery_app.task(
    name="fetch_calculate_and_update_stock_dividend_data",
)
def fetch_calculate_and_update_stock_dividend_data():
    """
    Background task to store NSE company data
    """
    run_async_task(fetch_calculate_and_update_stock_dividend_data_async())

@celery_app.task(
    name="fetch_calculate_and_update_stock_book_value_data",
)
def fetch_calculate_and_update_stock_book_value_data():
    """
    Background task to store NSE company data
    """
    run_async_task(fetch_calculate_and_update_stock_book_value_data_async())

@celery_app.task(
    name="fetch_calculate_and_update_stock_roce_data",
)
def fetch_calculate_and_update_stock_roce_data():
    """
    Background task to store NSE company data
    """
    run_async_task(fetch_calculate_and_update_stock_roce_data_async())