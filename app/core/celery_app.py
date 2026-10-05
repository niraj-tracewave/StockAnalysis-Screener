import os

from celery import Celery
from celery.schedules import crontab
from dotenv import load_dotenv


load_dotenv()
LEGACY_MARKET_JOBS_ENABLED = os.environ.get(
    "ENABLE_LEGACY_MARKET_JOBS", "false"
).lower() in {"1", "true", "yes"}
EXCHANGE_MARKET_JOBS_ENABLED = os.environ.get(
    "ENABLE_EXCHANGE_MARKET_JOBS", "true"
).lower() in {"1", "true", "yes"}
YAHOO_MARKET_JOBS_ENABLED = os.environ.get(
    "ENABLE_YAHOO_MARKET_JOBS", "false"
).lower() in {"1", "true", "yes"}
NSE_MARKET_JOBS_ENABLED = os.environ.get(
    "ENABLE_NSE_MARKET_JOBS", "true"
).lower() in {"1", "true", "yes"}
BSE_MARKET_JOBS_ENABLED = os.environ.get(
    "ENABLE_BSE_MARKET_JOBS", "false"
).lower() in {"1", "true", "yes"}
BROKER_URL = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/2")
RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379/3")
REDIS_BROKER = BROKER_URL.startswith(("redis://", "rediss://"))


def queue_priority(redis_priority: int) -> int:
    """Keep named priority intent when switching Redis to RabbitMQ."""

    return redis_priority if REDIS_BROKER else 9 - redis_priority

task_modules = []
if EXCHANGE_MARKET_JOBS_ENABLED:
    task_modules.append("app.tasks.exchange_data_tasks")
if YAHOO_MARKET_JOBS_ENABLED:
    task_modules.append("app.tasks.market_data_tasks")
if LEGACY_MARKET_JOBS_ENABLED:
    task_modules.append("app.tasks.tasks")

celery_app = Celery(
    "company_tasks",
    broker=BROKER_URL,
    backend=RESULT_BACKEND,
    include=task_modules,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Kolkata",
    enable_utc=True,
    task_soft_time_limit=10200,
    task_time_limit=10800,
    worker_concurrency=int(os.environ.get("SCREENER_WORKER_CONCURRENCY", "8")),
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    broker_connection_retry_on_startup=True,
    task_default_priority=5,
    task_queue_max_priority=10,
    broker_transport_options={
        "priority_steps": list(range(10)),
        "queue_order_strategy": "priority",
    } if REDIS_BROKER else {},
    task_routes={
        "refresh_exchange_quote_batch": {"queue": "exchange-control", "priority": queue_priority(0)},
        "refresh_exchange_quote_shard": {"queue": "nse-quotes", "priority": queue_priority(1)},
        "retry_exchange_quote_failures": {"queue": "nse-quotes", "priority": queue_priority(3)},
        "refresh_yahoo_quote_batch": {"queue": "market-control", "priority": queue_priority(0)},
        "refresh_yahoo_quote_shard": {"queue": "market-quotes", "priority": queue_priority(1)},
        "retry_yahoo_quote_failures": {"queue": "market-quotes", "priority": queue_priority(3)},
        "backfill_yahoo_history_batch": {"queue": "market-history", "priority": queue_priority(9)},
    },
)

celery_app.conf.beat_schedule = {
    "fast-nse-quote-batches": {
        "task": "refresh_exchange_quote_batch",
        "schedule": crontab(minute="*", hour="9-16", day_of_week="mon-fri"),
        "args": ("nse",),
        "options": {"queue": "exchange-control", "priority": queue_priority(0)},
    },
    "fast-bse-quote-batches": {
        "task": "refresh_exchange_quote_batch",
        "schedule": crontab(minute="*", hour="9-16", day_of_week="mon-fri"),
        "args": ("bse",),
        "options": {"queue": "exchange-control", "priority": queue_priority(0)},
    },
    "fast-nse-retry-batches": {
        "task": "retry_exchange_quote_failures",
        "schedule": crontab(minute="*", hour="9-16", day_of_week="mon-fri"),
        "args": ("nse",),
        "options": {"queue": "nse-quotes", "priority": queue_priority(3)},
    },
    "fast-bse-retry-batches": {
        "task": "retry_exchange_quote_failures",
        "schedule": crontab(minute="*", hour="9-16", day_of_week="mon-fri"),
        "args": ("bse",),
        "options": {"queue": "bse-quotes", "priority": queue_priority(3)},
    },
    "fast-yahoo-quote-batches": {
        "task": "refresh_yahoo_quote_batch",
        "schedule": crontab(minute="*", hour="9-16", day_of_week="mon-fri"),
        "options": {"queue": "market-control", "priority": queue_priority(0)},
    },
    "fast-yahoo-retry-batches": {
        "task": "retry_yahoo_quote_failures",
        "schedule": crontab(minute="*", hour="9-16", day_of_week="mon-fri"),
        "options": {"queue": "market-quotes", "priority": queue_priority(3)},
    },
    # "fetch-nse-company-data-every-20-minutes": {
    #     "task": "fetch_and_store_company_data_from_top_50",
    #     "schedule": crontab(minute="*/15"),
    # },
    # "fetch_30y_stock_chart_dat_from_nse_bse": {
    #     "task": "fetch_30y_stock_chart_data",
    #     "schedule": crontab(hour=18, minute=8),
    # },
    # "fetch_30y_stock_chart_dat_from_nse_bse": {
    #     "task": "fetch_30y_stock_chart_data",
    #     "schedule": crontab(hour=18, minute=8),
    # },
    # "fetch_30y_stock_chart_dat_from_nse_bse_new": {
    #         "task": "fetch_and_update_30y_stock_chart_data",
    #         "schedule": crontab(minute=25),
    #     },
    "fetch_quarterly_result_data_from_nse_bse": {
        "task": "fetch_quarterly_result_data",
        "schedule": crontab(minute=55),
    },
    "fetch_and_update_nse_bse_scrip_code": {
        "task": "update_nse_bse_scrip_code",
        "schedule": crontab(minute="12,42"),
    },
    "fetch_and_update_nse_bse_stock_information": {
        "task": "update_nse_bse_stock_information",
        "schedule": crontab(minute="5,20,35,50"),
    },
    "fetch_and_update_newly_company_data": {
        "task": "fetch_and_store_newly_company_data_from_nse_bse",
        "schedule": crontab(
                minute=2,
                hour="10,13,16,19",
                day_of_week="mon-fri",  # Monday to Friday
        ),
    },
    "fetch_and_update_stock_shareholding_pattern": {
        "task": "fetch_and_update_stock_shareholding_pattern_data",
        "schedule": crontab(minute="*/15"),
    },
    "fetch_and_update_stock_balance_sheet_profit_loss_cash_flow_consolidated_data": {
            "task": "fetch_and_update_stock_balance_sheet_consolidated_data",
            "schedule": crontab(minute="*/16"),
    },
    "fetch_and_update_stock_balance_sheet_profit_loss_cash_flow_standalone_data": {
        "task": "fetch_and_update_stock_balance_sheet_standalone_data",
        "schedule": crontab(minute="*/17"),
    },
    "fetch_calculate_and_update_stock_dividend": {
        "task": "fetch_calculate_and_update_stock_dividend_data",
        "schedule": crontab(minute="*/12"),
    },
    "fetch_calculate_and_update_stock_book_value": {
        "task": "fetch_calculate_and_update_stock_book_value_data",
        "schedule": crontab(minute="*/18"),
    },
    "fetch_calculate_and_update_stock_roce": {
        "task": "fetch_calculate_and_update_stock_roce_data",
        "schedule": crontab(minute="*/22"),
    },
    "convert_stock_quarterly_result": {
        "task": "convert_stock_quarterly_result_data",
        "schedule": crontab(minute="*/30"),
    },
    "fetch_stock_quarterly_result_standalone": {
        "task": "fetch_stock_quarterly_result_standalone_data",
        "schedule": crontab(minute="*/25"),
    },
    "fetch_nse_bse_gross_deliverables": {
        "task": "fetch_gross_deliverables_nse_bse_stock_information",
        "schedule": crontab(minute="*/20"),
    },
    "fetch_bse_gross_deliverables": {
        "task": "fetch_daily_gross_deliverables_bse_stock_information",
        "schedule": crontab(minute="*/15"),
    },
    "fetch_nse_gross_deliverables": {
        "task": "fetch_daily_gross_deliverables_nse_stock_information",
        "schedule": crontab(minute="*/15"),
    },
    # "daily_gross_deliverables_nse_stock": {
    #     "task" : "daily_gross_deliverables_nse_stock_information",
    #     "schedule": crontab(minute="*/15"),
    # },
    "fetch_30y_stock_chart_dat_from_nse_bse_stock": {
        "task": "fetch_and_update_basic_and_30y_stock_chart_data",
        "schedule": crontab(minute="*/15"),
    },
    "fetch_daily_short_selling": {
        "task": "fetch_daily_short_selling_data",
        "schedule": crontab(minute=0)
    },
    "fetch_daily_bulk_deal": {
        "task": "fetch_daily_bulk_deal_data",
        "schedule": crontab(minute=0)
    },
    "fetch_daily_block_deal": {
        "task": "fetch_daily_block_deal_data",
        "schedule": crontab(minute=0)
    }
}

celery_app.conf.beat_schedule = {
    name: schedule
    for name, schedule in celery_app.conf.beat_schedule.items()
    if (
        name.startswith("fast-nse-")
        and EXCHANGE_MARKET_JOBS_ENABLED
        and NSE_MARKET_JOBS_ENABLED
    )
    or (
        name.startswith("fast-bse-")
        and EXCHANGE_MARKET_JOBS_ENABLED
        and BSE_MARKET_JOBS_ENABLED
    )
    or (name.startswith("fast-yahoo-") and YAHOO_MARKET_JOBS_ENABLED)
    or (not name.startswith("fast-") and LEGACY_MARKET_JOBS_ENABLED)
}
