from celery import Celery
from celery.schedules import crontab

celery_app = Celery(
    "company_tasks",
    broker="redis://localhost:6379/2",
    backend="redis://localhost:6379/3",
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Kolkata",
    enable_utc=True,
    task_soft_time_limit=10200,
    task_time_limit=10800,
)

celery_app.autodiscover_tasks(["app.tasks"])


celery_app.conf.beat_schedule = {
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
    "fetch_30y_stock_chart_dat_from_nse_bse_new": {
            "task": "fetch_and_update_30y_stock_chart_data",
            "schedule": crontab(minute="*/20"),
        },
    "fetch_quarterly_result_data_from_nse_bse": {
        "task": "fetch_quarterly_result_data",
        "schedule": crontab(minute="*/30"),
    },
    "fetch_and_update_nse_bse_scrip_code": {
        "task": "update_nse_bse_scrip_code",
        "schedule": crontab(minute="*/10"),
    },
    "fetch_and_update_nse_bse_stock_information": {
        "task": "update_nse_bse_stock_information",
        "schedule": crontab(minute="*/7"),
    },
    "fetch_and_update_newly_company_data": {
        "task": "fetch_and_store_newly_company_data_from_nse_bse",
        "schedule": crontab(
                minute=0,
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
    }
}