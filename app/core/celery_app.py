from celery import Celery
from celery.schedules import crontab

celery_app = Celery(
    "company_tasks",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/1",
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Kolkata",
    enable_utc=True,
)

celery_app.autodiscover_tasks(["app.tasks"])


celery_app.conf.beat_schedule = {
    "fetch-nse-company-data-every-30-minutes": {
        "task": "fetch_and_store_company_data_from_top_50",
        "schedule": crontab(minute="*/30"),
    },
    "fetch_30y_stock_chart_dat_from_nse_bse": {
        "task": "fetch_30y_stock_chart_data",
        "schedule": crontab(hour=18, minute=8),
    },
}