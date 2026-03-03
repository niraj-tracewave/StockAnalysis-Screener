import logging
from logging.config import dictConfig


def setup_logging() -> None:
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "standard",
                "level": "INFO",
            },
            "function_file": {
                "class": "logging.handlers.TimedRotatingFileHandler",
                "formatter": "standard",
                "filename": "logs/missing_symbols.log",
                "when": "midnight",
                "interval": 1,
                "backupCount": 3,
                "encoding": "utf-8",
                "level": "INFO",
            },
        },
        "root": {
            "handlers": ["console"],
            "level": "INFO",
        },
        "loggers": {
            "missing_symbols_logger": {
                "handlers": ["function_file"],
                "level": "INFO",
                "propagate": False,
            },
        },
    }

    dictConfig(logging_config)


logger = logging.getLogger("stock_screener")
special_logger = logging.getLogger("missing_symbols_logger")