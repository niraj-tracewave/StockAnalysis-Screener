from typing import Optional

from pydantic import BaseModel


class SearchCompanyStockSchema(BaseModel):
    symbol: Optional[str] = None
    scrip: Optional[str] = None
