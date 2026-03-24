from typing import Optional, Any

from pydantic import BaseModel


class SearchCompanyStockSchema(BaseModel):
    symbol: Optional[str] = None
    scrip: Optional[str] = None

class QuarterlyResultSchema(BaseModel):
    values: Any

    class Config:
        from_attributes = True

class UpdateStockPriceSchema(BaseModel):
    price: float
    scrip: Optional[str] = None
    symbol: Optional[str] = None