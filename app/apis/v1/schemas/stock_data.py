from pydantic import BaseModel


class SearchCompanyStockSchema(BaseModel):
    search : str