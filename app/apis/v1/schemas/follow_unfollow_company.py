from pydantic import BaseModel


class SearchCompanySchema(BaseModel):
    search : str