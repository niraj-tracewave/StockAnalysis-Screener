from sqlalchemy import Column, String, Integer
from app.db.postgres.base import Base


# ───────────────────────────────────────────────────────────────────────────────
# CompanyStock Table
# ───────────────────────────────────────────────────────────────────────────────

class CompanyStock(Base):
    __tablename__ = "company_stock"

    id = Column(Integer, primary_key=True)

    name = Column(
        String,
        index=True,
        comment="Company full name (e.g., Tata Steel Ltd)"
    )

    website = Column(
        String,
        nullable=True,
        comment="Official company website"
    )

    bse_code = Column(
        String,
        index=True,
        nullable=True,
        comment="BSE stock code"
    )

    nse_symbol = Column(
        String,
        index=True,
        nullable=True,
        comment="NSE symbol"
    )