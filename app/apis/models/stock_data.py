from sqlalchemy import Column, String, Integer, ForeignKey, Float
from sqlalchemy.orm import relationship

from app.db.postgres.base import Base
from sqlalchemy.dialects.postgresql import JSONB


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

    details = relationship(
        "KeyDetailsForCS",
        back_populates="company",
        cascade="all, delete-orphan",
        uselist=False
    )


class KeyDetailsForCS(Base):
    __tablename__ = "key_details_for_cs"

    id = Column(Integer, primary_key=True, index=True)

    company_id = Column(
        Integer,
        ForeignKey("company_stock.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    market_cap = Column(Float, nullable=True)
    current_price = Column(Float, nullable=True)
    high_price = Column(Float, nullable=True)
    low_price = Column(Float, nullable=True)

    pe_ratio = Column(Float, nullable=True)
    book_value = Column(Float, nullable=True)
    dividend_yield = Column(Float, nullable=True)
    roce = Column(Float, nullable=True)
    roe = Column(Float, nullable=True)
    face_value = Column(Float, nullable=True)

    about = Column(JSONB, nullable=True)
    key_points = Column(JSONB, nullable=True)
    pros = Column(JSONB, nullable=True)
    cons = Column(JSONB, nullable=True)

    company = relationship(
        "CompanyStock",
        back_populates="details"
    )

class ChartDataset(Base):
    __tablename__ = "chart_datasets"

    id = Column(Integer, primary_key=True, index=True)

    company_id = Column(
        Integer,
        ForeignKey("company_stock.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    metric = Column(String(100), nullable=False)
    label = Column(String(150), nullable=True)

    values = Column(JSONB, nullable=False)
    meta = Column(JSONB, nullable=True)

    company = relationship(
        "CompanyStock",
        back_populates="charts"
    )