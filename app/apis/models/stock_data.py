import enum

from sqlalchemy import Column, String, Integer, ForeignKey, Float, Date, UniqueConstraint, Numeric, Enum
from sqlalchemy.orm import relationship

from app.db.postgres.base import Base
from sqlalchemy.dialects.postgresql import JSONB



class ResultFormatEnum(str, enum.Enum):
    standalone = "standalone"
    consolidated = "consolidated"

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

    nse_code = Column(
        String,
        index=True,
        nullable=True,
        comment="NSE stock code"
    )

    macro_economic_sector = Column(
        String,
        nullable=True,
    )

    sector = Column(
        String,
        nullable=True,
    )

    industry = Column(
        String,
        nullable=True,
    )

    basic_industry = Column(
        String,
        index=True,
        nullable=True,
    )

    details = relationship(
        "KeyDetailsForCS",
        back_populates="company",
        cascade="all, delete-orphan",
        uselist=False
    )

    charts = relationship(
        "ChartDataset",
        back_populates="company",
        cascade="all, delete-orphan"
    )

    stock_peer = relationship(
        "StockPeerDataset",
        back_populates="company",
        cascade="all, delete-orphan"
    )

    quarterly_result = relationship(
        "QuarterlyResultDateset",
        back_populates="company",
        cascade="all, delete-orphan"
    )

    profit_loss = relationship(
        "ProfitLossDataset",
        back_populates="company",
        cascade="all, delete-orphan"
    )

    balance_sheet = relationship(
        "BalanceSheetDataset",
        back_populates="company",
        cascade="all, delete-orphan"
    )

    cash_flow = relationship(
        "CashFlowDataset",
        back_populates="company",
        cascade="all, delete-orphan"
    )

    ratios = relationship(
        "RatiosDataset",
        back_populates="company",
        cascade="all, delete-orphan"
    )

    share_holding_pattern = relationship(
        "ShareHoldingPeriod",
        back_populates="company",
        cascade="all, delete-orphan"
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

class StockPeerDataset(Base):
    __tablename__ = "stock_peer_datasets"
    id = Column(Integer, primary_key=True, index=True)

    company_id = Column(
        Integer,
        ForeignKey("company_stock.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    np_quarterly = Column(Float, nullable=True)
    sales_quarterly = Column(Float, nullable=True)
    quarterly_profit_var = Column(Float, nullable=True)
    quarterly_sales_var = Column(Float, nullable=True)

    company = relationship(
        "CompanyStock",
        back_populates="stock_peer"
    )

class QuarterlyResultDateset(Base):
    __tablename__ = "quarterly_result_dateset"
    id = Column(Integer, primary_key=True, index=True)

    company_id = Column(
        Integer,
        ForeignKey("company_stock.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    values = Column(JSONB, nullable=False)

    result_format =  Column(Enum(ResultFormatEnum), nullable=True)

    company = relationship(
        "CompanyStock",
        back_populates="quarterly_result"
    )

class ProfitLossDataset(Base):
    __tablename__ = "profit_loss_dataset"
    id = Column(Integer, primary_key=True, index=True)

    company_id = Column(
        Integer,
        ForeignKey("company_stock.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    values = Column(JSONB, nullable=False)

    company = relationship(
        "CompanyStock",
        back_populates="profit_loss"
    )

class BalanceSheetDataset(Base):
    __tablename__ = "balance_sheet_dataset"
    id = Column(Integer, primary_key=True, index=True)

    company_id = Column(
        Integer,
        ForeignKey("company_stock.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    values = Column(JSONB, nullable=False)

    company = relationship(
        "CompanyStock",
        back_populates="balance_sheet"
    )

class CashFlowDataset(Base):
    __tablename__ = "cash_flow_dataset"
    id = Column(Integer, primary_key=True, index=True)

    company_id = Column(
        Integer,
        ForeignKey("company_stock.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    values = Column(JSONB, nullable=False)

    company = relationship(
        "CompanyStock",
        back_populates="cash_flow"
    )

class RatiosDataset(Base):
    __tablename__ = "ratios_dataset"
    id = Column(Integer, primary_key=True, index=True)

    company_id = Column(
        Integer,
        ForeignKey("company_stock.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    values = Column(JSONB, nullable=False)

    company = relationship(
        "CompanyStock",
        back_populates="ratios"
    )

# class ShareHoldingPatternDataset(Base):
#     __tablename__ = "share_holding_pattern_dataset"
#     id = Column(Integer, primary_key=True, index=True)
#
#     company_id = Column(
#         Integer,
#         ForeignKey("company_stock.id", ondelete="CASCADE"),
#         nullable=False,
#         index=True
#     )
#
#     values = Column(JSONB, nullable=False)
#
#     company = relationship(
#         "ShareHoldingPatternDataset",
#         back_populates="share_holding_pattern"
#     )

class ShareHoldingPeriod(Base):
    __tablename__ = "share_holding_period"

    id = Column(Integer, primary_key=True)

    company_id = Column(
        Integer,
        ForeignKey("company_stock.id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    period_date = Column(Date, nullable=False)         # 2023-06-30
    period_type = Column(String(10), nullable=False)  # quarterly / yearly

    __table_args__ = (
        UniqueConstraint("company_id", "period_date", "period_type"),
    )

    sections = relationship(
        "ShareHoldingSection",
        back_populates="period",
        cascade="all, delete-orphan"
    )
    company = relationship(
            "CompanyStock",
            back_populates="share_holding_pattern"
        )

class ShareHoldingSection(Base):
    __tablename__ = "share_holding_section"

    id = Column(Integer, primary_key=True)

    period_id = Column(
        Integer,
        ForeignKey("share_holding_period.id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    key = Column(String(50), nullable=False)           # promoters / fiis / public / shareholders
    label = Column(String(100), nullable=False)        # Promoters / FIIs / Public
    value_type = Column(String(20), nullable=False)    # percent / number

    total_value = Column(Numeric(10, 2), nullable=False)

    period = relationship("ShareHoldingPeriod", back_populates="sections")

    children = relationship(
        "ShareHoldingSectionChild",
        back_populates="section",
        cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("period_id", "key"),
    )

class ShareHoldingSectionChild(Base):
    __tablename__ = "share_holding_section_child"

    id = Column(Integer, primary_key=True)

    section_id = Column(
        Integer,
        ForeignKey("share_holding_section.id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    label = Column(String(255), nullable=False)
    value = Column(Numeric(10, 2), nullable=False)

    section = relationship("ShareHoldingSection", back_populates="children")
