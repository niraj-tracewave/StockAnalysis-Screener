"""add fast market data fields

Revision ID: d6f38b24c901
Revises: 7ef08c36dccc
Create Date: 2026-10-01 19:40:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "d6f38b24c901"
down_revision: Union[str, Sequence[str], None] = "7ef08c36dccc"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "company_stock",
        sa.Column("yahoo_symbol", sa.String(length=40), nullable=True),
    )
    op.add_column(
        "company_stock",
        sa.Column("primary_exchange", sa.String(length=10), nullable=True),
    )
    op.create_index(
        "ix_company_stock_yahoo_symbol",
        "company_stock",
        ["yahoo_symbol"],
        unique=True,
    )
    op.create_index(
        "ix_company_stock_primary_exchange",
        "company_stock",
        ["primary_exchange"],
        unique=False,
    )

    op.add_column(
        "key_details_for_cs",
        sa.Column("data_source", sa.String(length=30), nullable=True),
    )
    op.add_column(
        "key_details_for_cs",
        sa.Column("market_data_updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_key_details_for_cs_market_data_updated_at",
        "key_details_for_cs",
        ["market_data_updated_at"],
        unique=False,
    )
    op.create_unique_constraint(
        "uq_key_details_for_cs_company_id",
        "key_details_for_cs",
        ["company_id"],
    )

    op.execute(
        """
        UPDATE company_stock
        SET primary_exchange = CASE
                WHEN nse_code IS NOT NULL THEN 'NSE'
                WHEN bse_code IS NOT NULL THEN 'BSE'
                ELSE NULL
            END,
            yahoo_symbol = CASE
                WHEN nse_code IS NOT NULL AND nse_symbol IS NOT NULL THEN nse_symbol || '.NS'
                WHEN bse_code IS NOT NULL AND nse_symbol IS NOT NULL THEN nse_symbol || '.BO'
                ELSE NULL
            END
        WHERE yahoo_symbol IS NULL
        """
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_key_details_for_cs_company_id",
        "key_details_for_cs",
        type_="unique",
    )
    op.drop_index(
        "ix_key_details_for_cs_market_data_updated_at",
        table_name="key_details_for_cs",
    )
    op.drop_column("key_details_for_cs", "market_data_updated_at")
    op.drop_column("key_details_for_cs", "data_source")

    op.drop_index("ix_company_stock_primary_exchange", table_name="company_stock")
    op.drop_index("ix_company_stock_yahoo_symbol", table_name="company_stock")
    op.drop_column("company_stock", "primary_exchange")
    op.drop_column("company_stock", "yahoo_symbol")
