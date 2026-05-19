"""add result_format enum to quarterly_result_dateset

Revision ID: 3ee46acacce7
Revises: 83805cca507a
Create Date: 2026-03-09 13:47:20.952931

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = '3ee46acacce7'
down_revision: Union[str, Sequence[str], None] = '83805cca507a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

result_format_enum = postgresql.ENUM(
    "standalone",
    "consolidated",
    name="result_format_enum",
    create_type=False,
)

def upgrade() -> None:
    """Upgrade schema."""
    op.execute(
        """
        DO $$
        BEGIN
            CREATE TYPE result_format_enum AS ENUM ('standalone', 'consolidated');
        EXCEPTION WHEN duplicate_object THEN
            NULL;
        END $$;
        """
    )

    # add column
    op.add_column(
        "quarterly_result_dateset",
        sa.Column("result_format", result_format_enum, nullable=True)
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("quarterly_result_dateset", "result_format")

    # drop enum type
    result_format_enum.drop(op.get_bind(), checkfirst=True)
