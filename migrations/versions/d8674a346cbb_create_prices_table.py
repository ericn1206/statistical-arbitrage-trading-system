"""create prices table

Revision ID: d8674a346cbb
Revises: 
Create Date: 2026-01-15 15:41:12.623435

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd8674a346cbb'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.create_table(
        "prices",
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("ts", sa.DateTime(), nullable=False),
        sa.Column("open", sa.Numeric(), nullable=False),
        sa.Column("high", sa.Numeric(), nullable=False),
        sa.Column("low", sa.Numeric(), nullable=False),
        sa.Column("close", sa.Numeric(), nullable=False),
        sa.Column("volume", sa.Numeric(), nullable=True),
        sa.UniqueConstraint("symbol", "ts", name="uq_prices_symbol_ts"),
    )


def downgrade():
    op.drop_table("prices")