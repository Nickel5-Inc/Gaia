"""add_idx_wfr_status_init_time_to_weather_forecast_runs

Revision ID: e7319ba352ac
Revises: 54516f21dc9c
Create Date: 2025-05-23 15:46:08.124380

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e7319ba352ac'
down_revision: Union[str, None] = '54516f21dc9c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_index('idx_wfr_status_init_time', 'weather_forecast_runs', ['status', 'run_initiation_time'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index('idx_wfr_status_init_time', table_name='weather_forecast_runs')
