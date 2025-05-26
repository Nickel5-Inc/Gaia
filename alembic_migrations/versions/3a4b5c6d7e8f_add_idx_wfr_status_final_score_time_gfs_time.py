"""add_idx_wfr_status_final_score_time_gfs_time

Revision ID: 3a4b5c6d7e8f
Revises: 2dade38128f1
Create Date: 2025-05-26 00:01:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3a4b5c6d7e8f'
down_revision: Union[str, None] = '2dade38128f1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add index on status, final_scoring_attempted_time, and gfs_init_time_utc to weather_forecast_runs."""
    op.create_index(
        'idx_wfr_status_final_score_time_gfs_time',
        'weather_forecast_runs',
        ['status', 'final_scoring_attempted_time', 'gfs_init_time_utc'],
        unique=False
    )


def downgrade() -> None:
    """Remove index on status, final_scoring_attempted_time, and gfs_init_time_utc from weather_forecast_runs."""
    op.drop_index('idx_wfr_status_final_score_time_gfs_time', table_name='weather_forecast_runs') 