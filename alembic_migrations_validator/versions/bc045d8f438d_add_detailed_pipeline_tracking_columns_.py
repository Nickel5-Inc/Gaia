"""Add detailed pipeline tracking columns to weather_forecast_stats

Revision ID: bc045d8f438d
Revises: f83c6d760854
Create Date: 2025-08-14 16:45:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = 'bc045d8f438d'
down_revision: Union[str, None] = 'f83c6d760854'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add detailed pipeline tracking columns to weather_forecast_stats table."""
    
    # Add current_forecast_stage column
    op.add_column('weather_forecast_stats', 
        sa.Column('current_forecast_stage', sa.VARCHAR(100), nullable=True,
                  comment='Detailed pipeline stage: init, gfs_fetch, inference_requested, inference_running, day1_scoring, era5_scoring_24h, era5_scoring_48h, etc., complete'))
    
    # Add current_forecast_status column
    op.add_column('weather_forecast_stats',
        sa.Column('current_forecast_status', sa.VARCHAR(50), nullable=True,
                  comment='Status within current stage: initialized, in_progress, scoring, error, waiting_retry, completed'))
    
    # Add last_error_message column
    op.add_column('weather_forecast_stats',
        sa.Column('last_error_message', sa.Text(), nullable=True,
                  comment='Most recent error message encountered'))
    
    # Add retries_remaining column
    op.add_column('weather_forecast_stats',
        sa.Column('retries_remaining', sa.Integer(), server_default=sa.text('3'), nullable=True,
                  comment='Number of retry attempts remaining (0-3)'))
    
    # Add next_scheduled_retry column
    op.add_column('weather_forecast_stats',
        sa.Column('next_scheduled_retry', postgresql.TIMESTAMP(timezone=True), nullable=True,
                  comment='UTC timestamp for next retry attempt'))


def downgrade() -> None:
    """Remove detailed pipeline tracking columns from weather_forecast_stats table."""
    
    op.drop_column('weather_forecast_stats', 'next_scheduled_retry')
    op.drop_column('weather_forecast_stats', 'retries_remaining')
    op.drop_column('weather_forecast_stats', 'last_error_message')
    op.drop_column('weather_forecast_stats', 'current_forecast_status')
    op.drop_column('weather_forecast_stats', 'current_forecast_stage')