"""add_pipeline_timing_fields_to_weather_miner_responses

Revision ID: pipeline_timing_001
Revises: 8c00159f39c7
Create Date: 2025-08-20 17:15:00.000000

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'pipeline_timing_001'
down_revision = '83c229985d5c'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add pipeline timing fields to weather_miner_responses table."""
    
    # Add pipeline timing columns to weather_miner_responses
    op.add_column('weather_miner_responses', 
        sa.Column('job_accepted_at', postgresql.TIMESTAMP(timezone=True), nullable=True,
                 comment='UTC timestamp when the miner accepted the job (fetch_accepted)')
    )
    op.add_column('weather_miner_responses', 
        sa.Column('inference_completed_at', postgresql.TIMESTAMP(timezone=True), nullable=True,
                 comment='UTC timestamp when inference was completed and forecast submitted')
    )
    op.add_column('weather_miner_responses', 
        sa.Column('day1_scoring_completed_at', postgresql.TIMESTAMP(timezone=True), nullable=True,
                 comment='UTC timestamp when Day1 scoring was completed')
    )
    op.add_column('weather_miner_responses', 
        sa.Column('era5_scoring_completed_at', postgresql.TIMESTAMP(timezone=True), nullable=True,
                 comment='UTC timestamp when ERA5 scoring was completed')
    )
    op.add_column('weather_miner_responses', 
        sa.Column('total_pipeline_duration_seconds', sa.Integer(), nullable=True,
                 comment='Total time from job acceptance to final scoring completion in seconds')
    )
    
    # Add indexes for efficient querying of timing data
    op.create_index('idx_wmr_job_accepted_at', 'weather_miner_responses', ['job_accepted_at'])
    op.create_index('idx_wmr_inference_completed_at', 'weather_miner_responses', ['inference_completed_at'])
    op.create_index('idx_wmr_day1_completed_at', 'weather_miner_responses', ['day1_scoring_completed_at'])
    op.create_index('idx_wmr_era5_completed_at', 'weather_miner_responses', ['era5_scoring_completed_at'])
    op.create_index('idx_wmr_total_duration', 'weather_miner_responses', ['total_pipeline_duration_seconds'])


def downgrade() -> None:
    """Remove pipeline timing fields from weather_miner_responses table."""
    
    # Remove indexes
    op.drop_index('idx_wmr_total_duration', table_name='weather_miner_responses')
    op.drop_index('idx_wmr_era5_completed_at', table_name='weather_miner_responses')
    op.drop_index('idx_wmr_day1_completed_at', table_name='weather_miner_responses')
    op.drop_index('idx_wmr_inference_completed_at', table_name='weather_miner_responses')
    op.drop_index('idx_wmr_job_accepted_at', table_name='weather_miner_responses')
    
    # Remove columns
    op.drop_column('weather_miner_responses', 'total_pipeline_duration_seconds')
    op.drop_column('weather_miner_responses', 'era5_scoring_completed_at')
    op.drop_column('weather_miner_responses', 'day1_scoring_completed_at')
    op.drop_column('weather_miner_responses', 'inference_completed_at')
    op.drop_column('weather_miner_responses', 'job_accepted_at')
