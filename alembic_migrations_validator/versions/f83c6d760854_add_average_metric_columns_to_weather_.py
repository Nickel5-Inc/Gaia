"""Add average metric columns to weather_forecast_stats

Revision ID: f83c6d760854
Revises: 8c00159f39c7
Create Date: 2025-08-14 16:35:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'f83c6d760854'
down_revision: Union[str, None] = '8c00159f39c7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add average RMSE, ACC, and skill score columns to weather_forecast_stats table."""
    
    # Add avg_rmse column
    op.add_column('weather_forecast_stats', 
        sa.Column('avg_rmse', sa.Float(), nullable=True, 
                  comment='Average RMSE across all variables and timesteps'))
    
    # Add avg_acc column
    op.add_column('weather_forecast_stats', 
        sa.Column('avg_acc', sa.Float(), nullable=True,
                  comment='Average ACC across all variables and timesteps'))
    
    # Add avg_skill_score column  
    op.add_column('weather_forecast_stats',
        sa.Column('avg_skill_score', sa.Float(), nullable=True,
                  comment='Average skill score across all variables and timesteps'))


def downgrade() -> None:
    """Remove average metric columns from weather_forecast_stats table."""
    
    op.drop_column('weather_forecast_stats', 'avg_skill_score')
    op.drop_column('weather_forecast_stats', 'avg_acc')
    op.drop_column('weather_forecast_stats', 'avg_rmse')