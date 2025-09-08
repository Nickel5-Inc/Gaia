"""Add weather_forecast_component_scores table for detailed metrics

Revision ID: 8c00159f39c7
Revises: 103b798d87f1
Create Date: 2025-08-14 16:15:40.674587

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '8c00159f39c7'
down_revision: Union[str, None] = '103b798d87f1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create weather_forecast_component_scores table for detailed transparency."""
    
    # Create the table
    op.create_table(
        'weather_forecast_component_scores',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        
        # Foreign Keys
        sa.Column('run_id', sa.Integer(), nullable=False, comment='Reference to the forecast run'),
        sa.Column('response_id', sa.Integer(), nullable=False, comment='Reference to the miner response'),
        sa.Column('miner_uid', sa.Integer(), nullable=False, comment="Miner's UID (0-255)"),
        sa.Column('miner_hotkey', sa.VARCHAR(length=255), nullable=False, comment="Miner's hotkey"),
        
        # Scoring context
        sa.Column('score_type', sa.VARCHAR(length=50), nullable=False, comment="Type of scoring: 'day1' or 'era5'"),
        sa.Column('lead_hours', sa.Integer(), nullable=False, comment='Forecast lead time in hours (6, 12, 24, 48, 72, etc.)'),
        sa.Column('valid_time_utc', postgresql.TIMESTAMP(timezone=True), nullable=False, comment='Valid time for this forecast'),
        
        # Variable identification
        sa.Column('variable_name', sa.VARCHAR(length=20), nullable=False, comment="Variable name: '2t', 'msl', 't', 'u', 'v', 'q', 'z', '10u', '10v'"),
        sa.Column('pressure_level', sa.Integer(), nullable=True, comment='Pressure level in hPa (NULL for surface variables, e.g., 500, 850)'),
        
        # Component Metrics
        sa.Column('rmse', sa.Float(), nullable=True, comment='Root Mean Square Error'),
        sa.Column('mse', sa.Float(), nullable=True, comment='Mean Square Error'),
        sa.Column('acc', sa.Float(), nullable=True, comment='Anomaly Correlation Coefficient (-1 to 1)'),
        sa.Column('skill_score', sa.Float(), nullable=True, comment='Skill score vs reference forecast (typically 0 to 1, can be negative)'),
        sa.Column('skill_score_gfs', sa.Float(), nullable=True, comment='Skill score specifically vs GFS forecast'),
        sa.Column('skill_score_climatology', sa.Float(), nullable=True, comment='Skill score vs climatology'),
        sa.Column('bias', sa.Float(), nullable=True, comment='Mean bias of forecast'),
        sa.Column('mae', sa.Float(), nullable=True, comment='Mean Absolute Error'),
        
        # Sanity check results
        sa.Column('climatology_check_passed', sa.Boolean(), nullable=True, comment='Whether forecast passed climatology bounds check'),
        sa.Column('pattern_correlation', sa.Float(), nullable=True, comment='Pattern correlation with reference'),
        sa.Column('pattern_correlation_passed', sa.Boolean(), nullable=True, comment='Whether pattern correlation exceeded threshold'),
        
        # Penalties applied
        sa.Column('clone_penalty', sa.Float(), server_default=sa.text('0'), nullable=True, comment='Penalty applied for clone detection'),
        sa.Column('quality_penalty', sa.Float(), server_default=sa.text('0'), nullable=True, comment='Penalty for quality issues'),
        
        # Final weighted contribution
        sa.Column('weighted_score', sa.Float(), nullable=True, comment="This variable's weighted contribution to final score"),
        sa.Column('variable_weight', sa.Float(), nullable=True, comment='Weight used for this variable in aggregation'),
        
        # Metadata
        sa.Column('calculated_at', postgresql.TIMESTAMP(timezone=True), server_default=sa.func.current_timestamp(), nullable=False, comment='When this metric was calculated'),
        sa.Column('calculation_duration_ms', sa.Integer(), nullable=True, comment='How long this metric calculation took in milliseconds'),
        
        # Constraints
        sa.ForeignKeyConstraint(['miner_uid'], ['node_table.uid'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['response_id'], ['weather_miner_responses.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['run_id'], ['weather_forecast_runs.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('response_id', 'score_type', 'lead_hours', 'variable_name', 'pressure_level', name='uq_wfcs_response_variable_lead'),
        comment='Stores detailed component scores for each variable and lead time in weather forecasts'
    )
    
    # Create indexes for efficient querying
    op.create_index('idx_wfcs_calculated_at', 'weather_forecast_component_scores', ['calculated_at'], unique=False)
    op.create_index('idx_wfcs_miner_hotkey', 'weather_forecast_component_scores', ['miner_hotkey'], unique=False)
    op.create_index('idx_wfcs_miner_uid', 'weather_forecast_component_scores', ['miner_uid'], unique=False)
    op.create_index('idx_wfcs_run_id', 'weather_forecast_component_scores', ['run_id'], unique=False)
    op.create_index('idx_wfcs_score_type_lead', 'weather_forecast_component_scores', ['score_type', 'lead_hours'], unique=False)
    op.create_index('idx_wfcs_valid_time', 'weather_forecast_component_scores', ['valid_time_utc'], unique=False)
    op.create_index('idx_wfcs_variable', 'weather_forecast_component_scores', ['variable_name', 'pressure_level'], unique=False)


def downgrade() -> None:
    """Drop weather_forecast_component_scores table."""
    
    # Drop indexes
    op.drop_index('idx_wfcs_variable', table_name='weather_forecast_component_scores')
    op.drop_index('idx_wfcs_valid_time', table_name='weather_forecast_component_scores')
    op.drop_index('idx_wfcs_score_type_lead', table_name='weather_forecast_component_scores')
    op.drop_index('idx_wfcs_run_id', table_name='weather_forecast_component_scores')
    op.drop_index('idx_wfcs_miner_uid', table_name='weather_forecast_component_scores')
    op.drop_index('idx_wfcs_miner_hotkey', table_name='weather_forecast_component_scores')
    op.drop_index('idx_wfcs_calculated_at', table_name='weather_forecast_component_scores')
    
    # Drop table
    op.drop_table('weather_forecast_component_scores')