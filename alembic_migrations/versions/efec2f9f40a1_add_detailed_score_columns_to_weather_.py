"""add_detailed_score_columns_to_weather_miner_scores

Revision ID: efec2f9f40a1
Revises: 45fb64e1ae3c
Create Date: 2025-05-26 15:04:42.031081

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = 'efec2f9f40a1'
down_revision: Union[str, None] = '45fb64e1ae3c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('weather_miner_scores', sa.Column('lead_hours', sa.Integer(), nullable=True))
    op.add_column('weather_miner_scores', sa.Column('variable_level', sa.String(length=50), nullable=True))
    op.add_column('weather_miner_scores', sa.Column('valid_time_utc', sa.TIMESTAMP(timezone=True), nullable=True))

    try:
        op.drop_constraint('uq_weather_miner_scores_response_id_score_type', 'weather_miner_scores', type_='unique')
        print("Dropped old constraint: uq_weather_miner_scores_response_id_score_type")
    except Exception as e:
        print(f"Could not drop constraint uq_weather_miner_scores_response_id_score_type (it might not exist or name is different): {e}")


    op.create_unique_constraint(
        'uq_wms_response_scoretype_lead_var_time',
        'weather_miner_scores',
        ['response_id', 'score_type', 'lead_hours', 'variable_level', 'valid_time_utc']
    )

    op.create_index(op.f('idx_wms_lead_hours'), 'weather_miner_scores', ['lead_hours'], unique=False)
    op.create_index(op.f('idx_wms_variable_level'), 'weather_miner_scores', ['variable_level'], unique=False)
    op.create_index(op.f('idx_wms_valid_time_utc'), 'weather_miner_scores', ['valid_time_utc'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f('idx_wms_valid_time_utc'), table_name='weather_miner_scores')
    op.drop_index(op.f('idx_wms_variable_level'), table_name='weather_miner_scores')
    op.drop_index(op.f('idx_wms_lead_hours'), table_name='weather_miner_scores')
    
    op.drop_constraint('uq_wms_response_scoretype_lead_var_time', 'weather_miner_scores', type_='unique')
    try:
        op.create_unique_constraint('uq_weather_miner_scores_response_id_score_type', 'weather_miner_scores', ['response_id', 'score_type'])
        print("Recreated old constraint: uq_weather_miner_scores_response_id_score_type")
    except Exception as e:
        print(f"Could not recreate constraint uq_weather_miner_scores_response_id_score_type during downgrade (it might already exist or other issue): {e}")


    op.drop_column('weather_miner_scores', 'valid_time_utc')
    op.drop_column('weather_miner_scores', 'variable_level')
    op.drop_column('weather_miner_scores', 'lead_hours')
