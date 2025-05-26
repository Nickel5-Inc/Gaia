"""add_status_created_at_idx_to_score_table

Revision ID: 2b1ae3c9f0d8
Revises: 14cf9f1bd932
Create Date: 2025-05-26 00:00:00.000000 

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2b1ae3c9f0d8'
down_revision: Union[str, None] = '14cf9f1bd932' # Previous score_table related migration
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add index on status and created_at to score_table."""
    op.create_index(
        'idx_score_table_status_created_at',
        'score_table',
        ['status', 'created_at'],
        unique=False
    )


def downgrade() -> None:
    """Remove index on status and created_at from score_table."""
    op.drop_index('idx_score_table_status_created_at', table_name='score_table') 