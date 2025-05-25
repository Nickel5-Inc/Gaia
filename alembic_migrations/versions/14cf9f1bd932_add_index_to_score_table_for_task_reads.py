"""add_index_to_score_table_for_task_reads

Revision ID: 14cf9f1bd932
Revises: e7319ba352ac
Create Date: 2025-05-24 14:45:49.315909

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '14cf9f1bd932'
down_revision: Union[str, None] = 'e7319ba352ac'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index('idx_score_table_task_name_created_at_desc', 'score_table', ['task_name', sa.text('created_at DESC')])


def downgrade() -> None:
    op.drop_index('idx_score_table_task_name_created_at_desc', table_name='score_table')

