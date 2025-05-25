"""add_node_table_uid_index_for_updates

Revision ID: 615ffca88dc8
Revises: 14cf9f1bd932
Create Date: 2025-05-24 16:00:55.089089

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '615ffca88dc8'
down_revision: Union[str, None] = '14cf9f1bd932'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index('idx_node_table_uid', 'node_table', ['uid'])
    op.create_index('idx_node_table_uid_last_updated', 'node_table', ['uid', 'last_updated'])


def downgrade() -> None:
    op.drop_index('idx_node_table_uid_last_updated', table_name='node_table')
    op.drop_index('idx_node_table_uid', table_name='node_table')
