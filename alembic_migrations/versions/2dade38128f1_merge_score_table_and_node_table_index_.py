"""merge_score_table_and_node_table_index_branches

Revision ID: 2dade38128f1
Revises: 2b1ae3c9f0d8, 615ffca88dc8
Create Date: 2025-05-25 20:14:11.885002

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2dade38128f1'
down_revision: Union[str, None] = ('2b1ae3c9f0d8', '615ffca88dc8')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
