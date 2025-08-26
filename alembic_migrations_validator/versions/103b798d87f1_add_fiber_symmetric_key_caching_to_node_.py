"""Add Fiber symmetric key caching to node_table

Revision ID: 103b798d87f1
Revises: aeb6e0fada34
Create Date: 2025-08-14 15:53:30.953695

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '103b798d87f1'
down_revision: Union[str, None] = 'aeb6e0fada34'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add Fiber symmetric key caching columns to node_table."""
    # Add columns for caching Fiber symmetric keys
    op.add_column('node_table', sa.Column('fiber_symmetric_key', sa.Text(), nullable=True, comment='Cached Fiber symmetric key for encrypted communication'))
    op.add_column('node_table', sa.Column('fiber_symmetric_key_uuid', sa.Text(), nullable=True, comment='UUID for the cached symmetric key'))
    op.add_column('node_table', sa.Column('fiber_key_cached_at', sa.DateTime(timezone=True), nullable=True, comment='When the symmetric key was cached'))
    op.add_column('node_table', sa.Column('fiber_key_expires_at', sa.DateTime(timezone=True), nullable=True, comment='When the cached symmetric key expires'))
    
    # Create index for faster lookups by expiration
    op.create_index('idx_node_fiber_key_expires', 'node_table', ['fiber_key_expires_at'], unique=False)


def downgrade() -> None:
    """Remove Fiber symmetric key caching columns from node_table."""
    op.drop_index('idx_node_fiber_key_expires', table_name='node_table')
    op.drop_column('node_table', 'fiber_key_expires_at')
    op.drop_column('node_table', 'fiber_key_cached_at')
    op.drop_column('node_table', 'fiber_symmetric_key_uuid')
    op.drop_column('node_table', 'fiber_symmetric_key')
