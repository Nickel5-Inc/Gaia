"""add_retry_columns_to_predictions_tables

Revision ID: 9184456655c8
Revises: 3704fd24c76d
Create Date: 2025-06-16 23:12:47.783280

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9184456655c8'
down_revision: Union[str, None] = '3704fd24c76d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Helper function to check if column exists
    def column_exists(table_name, column_name):
        connection = op.get_bind()
        result = connection.execute(sa.text("""
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.columns 
                WHERE table_name = :table_name 
                AND column_name = :column_name
            )
        """), {"table_name": table_name, "column_name": column_name})
        return result.scalar()

    # Add retry columns to geomagnetic_predictions table
    retry_columns = [
        ('retry_count', sa.Column('retry_count', sa.Integer(), nullable=True, default=0)),
        ('next_retry_time', sa.Column('next_retry_time', sa.DateTime(timezone=True), nullable=True)),
        ('last_retry_attempt', sa.Column('last_retry_attempt', sa.DateTime(timezone=True), nullable=True)),
        ('retry_error_message', sa.Column('retry_error_message', sa.Text(), nullable=True))
    ]
    
    # Add columns to geomagnetic_predictions if they don't exist
    for column_name, column_def in retry_columns:
        if not column_exists('geomagnetic_predictions', column_name):
            op.add_column('geomagnetic_predictions', column_def)

    # Add columns to soil_moisture_predictions if they don't exist  
    for column_name, column_def in retry_columns:
        if not column_exists('soil_moisture_predictions', column_name):
            op.add_column('soil_moisture_predictions', column_def)


def downgrade() -> None:
    """Downgrade schema."""
    # Remove retry columns from soil_moisture_predictions table
    with op.batch_alter_table('soil_moisture_predictions', schema=None) as batch_op:
        batch_op.drop_column('retry_error_message')
        batch_op.drop_column('last_retry_attempt')
        batch_op.drop_column('next_retry_time')
        batch_op.drop_column('retry_count')

    # Remove retry columns from geomagnetic_predictions table
    with op.batch_alter_table('geomagnetic_predictions', schema=None) as batch_op:
        batch_op.drop_column('retry_error_message')
        batch_op.drop_column('last_retry_attempt')
        batch_op.drop_column('next_retry_time')
        batch_op.drop_column('retry_count')
