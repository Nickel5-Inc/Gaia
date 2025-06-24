"""add_unique_constraint_soil_moisture_history

Revision ID: a1b2c3d4e5f6
Revises: 9184456655c8
Create Date: 2025-06-24 17:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, None] = '9184456655c8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add unique constraint to soil_moisture_history table."""
    
    # Helper function to check if constraint exists
    def constraint_exists(table_name, constraint_name):
        connection = op.get_bind()
        result = connection.execute(sa.text("""
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.table_constraints 
                WHERE table_name = :table_name 
                AND constraint_name = :constraint_name
                AND constraint_type = 'UNIQUE'
            )
        """), {"table_name": table_name, "constraint_name": constraint_name})
        return result.scalar()
    
    # Check for and remove duplicate records before adding constraint
    connection = op.get_bind()
    
    # Find and log duplicates
    duplicate_check = connection.execute(sa.text("""
        SELECT region_id, miner_uid, target_time, COUNT(*) as count
        FROM soil_moisture_history
        GROUP BY region_id, miner_uid, target_time
        HAVING COUNT(*) > 1
        ORDER BY count DESC
    """))
    duplicates = duplicate_check.fetchall()
    
    if duplicates:
        print(f"Found {len(duplicates)} sets of duplicate records in soil_moisture_history")
        
        # Remove duplicates, keeping the most recent record (highest id)
        connection.execute(sa.text("""
            DELETE FROM soil_moisture_history
            WHERE id NOT IN (
                SELECT MAX(id)
                FROM soil_moisture_history
                GROUP BY region_id, miner_uid, target_time
            )
        """))
        print("Removed duplicate records from soil_moisture_history")
    
    # Add the unique constraint if it doesn't exist
    if not constraint_exists('soil_moisture_history', 'uq_smh_region_miner_target_time'):
        with op.batch_alter_table('soil_moisture_history', schema=None) as batch_op:
            batch_op.create_unique_constraint(
                'uq_smh_region_miner_target_time',
                ['region_id', 'miner_uid', 'target_time']
            )
        print("Added unique constraint uq_smh_region_miner_target_time to soil_moisture_history")
    else:
        print("Unique constraint uq_smh_region_miner_target_time already exists")


def downgrade() -> None:
    """Remove unique constraint from soil_moisture_history table."""
    with op.batch_alter_table('soil_moisture_history', schema=None) as batch_op:
        batch_op.drop_constraint('uq_smh_region_miner_target_time', type_='unique') 