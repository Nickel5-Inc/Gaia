"""squashed_initial_miner_schema

Revision ID: 79b314575524
Revises: 
Create Date: 2025-05-28 00:11:57.113495

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.dialects import postgresql

# Assuming gaia.database.miner_schema.MinerBase can be imported by Alembic
# This might require PYTHONPATH adjustments or sys.path manipulations in env.py
from gaia.database.miner_schema import MinerBase


# revision identifiers, used by Alembic.
revision: str = '79b314575524'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    inspector = Inspector.from_engine(bind)
    
    # 1. Drop the alembic_version table to reset Alembic history for this DB.
    print("Attempting to drop existing alembic_version table to reset migration history...")
    op.execute("DROP TABLE IF EXISTS alembic_version CASCADE;")
    print("Dropped alembic_version table (if it existed).")
    
    # 2. Explicitly re-create the alembic_version table structure that Alembic expects.
    # This is crucial so Alembic can record the current migration successfully.
    op.create_table(
        'alembic_version',
        sa.Column('version_num', sa.String(length=32), nullable=False),
        sa.PrimaryKeyConstraint('version_num')
    )
    print("Re-created empty alembic_version table structure.")
    
    # 3. Get all table names from the database (alembic_version should now exist and be empty)
    db_tables = inspector.get_table_names()
    
    # 4. Get all table names defined in MinerBase.metadata
    metadata_table_names = set(MinerBase.metadata.tables.keys())
    
    # 5. Tables to drop are those in db_tables but not in metadata_table_names,
    #    and not 'alembic_version' itself (though it was just re-created empty).
    tables_to_drop = [
        name for name in db_tables 
        if name not in metadata_table_names and name != 'alembic_version' 
    ]
    
    for table_name in tables_to_drop:
        op.drop_table(table_name)
        print(f"Dropped table '{table_name}' as it's not in MinerBase.metadata.")

    # 6. Create all application tables defined in MinerBase.metadata.
    MinerBase.metadata.create_all(bind=bind)
    print(f"Ensured all tables from MinerBase.metadata are created.")
    # After this function, Alembic will INSERT this migration's revision into the (now existing) alembic_version table.


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    # Drop application tables
    MinerBase.metadata.drop_all(bind=bind)
    print(f"Dropped all tables from MinerBase.metadata.")

    # For a full reset upon downgrade, the alembic_version table would also need to be dropped.
    # However, doing it here might interfere with Alembic's ability to manage further downgrades if any existed.
    # If this is the ONLY migration, dropping it is fine.
    print("Attempting to drop alembic_version table during downgrade...")
    op.execute("DROP TABLE IF EXISTS alembic_version CASCADE;")
    print("Dropped alembic_version table (if it existed) during downgrade.")
