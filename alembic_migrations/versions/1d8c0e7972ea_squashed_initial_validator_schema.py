"""squashed_initial_validator_schema

Revision ID: 1d8c0e7972ea
Revises: 79b314575524
Create Date: 2025-05-27 22:34:47.909874

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector
# Import the MetaData object directly for validator schema
from gaia.database.validator_schema import validator_metadata


# revision identifiers, used by Alembic.
revision: str = '1d8c0e7972ea'
down_revision: Union[str, None] = None # Set to None as this is the new root for validator_db
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema for validator_db."""
    bind = op.get_bind()
    inspector = Inspector.from_engine(bind)

    # 1. Drop the alembic_version table to reset Alembic history for this DB.
    print("[Validator Schema] Attempting to drop existing alembic_version table to reset migration history...")
    op.execute("DROP TABLE IF EXISTS alembic_version CASCADE;")
    print("[Validator Schema] Dropped alembic_version table (if it existed).")

    # 2. Explicitly re-create the alembic_version table structure that Alembic expects.
    op.create_table(
        'alembic_version',
        sa.Column('version_num', sa.String(length=32), nullable=False),
        sa.PrimaryKeyConstraint('version_num')
    )
    print("[Validator Schema] Re-created empty alembic_version table structure.")
    
    # 3. Get all table names from the database (alembic_version should now exist and be empty)
    db_tables = inspector.get_table_names()
    
    # 4. Get all table names defined in validator_metadata (SQLAlchemy Core style)
    metadata_table_names = set(validator_metadata.tables.keys())
    
    # 5. Tables to drop are those in db_tables but not in metadata_table_names,
    #    and not 'alembic_version' itself.
    tables_to_drop = [
        name for name in db_tables 
        if name not in metadata_table_names and name != 'alembic_version' 
    ]
    
    for table_name in tables_to_drop:
        op.drop_table(table_name)
        print(f"[Validator Schema] Dropped table '{table_name}' as it's not in validator_metadata.")

    # 6. Create all application tables defined in validator_metadata.
    validator_metadata.create_all(bind=bind)
    print(f"[Validator Schema] Ensured all tables from validator_metadata are created.")
    # Alembic will INSERT this migration's revision into alembic_version after this.


def downgrade() -> None:
    """Downgrade schema for validator_db."""
    bind = op.get_bind()
    # Drop application tables
    validator_metadata.drop_all(bind=bind)
    print(f"[Validator Schema] Dropped all tables from validator_metadata.")

    # Drop the alembic_version table for a full reset on downgrade.
    print("[Validator Schema] Attempting to drop alembic_version table during downgrade...")
    op.execute("DROP TABLE IF EXISTS alembic_version CASCADE;")
    print("[Validator Schema] Dropped alembic_version table (if it existed) during downgrade.")
