from fiber.logging_utils import get_logger

logger = get_logger(__name__)

# from sqlalchemy import engine_from_config # No longer needed
from sqlalchemy import pool
from sqlalchemy import create_engine # Changed from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
import sqlalchemy as sa # Added
from sqlalchemy.schema import MetaData # Import MetaData for combining

from alembic import context

# Import the MetaData object from your validator schema file
from gaia.database.validator_schema import validator_metadata

# Import the Base for miner-specific tables
from gaia.database.miner_schema import MinerBase as MinerSpecificBase

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
# if config.config_file_name is not None: # Keep commented out to prevent hang
#     fileConfig(config.config_file_name)  # Keep commented out

# --- New combined metadata setup ---
# Create a new MetaData instance to hold all tables.
combined_metadata = MetaData()

# Function to add tables from a given metadata object to the combined_metadata
def include_metadata(source_metadata, target_metadata_obj):
    if source_metadata is not None:
        for table in source_metadata.tables.values():
            table.tometadata(target_metadata_obj)

# Include tables from the validator schema
include_metadata(validator_metadata, combined_metadata)

# Include tables from the miner schema
# (MinerSpecificBase.metadata contains the tables defined using MinerBase)
include_metadata(MinerSpecificBase.metadata, combined_metadata)

target_metadata = combined_metadata
# --- End of new combined metadata setup ---

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_as_batch=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection):
    context.configure(
        connection=connection, 
        target_metadata=target_metadata,
        render_as_batch=True, # Added render_as_batch=True for SQLite compatibility if needed, and general good practice
    )

    with context.begin_transaction():
        context.run_migrations()

# Changed from async def to def
def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    
    # Get the database URL from alembic.ini
    db_url = config.get_main_option("sqlalchemy.url")

    # Create a synchronous engine using the database URL
    # Use pool.NullPool as recommended for Alembic's short-lived script access
    connectable = create_engine(db_url, poolclass=pool.NullPool) # Changed to synchronous create_engine

    # Connect and run migrations
    with connectable.connect() as connection: # Synchronous connection
        # The do_run_migrations function will configure the context
        # with this connection and run migrations
        do_run_migrations(connection) # Direct synchronous call

    # Synchronous engines often don't require explicit disposal with NullPool,
    # but if issues arise or a different pool class is used, `connectable.dispose()` might be needed.

if context.is_offline_mode():
    run_migrations_offline()
else:
    # Removed asyncio.run, directly call the synchronous function
    run_migrations_online()
