import os  # Add os import for environment variables
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

# --- Determine DB target from environment variable ---
db_target_env = os.environ.get("DB_TARGET")
db_connection_type = os.environ.get("DB_CONNECTION_TYPE", "tcp").lower() # Default to tcp

if db_target_env == "miner":
    logger.info("DB_TARGET is 'miner'. Configuring for miner database and schema.")
    db_url_key = "miner_db_socket_url" if db_connection_type == "socket" else "miner_db_url"
    target_metadata = MinerSpecificBase.metadata
    # Ensure the miner schema is initialized if it wasn\'t already
    if target_metadata is None: # Should not happen if MinerBase is defined correctly
        logger.error("MinerSpecificBase.metadata is None. Please check miner_schema.py")
        raise ValueError("MinerSpecificBase.metadata is None")
elif db_target_env == "validator":
    logger.info("DB_TARGET is 'validator'. Configuring for validator database and schema.")
    db_url_key = "validator_db_socket_url" if db_connection_type == "socket" else "validator_db_url"
    target_metadata = validator_metadata
    if target_metadata is None: # Should not happen
        logger.error("validator_metadata is None. Please check validator_schema.py")
        raise ValueError("validator_metadata is None")
else:
    # Default behavior or error if DB_TARGET is not set or invalid
    # For safety, defaulting to validator, or you can raise an error
    logger.warning(
        f"DB_TARGET environment variable is not set or is invalid ('{db_target_env}'). "
        f"Defaulting to 'validator' database (connection type: {db_connection_type}). "
        "Set DB_TARGET to 'miner' or 'validator' for explicit control."
    )
    # If you prefer to make it mandatory:
    # raise ValueError("DB_TARGET environment variable must be set to 'miner' or 'validator'")
    db_url_key = "validator_db_socket_url" if db_connection_type == "socket" else "validator_db_url" # Default to validator
    target_metadata = validator_metadata

# Get the actual URL string from the .ini file based on the determined key
specific_db_url = config.get_main_option(db_url_key)
if not specific_db_url:
    raise ValueError(f"Database URL for '{db_url_key}' not found in alembic.ini.")

# Set the 'sqlalchemy.url' dynamically for Alembic to use
config.set_main_option("sqlalchemy.url", specific_db_url)
logger.info(f"Alembic will use database URL: {specific_db_url} (from key: {db_url_key})")
# --- End DB target determination ---


# Interpret the config file for Python logging.
# This line sets up loggers basically.
# if config.config_file_name is not None: # Keep commented out to prevent hang
#     fileConfig(config.config_file_name)  # Keep commented out

# --- New combined metadata setup ---
# Create a new MetaData instance to hold all tables.
# combined_metadata = MetaData() # No longer using combined metadata for now

# Function to add tables from a given metadata object to the combined_metadata
# def include_metadata(source_metadata, target_metadata_obj):
#     if source_metadata is not None:
#         for table in source_metadata.tables.values():
#             table.tometadata(target_metadata_obj)

# Include tables from the validator schema
# include_metadata(validator_metadata, combined_metadata)

# Include tables from the miner schema
# (MinerSpecificBase.metadata contains the tables defined using MinerBase)
# include_metadata(MinerSpecificBase.metadata, combined_metadata)

# target_metadata = combined_metadata # Now set based on DB_TARGET
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
