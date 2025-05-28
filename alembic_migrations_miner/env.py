import os
from sqlalchemy import pool, create_engine
from alembic import context

from fiber.logging_utils import get_logger
logger = get_logger(__name__)

from gaia.database.miner_schema import MinerBase
target_metadata = MinerBase.metadata

config = context.config

# Construct DB URL for miner
db_connection_type = os.getenv("DB_CONNECTION_TYPE", "host")

if db_connection_type == "socket":
    db_name = os.getenv("POSTGRES_DB", "miner_db")
    db_user = os.getenv("POSTGRES_USER", "postgres")
    db_password = os.getenv("POSTGRES_PASSWORD", "postgres")
    db_url = f"postgresql+psycopg2://{db_user}:{db_password}@/{db_name}?host=/var/run/postgresql"
    logger.info("Using socket-based DB connection for miner.")
else:
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_user = os.getenv("POSTGRES_USER", "postgres")
    db_password = os.getenv("POSTGRES_PASSWORD", "postgres")
    db_name = os.getenv("POSTGRES_DB", "miner_db")
    db_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    logger.info("Using host-based DB connection for miner.")

config.set_main_option("sqlalchemy.url", db_url)
logger.info(f"Miner Alembic will use database URL: {db_url}")

def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_as_batch=True,
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()

def do_run_migrations(connection):
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        render_as_batch=True,
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online() -> None:
    db_url_to_connect = config.get_main_option("sqlalchemy.url")
    if not db_url_to_connect:
        logger.error("Database URL is not configured in run_migrations_online.")
        raise ValueError("Database URL is not configured.")
        
    connectable = create_engine(db_url_to_connect, poolclass=pool.NullPool)
    with connectable.connect() as connection:
        do_run_migrations(connection)

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online() 