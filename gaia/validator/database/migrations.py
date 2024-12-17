from datetime import datetime
from typing import List, Dict, Any
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

# List of migrations in order of application
MIGRATIONS = [
    {
        "version": "1.0.0",
        "description": "Initial schema creation",
        "up": """
            CREATE TABLE IF NOT EXISTS migrations (
                id SERIAL PRIMARY KEY,
                version VARCHAR(50) NOT NULL,
                description TEXT,
                applied_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """,
        "down": """
            DROP TABLE IF EXISTS migrations;
        """
    },
    {
        "version": "1.0.1",
        "description": "Add indexes to node_table",
        "up": """
            CREATE INDEX IF NOT EXISTS idx_node_hotkey ON node_table(hotkey);
            CREATE INDEX IF NOT EXISTS idx_node_last_updated ON node_table(last_updated);
        """,
        "down": """
            DROP INDEX IF EXISTS idx_node_hotkey;
            DROP INDEX IF EXISTS idx_node_last_updated;
        """
    },
    {
        "version": "1.0.2",
        "description": "Add indexes to score_table",
        "up": """
            CREATE INDEX IF NOT EXISTS idx_score_task ON score_table(task_name, task_id);
            CREATE INDEX IF NOT EXISTS idx_score_status ON score_table(status);
            CREATE INDEX IF NOT EXISTS idx_score_created_at ON score_table(created_at);
        """,
        "down": """
            DROP INDEX IF EXISTS idx_score_task;
            DROP INDEX IF EXISTS idx_score_status;
            DROP INDEX IF EXISTS idx_score_created_at;
        """
    },
    {
        "version": "1.0.3",
        "description": "Add indexes to task_state table",
        "up": """
            CREATE INDEX IF NOT EXISTS idx_task_state_flow ON task_state(flow_name);
            CREATE INDEX IF NOT EXISTS idx_task_state_status ON task_state(status);
            CREATE INDEX IF NOT EXISTS idx_task_state_updated ON task_state(updated_at);
        """,
        "down": """
            DROP INDEX IF EXISTS idx_task_state_flow;
            DROP INDEX IF EXISTS idx_task_state_status;
            DROP INDEX IF EXISTS idx_task_state_updated;
        """
    },
    {
        "version": "1.0.4",
        "description": "Migrate work persistence data to task_state",
        "up": """
            -- First ensure work_persistence table exists to avoid errors
            CREATE TABLE IF NOT EXISTS work_persistence (
                task_id SERIAL PRIMARY KEY,
                flow_name VARCHAR(255) NOT NULL,
                task_name VARCHAR(255) NOT NULL,
                status VARCHAR(50) NOT NULL,
                error_message TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                metadata JSONB
            );

            -- Migrate data from work_persistence to task_state if it exists
            INSERT INTO task_state (flow_name, task_name, status, error_message, metadata, created_at, updated_at)
            SELECT flow_name, task_name, status, error_message, metadata, created_at, updated_at
            FROM work_persistence
            ON CONFLICT DO NOTHING;

            -- Drop the old table after migration
            DROP TABLE IF EXISTS work_persistence;
        """,
        "down": """
            -- Recreate work_persistence table
            CREATE TABLE IF NOT EXISTS work_persistence (
                task_id SERIAL PRIMARY KEY,
                flow_name VARCHAR(255) NOT NULL,
                task_name VARCHAR(255) NOT NULL,
                status VARCHAR(50) NOT NULL,
                error_message TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                metadata JSONB
            );

            -- Migrate data back from task_state
            INSERT INTO work_persistence (flow_name, task_name, status, error_message, metadata, created_at, updated_at)
            SELECT flow_name, task_name, status, error_message, metadata, created_at, updated_at
            FROM task_state;
        """
    },
    {
        "version": "1.1.0",
        "description": "Add geomagnetic task tables",
        "up": """
            -- Create geomagnetic predictions table
            CREATE TABLE IF NOT EXISTS geomagnetic_predictions (
                id UUID PRIMARY KEY,
                miner_uid VARCHAR(255) NOT NULL,
                miner_hotkey VARCHAR(255) NOT NULL,
                predicted_value FLOAT NOT NULL,
                query_time TIMESTAMP WITH TIME ZONE NOT NULL,
                status VARCHAR(50) DEFAULT 'pending',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT fk_miner_hotkey FOREIGN KEY (miner_hotkey) REFERENCES node_table (hotkey)
            );

            -- Create geomagnetic history table
            CREATE TABLE IF NOT EXISTS geomagnetic_history (
                id SERIAL PRIMARY KEY,
                miner_uid VARCHAR(255) NOT NULL,
                miner_hotkey VARCHAR(255) NOT NULL,
                query_time TIMESTAMP WITH TIME ZONE NOT NULL,
                predicted_value FLOAT NOT NULL,
                ground_truth_value FLOAT NOT NULL,
                score FLOAT NOT NULL,
                scored_at TIMESTAMP WITH TIME ZONE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );

            -- Create geomagnetic scores table
            CREATE TABLE IF NOT EXISTS geomagnetic_scores (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                scores FLOAT[] NOT NULL,
                mean_score FLOAT,
                std_score FLOAT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );

            -- Add indexes
            CREATE INDEX IF NOT EXISTS idx_geomag_pred_status ON geomagnetic_predictions(status);
            CREATE INDEX IF NOT EXISTS idx_geomag_pred_query_time ON geomagnetic_predictions(query_time);
            CREATE INDEX IF NOT EXISTS idx_geomag_hist_query_time ON geomagnetic_history(query_time);
            CREATE INDEX IF NOT EXISTS idx_geomag_hist_scored_at ON geomagnetic_history(scored_at);
            CREATE INDEX IF NOT EXISTS idx_geomag_scores_timestamp ON geomagnetic_scores(timestamp);
        """,
        "down": """
            DROP TABLE IF EXISTS geomagnetic_scores;
            DROP TABLE IF EXISTS geomagnetic_history;
            DROP TABLE IF EXISTS geomagnetic_predictions;
        """
    }
]

async def get_applied_migrations(session: AsyncSession) -> List[str]:
    """Get list of already applied migration versions."""
    try:
        result = await session.execute(
            text("SELECT version FROM migrations ORDER BY applied_at")
        )
        return [row[0] for row in result.fetchall()]
    except Exception:
        return []

async def apply_migration(session: AsyncSession, migration: Dict[str, Any]) -> bool:
    """Apply a single migration."""
    try:
        # Apply the migration
        await session.execute(text(migration["up"]))
        
        # Record the migration
        await session.execute(
            text("""
                INSERT INTO migrations (version, description)
                VALUES (:version, :description)
            """),
            {
                "version": migration["version"],
                "description": migration["description"]
            }
        )
        
        await session.commit()
        logger.info(f"Applied migration {migration['version']}: {migration['description']}")
        return True
    except Exception as e:
        await session.rollback()
        logger.error(f"Error applying migration {migration['version']}: {e}")
        return False

async def run_migrations(session: AsyncSession) -> bool:
    """Run all pending migrations in order."""
    try:
        # Get applied migrations
        applied = await get_applied_migrations(session)
        
        # Apply pending migrations in order
        for migration in MIGRATIONS:
            if migration["version"] not in applied:
                success = await apply_migration(session, migration)
                if not success:
                    return False
        return True
    except Exception as e:
        logger.error(f"Error running migrations: {e}")
        return False

async def get_migration_history(session: AsyncSession) -> List[Dict[str, Any]]:
    """Get history of applied migrations."""
    try:
        result = await session.execute(
            text("""
                SELECT version, description, applied_at
                FROM migrations
                ORDER BY applied_at
            """)
        )
        return [
            {
                "version": row[0],
                "description": row[1],
                "applied_at": row[2]
            }
            for row in result.fetchall()
        ]
    except Exception as e:
        logger.error(f"Error getting migration history: {e}")
        return [] 