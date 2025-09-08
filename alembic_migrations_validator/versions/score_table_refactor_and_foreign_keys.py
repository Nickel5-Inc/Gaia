"""Score table refactor and add foreign keys

Revision ID: score_table_refactor
Revises: f75e4f7343a1
Create Date: 2024-12-19

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import ProgrammingError

# revision identifiers, used by Alembic.
revision: str = "score_table_refactor"
down_revision: Union[str, None] = "8d832ba6c04d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    1. Create new score_table with individual UID columns
    2. Migrate data from old array format to new columns
    3. Add foreign key constraints with CASCADE delete
    4. Update data types for miner_uid columns
    """

    # Step 1: Create new score_table with individual columns
    print("Creating new score_table with individual UID columns...")

    # Build column list
    columns = [
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("task_name", sa.VARCHAR(255), nullable=True),
        sa.Column("task_id", sa.Text(), nullable=True),
    ]

    # Add individual score columns for each UID
    for uid in range(256):
        columns.append(sa.Column(f"uid_{uid}_score", sa.Float(), nullable=True))

    columns.extend(
        [
            sa.Column(
                "created_at",
                postgresql.TIMESTAMP(timezone=True),
                server_default=sa.text("CURRENT_TIMESTAMP"),
                nullable=False,
            ),
            sa.Column(
                "status",
                sa.VARCHAR(50),
                server_default=sa.text("'pending'"),
                nullable=True,
            ),
        ]
    )

    # Create new table
    op.create_table(
        "score_table_new",
        *columns,
        sa.UniqueConstraint(
            "task_name", "task_id", name="uq_score_table_new_task_name_task_id"
        ),
    )

    # Step 2: Migrate data from old score_table to new format
    print("Migrating data from array format to individual columns...")

    # Use raw SQL for efficient data migration
    migrate_sql = (
        """
    INSERT INTO score_table_new (
        task_name, task_id, created_at, status,
        """
        + ", ".join([f"uid_{i}_score" for i in range(256)])
        + """
    )
    SELECT 
        task_name, task_id, created_at, status,
        """
        + ", ".join([f"score[{i+1}]" for i in range(256)])
        + """
    FROM score_table;
    """
    )

    op.execute(migrate_sql)

    # Step 3: Drop old table and rename new one
    print("Replacing old score_table with new structure...")
    op.drop_table("score_table")
    op.rename_table("score_table_new", "score_table")

    # Recreate indexes
    op.create_index(
        "idx_score_created_at_on_score_table", "score_table", ["created_at"]
    )
    op.create_index(
        "idx_score_task_name_created_at_desc_on_score_table",
        "score_table",
        ["task_name", sa.text("created_at DESC")],
    )

    # Step 4: Update miner_uid columns from Text to Integer with foreign keys
    print("Updating miner_uid columns to Integer type with foreign keys...")

    # Weather tables
    print("  - Updating weather_miner_responses...")
    # First, check if unique constraint exists and drop it
    connection = op.get_bind()
    result = connection.execute(
        sa.text(
            """
        SELECT COUNT(*) FROM pg_constraint 
        WHERE conname = 'uq_weather_miner_responses_run_miner'
    """
        )
    )
    if result.scalar() > 0:
        op.drop_constraint(
            "uq_weather_miner_responses_run_miner", "weather_miner_responses"
        )

    # Convert miner_uid from Integer to Integer with FK (it's already Integer)
    op.create_foreign_key(
        "fk_weather_miner_responses_miner_uid",
        "weather_miner_responses",
        "node_table",
        ["miner_uid"],
        ["uid"],
        ondelete="CASCADE",
    )

    # Recreate the unique constraint
    op.create_unique_constraint(
        "uq_weather_miner_responses_run_miner",
        "weather_miner_responses",
        ["run_id", "miner_uid"],
    )

    print("  - Updating weather_miner_scores...")
    op.create_foreign_key(
        "fk_weather_miner_scores_miner_uid",
        "weather_miner_scores",
        "node_table",
        ["miner_uid"],
        ["uid"],
        ondelete="CASCADE",
    )

    # Soil moisture tables - need to convert from Text to Integer
    print("  - Updating soil_moisture_predictions...")
    # Add new column
    op.add_column(
        "soil_moisture_predictions",
        sa.Column("miner_uid_new", sa.Integer(), nullable=True),
    )

    # Copy and convert data
    op.execute(
        """
        UPDATE soil_moisture_predictions 
        SET miner_uid_new = CAST(miner_uid AS INTEGER)
        WHERE miner_uid ~ '^[0-9]+$'
    """
    )

    # Drop old column and rename new one
    op.drop_column("soil_moisture_predictions", "miner_uid")
    op.alter_column(
        "soil_moisture_predictions",
        "miner_uid_new",
        new_column_name="miner_uid",
        nullable=False,
    )

    # Add foreign key
    op.create_foreign_key(
        "fk_soil_moisture_predictions_miner_uid",
        "soil_moisture_predictions",
        "node_table",
        ["miner_uid"],
        ["uid"],
        ondelete="CASCADE",
    )

    print("  - Updating soil_moisture_history...")
    # Add new column
    op.add_column(
        "soil_moisture_history", sa.Column("miner_uid_new", sa.Integer(), nullable=True)
    )

    # Copy and convert data
    op.execute(
        """
        UPDATE soil_moisture_history 
        SET miner_uid_new = CAST(miner_uid AS INTEGER)
        WHERE miner_uid ~ '^[0-9]+$'
    """
    )

    # Drop old unique constraint first (if it exists)
    result = connection.execute(
        sa.text(
            """
        SELECT COUNT(*) FROM pg_constraint 
        WHERE conname = 'uq_smh_region_miner_target_time'
    """
        )
    )
    if result.scalar() > 0:
        op.drop_constraint("uq_smh_region_miner_target_time", "soil_moisture_history")

    # Drop old column and rename new one
    op.drop_column("soil_moisture_history", "miner_uid")
    op.alter_column(
        "soil_moisture_history",
        "miner_uid_new",
        new_column_name="miner_uid",
        nullable=False,
    )

    # Add foreign key
    op.create_foreign_key(
        "fk_soil_moisture_history_miner_uid",
        "soil_moisture_history",
        "node_table",
        ["miner_uid"],
        ["uid"],
        ondelete="CASCADE",
    )

    # Recreate unique constraint
    op.create_unique_constraint(
        "uq_smh_region_miner_target_time",
        "soil_moisture_history",
        ["region_id", "miner_uid", "target_time"],
    )

    # Geomagnetic tables - need to convert from Text to Integer
    print("  - Updating geomagnetic_predictions...")
    # Add new column
    op.add_column(
        "geomagnetic_predictions",
        sa.Column("miner_uid_new", sa.Integer(), nullable=True),
    )

    # Copy and convert data
    op.execute(
        """
        UPDATE geomagnetic_predictions 
        SET miner_uid_new = CAST(miner_uid AS INTEGER)
        WHERE miner_uid ~ '^[0-9]+$'
    """
    )

    # Drop old column and rename new one
    op.drop_column("geomagnetic_predictions", "miner_uid")
    op.alter_column(
        "geomagnetic_predictions",
        "miner_uid_new",
        new_column_name="miner_uid",
        nullable=False,
    )

    # Add foreign key
    op.create_foreign_key(
        "fk_geomagnetic_predictions_miner_uid",
        "geomagnetic_predictions",
        "node_table",
        ["miner_uid"],
        ["uid"],
        ondelete="CASCADE",
    )

    print("  - Updating geomagnetic_history...")
    # Add new column
    op.add_column(
        "geomagnetic_history", sa.Column("miner_uid_new", sa.Integer(), nullable=True)
    )

    # Copy and convert data
    op.execute(
        """
        UPDATE geomagnetic_history 
        SET miner_uid_new = CAST(miner_uid AS INTEGER)
        WHERE miner_uid ~ '^[0-9]+$'
    """
    )

    # Drop old column and rename new one
    op.drop_column("geomagnetic_history", "miner_uid")
    op.alter_column(
        "geomagnetic_history",
        "miner_uid_new",
        new_column_name="miner_uid",
        nullable=False,
    )

    # Add foreign key
    op.create_foreign_key(
        "fk_geomagnetic_history_miner_uid",
        "geomagnetic_history",
        "node_table",
        ["miner_uid"],
        ["uid"],
        ondelete="CASCADE",
    )

    # Miner performance stats table - need to convert from Text to Integer
    print("  - Updating miner_performance_stats...")
    # Add new column
    op.add_column(
        "miner_performance_stats",
        sa.Column("miner_uid_new", sa.Integer(), nullable=True),
    )

    # Copy and convert data
    op.execute(
        """
        UPDATE miner_performance_stats 
        SET miner_uid_new = CAST(miner_uid AS INTEGER)
        WHERE miner_uid ~ '^[0-9]+$'
    """
    )

    # Drop old unique constraint first (if it exists)
    result = connection.execute(
        sa.text(
            """
        SELECT COUNT(*) FROM pg_constraint 
        WHERE conname = 'uq_mps_miner_period'
    """
        )
    )
    if result.scalar() > 0:
        op.drop_constraint("uq_mps_miner_period", "miner_performance_stats")

    # Drop old column and rename new one
    op.drop_column("miner_performance_stats", "miner_uid")
    op.alter_column(
        "miner_performance_stats",
        "miner_uid_new",
        new_column_name="miner_uid",
        nullable=False,
    )

    # Add foreign key
    op.create_foreign_key(
        "fk_miner_performance_stats_miner_uid",
        "miner_performance_stats",
        "node_table",
        ["miner_uid"],
        ["uid"],
        ondelete="CASCADE",
    )

    # Recreate unique constraint
    op.create_unique_constraint(
        "uq_mps_miner_period",
        "miner_performance_stats",
        ["miner_uid", "period_start", "period_end", "period_type"],
    )

    print("Migration completed successfully!")


def downgrade() -> None:
    """
    Revert all changes - convert back to array format and remove foreign keys
    """

    # Step 1: Remove foreign key constraints
    print("Removing foreign key constraints...")

    # Weather tables
    op.drop_constraint(
        "fk_weather_miner_responses_miner_uid", "weather_miner_responses"
    )
    op.drop_constraint("fk_weather_miner_scores_miner_uid", "weather_miner_scores")

    # Soil moisture tables
    op.drop_constraint(
        "fk_soil_moisture_predictions_miner_uid", "soil_moisture_predictions"
    )
    op.drop_constraint("fk_soil_moisture_history_miner_uid", "soil_moisture_history")

    # Geomagnetic tables
    op.drop_constraint(
        "fk_geomagnetic_predictions_miner_uid", "geomagnetic_predictions"
    )
    op.drop_constraint("fk_geomagnetic_history_miner_uid", "geomagnetic_history")

    # Miner performance stats
    op.drop_constraint(
        "fk_miner_performance_stats_miner_uid", "miner_performance_stats"
    )

    # Step 2: Convert miner_uid columns back to Text
    print("Converting miner_uid columns back to Text...")

    # For each table, add Text column, copy data, drop Integer column
    tables_to_convert = [
        "soil_moisture_predictions",
        "soil_moisture_history",
        "geomagnetic_predictions",
        "geomagnetic_history",
        "miner_performance_stats",
    ]

    for table in tables_to_convert:
        op.add_column(table, sa.Column("miner_uid_text", sa.Text(), nullable=True))
        op.execute(f"UPDATE {table} SET miner_uid_text = CAST(miner_uid AS TEXT)")
        op.drop_column(table, "miner_uid")
        op.alter_column(
            table, "miner_uid_text", new_column_name="miner_uid", nullable=False
        )

    # Step 3: Convert score_table back to array format
    print("Converting score_table back to array format...")

    # Create old-style table
    op.create_table(
        "score_table_old",
        sa.Column("task_name", sa.VARCHAR(255), nullable=True),
        sa.Column("task_id", sa.Text(), nullable=True),
        sa.Column("score", postgresql.ARRAY(sa.Float()), nullable=True),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "status", sa.VARCHAR(50), server_default=sa.text("'pending'"), nullable=True
        ),
        sa.UniqueConstraint(
            "task_name", "task_id", name="uq_score_table_task_name_task_id"
        ),
    )

    # Migrate data back to array format
    uid_cols = ", ".join([f"uid_{i}_score" for i in range(256)])
    array_construct = "ARRAY[" + ", ".join([f"uid_{i}_score" for i in range(256)]) + "]"

    migrate_back_sql = f"""
    INSERT INTO score_table_old (task_name, task_id, score, created_at, status)
    SELECT task_name, task_id, {array_construct}, created_at, status
    FROM score_table;
    """

    op.execute(migrate_back_sql)

    # Drop new table and rename old one
    op.drop_table("score_table")
    op.rename_table("score_table_old", "score_table")

    # Recreate original indexes
    op.create_index(
        "idx_score_created_at_on_score_table", "score_table", ["created_at"]
    )
    op.create_index(
        "idx_score_task_name_created_at_desc_on_score_table",
        "score_table",
        ["task_name", sa.text("created_at DESC")],
    )

    print("Downgrade completed successfully!")
