"""add generic validator_jobs queue and logs, link from weather_forecast_steps

Revision ID: b7c1a2d38f10
Revises: 803c6cff9b58
Create Date: 2025-08-13 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "b7c1a2d38f10"
down_revision: Union[str, None] = "803c6cff9b58"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create validator_jobs table
    op.create_table(
        "validator_jobs",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("job_type", sa.String(length=64), nullable=False),
        sa.Column("priority", sa.Integer(), server_default="100", nullable=False),
        sa.Column(
            "status",
            sa.String(length=32),
            server_default=sa.text("'pending'"),
            nullable=False,
        ),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("result", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("max_attempts", sa.Integer(), server_default="5", nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.Column("scheduled_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("started_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("completed_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("next_retry_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("lease_expires_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("claimed_by", sa.String(length=64), nullable=True),
        sa.Column("run_id", sa.Integer(), nullable=True),
        sa.Column("miner_uid", sa.Integer(), nullable=True),
        sa.Column("response_id", sa.Integer(), nullable=True),
        sa.Column("step_id", sa.BigInteger(), nullable=True),
        sa.ForeignKeyConstraint(["run_id"], ["weather_forecast_runs.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["miner_uid"], ["node_table.uid"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["response_id"], ["weather_miner_responses.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["step_id"], ["weather_forecast_steps.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        comment="Generic validator job queue with leases and retries.",
    )

    # Indexes for validator_jobs
    op.create_index(
        "idx_vjobs_status_sched",
        "validator_jobs",
        ["status", "scheduled_at"],
        unique=False,
    )
    op.create_index(
        "idx_vjobs_type_status",
        "validator_jobs",
        ["job_type", "status"],
        unique=False,
    )
    op.create_index("idx_vjobs_priority", "validator_jobs", ["priority"], unique=False)
    op.create_index("idx_vjobs_run", "validator_jobs", ["run_id"], unique=False)
    op.create_index("idx_vjobs_miner", "validator_jobs", ["miner_uid"], unique=False)
    op.create_index(
        "idx_vjobs_response", "validator_jobs", ["response_id"], unique=False
    )
    # Unique partial index for run-level scoring jobs
    op.create_index(
        "uq_vjobs_run_type_scoring",
        "validator_jobs",
        ["run_id", "job_type"],
        unique=True,
        postgresql_where=sa.text(
            "job_type IN ('weather.scoring.day1_qc','weather.scoring.era5_final')"
        ),
    )

    # Create validator_job_logs table
    op.create_table(
        "validator_job_logs",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("job_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.Column("level", sa.String(length=16), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(["job_id"], ["validator_jobs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        comment="Per-job log messages for auditing and debugging.",
    )
    op.create_index("idx_vjob_logs_job", "validator_job_logs", ["job_id"], unique=False)

    # Link from weather_forecast_steps to validator_jobs (nullable)
    op.add_column(
        "weather_forecast_steps",
        sa.Column("job_id", sa.BigInteger(), nullable=True),
    )
    op.create_foreign_key(
        "fk_wfsteps_job_id",
        source_table="weather_forecast_steps",
        referent_table="validator_jobs",
        local_cols=["job_id"],
        remote_cols=["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    # Remove link from steps
    op.drop_constraint("fk_wfsteps_job_id", "weather_forecast_steps", type_="foreignkey")
    op.drop_column("weather_forecast_steps", "job_id")

    # Drop logs table + index
    op.drop_index("idx_vjob_logs_job", table_name="validator_job_logs")
    op.drop_table("validator_job_logs")

    # Drop indexes and jobs table
    op.drop_index("uq_vjobs_run_type_scoring", table_name="validator_jobs")
    op.drop_index("idx_vjobs_response", table_name="validator_jobs")
    op.drop_index("idx_vjobs_miner", table_name="validator_jobs")
    op.drop_index("idx_vjobs_run", table_name="validator_jobs")
    op.drop_index("idx_vjobs_priority", table_name="validator_jobs")
    op.drop_index("idx_vjobs_type_status", table_name="validator_jobs")
    op.drop_index("idx_vjobs_status_sched", table_name="validator_jobs")
    op.drop_table("validator_jobs")


