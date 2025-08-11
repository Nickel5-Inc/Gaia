"""add_perturb_seed_table

Revision ID: 5fda479e51b2
Revises: score_table_refactor
Create Date: 2025-08-07 18:41:50.957856

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "5fda479e51b2"
down_revision: Union[str, None] = "score_table_refactor"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add perturb_seed table for anti-weight-copying mechanism with safe timing."""
    # Enable pgcrypto extension if not already enabled
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")

    # Drop old table if it exists (from earlier migration)
    op.execute("DROP TABLE IF EXISTS perturb_seed")

    # Create the new perturb_seed table with activation times
    op.create_table(
        "perturb_seed",
        sa.Column("activation_hour", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("seed_hex", sa.String(64), nullable=False),
        sa.Column(
            "generated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("activation_hour"),
    )

    # Create index for efficient lookups
    op.create_index("idx_perturb_seed_activation", "perturb_seed", ["activation_hour"])

    # Insert initial seeds for current and next hour
    op.execute(
        """
        INSERT INTO perturb_seed (activation_hour, seed_hex)
        VALUES 
        (date_trunc('hour', NOW()), encode(gen_random_bytes(32), 'hex')),
        (date_trunc('hour', NOW()) + INTERVAL '1 hour', encode(gen_random_bytes(32), 'hex'))
        ON CONFLICT (activation_hour) DO NOTHING
    """
    )


def downgrade() -> None:
    """Remove perturb_seed table."""
    op.drop_table("perturb_seed")
