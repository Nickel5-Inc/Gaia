"""comprehensive_miner_schema_convergence

Revision ID: 15952c9da69b
Revises: d5694f31dd48
Create Date: 2025-05-28 15:31:18.768401

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy import inspect

# revision identifiers, used by Alembic.
revision: str = '15952c9da69b'
down_revision: Union[str, None] = 'd5694f31dd48'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Comprehensive upgrade to ensure exact schema convergence."""
    
    # Get current connection and inspector
    connection = op.get_bind()
    inspector = inspect(connection)
    existing_tables = inspector.get_table_names()
    
    print(f"Starting comprehensive miner schema convergence. Found {len(existing_tables)} existing tables.")
    
    # Define the exact target schema for miner database
    target_table = 'weather_miner_jobs'
    target_columns = {
        'id': {'type': 'VARCHAR(100)', 'nullable': False, 'primary_key': True},
        'validator_request_time': {'type': 'TIMESTAMP WITH TIME ZONE', 'nullable': False},
        'validator_hotkey': {'type': 'VARCHAR(255)', 'nullable': True},
        'gfs_init_time_utc': {'type': 'TIMESTAMP WITH TIME ZONE', 'nullable': False},
        'gfs_input_metadata': {'type': 'JSONB', 'nullable': True},
        'processing_start_time': {'type': 'TIMESTAMP WITH TIME ZONE', 'nullable': True},
        'processing_end_time': {'type': 'TIMESTAMP WITH TIME ZONE', 'nullable': True},
        'target_netcdf_path': {'type': 'TEXT', 'nullable': True},
        'kerchunk_json_path': {'type': 'TEXT', 'nullable': True},
        'verification_hash': {'type': 'VARCHAR(64)', 'nullable': True},
        'ipfs_cid': {'type': 'VARCHAR(255)', 'nullable': True},
        'status': {'type': 'VARCHAR(50)', 'nullable': False, 'default': "'received'"},
        'error_message': {'type': 'TEXT', 'nullable': True},
        'updated_at': {'type': 'TIMESTAMP WITH TIME ZONE', 'nullable': True, 'default': 'now()'},
        'gfs_t_minus_6_time_utc': {'type': 'TIMESTAMP WITH TIME ZONE', 'nullable': False},
        'input_data_hash': {'type': 'TEXT', 'nullable': True}
    }
    
    target_indexes = [
        {'name': 'ix_weather_miner_jobs_validator_request_time', 'columns': ['validator_request_time']},
        {'name': 'ix_weather_miner_jobs_gfs_init_time_utc', 'columns': ['gfs_init_time_utc']},
        {'name': 'ix_weather_miner_jobs_status', 'columns': ['status']}
    ]
    
    # Tables that should NOT exist in miner database
    forbidden_tables = [
        'node_table', 'score_table', 'soil_moisture_regions', 'soil_moisture_predictions',
        'soil_moisture_history', 'weather_forecast_runs', 'weather_miner_responses',
        'weather_miner_scores', 'weather_ensemble_forecasts', 'weather_ensemble_components',
        'weather_historical_weights', 'geomagnetic_predictions', 'geomagnetic_history',
        'alembic_version_validator'  # In case there's cross-contamination
    ]
    
    # Step 1: Drop any tables that shouldn't exist in miner database
    for table_name in existing_tables:
        if table_name in forbidden_tables:
            print(f"Dropping forbidden table from miner database: {table_name}")
            op.drop_table(table_name)
    
    # Step 2: Handle the target table
    if target_table not in existing_tables:
        print(f"Creating missing table: {target_table}")
        # Create table from scratch
        op.create_table(target_table,
            sa.Column('id', sa.String(length=100), nullable=False),
            sa.Column('validator_request_time', sa.DateTime(timezone=True), nullable=False),
            sa.Column('validator_hotkey', sa.String(length=255), nullable=True),
            sa.Column('gfs_init_time_utc', sa.DateTime(timezone=True), nullable=False),
            sa.Column('gfs_input_metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
            sa.Column('processing_start_time', sa.DateTime(timezone=True), nullable=True),
            sa.Column('processing_end_time', sa.DateTime(timezone=True), nullable=True),
            sa.Column('target_netcdf_path', sa.Text(), nullable=True),
            sa.Column('kerchunk_json_path', sa.Text(), nullable=True),
            sa.Column('verification_hash', sa.String(length=64), nullable=True),
            sa.Column('ipfs_cid', sa.String(length=255), nullable=True),
            sa.Column('status', sa.String(length=50), server_default='received', nullable=False),
            sa.Column('error_message', sa.Text(), nullable=True),
            sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
            sa.Column('gfs_t_minus_6_time_utc', sa.DateTime(timezone=True), nullable=False),
            sa.Column('input_data_hash', sa.Text(), nullable=True),
            sa.PrimaryKeyConstraint('id')
        )
    else:
        print(f"Table {target_table} exists, checking columns...")
        # Get existing columns
        existing_columns = {col['name']: col for col in inspector.get_columns(target_table)}
        
        # Add missing columns
        for col_name, col_spec in target_columns.items():
            if col_name not in existing_columns:
                print(f"Adding missing column: {target_table}.{col_name}")
                if col_name == 'validator_request_time':
                    op.add_column(target_table, sa.Column('validator_request_time', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')))
                elif col_name == 'gfs_init_time_utc':
                    op.add_column(target_table, sa.Column('gfs_init_time_utc', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')))
                elif col_name == 'gfs_t_minus_6_time_utc':
                    op.add_column(target_table, sa.Column('gfs_t_minus_6_time_utc', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')))
                elif col_name == 'gfs_input_metadata':
                    op.add_column(target_table, sa.Column('gfs_input_metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
                elif col_name == 'processing_start_time':
                    op.add_column(target_table, sa.Column('processing_start_time', sa.DateTime(timezone=True), nullable=True))
                elif col_name == 'processing_end_time':
                    op.add_column(target_table, sa.Column('processing_end_time', sa.DateTime(timezone=True), nullable=True))
                elif col_name == 'target_netcdf_path':
                    op.add_column(target_table, sa.Column('target_netcdf_path', sa.Text(), nullable=True))
                elif col_name == 'kerchunk_json_path':
                    op.add_column(target_table, sa.Column('kerchunk_json_path', sa.Text(), nullable=True))
                elif col_name == 'verification_hash':
                    op.add_column(target_table, sa.Column('verification_hash', sa.String(length=64), nullable=True))
                elif col_name == 'ipfs_cid':
                    op.add_column(target_table, sa.Column('ipfs_cid', sa.String(length=255), nullable=True))
                elif col_name == 'status':
                    op.add_column(target_table, sa.Column('status', sa.String(length=50), server_default='received', nullable=False))
                elif col_name == 'error_message':
                    op.add_column(target_table, sa.Column('error_message', sa.Text(), nullable=True))
                elif col_name == 'updated_at':
                    op.add_column(target_table, sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True))
                elif col_name == 'input_data_hash':
                    op.add_column(target_table, sa.Column('input_data_hash', sa.Text(), nullable=True))
                elif col_name == 'validator_hotkey':
                    op.add_column(target_table, sa.Column('validator_hotkey', sa.String(length=255), nullable=True))
                elif col_name == 'id':
                    # Primary key should already exist, but check type
                    pass
        
        # Remove columns that shouldn't exist
        allowed_columns = set(target_columns.keys())
        for existing_col in existing_columns:
            if existing_col not in allowed_columns:
                print(f"Dropping unexpected column: {target_table}.{existing_col}")
                op.drop_column(target_table, existing_col)
    
    # Step 3: Ensure correct indexes exist
    existing_indexes = {idx['name']: idx for idx in inspector.get_indexes(target_table)}
    
    # Drop indexes that shouldn't exist
    for idx_name in existing_indexes:
        if idx_name not in [idx['name'] for idx in target_indexes] and not idx_name.startswith('pk_'):
            print(f"Dropping unexpected index: {idx_name}")
            op.drop_index(idx_name, table_name=target_table)
    
    # Create missing indexes
    for target_idx in target_indexes:
        if target_idx['name'] not in existing_indexes:
            print(f"Creating missing index: {target_idx['name']}")
            op.create_index(target_idx['name'], target_table, target_idx['columns'])
    
    print("Comprehensive miner schema convergence completed.")


def downgrade() -> None:
    """Downgrade schema."""
    # For a comprehensive migration, downgrade is complex
    # This would restore to previous state, but for safety we'll just pass
    print("Downgrade not implemented for comprehensive convergence migration")
    pass
