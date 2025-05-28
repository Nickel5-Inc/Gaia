"""comprehensive_validator_schema_convergence

Revision ID: 2e00df6800b9
Revises: f75e4f7343a1
Create Date: 2025-05-28 15:32:01.464518

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy import inspect

# revision identifiers, used by Alembic.
revision: str = '2e00df6800b9'
down_revision: Union[str, None] = 'f75e4f7343a1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Schema validation and essential convergence."""
    
    # Get current connection and inspector
    connection = op.get_bind()
    inspector = inspect(connection)
    existing_tables = inspector.get_table_names()
    
    print(f"Starting validator schema validation. Found {len(existing_tables)} existing tables.")
    
    # Define core required tables (the absolute minimum needed)
    core_tables = {
        'node_table',
        'score_table', 
        'weather_forecast_runs',
        'weather_miner_responses'
    }
    
    # Tables that should NOT exist in validator database (from miner)
    forbidden_tables = [
        'weather_miner_jobs',  # This belongs only in miner database
    ]
    
    # Step 1: Drop forbidden tables if they exist
    for table_name in existing_tables:
        if table_name in forbidden_tables:
            print(f"Dropping forbidden table from validator database: {table_name}")
            try:
                op.drop_table(table_name)
            except Exception as e:
                print(f"Warning: Could not drop forbidden table {table_name}: {e}")
    
    # Step 2: Validate core tables exist
    missing_core_tables = core_tables - set(existing_tables)
    if missing_core_tables:
        print(f"ERROR: Missing core tables: {missing_core_tables}")
        print("The validator database is missing essential tables.")
        print("Please run the initial schema migration first or restore from backup.")
        raise Exception(f"Missing core tables: {missing_core_tables}")
    
    # Step 3: Basic column validation for core tables
    try:
        # Check node_table has uid column
        node_cols = {col['name'] for col in inspector.get_columns('node_table')}
        if 'uid' not in node_cols:
            print("ERROR: node_table missing uid column")
            raise Exception("node_table missing uid column")
        
        # Check score_table has basic columns
        score_cols = {col['name'] for col in inspector.get_columns('score_table')}
        if 'task_name' not in score_cols or 'score' not in score_cols:
            print("ERROR: score_table missing required columns")
            raise Exception("score_table missing required columns")
        
        # Check weather_forecast_runs has basic columns
        wfr_cols = {col['name'] for col in inspector.get_columns('weather_forecast_runs')}
        if 'id' not in wfr_cols or 'status' not in wfr_cols:
            print("ERROR: weather_forecast_runs missing required columns")
            raise Exception("weather_forecast_runs missing required columns")
        
        # Check weather_miner_responses has basic columns
        wmr_cols = {col['name'] for col in inspector.get_columns('weather_miner_responses')}
        if 'run_id' not in wmr_cols or 'miner_uid' not in wmr_cols:
            print("ERROR: weather_miner_responses missing required columns")
            raise Exception("weather_miner_responses missing required columns")
        
        print("Core table validation passed.")
        
    except Exception as e:
        print(f"Table validation failed: {e}")
        raise
    
    # Step 4: Report current state
    print("\n=== VALIDATOR DATABASE SCHEMA REPORT ===")
    print(f"Total tables: {len(existing_tables)}")
    print(f"Tables present: {sorted(existing_tables)}")
    
    # Check for some common optional tables
    optional_tables = [
        'soil_moisture_regions', 'soil_moisture_predictions', 'soil_moisture_history',
        'weather_miner_scores', 'weather_ensemble_forecasts', 'weather_ensemble_components',
        'weather_historical_weights', 'geomagnetic_predictions', 'geomagnetic_history'
    ]
    
    missing_optional = [t for t in optional_tables if t not in existing_tables]
    if missing_optional:
        print(f"Missing optional tables: {missing_optional}")
        print("Note: These tables may be created by application code as needed.")
    
    print("=== END REPORT ===\n")
    
    print("Validator schema validation completed successfully.")
    print("The database has the minimum required schema for validator operations.")


def downgrade() -> None:
    """Downgrade schema."""
    print("Downgrade not implemented for schema validation migration")
    pass
