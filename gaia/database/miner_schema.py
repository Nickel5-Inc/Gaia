from sqlalchemy import Column, Integer, String, Float, DateTime, LargeBinary, Boolean, Text
from sqlalchemy.dialects.postgresql import JSONB # For JSONB type
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func
import datetime

# Define a new Base for miner-specific tables
# This helps in organizing models and potentially managing them separately if needed in the future,
# though for a shared Alembic environment, their metadata will be combined.
MinerBase = declarative_base()



class WeatherMinerJobs(MinerBase):
    """
    Tracks weather forecast jobs processed by the miner.
    Based on weather/schema.json for weather_miner_jobs.
    This is the primary table for the miner_db.
    """
    __tablename__ = "weather_miner_jobs"

    id = Column(String(100), primary_key=True)
    validator_request_time = Column(DateTime(timezone=True), nullable=False, index=True)
    validator_hotkey = Column(String(255), nullable=True) # Based on db output, miner_hotkey not present here
    # miner_hotkey = Column(String(255), nullable=True) # This was in the previous model, but not in db output for this table
    gfs_init_time_utc = Column(DateTime(timezone=True), nullable=False, index=True)
    gfs_input_metadata = Column(JSONB, nullable=True)
    processing_start_time = Column(DateTime(timezone=True), nullable=True)
    processing_end_time = Column(DateTime(timezone=True), nullable=True)
    target_netcdf_path = Column(Text, nullable=True)
    kerchunk_json_path = Column(Text, nullable=True)
    verification_hash = Column(String(64), nullable=True)
    ipfs_cid = Column(String(255), nullable=True)
    status = Column(String(50), nullable=False, server_default='received', index=True)
    error_message = Column(Text, nullable=True)
    # For updated_at, server_default=func.now() sets it on insert if not provided by app.
    # onupdate=func.now() is an ORM hook to update it when an ORM object instance is changed.
    # The database default `CURRENT_TIMESTAMP` on the column itself is the most robust for direct DB updates.
    # We will rely on Alembic to set the database-level default for updated_at if that's intended.
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=True) 
    gfs_t_minus_6_time_utc = Column(DateTime(timezone=True), nullable=False)
    input_data_hash = Column(Text, nullable=True)
    input_batch_pickle_path = Column(Text, nullable=True) # Path to the pickled aurora.Batch input

    def __repr__(self):
        return f"<WeatherMinerJobs(id='{self.id}', status='{self.status}')>"

# You can add more miner-specific tables here following the same pattern.
# For example, tables for storing results, logs, performance metrics, etc.

# To make this table known to Alembic for autogeneration,
# ensure MinerBase.metadata is the target_metadata in Alembic's env.py
# when DB_TARGET=miner. 