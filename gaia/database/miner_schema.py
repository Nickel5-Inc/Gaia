from sqlalchemy import Column, Integer, String, Float, DateTime, LargeBinary, Boolean
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func
import datetime

# Define a new Base for miner-specific tables
# This helps in organizing models and potentially managing them separately if needed in the future,
# though for a shared Alembic environment, their metadata will be combined.
MinerBase = declarative_base()

class MinerState(MinerBase):
    """
    Table to store miner-specific state or operational data.
    This is an example table.
    """
    __tablename__ = "miner_state"

    id = Column(Integer, primary_key=True, index=True)
    miner_hotkey = Column(String, unique=True, index=True, nullable=False)
    last_active_timestamp = Column(DateTime(timezone=True), server_default=func.now())
    software_version = Column(String)
    processed_tasks_count = Column(Integer, default=0)
    current_status = Column(String, default="idle") # e.g., idle, processing_task, error

    def __repr__(self):
        return f"<MinerState(miner_hotkey='{self.miner_hotkey}', status='{self.current_status}')>"

class MinerTaskAssignments(MinerBase):
    """
    Table to track tasks assigned to this miner by validators.
    """
    __tablename__ = "miner_task_assignments"

    id = Column(Integer, primary_key=True, index=True)
    task_uuid = Column(String, unique=True, index=True, nullable=False) # Unique ID for the task
    validator_hotkey = Column(String, index=True, nullable=False)
    task_type = Column(String, nullable=False) # e.g., 'geomagnetic', 'soil_moisture'
    payload_cid = Column(String) # IPFS CID for task payload, if applicable
    assigned_at = Column(DateTime(timezone=True), server_default=func.now())
    status = Column(String, default="pending") # e.g., pending, processing, completed, failed
    result_cid = Column(String, nullable=True) # IPFS CID for task result
    completed_at = Column(DateTime(timezone=True), nullable=True)
    error_message = Column(String, nullable=True)

    def __repr__(self):
        return f"<MinerTaskAssignments(task_uuid='{self.task_uuid}', status='{self.status}')>"

# You can add more miner-specific tables here following the same pattern.
# For example, tables for storing results, logs, performance metrics, etc.

# To make these tables known to Alembic, their metadata (MinerBase.metadata)
# will need to be included in the target_metadata in Alembic's env.py. 