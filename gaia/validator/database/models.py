from datetime import datetime
from typing import Optional, Dict, Any, List
from sqlalchemy import Column, Integer, String, Float, DateTime, JSON, ForeignKey, ARRAY, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import CheckConstraint, Index

Base = declarative_base()

class Node(Base):
    """Model for storing miner/node information."""
    __tablename__ = "node_table"

    uid = Column(Integer, primary_key=True)
    hotkey = Column(String)
    coldkey = Column(String)
    ip = Column(String)
    ip_type = Column(String)
    port = Column(Integer)
    incentive = Column(Float)
    stake = Column(Float)
    trust = Column(Float)
    vtrust = Column(Float)
    protocol = Column(String)
    last_updated = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        # Ensure uid is between 0 and 255
        CheckConstraint('uid >= 0 AND uid < 256', name='check_uid_range'),
    )

class Score(Base):
    """Model for storing task scores."""
    __tablename__ = "score_table"

    id = Column(Integer, primary_key=True)
    task_name = Column(String(100), nullable=False)
    task_id = Column(String, nullable=False)
    score = Column(ARRAY(Float), nullable=False)
    status = Column(String(50), default='pending')
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class TaskState(Base):
    """Model for storing task execution state."""
    __tablename__ = "task_state"

    id = Column(Integer, primary_key=True)
    flow_name = Column(String(100), nullable=False)
    task_name = Column(String(100), nullable=False)
    status = Column(String(50), nullable=False)
    error_message = Column(Text)
    metadata = Column(JSON)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

class ProcessQueue(Base):
    """Model for process queue management."""
    __tablename__ = "process_queue"

    id = Column(Integer, primary_key=True)
    process_type = Column(String(50), nullable=False)
    process_name = Column(String(100), nullable=False)
    task_id = Column(Integer)
    task_name = Column(String(100))
    priority = Column(Integer, default=0)
    status = Column(String(50), default='pending')
    payload = Column(JSON)  # Changed from BYTEA to JSON for better compatibility
    start_processing_time = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    started_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))
    complete_by = Column(DateTime(timezone=True))
    expected_execution_time = Column(Integer)
    execution_time = Column(Integer)
    error = Column(Text)
    retries = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)

    __table_args__ = (
        # Add indexes for commonly queried fields
        Index('idx_process_queue_status', 'status'),
        Index('idx_process_queue_priority', 'priority'),
    ) 

class GeomagneticPrediction(Base):
    """Model for storing pending geomagnetic predictions."""
    __tablename__ = "geomagnetic_predictions"

    id = Column(String, primary_key=True)
    miner_uid = Column(String, nullable=False)
    miner_hotkey = Column(String, ForeignKey('node_table.hotkey'), nullable=False)
    predicted_value = Column(Float, nullable=False)
    query_time = Column(DateTime(timezone=True), nullable=False)
    status = Column(String(50), default='pending')
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class GeomagneticHistory(Base):
    """Model for storing scored geomagnetic predictions."""
    __tablename__ = "geomagnetic_history"

    id = Column(Integer, primary_key=True)
    miner_uid = Column(String, nullable=False)
    miner_hotkey = Column(String, nullable=False)
    query_time = Column(DateTime(timezone=True), nullable=False)
    predicted_value = Column(Float, nullable=False)
    ground_truth_value = Column(Float, nullable=False)
    score = Column(Float, nullable=False)
    scored_at = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class GeomagneticScore(Base):
    """Model for storing aggregated geomagnetic scores."""
    __tablename__ = "geomagnetic_scores"

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    scores = Column(ARRAY(Float), nullable=False)
    mean_score = Column(Float)
    std_score = Column(Float)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index('idx_geomag_scores_timestamp', 'timestamp'),
    )