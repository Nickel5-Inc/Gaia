import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# Central MetaData object for the validator schema
validator_metadata = sa.MetaData()

node_table = sa.Table(
    "node_table",
    validator_metadata,
    sa.Column(
        "uid", sa.Integer, primary_key=True, comment="Unique ID for the node, 0-255"
    ),
    sa.Column("hotkey", sa.Text, nullable=True, comment="Hotkey of the node"),
    sa.Column("coldkey", sa.Text, nullable=True, comment="Coldkey of the node"),
    sa.Column("ip", sa.Text, nullable=True, comment="IP address of the node"),
    sa.Column(
        "ip_type", sa.Text, nullable=True, comment="IP address type (e.g., IPv4, IPv6)"
    ),
    sa.Column(
        "port", sa.Integer, nullable=True, comment="Port number for the node's services"
    ),
    sa.Column(
        "incentive",
        sa.Float,
        nullable=True,
        comment="Current incentive score of the node",
    ),
    sa.Column("stake", sa.Float, nullable=True, comment="Current stake of the node"),
    sa.Column(
        "trust", sa.Float, nullable=True, comment="Current trust score of the node"
    ),
    sa.Column(
        "vtrust",
        sa.Float,
        nullable=True,
        comment="Current validator trust score of the node",
    ),
    sa.Column(
        "protocol", sa.Text, nullable=True, comment="Protocol version used by the node"
    ),
    sa.Column(
        "last_updated",
        postgresql.TIMESTAMP(timezone=True),
        server_default=sa.func.current_timestamp(),
        nullable=False,  # Typically, last_updated should not be null
        comment="Timestamp of the last update for this node's record",
    ),
    # Fiber handshake caching columns
    sa.Column(
        "fiber_symmetric_key",
        sa.Text,
        nullable=True,
        comment="Cached Fiber symmetric key for encrypted communication",
    ),
    sa.Column(
        "fiber_symmetric_key_uuid",
        sa.Text,
        nullable=True,
        comment="UUID for the cached symmetric key",
    ),
    sa.Column(
        "fiber_key_cached_at",
        sa.DateTime(timezone=True),
        nullable=True,
        comment="When the symmetric key was cached",
    ),
    sa.Column(
        "fiber_key_expires_at",
        sa.DateTime(timezone=True),
        nullable=True,
        comment="When the cached symmetric key expires",
    ),
    sa.CheckConstraint("uid >= 0 AND uid < 256", name="node_table_uid_check"),
    comment="Table storing information about registered nodes (miners/validators).",
)
sa.Index(
    "idx_node_hotkey_on_node_table", node_table.c.hotkey
)  # For faster lookups by hotkey

# Build score_table with individual UID columns for better query performance
score_columns = [
    sa.Column(
        "task_name",
        sa.VARCHAR(255),
        nullable=True,
        comment="Name of the task being scored",
    ),
    sa.Column(
        "task_id",
        sa.Text,
        nullable=True,
        comment="Unique ID for the specific task instance",
    ),
]

# Add individual score columns for each UID (0-255)
for uid in range(256):
    score_columns.append(
        sa.Column(
            f"uid_{uid}_score", sa.Float, nullable=True, comment=f"Score for UID {uid}"
        )
    )

score_columns.extend(
    [
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.func.current_timestamp(),
            nullable=False,
            comment="Timestamp of score creation",
        ),
        sa.Column(
            "status",
            sa.VARCHAR(50),
            server_default=sa.text("'pending'"),
            nullable=True,
            comment="Status of the scoring process",
        ),
    ]
)

score_table = sa.Table(
    "score_table",
    validator_metadata,
    *score_columns,
    sa.UniqueConstraint(
        "task_name", "task_id", name="uq_score_table_task_name_task_id"
    ),
    comment="Table to store scores for various tasks with individual UID columns for efficient querying.",
)
sa.Index(
    "idx_score_created_at_on_score_table", score_table.c.created_at
)  # Explicit index definition
sa.Index(
    "idx_score_task_name_created_at_desc_on_score_table",
    score_table.c.task_name,
    score_table.c.created_at.desc(),
)

baseline_predictions_table = sa.Table(
    "baseline_predictions",
    validator_metadata,
    sa.Column(
        "id",
        sa.Integer,
        primary_key=True,
        autoincrement=True,
        comment="Serial ID for the prediction entry",
    ),
    sa.Column(
        "task_name",
        sa.Text,
        nullable=False,
        comment="Name of the task (e.g., geomagnetic, soil_moisture)",
    ),
    sa.Column(
        "task_id", sa.Text, nullable=False, comment="ID of the specific task execution"
    ),
    sa.Column(
        "region_id",
        sa.Text,
        nullable=True,
        comment="For region-specific tasks, the region identifier",
    ),
    sa.Column(
        "timestamp",
        postgresql.TIMESTAMP(timezone=True),
        nullable=False,
        comment="Timestamp for when the prediction was made/is valid for",
    ),
    sa.Column(
        "prediction",
        postgresql.JSONB,
        nullable=False,
        comment="The model's prediction data",
    ),
    sa.Column(
        "created_at",
        postgresql.TIMESTAMP(timezone=True),
        server_default=sa.func.current_timestamp(),
        nullable=False,
        comment="Timestamp of prediction storage",
    ),
    comment="Stores baseline model predictions for various tasks.",
)
# Primary composite index for the most common query pattern
sa.Index(
    "idx_baseline_task_on_baseline_predictions",
    baseline_predictions_table.c.task_name,
    baseline_predictions_table.c.task_id,
)
# Additional indexes for performance optimization
sa.Index("idx_baseline_task_name_only", baseline_predictions_table.c.task_name)
sa.Index("idx_baseline_created_at", baseline_predictions_table.c.created_at.desc())
sa.Index(
    "idx_baseline_task_name_created_at",
    baseline_predictions_table.c.task_name,
    baseline_predictions_table.c.created_at.desc(),
)
sa.Index(
    "idx_baseline_region_task",
    baseline_predictions_table.c.region_id,
    baseline_predictions_table.c.task_name,
)

# --- Soil Moisture Tables ---
soil_moisture_regions_table = sa.Table(
    "soil_moisture_regions",
    validator_metadata,
    sa.Column(
        "id", sa.Integer, primary_key=True, autoincrement=True
    ),  # SERIAL PRIMARY KEY
    sa.Column("region_date", sa.Date, nullable=False),
    sa.Column("target_time", postgresql.TIMESTAMP(timezone=True), nullable=False),
    sa.Column("bbox", postgresql.JSONB, nullable=False),
    sa.Column(
        "combined_data", postgresql.BYTEA, nullable=True
    ),  # Assuming BYTEA can be nullable based on usage
    sa.Column("sentinel_bounds", postgresql.ARRAY(sa.Float), nullable=True),
    sa.Column("sentinel_crs", sa.Integer, nullable=True),
    sa.Column("status", sa.Text, nullable=False, server_default=sa.text("'pending'")),
    sa.Column(
        "created_at",
        postgresql.TIMESTAMP(timezone=True),
        server_default=sa.func.current_timestamp(),
        nullable=False,
    ),
    sa.Column("data_cleared_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
    sa.Column("array_shape", postgresql.ARRAY(sa.Integer), nullable=False),
)
sa.Index("idx_smr_region_date", soil_moisture_regions_table.c.region_date)
sa.Index("idx_smr_target_time", soil_moisture_regions_table.c.target_time)
sa.Index("idx_smr_status", soil_moisture_regions_table.c.status)

soil_moisture_predictions_table = sa.Table(
    "soil_moisture_predictions",
    validator_metadata,
    sa.Column(
        "id", sa.Integer, primary_key=True, autoincrement=True
    ),  # SERIAL PRIMARY KEY
    sa.Column(
        "region_id",
        sa.Integer,
        sa.ForeignKey("soil_moisture_regions.id", ondelete="SET NULL"),
        nullable=True,
    ),
    sa.Column(
        "miner_uid",
        sa.Integer,
        sa.ForeignKey("node_table.uid", ondelete="CASCADE"),
        nullable=False,
    ),
    sa.Column("miner_hotkey", sa.Text, nullable=False),
    sa.Column("target_time", postgresql.TIMESTAMP(timezone=True), nullable=False),
    sa.Column(
        "surface_sm", postgresql.ARRAY(sa.Float, dimensions=2), nullable=True
    ),  # Corrected for FLOAT[][]
    sa.Column(
        "rootzone_sm", postgresql.ARRAY(sa.Float, dimensions=2), nullable=True
    ),  # Corrected for FLOAT[][]
    sa.Column(
        "uncertainty_surface", postgresql.ARRAY(sa.Float, dimensions=2), nullable=True
    ),  # Corrected for FLOAT[][]
    sa.Column(
        "uncertainty_rootzone", postgresql.ARRAY(sa.Float, dimensions=2), nullable=True
    ),  # Corrected for FLOAT[][]
    sa.Column(
        "created_at",
        postgresql.TIMESTAMP(timezone=True),
        server_default=sa.func.current_timestamp(),
        nullable=False,
    ),
    sa.Column("sentinel_bounds", postgresql.ARRAY(sa.Float), nullable=True),
    sa.Column("sentinel_crs", sa.Integer, nullable=True),
    sa.Column(
        "status", sa.Text, nullable=False, server_default=sa.text("'sent_to_miner'")
    ),
    sa.Column("retry_count", sa.Integer, server_default=sa.text("0"), nullable=True),
    sa.Column("next_retry_time", postgresql.TIMESTAMP(timezone=True), nullable=True),
    sa.Column("last_retry_attempt", postgresql.TIMESTAMP(timezone=True), nullable=True),
    sa.Column("retry_error_message", sa.Text, nullable=True),
    sa.Column("last_error", sa.Text, nullable=True),
)
sa.Index("idx_smp_region_id", soil_moisture_predictions_table.c.region_id)
sa.Index("idx_smp_miner_uid", soil_moisture_predictions_table.c.miner_uid)
sa.Index("idx_smp_miner_hotkey", soil_moisture_predictions_table.c.miner_hotkey)
sa.Index("idx_smp_target_time", soil_moisture_predictions_table.c.target_time)
sa.Index("idx_smp_status", soil_moisture_predictions_table.c.status)

soil_moisture_history_table = sa.Table(
    "soil_moisture_history",
    validator_metadata,
    sa.Column(
        "id", sa.Integer, primary_key=True, autoincrement=True
    ),  # SERIAL PRIMARY KEY
    sa.Column(
        "region_id",
        sa.Integer,
        sa.ForeignKey("soil_moisture_regions.id", ondelete="CASCADE"),
        nullable=False,
    ),
    sa.Column(
        "miner_uid",
        sa.Integer,
        sa.ForeignKey("node_table.uid", ondelete="CASCADE"),
        nullable=False,
    ),
    sa.Column("miner_hotkey", sa.Text, nullable=False),
    sa.Column("target_time", postgresql.TIMESTAMP(timezone=True), nullable=False),
    sa.Column(
        "surface_sm_pred", postgresql.ARRAY(sa.Float, dimensions=2), nullable=True
    ),  # Corrected for FLOAT[][]
    sa.Column(
        "rootzone_sm_pred", postgresql.ARRAY(sa.Float, dimensions=2), nullable=True
    ),  # Corrected for FLOAT[][]
    sa.Column(
        "surface_sm_truth", postgresql.ARRAY(sa.Float, dimensions=2), nullable=True
    ),  # Corrected for FLOAT[][]
    sa.Column(
        "rootzone_sm_truth", postgresql.ARRAY(sa.Float, dimensions=2), nullable=True
    ),  # Corrected for FLOAT[][]
    sa.Column("surface_rmse", sa.Float, nullable=True),
    sa.Column("rootzone_rmse", sa.Float, nullable=True),
    sa.Column("surface_structure_score", sa.Float, nullable=True),
    sa.Column("rootzone_structure_score", sa.Float, nullable=True),
    sa.Column("sentinel_bounds", postgresql.ARRAY(sa.Float), nullable=True),
    sa.Column("sentinel_crs", sa.Integer, nullable=True),
    sa.Column(
        "scored_at",
        postgresql.TIMESTAMP(timezone=True),
        server_default=sa.func.current_timestamp(),
        nullable=False,
    ),
    sa.UniqueConstraint(
        "region_id", "miner_uid", "target_time", name="uq_smh_region_miner_target_time"
    ),
)
sa.Index("idx_smh_region_id", soil_moisture_history_table.c.region_id)
sa.Index("idx_smh_miner_uid", soil_moisture_history_table.c.miner_uid)
sa.Index("idx_smh_miner_hotkey", soil_moisture_history_table.c.miner_hotkey)
sa.Index("idx_smh_target_time", soil_moisture_history_table.c.target_time)

# --- Weather Tables ---
weather_forecast_runs_table = sa.Table(
    "weather_forecast_runs",
    validator_metadata,
    sa.Column(
        "id", sa.Integer, primary_key=True, autoincrement=True
    ),  # SERIAL PRIMARY KEY
    sa.Column(
        "run_initiation_time", postgresql.TIMESTAMP(timezone=True), nullable=False
    ),
    sa.Column(
        "target_forecast_time_utc", postgresql.TIMESTAMP(timezone=True), nullable=False
    ),
    sa.Column("gfs_init_time_utc", postgresql.TIMESTAMP(timezone=True), nullable=False),
    sa.Column("gfs_input_metadata", postgresql.JSONB, nullable=True),
    sa.Column(
        "status", sa.VARCHAR(50), nullable=False, server_default=sa.text("'pending'")
    ),
    sa.Column("completion_time", postgresql.TIMESTAMP(timezone=True), nullable=True),
    sa.Column(
        "final_scoring_attempted_time",
        postgresql.TIMESTAMP(timezone=True),
        nullable=True,
    ),
    sa.Column("error_message", sa.Text, nullable=True),
    comment="Tracks each weather forecast run initiated by the validator.",
)
sa.Index("idx_wfr_run_init_time", weather_forecast_runs_table.c.run_initiation_time)
sa.Index(
    "idx_wfr_target_forecast_time",
    weather_forecast_runs_table.c.target_forecast_time_utc,
)
sa.Index("idx_wfr_gfs_init_time", weather_forecast_runs_table.c.gfs_init_time_utc)
sa.Index("idx_wfr_status", weather_forecast_runs_table.c.status)
sa.Index(
    "idx_wfr_status_init_time",
    weather_forecast_runs_table.c.status,
    weather_forecast_runs_table.c.run_initiation_time,
)

weather_miner_responses_table = sa.Table(
    "weather_miner_responses",
    validator_metadata,
    sa.Column(
        "id", sa.Integer, primary_key=True, autoincrement=True
    ),  # SERIAL PRIMARY KEY
    sa.Column(
        "run_id",
        sa.Integer,
        sa.ForeignKey("weather_forecast_runs.id", ondelete="CASCADE"),
        nullable=False,
    ),
    sa.Column(
        "miner_uid",
        sa.Integer,
        sa.ForeignKey("node_table.uid", ondelete="CASCADE"),
        nullable=False,
    ),
    sa.Column("miner_hotkey", sa.VARCHAR(255), nullable=False),
    sa.Column("response_time", postgresql.TIMESTAMP(timezone=True), nullable=False),
    sa.Column("job_id", sa.VARCHAR(100), nullable=True),
    sa.Column("kerchunk_json_url", sa.Text, nullable=True),
    sa.Column("target_netcdf_url_template", sa.Text, nullable=True),
    sa.Column("kerchunk_json_retrieved", postgresql.JSONB, nullable=True),
    # Frozen per-file manifest (file path -> hash map) captured at first submission
    sa.Column("frozen_manifest_files", postgresql.JSONB, nullable=True),
    sa.Column("verification_hash_computed", sa.VARCHAR(64), nullable=True),
    sa.Column("verification_hash_claimed", sa.VARCHAR(64), nullable=True),
    sa.Column("verification_passed", sa.Boolean, nullable=True),
    sa.Column(
        "status", sa.VARCHAR(50), nullable=False, server_default=sa.text("'received'")
    ),
    sa.Column("error_message", sa.Text, nullable=True),
    sa.Column("input_hash_miner", sa.VARCHAR(64), nullable=True),
    sa.Column("input_hash_validator", sa.VARCHAR(64), nullable=True),
    sa.Column("input_hash_match", sa.Boolean, nullable=True),
    sa.Column(
        "retry_count",
        sa.Integer,
        server_default=sa.text("0"),
        nullable=True,
        comment="Number of retry attempts for this miner response",
    ),
    sa.Column(
        "next_retry_time",
        postgresql.TIMESTAMP(timezone=True),
        nullable=True,
        comment="UTC timestamp when the validator should attempt the next retry",
    ),
    sa.Column("last_polled_time", postgresql.TIMESTAMP(timezone=True), nullable=True),
    sa.Column(
        "inference_started_at",
        postgresql.TIMESTAMP(timezone=True),
        nullable=True,
        comment="UTC timestamp when the miner started inference processing",
    ),
    # Pipeline timing tracking for total completion time analysis
    sa.Column(
        "job_accepted_at",
        postgresql.TIMESTAMP(timezone=True),
        nullable=True,
        comment="UTC timestamp when the miner accepted the job (fetch_accepted)",
    ),
    sa.Column(
        "inference_completed_at",
        postgresql.TIMESTAMP(timezone=True),
        nullable=True,
        comment="UTC timestamp when inference was completed and forecast submitted",
    ),
    sa.Column(
        "day1_scoring_completed_at",
        postgresql.TIMESTAMP(timezone=True),
        nullable=True,
        comment="UTC timestamp when Day1 scoring was completed",
    ),
    sa.Column(
        "era5_scoring_completed_at",
        postgresql.TIMESTAMP(timezone=True),
        nullable=True,
        comment="UTC timestamp when ERA5 scoring was completed",
    ),
    sa.Column(
        "total_pipeline_duration_seconds",
        sa.Integer,
        nullable=True,
        comment="Total time from job acceptance to final scoring completion in seconds",
    ),
    sa.UniqueConstraint(
        "run_id", "miner_uid", name="uq_weather_miner_responses_run_miner"
    ),
    comment="Records miner responses for a specific forecast run. Tracks status through fetch, hash verification, and inference.",
)
sa.Index("idx_wmr_run_id", weather_miner_responses_table.c.run_id)
sa.Index("idx_wmr_miner_uid", weather_miner_responses_table.c.miner_uid)
sa.Index("idx_wmr_miner_hotkey", weather_miner_responses_table.c.miner_hotkey)
sa.Index(
    "idx_wmr_verification_passed", weather_miner_responses_table.c.verification_passed
)
sa.Index("idx_wmr_status", weather_miner_responses_table.c.status)
sa.Index("idx_wmr_job_id", weather_miner_responses_table.c.job_id)
sa.Index("idx_wmr_retry_count", weather_miner_responses_table.c.retry_count)
sa.Index("idx_wmr_next_retry_time", weather_miner_responses_table.c.next_retry_time)

weather_miner_scores_table = sa.Table(
    "weather_miner_scores",
    validator_metadata,
    sa.Column(
        "id", sa.Integer, primary_key=True, autoincrement=True
    ),  # SERIAL PRIMARY KEY
    sa.Column(
        "response_id",
        sa.Integer,
        sa.ForeignKey("weather_miner_responses.id", ondelete="CASCADE"),
        nullable=False,
    ),
    sa.Column(
        "run_id",
        sa.Integer,
        sa.ForeignKey("weather_forecast_runs.id", ondelete="CASCADE"),
        nullable=False,
    ),
    sa.Column(
        "miner_uid",
        sa.Integer,
        sa.ForeignKey("node_table.uid", ondelete="CASCADE"),
        nullable=False,
    ),
    sa.Column("miner_hotkey", sa.VARCHAR(255), nullable=False),
    sa.Column("score_type", sa.VARCHAR(50), nullable=False),
    sa.Column("calculation_time", postgresql.TIMESTAMP(timezone=True), nullable=False),
    sa.Column("metrics", postgresql.JSONB, nullable=True),
    sa.Column("score", sa.Float, nullable=True),
    sa.Column("error_message", sa.Text, nullable=True),
    sa.Column("lead_hours", sa.Integer(), nullable=True),
    sa.Column("variable_level", sa.String(length=50), nullable=True),
    sa.Column("valid_time_utc", sa.TIMESTAMP(timezone=True), nullable=True),
    sa.UniqueConstraint(
        "response_id",
        "score_type",
        "lead_hours",
        "variable_level",
        "valid_time_utc",
        name="uq_wms_response_scoretype_lead_var_time",
    ),
    comment="Stores calculated scores (e.g., gfs_rmse, era5_rmse) for each miner response, detailed by lead time and variable.",
)
sa.Index("idx_wms_run_id", weather_miner_scores_table.c.run_id)
sa.Index("idx_wms_miner_uid", weather_miner_scores_table.c.miner_uid)
sa.Index("idx_wms_miner_hotkey", weather_miner_scores_table.c.miner_hotkey)
sa.Index("idx_wms_calculation_time", weather_miner_scores_table.c.calculation_time)
sa.Index("idx_wms_score_type", weather_miner_scores_table.c.score_type)
sa.Index("idx_wms_lead_hours", weather_miner_scores_table.c.lead_hours)
sa.Index("idx_wms_variable_level", weather_miner_scores_table.c.variable_level)
sa.Index("idx_wms_valid_time_utc", weather_miner_scores_table.c.valid_time_utc)

weather_scoring_jobs_table = sa.Table(
    "weather_scoring_jobs",
    validator_metadata,
    sa.Column(
        "id",
        sa.Integer,
        primary_key=True,
        autoincrement=True,
        comment="Serial ID for the scoring job",
    ),
    sa.Column(
        "run_id",
        sa.Integer,
        sa.ForeignKey("weather_forecast_runs.id", ondelete="CASCADE"),
        nullable=False,
        comment="ID of the forecast run being scored",
    ),
    sa.Column(
        "score_type",
        sa.VARCHAR(50),
        nullable=False,
        comment="Type of scoring job (e.g., 'day1_qc', 'era5_final')",
    ),
    sa.Column(
        "status",
        sa.VARCHAR(20),
        nullable=False,
        server_default=sa.text("'queued'"),
        comment="Current status of the scoring job",
    ),
    sa.Column(
        "started_at",
        postgresql.TIMESTAMP(timezone=True),
        nullable=True,
        comment="When the scoring job was started",
    ),
    sa.Column(
        "completed_at",
        postgresql.TIMESTAMP(timezone=True),
        nullable=True,
        comment="When the scoring job was completed",
    ),
    sa.Column(
        "error_message",
        sa.Text,
        nullable=True,
        comment="Error message if the job failed",
    ),
    sa.Column(
        "created_at",
        postgresql.TIMESTAMP(timezone=True),
        server_default=sa.func.current_timestamp(),
        nullable=False,
        comment="When the scoring job was created",
    ),
    sa.UniqueConstraint("run_id", "score_type", name="uq_wsj_run_score_type"),
    comment="Tracks scoring jobs for restart resilience - ensures no scoring work is lost during validator restarts.",
)
sa.Index("idx_wsj_run_id", weather_scoring_jobs_table.c.run_id)
sa.Index("idx_wsj_score_type", weather_scoring_jobs_table.c.score_type)
sa.Index("idx_wsj_status", weather_scoring_jobs_table.c.status)
sa.Index(
    "idx_wsj_status_started",
    weather_scoring_jobs_table.c.status,
    weather_scoring_jobs_table.c.started_at,
)
sa.Index("idx_wsj_created_at", weather_scoring_jobs_table.c.created_at)

weather_ensemble_forecasts_table = sa.Table(
    "weather_ensemble_forecasts",
    validator_metadata,
    sa.Column(
        "id", sa.Integer, primary_key=True, autoincrement=True
    ),  # SERIAL PRIMARY KEY
    sa.Column(
        "forecast_run_id",
        sa.Integer,
        sa.ForeignKey("weather_forecast_runs.id", ondelete="CASCADE"),
        nullable=False,
    ),
    sa.Column(
        "creation_time",
        postgresql.TIMESTAMP(timezone=True),
        server_default=sa.func.current_timestamp(),
        nullable=False,
    ),
    sa.Column(
        "processing_end_time", postgresql.TIMESTAMP(timezone=True), nullable=True
    ),
    sa.Column("ensemble_path", sa.Text, nullable=True),
    sa.Column("ensemble_kerchunk_path", sa.Text, nullable=True),
    sa.Column("ensemble_verification_hash", sa.VARCHAR(64), nullable=True),
    sa.Column(
        "status", sa.VARCHAR(50), nullable=False, server_default=sa.text("'pending'")
    ),
    sa.Column("error_message", sa.Text, nullable=True),
    comment="Stores ensemble forecast information created by combining multiple miner forecasts.",
)
sa.Index("idx_wef_forecast_run_id", weather_ensemble_forecasts_table.c.forecast_run_id)

weather_ensemble_components_table = sa.Table(
    "weather_ensemble_components",
    validator_metadata,
    sa.Column(
        "id", sa.Integer, primary_key=True, autoincrement=True
    ),  # SERIAL PRIMARY KEY
    sa.Column(
        "ensemble_id",
        sa.Integer,
        sa.ForeignKey("weather_ensemble_forecasts.id", ondelete="CASCADE"),
        nullable=False,
    ),
    sa.Column(
        "response_id",
        sa.Integer,
        sa.ForeignKey("weather_miner_responses.id", ondelete="CASCADE"),
        nullable=False,
    ),
    sa.Column("weight", sa.Float, nullable=False),
    sa.Column(
        "created_at",
        postgresql.TIMESTAMP(timezone=True),
        server_default=sa.func.current_timestamp(),
        nullable=False,
    ),
    comment="Tracks which miner forecasts are included in an ensemble and their weights.",
)
sa.Index("idx_wec_ensemble_id", weather_ensemble_components_table.c.ensemble_id)
sa.Index("idx_wec_response_id", weather_ensemble_components_table.c.response_id)

weather_historical_weights_table = sa.Table(
    "weather_historical_weights",
    validator_metadata,
    sa.Column("miner_hotkey", sa.VARCHAR(255), nullable=False),
    sa.Column(
        "run_id",
        sa.Integer,
        sa.ForeignKey("weather_forecast_runs.id", ondelete="CASCADE"),
        nullable=False,
    ),
    sa.Column("score_type", sa.VARCHAR(50), nullable=False),
    sa.Column("score", sa.Float, nullable=True),
    sa.Column("weight", sa.Float, nullable=True),
    sa.Column("last_updated", postgresql.TIMESTAMP(timezone=True), nullable=False),
    # No explicit PK, composite PK (miner_hotkey, run_id, score_type) might be implied or desired
    # For Alembic, if no PK, it might require one for some operations. Let's assume for now it's okay.
    comment="Stores calculated scores and weights for miners on a per-run basis (e.g., initial GFS score).",
)
sa.Index("idx_whw_miner_hotkey", weather_historical_weights_table.c.miner_hotkey)
sa.Index("idx_whw_run_id", weather_historical_weights_table.c.run_id)
sa.Index("idx_whw_score_type", weather_historical_weights_table.c.score_type)

# --- Geomagnetic Tables ---
geomagnetic_predictions_table = sa.Table(
    "geomagnetic_predictions",
    validator_metadata,
    sa.Column(
        "id", sa.Text, primary_key=True, nullable=False
    ),  # TEXT UNIQUE NOT NULL -> PK
    sa.Column(
        "miner_uid",
        sa.Integer,
        sa.ForeignKey("node_table.uid", ondelete="CASCADE"),
        nullable=False,
    ),
    sa.Column("miner_hotkey", sa.Text, nullable=False),
    sa.Column("predicted_value", sa.Float, nullable=False),
    sa.Column(
        "query_time",
        postgresql.TIMESTAMP(timezone=True),
        server_default=sa.func.current_timestamp(),
        nullable=False,
    ),
    sa.Column("status", sa.Text, nullable=False, server_default=sa.text("'pending'")),
    sa.Column("retry_count", sa.Integer, server_default=sa.text("0"), nullable=True),
    sa.Column("next_retry_time", postgresql.TIMESTAMP(timezone=True), nullable=True),
    sa.Column("last_retry_attempt", postgresql.TIMESTAMP(timezone=True), nullable=True),
    sa.Column("retry_error_message", sa.Text, nullable=True),
)
# Existing indexes
sa.Index("idx_gp_miner_uid", geomagnetic_predictions_table.c.miner_uid)
sa.Index("idx_gp_miner_hotkey", geomagnetic_predictions_table.c.miner_hotkey)
sa.Index("idx_gp_query_time", geomagnetic_predictions_table.c.query_time)
sa.Index("idx_gp_status", geomagnetic_predictions_table.c.status)
# Additional performance indexes for common query patterns
sa.Index(
    "idx_gp_status_query_time",
    geomagnetic_predictions_table.c.status,
    geomagnetic_predictions_table.c.query_time.desc(),
)
sa.Index(
    "idx_gp_miner_uid_query_time",
    geomagnetic_predictions_table.c.miner_uid,
    geomagnetic_predictions_table.c.query_time.desc(),
)
sa.Index(
    "idx_gp_query_time_status",
    geomagnetic_predictions_table.c.query_time.desc(),
    geomagnetic_predictions_table.c.status,
)
# Partial index for pending tasks only - using postgresql_where parameter
sa.Index(
    "idx_gp_pending_tasks",
    geomagnetic_predictions_table.c.query_time.desc(),
    postgresql_where=geomagnetic_predictions_table.c.status == "pending",
)

geomagnetic_history_table = sa.Table(
    "geomagnetic_history",
    validator_metadata,
    sa.Column(
        "id", sa.Integer, primary_key=True, autoincrement=True
    ),  # SERIAL PRIMARY KEY
    sa.Column(
        "miner_uid",
        sa.Integer,
        sa.ForeignKey("node_table.uid", ondelete="CASCADE"),
        nullable=False,
    ),
    sa.Column("miner_hotkey", sa.Text, nullable=False),
    sa.Column("query_time", postgresql.TIMESTAMP(timezone=True), nullable=False),
    sa.Column("predicted_value", sa.Float, nullable=False),
    sa.Column("ground_truth_value", sa.Float, nullable=False),
    sa.Column("score", sa.Float, nullable=False),
    sa.Column(
        "scored_at",
        postgresql.TIMESTAMP(timezone=True),
        server_default=sa.func.current_timestamp(),
        nullable=False,
    ),
)
# Additional indexes for geomagnetic_history performance
sa.Index(
    "idx_gh_query_time_miner_uid",
    geomagnetic_history_table.c.query_time,
    geomagnetic_history_table.c.miner_uid,
)
sa.Index(
    "idx_gh_miner_hotkey_scored_at",
    geomagnetic_history_table.c.miner_hotkey,
    geomagnetic_history_table.c.scored_at.desc(),
)

# --- Weather Forecast Statistics Tables ---
# These tables are designed for comprehensive tracking and analysis of miner performance
# in weather forecasting tasks, with a focus on efficient querying and web server integration

weather_forecast_stats_table = sa.Table(
    "weather_forecast_stats",
    validator_metadata,
    sa.Column(
        "id", sa.Integer, primary_key=True, autoincrement=True
    ),  # SERIAL PRIMARY KEY
    sa.Column(
        "miner_uid",
        sa.Integer,
        sa.ForeignKey("node_table.uid", ondelete="CASCADE"),
        nullable=False,
        comment="Miner's UID (0-255)",
    ),
    sa.Column("miner_hotkey", sa.VARCHAR(255), nullable=False, comment="Miner's hotkey"),
    sa.Column(
        "miner_rank",
        sa.Integer,
        nullable=True,
        comment="Miner's rank for this forecast run",
    ),
    sa.Column(
        "forecast_run_id",
        sa.VARCHAR(100),
        nullable=False,
        comment="Deterministic run ID shared across validators",
    ),
    sa.Column(
        "run_id",
        sa.Integer,
        sa.ForeignKey("weather_forecast_runs.id", ondelete="CASCADE"),
        nullable=True,
        comment="Link to weather_forecast_runs table",
    ),
    sa.Column(
        "forecast_init_time",
        postgresql.TIMESTAMP(timezone=True),
        nullable=False,
        comment="GFS initialization time for the forecast",
    ),
    sa.Column(
        "forecast_status",
        sa.VARCHAR(50),
        nullable=False,
        comment="Current stage: pending, input_received, day1_scored, era5_scoring, completed, failed",
    ),
    sa.Column(
        "current_forecast_stage",
        sa.VARCHAR(100),
        nullable=True,
        comment="Detailed pipeline stage: init, gfs_fetch, inference_requested, inference_running, day1_scoring, era5_scoring_24h, era5_scoring_48h, etc., complete",
    ),
    sa.Column(
        "current_forecast_status",
        sa.VARCHAR(50),
        nullable=True,
        comment="Status within current stage: initialized, in_progress, scoring, error, waiting_retry, completed",
    ),
    sa.Column(
        "last_error_message",
        sa.Text,
        nullable=True,
        comment="Most recent error message encountered",
    ),
    sa.Column(
        "retries_remaining",
        sa.Integer,
        server_default=sa.text("3"),
        nullable=True,
        comment="Number of retry attempts remaining (0-3)",
    ),
    sa.Column(
        "next_scheduled_retry",
        postgresql.TIMESTAMP(timezone=True),
        nullable=True,
        comment="UTC timestamp for next retry attempt",
    ),
    sa.Column(
        "forecast_error_msg",
        postgresql.JSONB,
        nullable=True,
        comment="Detailed error information with sensitive data redacted",
    ),
    # Individual scoring columns
    sa.Column(
        "forecast_score_initial",
        sa.Float,
        nullable=True,
        comment="Day 1 GFS comparison score",
    ),
    # ERA5 scores for each timestep (every 24 hours up to 240 hours)
    sa.Column(
        "era5_score_24h",
        sa.Float,
        nullable=True,
        comment="ERA5 score at 24 hour lead time",
    ),
    sa.Column(
        "era5_score_48h",
        sa.Float,
        nullable=True,
        comment="ERA5 score at 48 hour lead time",
    ),
    sa.Column(
        "era5_score_72h",
        sa.Float,
        nullable=True,
        comment="ERA5 score at 72 hour lead time",
    ),
    sa.Column(
        "era5_score_96h",
        sa.Float,
        nullable=True,
        comment="ERA5 score at 96 hour lead time",
    ),
    sa.Column(
        "era5_score_120h",
        sa.Float,
        nullable=True,
        comment="ERA5 score at 120 hour lead time",
    ),
    sa.Column(
        "era5_score_144h",
        sa.Float,
        nullable=True,
        comment="ERA5 score at 144 hour lead time",
    ),
    sa.Column(
        "era5_score_168h",
        sa.Float,
        nullable=True,
        comment="ERA5 score at 168 hour lead time",
    ),
    sa.Column(
        "era5_score_192h",
        sa.Float,
        nullable=True,
        comment="ERA5 score at 192 hour lead time",
    ),
    sa.Column(
        "era5_score_216h",
        sa.Float,
        nullable=True,
        comment="ERA5 score at 216 hour lead time",
    ),
    sa.Column(
        "era5_score_240h",
        sa.Float,
        nullable=True,
        comment="ERA5 score at 240 hour lead time",
    ),
    # Aggregate metrics
    sa.Column(
        "era5_combined_score",
        sa.Float,
        nullable=True,
        comment="Average across all ERA5 timesteps (including 0s for incomplete)",
    ),
    sa.Column(
        "era5_completeness",
        sa.Float,
        nullable=True,
        comment="Fraction of ERA5 timesteps completed (0-1)",
    ),
    # Unified normalized overall forecast score (0-1) for weighting
    sa.Column(
        "overall_forecast_score",
        sa.Float,
        nullable=True,
        comment="Unified normalized overall forecast score used for validator weighting (0-1)",
    ),
    # Average component metrics across all variables/timesteps
    sa.Column(
        "avg_rmse",
        sa.Float,
        nullable=True,
        comment="Average RMSE across all variables and timesteps",
    ),
    sa.Column(
        "avg_acc",
        sa.Float,
        nullable=True,
        comment="Average ACC across all variables and timesteps",
    ),
    sa.Column(
        "avg_skill_score",
        sa.Float,
        nullable=True,
        comment="Average skill score across all variables and timesteps",
    ),
    # Additional metadata
    sa.Column(
        "forecast_type",
        sa.VARCHAR(50),
        server_default=sa.text("'10day'"),
        nullable=False,
        comment="Type of forecast: 10day, hindcast, cyclone_path, etc.",
    ),
    sa.Column(
        "forecast_inference_duration_seconds",
        sa.Integer,
        nullable=True,
        comment="Time taken for inference in seconds",
    ),
    sa.Column(
        "forecast_input_sources",
        postgresql.JSONB,
        nullable=True,
        comment="Input data sources used for the forecast",
    ),
    sa.Column(
        "hosting_status",
        sa.VARCHAR(50),
        nullable=True,
        comment="Status of hosting: accessible, inaccessible, timeout, error",
    ),
    sa.Column(
        "hosting_latency_ms",
        sa.Integer,
        nullable=True,
        comment="Latency in milliseconds for accessing hosted files",
    ),
    sa.Column(
        "validator_hotkey",
        sa.Text,
        nullable=True,
        comment="Validator that generated these stats",
    ),
    sa.Column(
        "created_at",
        postgresql.TIMESTAMP(timezone=True),
        server_default=sa.func.current_timestamp(),
        nullable=False,
        comment="When this record was created",
    ),
    sa.Column(
        "updated_at",
        postgresql.TIMESTAMP(timezone=True),
        server_default=sa.func.current_timestamp(),
        nullable=False,
        comment="When this record was last updated",
    ),
    sa.UniqueConstraint(
        "miner_uid", "forecast_run_id", name="uq_wfs_miner_forecast"
    ),
    comment="Comprehensive weather forecast statistics for each miner per forecast run",
)

# Indexes for efficient querying
sa.Index("idx_wfs_miner_uid", weather_forecast_stats_table.c.miner_uid)
sa.Index("idx_wfs_miner_hotkey", weather_forecast_stats_table.c.miner_hotkey)
sa.Index("idx_wfs_forecast_run_id", weather_forecast_stats_table.c.forecast_run_id)
sa.Index("idx_wfs_run_id", weather_forecast_stats_table.c.run_id)
sa.Index("idx_wfs_forecast_init_time", weather_forecast_stats_table.c.forecast_init_time.desc())
sa.Index("idx_wfs_forecast_status", weather_forecast_stats_table.c.forecast_status)
sa.Index("idx_wfs_miner_rank", weather_forecast_stats_table.c.miner_rank)
sa.Index("idx_wfs_era5_combined_score", weather_forecast_stats_table.c.era5_combined_score.desc())
sa.Index("idx_wfs_created_at", weather_forecast_stats_table.c.created_at.desc())
sa.Index(
    "idx_wfs_forecast_type_init_time",
    weather_forecast_stats_table.c.forecast_type,
    weather_forecast_stats_table.c.forecast_init_time.desc(),
)

# Aggregated miner statistics table
miner_stats_table = sa.Table(
    "miner_stats",
    validator_metadata,
    sa.Column(
        "miner_uid",
        sa.Integer,
        sa.ForeignKey("node_table.uid", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
        comment="Miner's UID (0-255)",
    ),
    sa.Column("miner_hotkey", sa.VARCHAR(255), nullable=False, comment="Miner's hotkey"),
    sa.Column(
        "miner_rank",
        sa.Integer,
        nullable=True,
        comment="Current overall rank among all miners",
    ),
    # Forecast performance metrics
    sa.Column(
        "avg_forecast_score",
        sa.Float,
        nullable=True,
        comment="Average score across all forecasts",
    ),
    sa.Column(
        "successful_forecasts",
        sa.Integer,
        server_default=sa.text("0"),
        nullable=False,
        comment="Number of successfully completed forecasts",
    ),
    sa.Column(
        "failed_forecasts",
        sa.Integer,
        server_default=sa.text("0"),
        nullable=False,
        comment="Number of failed forecasts",
    ),
    sa.Column(
        "forecast_success_ratio",
        sa.Float,
        nullable=True,
        comment="Ratio of successful to total forecasts (0-1)",
    ),
    # Hosting reliability metrics
    sa.Column(
        "hosting_successes",
        sa.Integer,
        server_default=sa.text("0"),
        nullable=False,
        comment="Number of successful kerchunk file retrievals",
    ),
    sa.Column(
        "hosting_failures",
        sa.Integer,
        server_default=sa.text("0"),
        nullable=False,
        comment="Number of failed kerchunk file retrievals",
    ),
    sa.Column(
        "host_reliability_ratio",
        sa.Float,
        nullable=True,
        comment="Ratio of successful hosting attempts (0-1)",
    ),
    sa.Column(
        "avg_hosting_latency_ms",
        sa.Float,
        nullable=True,
        comment="Average latency for accessing hosted files in milliseconds",
    ),
    # Performance breakdowns
    sa.Column(
        "avg_day1_score",
        sa.Float,
        nullable=True,
        comment="Average day 1 GFS comparison score",
    ),
    sa.Column(
        "avg_era5_score",
        sa.Float,
        nullable=True,
        comment="Average ERA5 comparison score across all timesteps",
    ),
    sa.Column(
        "avg_era5_completeness",
        sa.Float,
        nullable=True,
        comment="Average completeness of ERA5 scoring (0-1)",
    ),
    sa.Column(
        "best_forecast_score",
        sa.Float,
        nullable=True,
        comment="Best single forecast score achieved",
    ),
    sa.Column(
        "worst_forecast_score",
        sa.Float,
        nullable=True,
        comment="Worst single forecast score",
    ),
    sa.Column(
        "score_std_dev",
        sa.Float,
        nullable=True,
        comment="Standard deviation of forecast scores",
    ),
    # Temporal patterns
    sa.Column(
        "last_successful_forecast",
        postgresql.TIMESTAMP(timezone=True),
        nullable=True,
        comment="Timestamp of last successful forecast",
    ),
    sa.Column(
        "last_failed_forecast",
        postgresql.TIMESTAMP(timezone=True),
        nullable=True,
        comment="Timestamp of last failed forecast",
    ),
    sa.Column(
        "consecutive_successes",
        sa.Integer,
        server_default=sa.text("0"),
        nullable=False,
        comment="Current streak of successful forecasts",
    ),
    sa.Column(
        "consecutive_failures",
        sa.Integer,
        server_default=sa.text("0"),
        nullable=False,
        comment="Current streak of failed forecasts",
    ),
    sa.Column(
        "total_inference_time",
        sa.BigInteger,
        server_default=sa.text("0"),
        nullable=False,
        comment="Total inference time across all forecasts in seconds",
    ),
    sa.Column(
        "avg_inference_time",
        sa.Float,
        nullable=True,
        comment="Average inference time per forecast in seconds",
    ),
    # Error tracking
    sa.Column(
        "common_errors",
        postgresql.JSONB,
        nullable=True,
        comment="Most common error types and their frequencies",
    ),
    sa.Column(
        "error_rate_by_type",
        postgresql.JSONB,
        nullable=True,
        comment="Error rates broken down by forecast type",
    ),
    # Metadata
    sa.Column(
        "first_seen",
        postgresql.TIMESTAMP(timezone=True),
        nullable=True,
        comment="When this miner was first seen",
    ),
    sa.Column(
        "last_active",
        postgresql.TIMESTAMP(timezone=True),
        nullable=True,
        comment="Last time this miner was active",
    ),
    sa.Column(
        "validator_hotkey",
        sa.Text,
        nullable=True,
        comment="Validator that generated these stats",
    ),
    sa.Column(
        "updated_at",
        postgresql.TIMESTAMP(timezone=True),
        server_default=sa.func.current_timestamp(),
        nullable=False,
        comment="When these stats were last updated",
    ),
    comment="Aggregated statistics for each miner across all weather forecasting tasks",
)

# Indexes for the miner stats table
sa.Index("idx_ms_miner_hotkey", miner_stats_table.c.miner_hotkey)
sa.Index("idx_ms_miner_rank", miner_stats_table.c.miner_rank)
sa.Index("idx_ms_avg_forecast_score", miner_stats_table.c.avg_forecast_score.desc())
sa.Index("idx_ms_forecast_success_ratio", miner_stats_table.c.forecast_success_ratio.desc())
sa.Index("idx_ms_host_reliability_ratio", miner_stats_table.c.host_reliability_ratio.desc())
sa.Index("idx_ms_last_active", miner_stats_table.c.last_active.desc())
sa.Index("idx_ms_consecutive_failures", miner_stats_table.c.consecutive_failures)
sa.Index(
    "idx_ms_active_miners",
    miner_stats_table.c.last_active.desc(),
    postgresql_where=miner_stats_table.c.last_active.isnot(None),
)

# Per-miner, per-run step/sub-step event log with retry and error tracking
weather_forecast_steps_table = sa.Table(
    "weather_forecast_steps",
    validator_metadata,
    sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
    sa.Column(
        "run_id",
        sa.Integer,
        sa.ForeignKey("weather_forecast_runs.id", ondelete="CASCADE"),
        nullable=False,
    ),
    sa.Column(
        "miner_uid",
        sa.Integer,
        sa.ForeignKey("node_table.uid", ondelete="CASCADE"),
        nullable=False,
    ),
    sa.Column("miner_hotkey", sa.VARCHAR(255), nullable=False),
    sa.Column("step_name", sa.VARCHAR(32), nullable=False),
    sa.Column("substep", sa.VARCHAR(64), nullable=True),
    sa.Column("lead_hours", sa.Integer, nullable=True),
    sa.Column("status", sa.VARCHAR(32), nullable=False),
    sa.Column("started_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
    sa.Column("completed_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
    sa.Column("retry_count", sa.Integer, server_default=sa.text("0"), nullable=False),
    sa.Column("next_retry_time", postgresql.TIMESTAMP(timezone=True), nullable=True),
    sa.Column("latency_ms", sa.Integer, nullable=True),
    sa.Column("error_json", postgresql.JSONB, nullable=True),
    sa.Column("context", postgresql.JSONB, nullable=True),
    sa.Column(
        "job_id",
        sa.BigInteger,
        sa.ForeignKey("validator_jobs.id", ondelete="SET NULL"),
        nullable=True,
    ),
    sa.UniqueConstraint(
        "run_id", "miner_uid", "step_name", "substep", "lead_hours",
        name="uq_wfsteps_run_miner_step_sub_lead",
    ),
    comment="Event log of step/sub-step progress, retries, and errors per miner per run",
)
sa.Index("idx_wfsteps_run_miner", weather_forecast_steps_table.c.run_id, weather_forecast_steps_table.c.miner_uid)
sa.Index("idx_wfsteps_step_status", weather_forecast_steps_table.c.step_name, weather_forecast_steps_table.c.status)
sa.Index("idx_wfsteps_next_retry", weather_forecast_steps_table.c.next_retry_time)

# --- Generic Validator Job Queue ---
validator_jobs_table = sa.Table(
    "validator_jobs",
    validator_metadata,
    sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
    sa.Column("job_type", sa.String(64), nullable=False),
    sa.Column("priority", sa.Integer, nullable=False, server_default="100"),
    sa.Column(
        "status",
        sa.String(32),
        nullable=False,
        server_default=sa.text("'pending'"),
    ),
    sa.Column(
        "payload",
        postgresql.JSONB,
        nullable=False,
        server_default=sa.text("'{}'::jsonb"),
    ),
    sa.Column("result", postgresql.JSONB, nullable=True),
    sa.Column("attempts", sa.Integer, nullable=False, server_default="0"),
    sa.Column("max_attempts", sa.Integer, nullable=False, server_default="5"),
    sa.Column(
        "created_at",
        postgresql.TIMESTAMP(timezone=True),
        server_default=sa.text("NOW()"),
        nullable=False,
    ),
    sa.Column("scheduled_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
    sa.Column("started_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
    sa.Column("completed_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
    sa.Column("last_error", sa.Text, nullable=True),
    sa.Column("next_retry_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
    sa.Column("lease_expires_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
    sa.Column("claimed_by", sa.String(64), nullable=True),
    # Singleton support for unique job instances
    sa.Column(
        "singleton_key", 
        sa.String(255), 
        nullable=True,
        comment="Unique key for singleton jobs (e.g., 'initiate_fetch_run_1')"
    ),
    # Optional domain links
    sa.Column(
        "run_id",
        sa.Integer,
        sa.ForeignKey("weather_forecast_runs.id", ondelete="SET NULL"),
        nullable=True,
    ),
    sa.Column(
        "miner_uid",
        sa.Integer,
        sa.ForeignKey("node_table.uid", ondelete="SET NULL"),
        nullable=True,
    ),
    sa.Column(
        "response_id",
        sa.Integer,
        sa.ForeignKey("weather_miner_responses.id", ondelete="SET NULL"),
        nullable=True,
    ),
    sa.Column(
        "step_id",
        sa.BigInteger,
        sa.ForeignKey("weather_forecast_steps.id", ondelete="SET NULL"),
        nullable=True,
    ),
    comment="Generic validator job queue with leases and retries.",
)
sa.Index("idx_vjobs_status_sched", validator_jobs_table.c.status, validator_jobs_table.c.scheduled_at)
sa.Index("idx_vjobs_type_status", validator_jobs_table.c.job_type, validator_jobs_table.c.status)
sa.Index("idx_vjobs_priority", validator_jobs_table.c.priority)
sa.Index("idx_vjobs_run", validator_jobs_table.c.run_id)
sa.Index("idx_vjobs_miner", validator_jobs_table.c.miner_uid)
sa.Index("idx_vjobs_response", validator_jobs_table.c.response_id)
# Unique constraint for singleton jobs - only one active job per singleton_key
sa.Index(
    "uq_vjobs_singleton_active",
    validator_jobs_table.c.singleton_key,
    unique=True,
    postgresql_where=sa.text("status IN ('pending', 'claimed', 'retry_scheduled')"),
)
sa.Index(
    "uq_vjobs_run_type_scoring",
    validator_jobs_table.c.run_id,
    validator_jobs_table.c.job_type,
    unique=True,
    postgresql_where=sa.text("job_type IN ('weather.scoring.day1_qc','weather.scoring.era5_final')"),
)

validator_job_logs_table = sa.Table(
    "validator_job_logs",
    validator_metadata,
    sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
    sa.Column(
        "job_id",
        sa.BigInteger,
        sa.ForeignKey("validator_jobs.id", ondelete="CASCADE"),
        nullable=False,
    ),
    sa.Column(
        "created_at",
        postgresql.TIMESTAMP(timezone=True),
        server_default=sa.text("NOW()"),
        nullable=False,
    ),
    sa.Column("level", sa.String(16), nullable=False),
    sa.Column("message", sa.Text, nullable=False),
    comment="Per-job log messages for auditing and debugging.",
)
sa.Index("idx_vjob_logs_job", validator_job_logs_table.c.job_id)

# --- Perturbation Seed Table (Anti-Weight-Copying Mechanism) ---
# REDESIGNED: Now stores multiple seeds with activation times for safe synchronization
perturb_seed_table = sa.Table(
    "perturb_seed",
    validator_metadata,
    sa.Column(
        "activation_hour",
        postgresql.TIMESTAMP(timezone=True),
        primary_key=True,
        nullable=False,
    ),
    sa.Column("seed_hex", sa.String(64), nullable=False),  # 32-byte hex = 64 chars
    sa.Column(
        "generated_at",
        postgresql.TIMESTAMP(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    extend_existing=True,
)

# Placeholder for trigger function/trigger definitions if we move them here or handle in Alembic only
# For now, the check_node_table_size function and its trigger are defined in the first migration directly.
# If we make this validator_schema.py the *absolute* source, we might represent them here too,
# but Alembic op.execute() is fine for one-off DDL like functions/triggers in migrations.

# TODO for user: Review all nullable=True/False, server_defaults, and CHAR/VARCHAR lengths.
# TODO for user: Review primary key definitions, especially for weather_historical_weights if a composite PK is desired.

# Weather Forecast Component Scores Table
# Stores detailed per-variable, per-lead-time metrics for full transparency
weather_forecast_component_scores_table = sa.Table(
    "weather_forecast_component_scores",
    validator_metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    
    # Foreign Keys for relationships
    sa.Column(
        "run_id",
        sa.Integer,
        sa.ForeignKey("weather_forecast_runs.id", ondelete="CASCADE"),
        nullable=False,
        comment="Reference to the forecast run",
    ),
    sa.Column(
        "response_id",
        sa.Integer,
        sa.ForeignKey("weather_miner_responses.id", ondelete="CASCADE"),
        nullable=False,
        comment="Reference to the miner response",
    ),
    sa.Column(
        "miner_uid",
        sa.Integer,
        sa.ForeignKey("node_table.uid", ondelete="CASCADE"),
        nullable=False,
        comment="Miner's UID (0-255)",
    ),
    sa.Column("miner_hotkey", sa.VARCHAR(255), nullable=False, comment="Miner's hotkey"),
    
    # Scoring context
    sa.Column(
        "score_type",
        sa.VARCHAR(50),
        nullable=False,
        comment="Type of scoring: 'day1' or 'era5'",
    ),
    sa.Column(
        "lead_hours",
        sa.Integer,
        nullable=False,
        comment="Forecast lead time in hours (6, 12, 24, 48, 72, etc.)",
    ),
    sa.Column(
        "valid_time_utc",
        postgresql.TIMESTAMP(timezone=True),
        nullable=False,
        comment="Valid time for this forecast",
    ),
    
    # Variable identification
    sa.Column(
        "variable_name",
        sa.VARCHAR(20),
        nullable=False,
        comment="Variable name: '2t', 'msl', 't', 'u', 'v', 'q', 'z', '10u', '10v'",
    ),
    sa.Column(
        "pressure_level",
        sa.Integer,
        nullable=True,
        comment="Pressure level in hPa (NULL for surface variables, e.g., 500, 850)",
    ),
    
    # Component Metrics
    sa.Column("rmse", sa.Float, nullable=True, comment="Root Mean Square Error"),
    sa.Column("mse", sa.Float, nullable=True, comment="Mean Square Error"),
    sa.Column(
        "acc",
        sa.Float,
        nullable=True,
        comment="Anomaly Correlation Coefficient (-1 to 1)",
    ),
    sa.Column(
        "skill_score",
        sa.Float,
        nullable=True,
        comment="Skill score vs reference forecast (typically 0 to 1, can be negative)",
    ),
    sa.Column(
        "skill_score_gfs",
        sa.Float,
        nullable=True,
        comment="Skill score specifically vs GFS forecast",
    ),
    sa.Column(
        "skill_score_climatology",
        sa.Float,
        nullable=True,
        comment="Skill score vs climatology",
    ),
    sa.Column("bias", sa.Float, nullable=True, comment="Mean bias of forecast"),
    sa.Column("mae", sa.Float, nullable=True, comment="Mean Absolute Error"),
    
    # Sanity check results
    sa.Column(
        "climatology_check_passed",
        sa.Boolean,
        nullable=True,
        comment="Whether forecast passed climatology bounds check",
    ),
    sa.Column(
        "pattern_correlation",
        sa.Float,
        nullable=True,
        comment="Pattern correlation with reference",
    ),
    sa.Column(
        "pattern_correlation_passed",
        sa.Boolean,
        nullable=True,
        comment="Whether pattern correlation exceeded threshold",
    ),
    
    # Penalties applied
    sa.Column(
        "clone_penalty",
        sa.Float,
        server_default=sa.text("0"),
        nullable=True,
        comment="Penalty applied for clone detection",
    ),
    sa.Column(
        "quality_penalty",
        sa.Float,
        server_default=sa.text("0"),
        nullable=True,
        comment="Penalty for quality issues",
    ),
    
    # Final weighted contribution
    sa.Column(
        "weighted_score",
        sa.Float,
        nullable=True,
        comment="This variable's weighted contribution to final score",
    ),
    sa.Column(
        "variable_weight",
        sa.Float,
        nullable=True,
        comment="Weight used for this variable in aggregation",
    ),
    
    # Metadata
    sa.Column(
        "calculated_at",
        postgresql.TIMESTAMP(timezone=True),
        server_default=sa.func.current_timestamp(),
        nullable=False,
        comment="When this metric was calculated",
    ),
    sa.Column(
        "calculation_duration_ms",
        sa.Integer,
        nullable=True,
        comment="How long this metric calculation took in milliseconds",
    ),
    
    # Unique constraint to prevent duplicates
    sa.UniqueConstraint(
        "response_id",
        "score_type",
        "lead_hours",
        "variable_name",
        "pressure_level",
        name="uq_wfcs_response_variable_lead",
    ),
    comment="Stores detailed component scores for each variable and lead time in weather forecasts",
)

# Indexes for efficient querying
sa.Index("idx_wfcs_run_id", weather_forecast_component_scores_table.c.run_id)
sa.Index("idx_wfcs_miner_uid", weather_forecast_component_scores_table.c.miner_uid)
sa.Index("idx_wfcs_miner_hotkey", weather_forecast_component_scores_table.c.miner_hotkey)
sa.Index(
    "idx_wfcs_score_type_lead",
    weather_forecast_component_scores_table.c.score_type,
    weather_forecast_component_scores_table.c.lead_hours,
)
sa.Index(
    "idx_wfcs_variable",
    weather_forecast_component_scores_table.c.variable_name,
    weather_forecast_component_scores_table.c.pressure_level,
)
sa.Index("idx_wfcs_valid_time", weather_forecast_component_scores_table.c.valid_time_utc)
sa.Index("idx_wfcs_calculated_at", weather_forecast_component_scores_table.c.calculated_at)

sa.Index("idx_node_table_uid", node_table.c.uid)
sa.Index("idx_node_table_uid_last_updated", node_table.c.uid, node_table.c.last_updated)
sa.Index(
    "idx_score_table_task_name_created_at_desc",
    score_table.c.task_name,
    sa.text("created_at DESC"),
)
