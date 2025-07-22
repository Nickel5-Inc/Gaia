import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# Central MetaData object for the validator schema
validator_metadata = sa.MetaData()

node_table = sa.Table('node_table', validator_metadata,
    sa.Column('uid', sa.Integer, primary_key=True, comment="Unique ID for the node, 0-255"),
    sa.Column('hotkey', sa.Text, nullable=True, comment="Hotkey of the node"),
    sa.Column('coldkey', sa.Text, nullable=True, comment="Coldkey of the node"),
    sa.Column('ip', sa.Text, nullable=True, comment="IP address of the node"),
    sa.Column('ip_type', sa.Text, nullable=True, comment="IP address type (e.g., IPv4, IPv6)"),
    sa.Column('port', sa.Integer, nullable=True, comment="Port number for the node's services"),
    sa.Column('incentive', sa.Float, nullable=True, comment="Current incentive score of the node"),
    sa.Column('stake', sa.Float, nullable=True, comment="Current stake of the node"),
    sa.Column('trust', sa.Float, nullable=True, comment="Current trust score of the node"),
    sa.Column('vtrust', sa.Float, nullable=True, comment="Current validator trust score of the node"),
    sa.Column('protocol', sa.Text, nullable=True, comment="Protocol version used by the node"),
    sa.Column('last_updated', postgresql.TIMESTAMP(timezone=True), 
              server_default=sa.func.current_timestamp(), 
              nullable=False, # Typically, last_updated should not be null
              comment="Timestamp of the last update for this node's record"),
    sa.CheckConstraint('uid >= 0 AND uid < 256', name='node_table_uid_check'),
    comment="Table storing information about registered nodes (miners/validators)."
)
sa.Index('idx_node_hotkey_on_node_table', node_table.c.hotkey) # For faster lookups by hotkey

score_table = sa.Table('score_table', validator_metadata,
    sa.Column('task_name', sa.VARCHAR(255), nullable=True, comment="Name of the task being scored"), # Added length
    sa.Column('task_id', sa.Text, nullable=True, comment="Unique ID for the specific task instance"),
    sa.Column('score', postgresql.ARRAY(sa.Float), nullable=True, comment="Array of scores, typically per UID"),
    sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), server_default=sa.func.current_timestamp(), nullable=False, comment="Timestamp of score creation"),
    sa.Column('status', sa.VARCHAR(50), server_default=sa.text("'pending'"), nullable=True, comment="Status of the scoring process"),
    sa.UniqueConstraint('task_name', 'task_id', name='uq_score_table_task_name_task_id'),
    # Indexes will be defined in Alembic migration if not implicitly created by PK/FK, or for specific performance needs.
    # Example: sa.Index('idx_score_created_at', 'created_at') # Can be added here too
    comment="Table to store scores for various tasks."
)
sa.Index('idx_score_created_at_on_score_table', score_table.c.created_at) # Explicit index definition
sa.Index('idx_score_task_name_created_at_desc_on_score_table', score_table.c.task_name, score_table.c.created_at.desc())

baseline_predictions_table = sa.Table('baseline_predictions', validator_metadata,
    sa.Column('id', sa.Integer, primary_key=True, autoincrement=True, comment="Serial ID for the prediction entry"),
    sa.Column('task_name', sa.Text, nullable=False, comment="Name of the task (e.g., geomagnetic, soil_moisture)"),
    sa.Column('task_id', sa.Text, nullable=False, comment="ID of the specific task execution"),
    sa.Column('region_id', sa.Text, nullable=True, comment="For region-specific tasks, the region identifier"),
    sa.Column('timestamp', postgresql.TIMESTAMP(timezone=True), nullable=False, comment="Timestamp for when the prediction was made/is valid for"),
    sa.Column('prediction', postgresql.JSONB, nullable=False, comment="The model's prediction data"),
    sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), server_default=sa.func.current_timestamp(), nullable=False, comment="Timestamp of prediction storage"),
    comment="Stores baseline model predictions for various tasks."
)
# Primary composite index for the most common query pattern
sa.Index('idx_baseline_task_on_baseline_predictions', baseline_predictions_table.c.task_name, baseline_predictions_table.c.task_id)
# Additional indexes for performance optimization
sa.Index('idx_baseline_task_name_only', baseline_predictions_table.c.task_name)
sa.Index('idx_baseline_created_at', baseline_predictions_table.c.created_at.desc())
sa.Index('idx_baseline_task_name_created_at', baseline_predictions_table.c.task_name, baseline_predictions_table.c.created_at.desc())
sa.Index('idx_baseline_region_task', baseline_predictions_table.c.region_id, baseline_predictions_table.c.task_name)

# --- Soil Moisture Tables ---
soil_moisture_regions_table = sa.Table('soil_moisture_regions', validator_metadata,
    sa.Column('id', sa.Integer, primary_key=True, autoincrement=True), # SERIAL PRIMARY KEY
    sa.Column('region_date', sa.Date, nullable=False),
    sa.Column('target_time', postgresql.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('bbox', postgresql.JSONB, nullable=False),
    sa.Column('combined_data', postgresql.BYTEA, nullable=True), # Assuming BYTEA can be nullable based on usage
    sa.Column('sentinel_bounds', postgresql.ARRAY(sa.Float), nullable=True),
    sa.Column('sentinel_crs', sa.Integer, nullable=True),
    sa.Column('status', sa.Text, nullable=False, server_default=sa.text("'pending'")),
    sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), server_default=sa.func.current_timestamp(), nullable=False),
    sa.Column('data_cleared_at', postgresql.TIMESTAMP(timezone=True), nullable=True),
    sa.Column('array_shape', postgresql.ARRAY(sa.Integer), nullable=False)
)
sa.Index('idx_smr_region_date', soil_moisture_regions_table.c.region_date)
sa.Index('idx_smr_target_time', soil_moisture_regions_table.c.target_time)
sa.Index('idx_smr_status', soil_moisture_regions_table.c.status)

soil_moisture_predictions_table = sa.Table('soil_moisture_predictions', validator_metadata,
    sa.Column('id', sa.Integer, primary_key=True, autoincrement=True), # SERIAL PRIMARY KEY
    sa.Column('region_id', sa.Integer, sa.ForeignKey('soil_moisture_regions.id', ondelete='SET NULL'), nullable=True),
    sa.Column('miner_uid', sa.Text, nullable=False),
    sa.Column('miner_hotkey', sa.Text, nullable=False),
    sa.Column('target_time', postgresql.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('surface_sm', postgresql.ARRAY(sa.Float, dimensions=2), nullable=True), # Corrected for FLOAT[][]
    sa.Column('rootzone_sm', postgresql.ARRAY(sa.Float, dimensions=2), nullable=True), # Corrected for FLOAT[][]
    sa.Column('uncertainty_surface', postgresql.ARRAY(sa.Float, dimensions=2), nullable=True), # Corrected for FLOAT[][]
    sa.Column('uncertainty_rootzone', postgresql.ARRAY(sa.Float, dimensions=2), nullable=True), # Corrected for FLOAT[][]
    sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), server_default=sa.func.current_timestamp(), nullable=False),
    sa.Column('sentinel_bounds', postgresql.ARRAY(sa.Float), nullable=True),
    sa.Column('sentinel_crs', sa.Integer, nullable=True),
    sa.Column('status', sa.Text, nullable=False, server_default=sa.text("'sent_to_miner'")),
    sa.Column('retry_count', sa.Integer, server_default=sa.text('0'), nullable=True),
    sa.Column('next_retry_time', postgresql.TIMESTAMP(timezone=True), nullable=True),
    sa.Column('last_retry_attempt', postgresql.TIMESTAMP(timezone=True), nullable=True),
    sa.Column('retry_error_message', sa.Text, nullable=True),
    sa.Column('last_error', sa.Text, nullable=True)
)
sa.Index('idx_smp_region_id', soil_moisture_predictions_table.c.region_id)
sa.Index('idx_smp_miner_uid', soil_moisture_predictions_table.c.miner_uid)
sa.Index('idx_smp_miner_hotkey', soil_moisture_predictions_table.c.miner_hotkey)
sa.Index('idx_smp_target_time', soil_moisture_predictions_table.c.target_time)
sa.Index('idx_smp_status', soil_moisture_predictions_table.c.status)

soil_moisture_history_table = sa.Table('soil_moisture_history', validator_metadata,
    sa.Column('id', sa.Integer, primary_key=True, autoincrement=True), # SERIAL PRIMARY KEY
    sa.Column('region_id', sa.Integer, sa.ForeignKey('soil_moisture_regions.id', ondelete='CASCADE'), nullable=False),
    sa.Column('miner_uid', sa.Text, nullable=False),
    sa.Column('miner_hotkey', sa.Text, nullable=False),
    sa.Column('target_time', postgresql.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('surface_sm_pred', postgresql.ARRAY(sa.Float, dimensions=2), nullable=True), # Corrected for FLOAT[][]
    sa.Column('rootzone_sm_pred', postgresql.ARRAY(sa.Float, dimensions=2), nullable=True), # Corrected for FLOAT[][]
    sa.Column('surface_sm_truth', postgresql.ARRAY(sa.Float, dimensions=2), nullable=True), # Corrected for FLOAT[][]
    sa.Column('rootzone_sm_truth', postgresql.ARRAY(sa.Float, dimensions=2), nullable=True), # Corrected for FLOAT[][]
    sa.Column('surface_rmse', sa.Float, nullable=True),
    sa.Column('rootzone_rmse', sa.Float, nullable=True),
    sa.Column('surface_structure_score', sa.Float, nullable=True),
    sa.Column('rootzone_structure_score', sa.Float, nullable=True),
    sa.Column('sentinel_bounds', postgresql.ARRAY(sa.Float), nullable=True),
    sa.Column('sentinel_crs', sa.Integer, nullable=True),
    sa.Column('scored_at', postgresql.TIMESTAMP(timezone=True), server_default=sa.func.current_timestamp(), nullable=False),
    sa.UniqueConstraint('region_id', 'miner_uid', 'target_time', name='uq_smh_region_miner_target_time')
)
sa.Index('idx_smh_region_id', soil_moisture_history_table.c.region_id)
sa.Index('idx_smh_miner_uid', soil_moisture_history_table.c.miner_uid)
sa.Index('idx_smh_miner_hotkey', soil_moisture_history_table.c.miner_hotkey)
sa.Index('idx_smh_target_time', soil_moisture_history_table.c.target_time)

# --- Weather Tables ---
weather_forecast_runs_table = sa.Table('weather_forecast_runs', validator_metadata,
    sa.Column('id', sa.Integer, primary_key=True, autoincrement=True), # SERIAL PRIMARY KEY
    sa.Column('run_initiation_time', postgresql.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('target_forecast_time_utc', postgresql.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('gfs_init_time_utc', postgresql.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('gfs_input_metadata', postgresql.JSONB, nullable=True),
    sa.Column('status', sa.VARCHAR(50), nullable=False, server_default=sa.text("'pending'")),
    sa.Column('completion_time', postgresql.TIMESTAMP(timezone=True), nullable=True),
    sa.Column('final_scoring_attempted_time', postgresql.TIMESTAMP(timezone=True), nullable=True),
    sa.Column('error_message', sa.Text, nullable=True),
    comment="Tracks each weather forecast run initiated by the validator."
)
sa.Index('idx_wfr_run_init_time', weather_forecast_runs_table.c.run_initiation_time)
sa.Index('idx_wfr_target_forecast_time', weather_forecast_runs_table.c.target_forecast_time_utc)
sa.Index('idx_wfr_gfs_init_time', weather_forecast_runs_table.c.gfs_init_time_utc)
sa.Index('idx_wfr_status', weather_forecast_runs_table.c.status)
sa.Index('idx_wfr_status_init_time', weather_forecast_runs_table.c.status, weather_forecast_runs_table.c.run_initiation_time)

weather_miner_responses_table = sa.Table('weather_miner_responses', validator_metadata,
    sa.Column('id', sa.Integer, primary_key=True, autoincrement=True), # SERIAL PRIMARY KEY
    sa.Column('run_id', sa.Integer, sa.ForeignKey('weather_forecast_runs.id', ondelete='CASCADE'), nullable=False),
    sa.Column('miner_uid', sa.Integer, nullable=False),
    sa.Column('miner_hotkey', sa.VARCHAR(255), nullable=False),
    sa.Column('response_time', postgresql.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('job_id', sa.VARCHAR(100), nullable=True),
    sa.Column('kerchunk_json_url', sa.Text, nullable=True),
    sa.Column('target_netcdf_url_template', sa.Text, nullable=True),
    sa.Column('kerchunk_json_retrieved', postgresql.JSONB, nullable=True),
    sa.Column('verification_hash_computed', sa.VARCHAR(64), nullable=True),
    sa.Column('verification_hash_claimed', sa.VARCHAR(64), nullable=True),
    sa.Column('verification_passed', sa.Boolean, nullable=True),
    sa.Column('status', sa.VARCHAR(50), nullable=False, server_default=sa.text("'received'")),
    sa.Column('error_message', sa.Text, nullable=True),
    sa.Column('input_hash_miner', sa.VARCHAR(64), nullable=True),
    sa.Column('input_hash_validator', sa.VARCHAR(64), nullable=True),
    sa.Column('input_hash_match', sa.Boolean, nullable=True),
    sa.Column('retry_count', sa.Integer, server_default=sa.text('0'), nullable=True,
              comment="Number of retry attempts for this miner response"),
    sa.Column('next_retry_time', postgresql.TIMESTAMP(timezone=True), nullable=True,
              comment="UTC timestamp when the validator should attempt the next retry"),
    sa.Column('last_polled_time', postgresql.TIMESTAMP(timezone=True), nullable=True),
    sa.UniqueConstraint('run_id', 'miner_uid', name='uq_weather_miner_responses_run_miner'),
    comment="Records miner responses for a specific forecast run. Tracks status through fetch, hash verification, and inference."
)
sa.Index('idx_wmr_run_id', weather_miner_responses_table.c.run_id)
sa.Index('idx_wmr_miner_uid', weather_miner_responses_table.c.miner_uid)
sa.Index('idx_wmr_miner_hotkey', weather_miner_responses_table.c.miner_hotkey)
sa.Index('idx_wmr_verification_passed', weather_miner_responses_table.c.verification_passed)
sa.Index('idx_wmr_status', weather_miner_responses_table.c.status)
sa.Index('idx_wmr_job_id', weather_miner_responses_table.c.job_id)
sa.Index('idx_wmr_retry_count', weather_miner_responses_table.c.retry_count)
sa.Index('idx_wmr_next_retry_time', weather_miner_responses_table.c.next_retry_time)

weather_miner_scores_table = sa.Table('weather_miner_scores', validator_metadata,
    sa.Column('id', sa.Integer, primary_key=True, autoincrement=True), # SERIAL PRIMARY KEY
    sa.Column('response_id', sa.Integer, sa.ForeignKey('weather_miner_responses.id', ondelete='CASCADE'), nullable=False),
    sa.Column('run_id', sa.Integer, sa.ForeignKey('weather_forecast_runs.id', ondelete='CASCADE'), nullable=False),
    sa.Column('miner_uid', sa.Integer, nullable=False),
    sa.Column('miner_hotkey', sa.VARCHAR(255), nullable=False),
    sa.Column('score_type', sa.VARCHAR(50), nullable=False),
    sa.Column('calculation_time', postgresql.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('metrics', postgresql.JSONB, nullable=True),
    sa.Column('score', sa.Float, nullable=True),
    sa.Column('error_message', sa.Text, nullable=True),
    sa.Column('lead_hours', sa.Integer(), nullable=True),
    sa.Column('variable_level', sa.String(length=50), nullable=True),
    sa.Column('valid_time_utc', sa.TIMESTAMP(timezone=True), nullable=True),
    sa.UniqueConstraint('response_id', 'score_type', 'lead_hours', 'variable_level', 'valid_time_utc', name='uq_wms_response_scoretype_lead_var_time'),
    comment="Stores calculated scores (e.g., gfs_rmse, era5_rmse) for each miner response, detailed by lead time and variable."
)
sa.Index('idx_wms_run_id', weather_miner_scores_table.c.run_id)
sa.Index('idx_wms_miner_uid', weather_miner_scores_table.c.miner_uid)
sa.Index('idx_wms_miner_hotkey', weather_miner_scores_table.c.miner_hotkey)
sa.Index('idx_wms_calculation_time', weather_miner_scores_table.c.calculation_time)
sa.Index('idx_wms_score_type', weather_miner_scores_table.c.score_type)
sa.Index('idx_wms_lead_hours', weather_miner_scores_table.c.lead_hours)
sa.Index('idx_wms_variable_level', weather_miner_scores_table.c.variable_level)
sa.Index('idx_wms_valid_time_utc', weather_miner_scores_table.c.valid_time_utc)

weather_scoring_jobs_table = sa.Table('weather_scoring_jobs', validator_metadata,
    sa.Column('id', sa.Integer, primary_key=True, autoincrement=True, comment="Serial ID for the scoring job"),
    sa.Column('run_id', sa.Integer, sa.ForeignKey('weather_forecast_runs.id', ondelete='CASCADE'), nullable=False, comment="ID of the forecast run being scored"),
    sa.Column('score_type', sa.VARCHAR(50), nullable=False, comment="Type of scoring job (e.g., 'day1_qc', 'era5_final')"),
    sa.Column('status', sa.VARCHAR(20), nullable=False, server_default=sa.text("'queued'"), comment="Current status of the scoring job"),
    sa.Column('started_at', postgresql.TIMESTAMP(timezone=True), nullable=True, comment="When the scoring job was started"),
    sa.Column('completed_at', postgresql.TIMESTAMP(timezone=True), nullable=True, comment="When the scoring job was completed"),
    sa.Column('error_message', sa.Text, nullable=True, comment="Error message if the job failed"),
    sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), server_default=sa.func.current_timestamp(), nullable=False, comment="When the scoring job was created"),
    sa.UniqueConstraint('run_id', 'score_type', name='uq_wsj_run_score_type'),
    comment="Tracks scoring jobs for restart resilience - ensures no scoring work is lost during validator restarts."
)
sa.Index('idx_wsj_run_id', weather_scoring_jobs_table.c.run_id)
sa.Index('idx_wsj_score_type', weather_scoring_jobs_table.c.score_type)
sa.Index('idx_wsj_status', weather_scoring_jobs_table.c.status)
sa.Index('idx_wsj_status_started', weather_scoring_jobs_table.c.status, weather_scoring_jobs_table.c.started_at)
sa.Index('idx_wsj_created_at', weather_scoring_jobs_table.c.created_at)

weather_ensemble_forecasts_table = sa.Table('weather_ensemble_forecasts', validator_metadata,
    sa.Column('id', sa.Integer, primary_key=True, autoincrement=True), # SERIAL PRIMARY KEY
    sa.Column('forecast_run_id', sa.Integer, sa.ForeignKey('weather_forecast_runs.id', ondelete='CASCADE'), nullable=False),
    sa.Column('creation_time', postgresql.TIMESTAMP(timezone=True), server_default=sa.func.current_timestamp(), nullable=False),
    sa.Column('processing_end_time', postgresql.TIMESTAMP(timezone=True), nullable=True),
    sa.Column('ensemble_path', sa.Text, nullable=True),
    sa.Column('ensemble_kerchunk_path', sa.Text, nullable=True),
    sa.Column('ensemble_verification_hash', sa.VARCHAR(64), nullable=True),
    sa.Column('status', sa.VARCHAR(50), nullable=False, server_default=sa.text("'pending'")),
    sa.Column('error_message', sa.Text, nullable=True),
    comment="Stores ensemble forecast information created by combining multiple miner forecasts."
)
sa.Index('idx_wef_forecast_run_id', weather_ensemble_forecasts_table.c.forecast_run_id)

weather_ensemble_components_table = sa.Table('weather_ensemble_components', validator_metadata,
    sa.Column('id', sa.Integer, primary_key=True, autoincrement=True), # SERIAL PRIMARY KEY
    sa.Column('ensemble_id', sa.Integer, sa.ForeignKey('weather_ensemble_forecasts.id', ondelete='CASCADE'), nullable=False),
    sa.Column('response_id', sa.Integer, sa.ForeignKey('weather_miner_responses.id', ondelete='CASCADE'), nullable=False),
    sa.Column('weight', sa.Float, nullable=False),
    sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), server_default=sa.func.current_timestamp(), nullable=False),
    comment="Tracks which miner forecasts are included in an ensemble and their weights."
)
sa.Index('idx_wec_ensemble_id', weather_ensemble_components_table.c.ensemble_id)
sa.Index('idx_wec_response_id', weather_ensemble_components_table.c.response_id)

weather_historical_weights_table = sa.Table('weather_historical_weights', validator_metadata,
    sa.Column('miner_hotkey', sa.VARCHAR(255), nullable=False),
    sa.Column('run_id', sa.Integer, sa.ForeignKey('weather_forecast_runs.id', ondelete='CASCADE'), nullable=False),
    sa.Column('score_type', sa.VARCHAR(50), nullable=False),
    sa.Column('score', sa.Float, nullable=True),
    sa.Column('weight', sa.Float, nullable=True),
    sa.Column('last_updated', postgresql.TIMESTAMP(timezone=True), nullable=False),
    # No explicit PK, composite PK (miner_hotkey, run_id, score_type) might be implied or desired
    # For Alembic, if no PK, it might require one for some operations. Let's assume for now it's okay.
    comment="Stores calculated scores and weights for miners on a per-run basis (e.g., initial GFS score)."
)
sa.Index('idx_whw_miner_hotkey', weather_historical_weights_table.c.miner_hotkey)
sa.Index('idx_whw_run_id', weather_historical_weights_table.c.run_id)
sa.Index('idx_whw_score_type', weather_historical_weights_table.c.score_type)

# --- Geomagnetic Tables ---
geomagnetic_predictions_table = sa.Table('geomagnetic_predictions', validator_metadata,
    sa.Column('id', sa.Text, primary_key=True, nullable=False), # TEXT UNIQUE NOT NULL -> PK
    sa.Column('miner_uid', sa.Text, nullable=False),
    sa.Column('miner_hotkey', sa.Text, nullable=False),
    sa.Column('predicted_value', sa.Float, nullable=False),
    sa.Column('query_time', postgresql.TIMESTAMP(timezone=True), server_default=sa.func.current_timestamp(), nullable=False),
    sa.Column('status', sa.Text, nullable=False, server_default=sa.text("'pending'")),
    sa.Column('retry_count', sa.Integer, server_default=sa.text('0'), nullable=True),
    sa.Column('next_retry_time', postgresql.TIMESTAMP(timezone=True), nullable=True),
    sa.Column('last_retry_attempt', postgresql.TIMESTAMP(timezone=True), nullable=True),
    sa.Column('retry_error_message', sa.Text, nullable=True)
)
# Existing indexes
sa.Index('idx_gp_miner_uid', geomagnetic_predictions_table.c.miner_uid)
sa.Index('idx_gp_miner_hotkey', geomagnetic_predictions_table.c.miner_hotkey)
sa.Index('idx_gp_query_time', geomagnetic_predictions_table.c.query_time)
sa.Index('idx_gp_status', geomagnetic_predictions_table.c.status)
# Additional performance indexes for common query patterns
sa.Index('idx_gp_status_query_time', geomagnetic_predictions_table.c.status, geomagnetic_predictions_table.c.query_time.desc())
sa.Index('idx_gp_miner_uid_query_time', geomagnetic_predictions_table.c.miner_uid, geomagnetic_predictions_table.c.query_time.desc())
sa.Index('idx_gp_query_time_status', geomagnetic_predictions_table.c.query_time.desc(), geomagnetic_predictions_table.c.status)
# Partial index for pending tasks only - using postgresql_where parameter
sa.Index('idx_gp_pending_tasks', geomagnetic_predictions_table.c.query_time.desc(), postgresql_where=geomagnetic_predictions_table.c.status == 'pending')

geomagnetic_history_table = sa.Table('geomagnetic_history', validator_metadata,
    sa.Column('id', sa.Integer, primary_key=True, autoincrement=True), # SERIAL PRIMARY KEY
    sa.Column('miner_uid', sa.Text, nullable=False),
    sa.Column('miner_hotkey', sa.Text, nullable=False),
    sa.Column('query_time', postgresql.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('predicted_value', sa.Float, nullable=False),
    sa.Column('ground_truth_value', sa.Float, nullable=False),
    sa.Column('score', sa.Float, nullable=False),
    sa.Column('scored_at', postgresql.TIMESTAMP(timezone=True), server_default=sa.func.current_timestamp(), nullable=False)
)
# Additional indexes for geomagnetic_history performance
sa.Index('idx_gh_query_time_miner_uid', geomagnetic_history_table.c.query_time, geomagnetic_history_table.c.miner_uid)
sa.Index('idx_gh_miner_hotkey_scored_at', geomagnetic_history_table.c.miner_hotkey, geomagnetic_history_table.c.scored_at.desc())

# --- Miner Performance Statistics Table ---
miner_performance_stats_table = sa.Table('miner_performance_stats', validator_metadata,
    sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
    sa.Column('miner_uid', sa.Text, nullable=False, comment="Miner's UID"),
    sa.Column('miner_hotkey', sa.Text, nullable=False, comment="Miner's hotkey"),
    
    # Time period for this statistics snapshot
    sa.Column('period_start', postgresql.TIMESTAMP(timezone=True), nullable=False, comment="Start of the performance period"),
    sa.Column('period_end', postgresql.TIMESTAMP(timezone=True), nullable=False, comment="End of the performance period"),
    sa.Column('period_type', sa.VARCHAR(20), nullable=False, comment="Type of period: daily, weekly, monthly, all_time"),
    
    # Overall performance metrics
    sa.Column('total_tasks_attempted', sa.Integer, server_default=sa.text('0'), nullable=False, comment="Total tasks attempted across all types"),
    sa.Column('total_tasks_completed', sa.Integer, server_default=sa.text('0'), nullable=False, comment="Total tasks successfully completed"),
    sa.Column('total_tasks_scored', sa.Integer, server_default=sa.text('0'), nullable=False, comment="Total tasks that received scores"),
    sa.Column('overall_success_rate', sa.Float, nullable=True, comment="Completion rate (completed/attempted)"),
    sa.Column('overall_avg_score', sa.Float, nullable=True, comment="Weighted average score across all task types"),
    sa.Column('overall_rank', sa.Integer, nullable=True, comment="Overall rank among all miners for this period"),
    
    # Weather-specific metrics
    sa.Column('weather_tasks_attempted', sa.Integer, server_default=sa.text('0'), nullable=False),
    sa.Column('weather_tasks_completed', sa.Integer, server_default=sa.text('0'), nullable=False),
    sa.Column('weather_tasks_scored', sa.Integer, server_default=sa.text('0'), nullable=False),
    sa.Column('weather_avg_score', sa.Float, nullable=True, comment="Average weather forecast score"),
    sa.Column('weather_success_rate', sa.Float, nullable=True),
    sa.Column('weather_rank', sa.Integer, nullable=True, comment="Rank in weather forecasting"),
    sa.Column('weather_best_score', sa.Float, nullable=True, comment="Best weather score in period"),
    sa.Column('weather_latest_score', sa.Float, nullable=True, comment="Most recent weather score"),
    
    # Soil moisture-specific metrics  
    sa.Column('soil_moisture_tasks_attempted', sa.Integer, server_default=sa.text('0'), nullable=False),
    sa.Column('soil_moisture_tasks_completed', sa.Integer, server_default=sa.text('0'), nullable=False),
    sa.Column('soil_moisture_tasks_scored', sa.Integer, server_default=sa.text('0'), nullable=False),
    sa.Column('soil_moisture_avg_score', sa.Float, nullable=True, comment="Average combined soil moisture score"),
    sa.Column('soil_moisture_success_rate', sa.Float, nullable=True),
    sa.Column('soil_moisture_rank', sa.Integer, nullable=True),
    sa.Column('soil_moisture_surface_rmse_avg', sa.Float, nullable=True, comment="Average surface RMSE"),
    sa.Column('soil_moisture_rootzone_rmse_avg', sa.Float, nullable=True, comment="Average rootzone RMSE"),
    sa.Column('soil_moisture_best_score', sa.Float, nullable=True),
    sa.Column('soil_moisture_latest_score', sa.Float, nullable=True),
    
    # Geomagnetic-specific metrics
    sa.Column('geomagnetic_tasks_attempted', sa.Integer, server_default=sa.text('0'), nullable=False),
    sa.Column('geomagnetic_tasks_completed', sa.Integer, server_default=sa.text('0'), nullable=False),
    sa.Column('geomagnetic_tasks_scored', sa.Integer, server_default=sa.text('0'), nullable=False),
    sa.Column('geomagnetic_avg_score', sa.Float, nullable=True, comment="Average geomagnetic prediction score"),
    sa.Column('geomagnetic_success_rate', sa.Float, nullable=True),
    sa.Column('geomagnetic_rank', sa.Integer, nullable=True),
    sa.Column('geomagnetic_best_score', sa.Float, nullable=True),
    sa.Column('geomagnetic_latest_score', sa.Float, nullable=True),
    sa.Column('geomagnetic_avg_error', sa.Float, nullable=True, comment="Average prediction error magnitude"),
    
    # === NEW WEIGHT CALCULATION PIPELINE COLUMNS ===
    sa.Column('submitted_weight', sa.Float, nullable=True, comment="Final weight submitted to chain by this validator"),
    sa.Column('raw_calculated_weight', sa.Float, nullable=True, comment="Pre-normalization weight from scoring algorithm"),
    sa.Column('excellence_weight', sa.Float, nullable=True, comment="Weight from excellence pathway calculation"),
    sa.Column('diversity_weight', sa.Float, nullable=True, comment="Weight from diversity pathway calculation"),
    sa.Column('scoring_pathway', sa.VARCHAR(20), nullable=True, comment="Which pathway was selected: excellence, diversity, or none"),
    sa.Column('pathway_details', postgresql.JSONB, nullable=True, comment="Detailed breakdown of pathway calculation"),
    
    # === NEW CHAIN CONSENSUS INTEGRATION COLUMNS ===
    sa.Column('incentive', sa.Float, nullable=True, comment="Final incentive value from chain consensus"),
    sa.Column('consensus_rank', sa.Integer, nullable=True, comment="Miner rank based on final incentive values"),
    sa.Column('weight_submission_block', sa.BigInteger, nullable=True, comment="Block number when weights were submitted"),
    sa.Column('consensus_block', sa.BigInteger, nullable=True, comment="Block number when consensus was calculated"),
    
    # === NEW TASK WEIGHT CONTRIBUTIONS COLUMNS ===
    sa.Column('weather_weight_contribution', sa.Float, nullable=True, comment="Contribution of weather task to final weight"),
    sa.Column('geomagnetic_weight_contribution', sa.Float, nullable=True, comment="Contribution of geomagnetic task to final weight"),
    sa.Column('soil_weight_contribution', sa.Float, nullable=True, comment="Contribution of soil moisture task to final weight"),
    sa.Column('multi_task_bonus', sa.Float, nullable=True, comment="Bonus for performing multiple tasks well"),
    
    # === NEW PERFORMANCE ANALYSIS COLUMNS ===
    sa.Column('percentile_rank_weather', sa.Float, nullable=True, comment="Percentile rank in weather forecasting (0-100)"),
    sa.Column('percentile_rank_geomagnetic', sa.Float, nullable=True, comment="Percentile rank in geomagnetic predictions (0-100)"),
    sa.Column('percentile_rank_soil', sa.Float, nullable=True, comment="Percentile rank in soil moisture predictions (0-100)"),
    sa.Column('excellence_qualified_tasks', postgresql.ARRAY(sa.Text), nullable=True, comment="Array of tasks where miner qualified for excellence pathway"),
    sa.Column('validator_hotkey', sa.Text, nullable=True, comment="Which validator calculated these stats"),
    
    # Performance trends and metadata
    sa.Column('performance_trend', sa.VARCHAR(20), nullable=True, comment="improving, declining, stable, insufficient_data"),
    sa.Column('trend_confidence', sa.Float, nullable=True, comment="Confidence in trend assessment (0-1)"),
    sa.Column('last_active_time', postgresql.TIMESTAMP(timezone=True), nullable=True, comment="Last time miner submitted a task"),
    sa.Column('consecutive_failures', sa.Integer, server_default=sa.text('0'), nullable=False, comment="Number of consecutive failed tasks"),
    sa.Column('uptime_percentage', sa.Float, nullable=True, comment="Percentage of time miner was responsive"),
    
    # Detailed metrics storage
    sa.Column('detailed_metrics', postgresql.JSONB, nullable=True, comment="Additional detailed metrics and breakdowns"),
    sa.Column('score_distribution', postgresql.JSONB, nullable=True, comment="Score percentiles and distribution stats"),
    
    # Metadata
    sa.Column('calculated_at', postgresql.TIMESTAMP(timezone=True), server_default=sa.func.current_timestamp(), nullable=False),
    sa.Column('updated_at', postgresql.TIMESTAMP(timezone=True), server_default=sa.func.current_timestamp(), nullable=False),
    
    # Ensure one record per miner per period
    sa.UniqueConstraint('miner_uid', 'period_start', 'period_end', 'period_type', name='uq_mps_miner_period'),
    
    # Data integrity constraints
    sa.CheckConstraint("scoring_pathway IN ('excellence', 'diversity', 'none') OR scoring_pathway IS NULL", name='chk_scoring_pathway'),
    sa.CheckConstraint(
        "(percentile_rank_weather IS NULL OR (percentile_rank_weather >= 0 AND percentile_rank_weather <= 100)) AND "
        "(percentile_rank_geomagnetic IS NULL OR (percentile_rank_geomagnetic >= 0 AND percentile_rank_geomagnetic <= 100)) AND "
        "(percentile_rank_soil IS NULL OR (percentile_rank_soil >= 0 AND percentile_rank_soil <= 100))",
        name='chk_percentile_ranges'
    ),
    
    comment="Comprehensive miner performance statistics aggregated across all task types for visualization and analysis."
)

# Indexes for the miner performance stats table
sa.Index('idx_mps_miner_uid', miner_performance_stats_table.c.miner_uid)
sa.Index('idx_mps_miner_hotkey', miner_performance_stats_table.c.miner_hotkey)
sa.Index('idx_mps_period_type_start', miner_performance_stats_table.c.period_type, miner_performance_stats_table.c.period_start.desc())
sa.Index('idx_mps_overall_rank', miner_performance_stats_table.c.overall_rank, miner_performance_stats_table.c.period_type)
sa.Index('idx_mps_overall_score', miner_performance_stats_table.c.overall_avg_score.desc(), miner_performance_stats_table.c.period_type)
sa.Index('idx_mps_last_active', miner_performance_stats_table.c.last_active_time.desc())
sa.Index('idx_mps_calculated_at', miner_performance_stats_table.c.calculated_at.desc())
sa.Index('idx_mps_weather_rank', miner_performance_stats_table.c.weather_rank, miner_performance_stats_table.c.period_type)
sa.Index('idx_mps_soil_rank', miner_performance_stats_table.c.soil_moisture_rank, miner_performance_stats_table.c.period_type)
sa.Index('idx_mps_geomagnetic_rank', miner_performance_stats_table.c.geomagnetic_rank, miner_performance_stats_table.c.period_type)

# === NEW INDEXES FOR WEIGHT TRACKING AND CHAIN INTEGRATION ===
sa.Index('idx_mps_submitted_weight', miner_performance_stats_table.c.submitted_weight.desc(), postgresql_where=miner_performance_stats_table.c.submitted_weight.isnot(None))
sa.Index('idx_mps_scoring_pathway', miner_performance_stats_table.c.scoring_pathway, miner_performance_stats_table.c.period_type)
sa.Index('idx_mps_incentive', miner_performance_stats_table.c.incentive.desc(), postgresql_where=miner_performance_stats_table.c.incentive.isnot(None))
sa.Index('idx_mps_consensus_rank', miner_performance_stats_table.c.consensus_rank, miner_performance_stats_table.c.period_type, postgresql_where=miner_performance_stats_table.c.consensus_rank.isnot(None))
sa.Index('idx_mps_validator_hotkey', miner_performance_stats_table.c.validator_hotkey, miner_performance_stats_table.c.period_start.desc())
sa.Index('idx_mps_weight_submission_block', miner_performance_stats_table.c.weight_submission_block.desc(), postgresql_where=miner_performance_stats_table.c.weight_submission_block.isnot(None))

# === COMPOSITE INDEXES FOR COMMON QUERY PATTERNS ===
sa.Index('idx_mps_pathway_performance', miner_performance_stats_table.c.scoring_pathway, miner_performance_stats_table.c.submitted_weight.desc(), miner_performance_stats_table.c.period_type)
sa.Index('idx_mps_chain_integration', miner_performance_stats_table.c.weight_submission_block, miner_performance_stats_table.c.consensus_block, postgresql_where=miner_performance_stats_table.c.weight_submission_block.isnot(None))

# Placeholder for trigger function/trigger definitions if we move them here or handle in Alembic only
# For now, the check_node_table_size function and its trigger are defined in the first migration directly.
# If we make this validator_schema.py the *absolute* source, we might represent them here too,
# but Alembic op.execute() is fine for one-off DDL like functions/triggers in migrations.

# TODO for user: Review all nullable=True/False, server_defaults, and CHAR/VARCHAR lengths.
# TODO for user: Review primary key definitions, especially for weather_historical_weights if a composite PK is desired.

sa.Index('idx_node_table_uid', node_table.c.uid)
sa.Index('idx_node_table_uid_last_updated', node_table.c.uid, node_table.c.last_updated)
sa.Index('idx_score_table_task_name_created_at_desc', score_table.c.task_name, sa.text('created_at DESC'))
