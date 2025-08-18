# Sample Weather Statistics Data

## Table Schemas and Descriptions

### Table: `weather_forecast_stats`
**Purpose**: Comprehensive weather forecast statistics for each miner per forecast run. This is the main summary table for tracking miner performance over time.

| Column Name | Type | Description |
|------------|------|-------------|
| id | INTEGER (PK) | Auto-incrementing primary key |
| miner_uid | INTEGER (FK) | Miner's unique ID (0-255), references node_table.uid |
| miner_hotkey | VARCHAR(255) | Miner's blockchain hotkey identifier |
| miner_rank | INTEGER | Miner's performance rank for this forecast run (1=best) |
| forecast_run_id | VARCHAR(100) | Deterministic run ID shared across validators (e.g., "weather_2025080700") |
| run_id | INTEGER (FK) | References weather_forecast_runs.id |
| forecast_init_time | TIMESTAMP(TZ) | GFS model initialization time for the forecast |
| forecast_status | VARCHAR(50) | Current stage: pending, input_received, day1_scored, era5_scoring, completed, failed |
| current_forecast_stage | VARCHAR(100) | Detailed pipeline stage: init, gfs_fetch, inference_requested, inference_running, day1_scoring, era5_scoring_24h, era5_scoring_48h, etc., complete |
| current_forecast_status | VARCHAR(50) | Status within current stage: initialized, in_progress, scoring, error, waiting_retry, completed |
| last_error_message | TEXT | Most recent error message encountered |
| retries_remaining | INTEGER | Number of retry attempts remaining (0-3) |
| next_scheduled_retry | TIMESTAMP(TZ) | UTC timestamp for next retry attempt |
| forecast_score_initial | FLOAT | Day 1 GFS comparison score (0-1, higher is better) |
| era5_score_24h...240h | FLOAT | ERA5 comparison scores at each 24-hour interval |
| era5_combined_score | FLOAT | Average across all ERA5 timesteps (including 0s for incomplete) |
| era5_completeness | FLOAT | Fraction of ERA5 timesteps completed (0-1) |
| avg_rmse | FLOAT | Average RMSE across all variables and timesteps |
| avg_acc | FLOAT | Average ACC across all variables and timesteps (-1 to 1) |
| avg_skill_score | FLOAT | Average skill score across all variables and timesteps (0-1) |
| forecast_type | VARCHAR(50) | Type of forecast: 10day, hindcast, cyclone_path, etc. |
| forecast_inference_time | INTEGER | Time taken for model inference in seconds |
| hosting_status | VARCHAR(50) | Status of file hosting: accessible, inaccessible, timeout, error |
| hosting_latency_ms | INTEGER | Latency in milliseconds for accessing hosted files |
| created_at | TIMESTAMP(TZ) | When this record was created |
| updated_at | TIMESTAMP(TZ) | When this record was last updated |

### Table: `miner_stats`
**Purpose**: Aggregated lifetime statistics for each miner across all weather forecasting tasks. Updated after each forecast run.

| Column Name | Type | Description |
|------------|------|-------------|
| miner_uid | INTEGER (PK, FK) | Miner's unique ID (0-255), references node_table.uid |
| miner_hotkey | VARCHAR(255) | Miner's blockchain hotkey identifier |
| miner_rank | INTEGER | Current overall rank among all miners (1=best) |
| avg_forecast_score | FLOAT | Average score across all forecasts (0-1) |
| successful_forecasts | INTEGER | Count of successfully completed forecasts |
| failed_forecasts | INTEGER | Count of failed forecasts |
| forecast_success_ratio | FLOAT | Ratio of successful to total forecasts (0-1) |
| hosting_successes | INTEGER | Count of successful file retrievals |
| hosting_failures | INTEGER | Count of failed file retrievals |
| host_reliability_ratio | FLOAT | Ratio of successful hosting attempts (0-1) |
| avg_hosting_latency_ms | FLOAT | Average latency for accessing hosted files |
| avg_day1_score | FLOAT | Average day 1 GFS comparison score |
| avg_era5_score | FLOAT | Average ERA5 comparison score across all timesteps |
| avg_era5_completeness | FLOAT | Average completeness of ERA5 scoring (0-1) |
| best_forecast_score | FLOAT | Best single forecast score achieved |
| worst_forecast_score | FLOAT | Worst single forecast score |
| score_std_dev | FLOAT | Standard deviation of forecast scores |
| last_successful_forecast | TIMESTAMP(TZ) | Timestamp of last successful forecast |
| last_failed_forecast | TIMESTAMP(TZ) | Timestamp of last failed forecast |
| consecutive_successes | INTEGER | Current streak of successful forecasts |
| consecutive_failures | INTEGER | Current streak of failed forecasts |
| total_inference_time | BIGINT | Total inference time across all forecasts in seconds |
| avg_inference_time | FLOAT | Average inference time per forecast in seconds |
| first_seen | TIMESTAMP(TZ) | When this miner was first seen |
| last_active | TIMESTAMP(TZ) | Last time this miner was active |
| updated_at | TIMESTAMP(TZ) | When these stats were last updated |

### Table: `weather_forecast_component_scores`
**Purpose**: Stores detailed per-variable, per-lead-time metrics for full transparency into how aggregate scores are calculated.

| Column Name | Type | Description |
|------------|------|-------------|
| id | INTEGER (PK) | Auto-incrementing primary key |
| run_id | INTEGER (FK) | References weather_forecast_runs.id |
| response_id | INTEGER (FK) | References weather_miner_responses.id |
| miner_uid | INTEGER (FK) | Miner's unique ID (0-255), references node_table.uid |
| miner_hotkey | VARCHAR(255) | Miner's blockchain hotkey identifier |
| score_type | VARCHAR(50) | Type of scoring: 'day1' or 'era5' |
| lead_hours | INTEGER | Forecast lead time in hours (6, 12, 24, 48, 72, 96, 120, 144, 168, 192, 216, 240) |
| valid_time_utc | TIMESTAMP(TZ) | Valid time for this forecast (forecast_init_time + lead_hours) |
| variable_name | VARCHAR(20) | Weather variable: '2t', 'msl', 't', 'u', 'v', 'q', 'z', '10u', '10v' |
| pressure_level | INTEGER | Pressure level in hPa (NULL for surface vars, 850/500 for upper air) |
| rmse | FLOAT | Root Mean Square Error (lower is better) |
| mse | FLOAT | Mean Square Error (lower is better) |
| acc | FLOAT | Anomaly Correlation Coefficient (-1 to 1, higher is better) |
| skill_score | FLOAT | Skill score vs reference forecast (0-1 typically, can be negative) |
| skill_score_gfs | FLOAT | Skill score specifically vs GFS forecast |
| skill_score_climatology | FLOAT | Skill score vs climatology |
| bias | FLOAT | Mean bias of forecast (systematic over/under prediction) |
| mae | FLOAT | Mean Absolute Error (lower is better) |
| climatology_check_passed | BOOLEAN | Whether forecast passed reasonable bounds check |
| pattern_correlation | FLOAT | Spatial pattern correlation with reference (0-1) |
| pattern_correlation_passed | BOOLEAN | Whether pattern correlation exceeded threshold |
| clone_penalty | FLOAT | Penalty applied for suspected clone/copying (0-1) |
| quality_penalty | FLOAT | Penalty for quality issues (0-1) |
| weighted_score | FLOAT | This variable's weighted contribution to final score |
| variable_weight | FLOAT | Weight used for this variable in aggregation |
| calculated_at | TIMESTAMP(TZ) | When this metric was calculated |
| calculation_duration_ms | INTEGER | How long this metric calculation took in milliseconds |

## Variable Definitions
- **2t**: 2-meter temperature (surface)
- **msl**: Mean sea level pressure (surface)
- **10u**: 10-meter U-component of wind (surface, east-west)
- **10v**: 10-meter V-component of wind (surface, north-south)
- **t**: Temperature at pressure levels (850 hPa, 500 hPa)
- **q**: Specific humidity at pressure levels
- **z**: Geopotential height at pressure levels
- **u**: U-component of wind at pressure levels
- **v**: V-component of wind at pressure levels

## Context
- **Run 1**: Successful complete run (2025-08-07 00:00:00 UTC)
- **Run 2**: Successful complete run (2025-08-08 00:00:00 UTC)  
- **Run 3**: Partial run with network failure (2025-08-09 00:00:00 UTC)
- **Run 4**: Incomplete ERA5 scoring (2025-08-10 00:00:00 UTC, still in progress)

## Table: weather_forecast_stats

### CSV Format (Copy and paste into Excel/Google Sheets)
```csv
id,miner_uid,miner_hotkey,miner_rank,forecast_run_id,run_id,forecast_init_time,forecast_status,current_forecast_stage,current_forecast_status,last_error_message,retries_remaining,next_scheduled_retry,forecast_score_initial,era5_score_24h,era5_score_48h,era5_score_72h,era5_score_96h,era5_score_120h,era5_score_144h,era5_score_168h,era5_score_192h,era5_score_216h,era5_score_240h,era5_combined_score,era5_completeness,avg_rmse,avg_acc,avg_skill_score,forecast_type,forecast_inference_time,hosting_status,hosting_latency_ms,created_at,updated_at
1,42,5CqXdfAn...xyz,3,weather_2025080700,1,2025-08-07 00:00:00,completed,complete,completed,,3,,0.72,0.68,0.65,0.62,0.58,0.54,0.51,0.48,0.45,0.42,0.39,0.542,1.0,48.5,0.84,0.65,10day,180,accessible,250,2025-08-07 00:15:00,2025-08-17 12:00:00
2,87,5DmNbfRt...abc,1,weather_2025080700,1,2025-08-07 00:00:00,completed,complete,completed,,3,,0.85,0.82,0.79,0.76,0.73,0.70,0.67,0.64,0.61,0.58,0.55,0.700,1.0,42.3,0.86,0.68,10day,165,accessible,180,2025-08-07 00:15:00,2025-08-17 12:00:00
3,156,5EpQrYst...def,8,weather_2025080700,1,2025-08-07 00:00:00,completed,complete,completed,,3,,0.58,0.55,0.52,0.49,0.46,0.43,0.40,0.37,0.34,0.31,0.28,0.423,1.0,56.2,0.78,0.52,10day,195,accessible,320,2025-08-07 00:15:00,2025-08-17 12:00:00
4,42,5CqXdfAn...xyz,2,weather_2025080800,2,2025-08-08 00:00:00,completed,complete,completed,,3,,0.74,0.70,0.67,0.64,0.60,0.56,0.53,0.50,0.47,0.44,0.41,0.556,1.0,47.8,0.85,0.66,10day,175,accessible,230,2025-08-08 00:15:00,2025-08-18 12:00:00
5,87,5DmNbfRt...abc,1,weather_2025080800,2,2025-08-08 00:00:00,completed,complete,completed,,3,,0.86,0.83,0.80,0.77,0.74,0.71,0.68,0.65,0.62,0.59,0.56,0.710,1.0,41.9,0.87,0.69,10day,162,accessible,175,2025-08-08 00:15:00,2025-08-18 12:00:00
6,156,5EpQrYst...def,7,weather_2025080800,2,2025-08-08 00:00:00,completed,complete,completed,,3,,0.60,0.57,0.54,0.51,0.48,0.45,0.42,0.39,0.36,0.33,0.30,0.445,1.0,55.7,0.79,0.53,10day,198,accessible,310,2025-08-08 00:15:00,2025-08-18 12:00:00
7,42,5CqXdfAn...xyz,,weather_2025080900,3,2025-08-09 00:00:00,failed,day1_scoring,error,Timeout waiting for kerchunk file,0,,0.71,,,,,,,,,,,,0.0,45.2,0.83,0.64,10day,182,timeout,,2025-08-09 00:15:00,2025-08-09 03:30:00
8,87,5DmNbfRt...abc,,weather_2025080900,3,2025-08-09 00:00:00,failed,day1_scoring,error,Network error: Connection refused,0,,0.84,,,,,,,,,,,,0.0,40.1,0.86,0.67,10day,168,error,,2025-08-09 00:15:00,2025-08-09 03:30:00
9,156,5EpQrYst...def,,weather_2025080900,3,2025-08-09 00:00:00,failed,inference_requested,error,Failed to initiate fetch: 404 Not Found,1,2025-08-09 04:00:00,,,,,,,,,,,,,0.0,,,,10day,,inaccessible,,2025-08-09 00:15:00,2025-08-09 03:30:00
10,42,5CqXdfAn...xyz,4,weather_2025081000,4,2025-08-10 00:00:00,era5_scoring,era5_scoring_96h,in_progress,,3,,0.73,0.69,0.66,0.63,,,,,,,,,0.165,0.4,46.3,0.84,0.65,10day,178,accessible,240,2025-08-10 00:15:00,2025-08-14 06:00:00
11,87,5DmNbfRt...abc,2,weather_2025081000,4,2025-08-10 00:00:00,era5_scoring,era5_scoring_96h,in_progress,,3,,0.85,0.82,0.79,0.76,,,,,,,,,0.202,0.4,41.2,0.86,0.68,10day,164,accessible,185,2025-08-10 00:15:00,2025-08-14 06:00:00
12,156,5EpQrYst...def,9,weather_2025081000,4,2025-08-10 00:00:00,era5_scoring,era5_scoring_96h,in_progress,,3,,0.59,0.56,0.53,0.50,,,,,,,,,0.136,0.4,54.8,0.78,0.51,10day,192,accessible,315,2025-08-10 00:15:00,2025-08-14 06:00:00
```

## Table: miner_stats

### CSV Format
```csv
miner_uid,miner_hotkey,miner_rank,avg_forecast_score,successful_forecasts,failed_forecasts,forecast_success_ratio,hosting_successes,hosting_failures,host_reliability_ratio,avg_hosting_latency_ms,avg_day1_score,avg_era5_score,avg_era5_completeness,best_forecast_score,worst_forecast_score,score_std_dev,last_successful_forecast,last_failed_forecast,consecutive_successes,consecutive_failures,total_inference_time,avg_inference_time,first_seen,last_active,updated_at
42,5CqXdfAn...xyz,3,0.498,2,1,0.667,3,1,0.75,240,0.725,0.421,0.70,0.556,0.165,0.165,2025-08-10 00:15:00,2025-08-09 00:15:00,1,0,715,178.75,2025-08-07 00:00:00,2025-08-14 06:00:00,2025-08-14 06:00:00
87,5DmNbfRt...abc,1,0.571,2,1,0.667,3,1,0.75,181.25,0.850,0.456,0.70,0.710,0.202,0.223,2025-08-10 00:15:00,2025-08-09 00:15:00,1,0,659,164.75,2025-08-07 00:00:00,2025-08-14 06:00:00,2025-08-14 06:00:00
156,5EpQrYst...def,8,0.368,2,1,0.667,2,2,0.50,315,0.590,0.290,0.70,0.445,0.136,0.133,2025-08-10 00:15:00,2025-08-09 00:15:00,1,0,585,195,2025-08-07 00:00:00,2025-08-14 06:00:00,2025-08-14 06:00:00
```

## Table: weather_forecast_component_scores

*Showing subset of records for Run 4 (incomplete ERA5) - Miner 87 only, first 3 lead times*

### CSV Format (First 30 rows)
```csv
id,run_id,response_id,miner_uid,miner_hotkey,score_type,lead_hours,valid_time_utc,variable_name,pressure_level,rmse,mse,acc,skill_score,skill_score_gfs,skill_score_climatology,bias,mae,climatology_check_passed,pattern_correlation,pattern_correlation_passed,clone_penalty,quality_penalty,weighted_score,variable_weight,calculated_at,calculation_duration_ms
301,4,11,87,5DmNbfRt...abc,era5,24,2025-08-11 00:00:00,2t,,2.45,6.00,0.92,0.78,0.75,,-0.3,2.1,true,0.94,true,0.0,0.0,0.156,0.20,2025-08-12 06:00:00,450
302,4,11,87,5DmNbfRt...abc,era5,24,2025-08-11 00:00:00,msl,,185.2,34299,0.89,0.72,0.70,,15.5,145.3,true,0.91,true,0.0,0.0,0.108,0.15,2025-08-12 06:00:00,380
303,4,11,87,5DmNbfRt...abc,era5,24,2025-08-11 00:00:00,10u,,3.12,9.73,0.85,0.68,0.65,,0.8,2.8,true,0.88,true,0.0,0.0,0.068,0.10,2025-08-12 06:00:00,320
304,4,11,87,5DmNbfRt...abc,era5,24,2025-08-11 00:00:00,10v,,3.25,10.56,0.84,0.66,0.63,,-0.5,2.9,true,0.87,true,0.0,0.0,0.066,0.10,2025-08-12 06:00:00,315
305,4,11,87,5DmNbfRt...abc,era5,24,2025-08-11 00:00:00,t,850,2.88,8.29,0.88,0.71,0.68,,0.4,2.5,true,0.90,true,0.0,0.0,0.107,0.15,2025-08-12 06:00:00,420
306,4,11,87,5DmNbfRt...abc,era5,24,2025-08-11 00:00:00,t,500,3.15,9.92,0.86,0.69,0.66,,0.3,2.7,true,0.89,true,0.0,0.0,0.104,0.15,2025-08-12 06:00:00,410
307,4,11,87,5DmNbfRt...abc,era5,24,2025-08-11 00:00:00,q,850,0.0008,6.4e-7,0.83,0.64,0.61,,0.0001,0.0007,true,0.85,true,0.0,0.0,0.064,0.10,2025-08-12 06:00:00,350
308,4,11,87,5DmNbfRt...abc,era5,24,2025-08-11 00:00:00,z,500,125.5,15750,0.91,0.76,0.73,,-20.2,105.3,true,0.93,true,0.0,0.0,0.076,0.10,2025-08-12 06:00:00,440
309,4,11,87,5DmNbfRt...abc,era5,24,2025-08-11 00:00:00,u,850,4.25,18.06,0.82,0.62,0.59,,1.2,3.8,true,0.84,true,0.0,0.0,0.031,0.05,2025-08-12 06:00:00,290
310,4,11,87,5DmNbfRt...abc,era5,24,2025-08-11 00:00:00,v,850,4.38,19.18,0.81,0.60,0.57,,-0.9,3.9,true,0.83,true,0.0,0.0,0.030,0.05,2025-08-12 06:00:00,285
311,4,11,87,5DmNbfRt...abc,era5,48,2025-08-12 00:00:00,2t,,2.68,7.18,0.90,0.75,0.72,,-0.4,2.3,true,0.92,true,0.0,0.0,0.150,0.20,2025-08-13 06:00:00,455
312,4,11,87,5DmNbfRt...abc,era5,48,2025-08-12 00:00:00,msl,,198.7,39481,0.87,0.69,0.67,,18.2,158.4,true,0.89,true,0.0,0.0,0.104,0.15,2025-08-13 06:00:00,385
313,4,11,87,5DmNbfRt...abc,era5,48,2025-08-12 00:00:00,10u,,3.45,11.90,0.83,0.65,0.62,,0.9,3.1,true,0.85,true,0.0,0.0,0.065,0.10,2025-08-13 06:00:00,325
314,4,11,87,5DmNbfRt...abc,era5,48,2025-08-12 00:00:00,10v,,3.58,12.82,0.82,0.63,0.60,,-0.6,3.2,true,0.84,true,0.0,0.0,0.063,0.10,2025-08-13 06:00:00,318
315,4,11,87,5DmNbfRt...abc,era5,48,2025-08-12 00:00:00,t,850,3.12,9.73,0.86,0.68,0.65,,0.5,2.7,true,0.88,true,0.0,0.0,0.102,0.15,2025-08-13 06:00:00,425
316,4,11,87,5DmNbfRt...abc,era5,48,2025-08-12 00:00:00,t,500,3.42,11.70,0.84,0.66,0.63,,0.4,2.9,true,0.86,true,0.0,0.0,0.099,0.15,2025-08-13 06:00:00,415
317,4,11,87,5DmNbfRt...abc,era5,48,2025-08-12 00:00:00,q,850,0.0009,8.1e-7,0.81,0.61,0.58,,0.0001,0.0008,true,0.83,true,0.0,0.0,0.061,0.10,2025-08-13 06:00:00,355
318,4,11,87,5DmNbfRt...abc,era5,48,2025-08-12 00:00:00,z,500,138.2,19099,0.89,0.73,0.70,,-22.5,115.8,true,0.91,true,0.0,0.0,0.073,0.10,2025-08-13 06:00:00,445
319,4,11,87,5DmNbfRt...abc,era5,48,2025-08-12 00:00:00,u,850,4.68,21.90,0.80,0.59,0.56,,1.4,4.2,true,0.82,true,0.0,0.0,0.030,0.05,2025-08-13 06:00:00,295
320,4,11,87,5DmNbfRt...abc,era5,48,2025-08-12 00:00:00,v,850,4.82,23.23,0.79,0.57,0.54,,-1.0,4.3,true,0.81,true,0.0,0.0,0.029,0.05,2025-08-13 06:00:00,288
321,4,11,87,5DmNbfRt...abc,era5,72,2025-08-13 00:00:00,2t,,2.92,8.53,0.88,0.72,0.69,,-0.5,2.5,true,0.90,true,0.0,0.0,0.144,0.20,2025-08-14 06:00:00,460
322,4,11,87,5DmNbfRt...abc,era5,72,2025-08-13 00:00:00,msl,,215.3,46354,0.85,0.66,0.64,,20.8,171.5,true,0.87,true,0.0,0.0,0.099,0.15,2025-08-14 06:00:00,390
323,4,11,87,5DmNbfRt...abc,era5,72,2025-08-13 00:00:00,10u,,3.78,14.29,0.81,0.62,0.59,,1.0,3.4,true,0.83,true,0.0,0.0,0.062,0.10,2025-08-14 06:00:00,330
324,4,11,87,5DmNbfRt...abc,era5,72,2025-08-13 00:00:00,10v,,3.92,15.37,0.80,0.60,0.57,,-0.7,3.5,true,0.82,true,0.0,0.0,0.060,0.10,2025-08-14 06:00:00,322
325,4,11,87,5DmNbfRt...abc,era5,72,2025-08-13 00:00:00,t,850,3.38,11.42,0.84,0.65,0.62,,0.6,2.9,true,0.86,true,0.0,0.0,0.098,0.15,2025-08-14 06:00:00,430
326,4,11,87,5DmNbfRt...abc,era5,72,2025-08-13 00:00:00,t,500,3.71,13.76,0.82,0.63,0.60,,0.5,3.2,true,0.84,true,0.0,0.0,0.095,0.15,2025-08-14 06:00:00,420
327,4,11,87,5DmNbfRt...abc,era5,72,2025-08-13 00:00:00,q,850,0.0010,1.0e-6,0.79,0.58,0.55,,0.0002,0.0009,true,0.81,true,0.0,0.0,0.058,0.10,2025-08-14 06:00:00,360
328,4,11,87,5DmNbfRt...abc,era5,72,2025-08-13 00:00:00,z,500,152.1,23134,0.87,0.70,0.67,,-24.8,127.4,true,0.89,true,0.0,0.0,0.070,0.10,2025-08-14 06:00:00,450
329,4,11,87,5DmNbfRt...abc,era5,72,2025-08-13 00:00:00,u,850,5.12,26.21,0.78,0.56,0.53,,1.6,4.6,true,0.80,true,0.0,0.0,0.028,0.05,2025-08-14 06:00:00,300
330,4,11,87,5DmNbfRt...abc,era5,72,2025-08-13 00:00:00,v,850,5.28,27.88,0.77,0.54,0.51,,-1.1,4.7,true,0.79,true,0.0,0.0,0.027,0.05,2025-08-14 06:00:00,292
```

## Notes

### Data Relationships
- All tables are linked via `run_id` (1-4) and `miner_uid`/`miner_hotkey`
- Component scores show detailed metrics for each variable at each lead time
- For complete runs, there would be ~90 component score records per miner (9 variables × 10 lead times)
- For the incomplete run (Run 4), only the first 3 lead times have been scored so far

### Realistic Patterns
- **Score degradation**: Scores generally decrease with increasing lead time (forecast skill degrades over time)
- **Variable weights**: Based on the VARIABLE_WEIGHTS from the code (2t=0.20, msl=0.15, etc.)
- **RMSE increases**: Error metrics increase with lead time
- **ACC decreases**: Correlation decreases with lead time
- **Network failure** (Run 3): Shows NULL values for ERA5 scores when network issues prevented scoring
- **Incomplete scoring** (Run 4): Shows progressive scoring with only early lead times completed

### Metrics Relationships
- `weighted_score = skill_score × variable_weight` (approximately)
- `era5_combined_score` is the average of all available ERA5 scores
- `era5_completeness` shows fraction of expected scores that are available
- Higher rank (lower number) = better performance

## Example API Payloads

### 1. GET Weather Forecast Stats Response
**Endpoint**: `GET /api/v1/weather/forecast-stats?run_id=4&miner_uid=87`

```json
{
  "status": "success",
  "data": {
    "forecast_stats": {
      "id": 11,
      "miner_uid": 87,
      "miner_hotkey": "5DmNbfRt...abc",
      "miner_rank": 2,
      "forecast_run_id": "weather_2025081000",
      "run_id": 4,
      "forecast_init_time": "2025-08-10T00:00:00Z",
      "forecast_status": "era5_scoring",
      "forecast_score_initial": 0.85,
      "era5_scores": {
        "24h": 0.82,
        "48h": 0.79,
        "72h": 0.76,
        "96h": null,
        "120h": null,
        "144h": null,
        "168h": null,
        "192h": null,
        "216h": null,
        "240h": null
      },
      "era5_combined_score": 0.202,
      "era5_completeness": 0.4,
      "avg_rmse": 41.2,
      "avg_acc": 0.86,
      "avg_skill_score": 0.68,
      "forecast_type": "10day",
      "forecast_inference_time": 164,
      "hosting_status": "accessible",
      "hosting_latency_ms": 185,
      "created_at": "2025-08-10T00:15:00Z",
      "updated_at": "2025-08-14T06:00:00Z"
    }
  }
}
```

### 2. GET Component Scores Response
**Endpoint**: `GET /api/v1/weather/component-scores?run_id=4&miner_uid=87&lead_hours=24`

```json
{
  "status": "success",
  "data": {
    "component_scores": [
      {
        "id": 301,
        "variable_name": "2t",
        "pressure_level": null,
        "metrics": {
          "rmse": 2.45,
          "mse": 6.00,
          "acc": 0.92,
          "skill_score": 0.78,
          "skill_score_gfs": 0.75,
          "bias": -0.3,
          "mae": 2.1
        },
        "quality_checks": {
          "climatology_check_passed": true,
          "pattern_correlation": 0.94,
          "pattern_correlation_passed": true
        },
        "scoring": {
          "weighted_score": 0.156,
          "variable_weight": 0.20,
          "clone_penalty": 0.0,
          "quality_penalty": 0.0
        },
        "metadata": {
          "score_type": "era5",
          "lead_hours": 24,
          "valid_time_utc": "2025-08-11T00:00:00Z",
          "calculated_at": "2025-08-12T06:00:00Z",
          "calculation_duration_ms": 450
        }
      },
      {
        "id": 302,
        "variable_name": "msl",
        "pressure_level": null,
        "metrics": {
          "rmse": 185.2,
          "mse": 34299,
          "acc": 0.89,
          "skill_score": 0.72,
          "skill_score_gfs": 0.70,
          "bias": 15.5,
          "mae": 145.3
        },
        "quality_checks": {
          "climatology_check_passed": true,
          "pattern_correlation": 0.91,
          "pattern_correlation_passed": true
        },
        "scoring": {
          "weighted_score": 0.108,
          "variable_weight": 0.15,
          "clone_penalty": 0.0,
          "quality_penalty": 0.0
        },
        "metadata": {
          "score_type": "era5",
          "lead_hours": 24,
          "valid_time_utc": "2025-08-11T00:00:00Z",
          "calculated_at": "2025-08-12T06:00:00Z",
          "calculation_duration_ms": 380
        }
      }
    ],
    "summary": {
      "total_variables": 9,
      "variables_scored": 9,
      "average_rmse": 42.3,
      "average_acc": 0.86,
      "average_skill_score": 0.68,
      "total_weighted_score": 0.82
    }
  }
}
```

### 3. POST Batch Insert Component Scores Request
**Endpoint**: `POST /api/v1/weather/component-scores/batch`

```json
{
  "run_id": 4,
  "response_id": 11,
  "miner_uid": 87,
  "miner_hotkey": "5DmNbfRt...abc",
  "scores": [
    {
      "score_type": "era5",
      "lead_hours": 96,
      "valid_time_utc": "2025-08-14T00:00:00Z",
      "variable_name": "2t",
      "pressure_level": null,
      "rmse": 3.15,
      "mse": 9.92,
      "acc": 0.86,
      "skill_score": 0.69,
      "skill_score_gfs": 0.66,
      "bias": -0.6,
      "mae": 2.7,
      "climatology_check_passed": true,
      "pattern_correlation": 0.88,
      "pattern_correlation_passed": true,
      "clone_penalty": 0.0,
      "quality_penalty": 0.0,
      "weighted_score": 0.138,
      "variable_weight": 0.20,
      "calculation_duration_ms": 465
    },
    {
      "score_type": "era5",
      "lead_hours": 96,
      "valid_time_utc": "2025-08-14T00:00:00Z",
      "variable_name": "t",
      "pressure_level": 850,
      "rmse": 3.65,
      "mse": 13.32,
      "acc": 0.82,
      "skill_score": 0.62,
      "skill_score_gfs": 0.59,
      "bias": 0.7,
      "mae": 3.1,
      "climatology_check_passed": true,
      "pattern_correlation": 0.84,
      "pattern_correlation_passed": true,
      "clone_penalty": 0.0,
      "quality_penalty": 0.0,
      "weighted_score": 0.093,
      "variable_weight": 0.15,
      "calculation_duration_ms": 435
    }
  ]
}
```

### 4. GET Aggregated Miner Stats Response
**Endpoint**: `GET /api/v1/weather/miner-stats/87`

```json
{
  "status": "success",
  "data": {
    "miner_stats": {
      "miner_uid": 87,
      "miner_hotkey": "5DmNbfRt...abc",
      "miner_rank": 1,
      "performance": {
        "avg_forecast_score": 0.571,
        "successful_forecasts": 2,
        "failed_forecasts": 1,
        "forecast_success_ratio": 0.667,
        "best_forecast_score": 0.710,
        "worst_forecast_score": 0.202,
        "score_std_dev": 0.223
      },
      "reliability": {
        "hosting_successes": 3,
        "hosting_failures": 1,
        "host_reliability_ratio": 0.75,
        "avg_hosting_latency_ms": 181.25
      },
      "scoring_breakdown": {
        "avg_day1_score": 0.850,
        "avg_era5_score": 0.456,
        "avg_era5_completeness": 0.70
      },
      "activity": {
        "first_seen": "2025-08-07T00:00:00Z",
        "last_active": "2025-08-14T06:00:00Z",
        "last_successful_forecast": "2025-08-10T00:15:00Z",
        "last_failed_forecast": "2025-08-09T00:15:00Z",
        "consecutive_successes": 1,
        "consecutive_failures": 0
      },
      "inference": {
        "total_inference_time": 659,
        "avg_inference_time": 164.75
      },
      "updated_at": "2025-08-14T06:00:00Z"
    }
  }
}
```

### 5. WebSocket Event for Progressive ERA5 Scoring
**Event**: `weather.scoring.progress`

```json
{
  "event": "weather.scoring.progress",
  "timestamp": "2025-08-14T06:00:00Z",
  "data": {
    "run_id": 4,
    "miner_uid": 87,
    "scoring_update": {
      "lead_hours_completed": [24, 48, 72, 96],
      "lead_hours_remaining": [120, 144, 168, 192, 216, 240],
      "completeness": 0.4,
      "current_combined_score": 0.202,
      "next_era5_check": "2025-08-15T00:00:00Z"
    }
  }
}
```
