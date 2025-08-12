# Weather Statistics Database Schema Reference

## Overview

This document provides a comprehensive reference for the weather statistics database schema. The system consists of two main tables that track weather forecast performance metrics.

## Table Structures

### 1. weather_forecast_stats

**Purpose**: Stores detailed statistics for each miner per forecast run.

| Column Name | Data Type | Nullable | Default | Description |
|------------|-----------|----------|---------|-------------|
| **id** | INTEGER | NO | AUTO_INCREMENT | Primary key, auto-incrementing ID |
| **miner_uid** | INTEGER | NO | - | Miner's UID (0-255), Foreign key to node_table.uid |
| **miner_hotkey** | VARCHAR(255) | NO | - | Miner's hotkey address |
| **miner_rank** | INTEGER | YES | NULL | Miner's rank for this specific forecast run |
| **forecast_run_id** | VARCHAR(100) | NO | - | Deterministic run ID (format: `forecast_YYYYMMDD_HH_to_YYYYMMDD_HH`) |
| **run_id** | INTEGER | YES | NULL | Foreign key to weather_forecast_runs.id |
| **forecast_init_time** | TIMESTAMP WITH TIME ZONE | NO | - | GFS initialization time for the forecast |
| **forecast_status** | VARCHAR(50) | NO | - | Current stage: `pending`, `input_received`, `verified`, `verification_failed`, `day1_scored`, `era5_scoring`, `completed`, `failed` |
| **forecast_error_msg** | JSONB | YES | NULL | Sanitized error information (see Error Message Format below) |
| **forecast_score_initial** | DOUBLE PRECISION | YES | NULL | Day 1 GFS comparison score |
| **era5_score_24h** | DOUBLE PRECISION | YES | NULL | ERA5 score at 24 hour lead time |
| **era5_score_48h** | DOUBLE PRECISION | YES | NULL | ERA5 score at 48 hour lead time |
| **era5_score_72h** | DOUBLE PRECISION | YES | NULL | ERA5 score at 72 hour lead time |
| **era5_score_96h** | DOUBLE PRECISION | YES | NULL | ERA5 score at 96 hour lead time |
| **era5_score_120h** | DOUBLE PRECISION | YES | NULL | ERA5 score at 120 hour lead time |
| **era5_score_144h** | DOUBLE PRECISION | YES | NULL | ERA5 score at 144 hour lead time |
| **era5_score_168h** | DOUBLE PRECISION | YES | NULL | ERA5 score at 168 hour lead time |
| **era5_score_192h** | DOUBLE PRECISION | YES | NULL | ERA5 score at 192 hour lead time |
| **era5_score_216h** | DOUBLE PRECISION | YES | NULL | ERA5 score at 216 hour lead time |
| **era5_score_240h** | DOUBLE PRECISION | YES | NULL | ERA5 score at 240 hour lead time |
| **era5_combined_score** | DOUBLE PRECISION | YES | NULL | Average across all ERA5 timesteps (including 0s for incomplete) |
| **era5_completeness** | DOUBLE PRECISION | YES | NULL | Fraction of ERA5 timesteps completed (0-1) |
| **forecast_type** | VARCHAR(50) | NO | '10day' | Type: `10day`, `hindcast`, `cyclone_path`, etc. |
| **forecast_inference_time** | INTEGER | YES | NULL | Time taken for inference in seconds |
| **forecast_input_sources** | JSONB | YES | NULL | Input data sources used for the forecast |
| **hosting_status** | VARCHAR(50) | YES | NULL | Status: `accessible`, `inaccessible`, `timeout`, `error` |
| **hosting_latency_ms** | INTEGER | YES | NULL | Latency in milliseconds for accessing hosted files |
| **validator_hotkey** | TEXT | YES | NULL | Validator that generated these stats |
| **created_at** | TIMESTAMP WITH TIME ZONE | NO | CURRENT_TIMESTAMP | When this record was created |
| **updated_at** | TIMESTAMP WITH TIME ZONE | NO | CURRENT_TIMESTAMP | When this record was last updated |

**Constraints**:
- PRIMARY KEY: `id`
- FOREIGN KEY: `miner_uid` REFERENCES `node_table(uid)` ON DELETE CASCADE
- FOREIGN KEY: `run_id` REFERENCES `weather_forecast_runs(id)` ON DELETE CASCADE
- UNIQUE: `(miner_uid, forecast_run_id)`

**Indexes**:
- `idx_wfs_miner_uid` on `miner_uid`
- `idx_wfs_miner_hotkey` on `miner_hotkey`
- `idx_wfs_forecast_run_id` on `forecast_run_id`
- `idx_wfs_run_id` on `run_id`
- `idx_wfs_forecast_init_time` on `forecast_init_time DESC`
- `idx_wfs_forecast_status` on `forecast_status`
- `idx_wfs_miner_rank` on `miner_rank`
- `idx_wfs_era5_combined_score` on `era5_combined_score DESC`
- `idx_wfs_created_at` on `created_at DESC`
- `idx_wfs_forecast_type_init_time` on `(forecast_type, forecast_init_time DESC)`

---

### 2. miner_stats

**Purpose**: Aggregated statistics for each miner across all weather forecasting tasks.

| Column Name | Data Type | Nullable | Default | Description |
|------------|-----------|----------|---------|-------------|
| **miner_uid** | INTEGER | NO | - | PRIMARY KEY - Miner's UID (0-255), Foreign key to node_table.uid |
| **miner_hotkey** | VARCHAR(255) | NO | - | Miner's hotkey address |
| **miner_rank** | INTEGER | YES | NULL | Current overall rank among all miners |
| **avg_forecast_score** | DOUBLE PRECISION | YES | NULL | Average score across all forecasts |
| **successful_forecasts** | INTEGER | NO | 0 | Number of successfully completed forecasts |
| **failed_forecasts** | INTEGER | NO | 0 | Number of failed forecasts |
| **forecast_success_ratio** | DOUBLE PRECISION | YES | NULL | Ratio of successful to total forecasts (0-1) |
| **hosting_successes** | INTEGER | NO | 0 | Number of successful kerchunk file retrievals |
| **hosting_failures** | INTEGER | NO | 0 | Number of failed kerchunk file retrievals |
| **host_reliability_ratio** | DOUBLE PRECISION | YES | NULL | Ratio of successful hosting attempts (0-1) |
| **avg_hosting_latency_ms** | DOUBLE PRECISION | YES | NULL | Average latency for accessing hosted files in milliseconds |
| **avg_day1_score** | DOUBLE PRECISION | YES | NULL | Average day 1 GFS comparison score |
| **avg_era5_score** | DOUBLE PRECISION | YES | NULL | Average ERA5 comparison score across all timesteps |
| **avg_era5_completeness** | DOUBLE PRECISION | YES | NULL | Average completeness of ERA5 scoring (0-1) |
| **best_forecast_score** | DOUBLE PRECISION | YES | NULL | Best single forecast score achieved |
| **worst_forecast_score** | DOUBLE PRECISION | YES | NULL | Worst single forecast score |
| **score_std_dev** | DOUBLE PRECISION | YES | NULL | Standard deviation of forecast scores |
| **last_successful_forecast** | TIMESTAMP WITH TIME ZONE | YES | NULL | Timestamp of last successful forecast |
| **last_failed_forecast** | TIMESTAMP WITH TIME ZONE | YES | NULL | Timestamp of last failed forecast |
| **consecutive_successes** | INTEGER | NO | 0 | Current streak of successful forecasts |
| **consecutive_failures** | INTEGER | NO | 0 | Current streak of failed forecasts |
| **total_inference_time** | BIGINT | NO | 0 | Total inference time across all forecasts in seconds |
| **avg_inference_time** | DOUBLE PRECISION | YES | NULL | Average inference time per forecast in seconds |
| **common_errors** | JSONB | YES | NULL | Most common error types and their frequencies |
| **error_rate_by_type** | JSONB | YES | NULL | Error rates broken down by forecast type |
| **first_seen** | TIMESTAMP WITH TIME ZONE | YES | NULL | When this miner was first seen |
| **last_active** | TIMESTAMP WITH TIME ZONE | YES | NULL | Last time this miner was active |
| **validator_hotkey** | TEXT | YES | NULL | Validator that generated these stats |
| **updated_at** | TIMESTAMP WITH TIME ZONE | NO | CURRENT_TIMESTAMP | When these stats were last updated |

**Constraints**:
- PRIMARY KEY: `miner_uid`
- FOREIGN KEY: `miner_uid` REFERENCES `node_table(uid)` ON DELETE CASCADE

**Indexes**:
- `idx_ms_miner_hotkey` on `miner_hotkey`
- `idx_ms_miner_rank` on `miner_rank`
- `idx_ms_avg_forecast_score` on `avg_forecast_score DESC`
- `idx_ms_forecast_success_ratio` on `forecast_success_ratio DESC`
- `idx_ms_host_reliability_ratio` on `host_reliability_ratio DESC`
- `idx_ms_last_active` on `last_active DESC`
- `idx_ms_consecutive_failures` on `consecutive_failures`
- `idx_ms_active_miners` on `last_active DESC` WHERE `last_active IS NOT NULL`

---

## Data Formats

### Error Message Format (JSONB)

```json
{
  "type": "timeout|connection|not_found|verification_failed|inference_error|incomplete|unknown",
  "message": "Sanitized error message (max 500 chars, sensitive data redacted)",
  "timestamp": "2025-01-11T12:00:00Z"
}
```

### Common Errors Format (JSONB)

```json
{
  "timeout": 15,
  "connection": 8,
  "verification_failed": 3,
  "not_found": 2,
  "inference_error": 1
}
```

### Error Rate by Type Format (JSONB)

```json
{
  "10day": 0.05,
  "hindcast": 0.02,
  "cyclone_path": 0.08
}
```

### Forecast Input Sources Format (JSONB)

```json
{
  "primary": "GFS",
  "secondary": ["ECMWF", "NAM"],
  "resolution": "0.25deg",
  "timesteps": 240
}
```

---

## Relationships

```mermaid
erDiagram
    node_table ||--o{ weather_forecast_stats : "has"
    node_table ||--o| miner_stats : "has"
    weather_forecast_runs ||--o{ weather_forecast_stats : "contains"
    
    node_table {
        int uid PK
        text hotkey
        text coldkey
        text ip
        int port
        float incentive
        float stake
    }
    
    weather_forecast_runs {
        int id PK
        timestamp run_initiation_time
        timestamp target_forecast_time_utc
        timestamp gfs_init_time_utc
        varchar status
    }
    
    weather_forecast_stats {
        int id PK
        int miner_uid FK
        varchar forecast_run_id UK
        int run_id FK
        timestamp forecast_init_time
        varchar forecast_status
        float forecast_score_initial
        float era5_scores
        float era5_combined_score
    }
    
    miner_stats {
        int miner_uid PK_FK
        varchar miner_hotkey
        int miner_rank
        float avg_forecast_score
        int successful_forecasts
        int failed_forecasts
    }
```

---

## Common Queries

### Get latest forecast stats for a miner

```sql
SELECT * FROM weather_forecast_stats 
WHERE miner_uid = :uid 
ORDER BY updated_at DESC 
LIMIT 10;
```

### Get overall miner rankings

```sql
SELECT 
    miner_uid,
    miner_hotkey,
    miner_rank,
    avg_forecast_score,
    forecast_success_ratio,
    host_reliability_ratio
FROM miner_stats 
ORDER BY miner_rank ASC;
```

### Get run-specific rankings

```sql
SELECT 
    miner_uid,
    miner_hotkey,
    miner_rank,
    era5_combined_score,
    era5_completeness,
    forecast_status
FROM weather_forecast_stats 
WHERE forecast_run_id = :run_id 
ORDER BY miner_rank ASC;
```

### Get miners with hosting issues

```sql
SELECT 
    ms.miner_uid,
    ms.miner_hotkey,
    ms.host_reliability_ratio,
    ms.hosting_failures,
    ms.avg_hosting_latency_ms
FROM miner_stats ms
WHERE ms.host_reliability_ratio < 0.8
ORDER BY ms.host_reliability_ratio ASC;
```

### Get recent forecast performance by type

```sql
SELECT 
    forecast_type,
    COUNT(*) as total_forecasts,
    AVG(era5_combined_score) as avg_score,
    AVG(era5_completeness) as avg_completeness,
    COUNT(CASE WHEN forecast_status = 'completed' THEN 1 END) as completed,
    COUNT(CASE WHEN forecast_status = 'failed' THEN 1 END) as failed
FROM weather_forecast_stats
WHERE forecast_init_time > NOW() - INTERVAL '7 days'
GROUP BY forecast_type
ORDER BY avg_score DESC;
```

### Get miners with consecutive failures

```sql
SELECT 
    miner_uid,
    miner_hotkey,
    consecutive_failures,
    last_failed_forecast,
    common_errors
FROM miner_stats
WHERE consecutive_failures > 3
ORDER BY consecutive_failures DESC;
```

### Calculate average scores by lead time

```sql
SELECT 
    AVG(era5_score_24h) as avg_24h,
    AVG(era5_score_48h) as avg_48h,
    AVG(era5_score_72h) as avg_72h,
    AVG(era5_score_96h) as avg_96h,
    AVG(era5_score_120h) as avg_120h,
    AVG(era5_score_144h) as avg_144h,
    AVG(era5_score_168h) as avg_168h,
    AVG(era5_score_192h) as avg_192h,
    AVG(era5_score_216h) as avg_216h,
    AVG(era5_score_240h) as avg_240h
FROM weather_forecast_stats
WHERE forecast_init_time > NOW() - INTERVAL '24 hours'
    AND forecast_status = 'completed';
```

---

## Data Population Flow

1. **Response Received** → Insert/Update `weather_forecast_stats` with `status='input_received'`
2. **Verification** → Update `hosting_status`, `hosting_latency_ms`, `status='verified'` or `'verification_failed'`
3. **Day 1 Scoring** → Update `forecast_score_initial`, `status='day1_scored'`
4. **ERA5 Scoring** → Update `era5_score_*` fields progressively, `status='era5_scoring'`
5. **Completion** → Update `era5_combined_score`, `era5_completeness`, `status='completed'`
6. **Aggregation** → Update `miner_stats` with aggregated metrics
7. **Ranking** → Calculate and update `miner_rank` in both tables

---

## Notes

- All timestamps are stored with timezone (UTC)
- Scores are typically normalized values between 0 and 1 (lower is better for RMSE-based scores)
- The `forecast_run_id` is deterministic and consistent across validators
- Error messages are sanitized to remove sensitive information (IPs, ports, full paths)
- The system is designed for efficient querying with comprehensive indexing
- Foreign key constraints ensure data integrity with CASCADE deletes
