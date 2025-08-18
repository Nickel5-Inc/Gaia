# Sample Weather Statistics Data (CSV Format)

This document contains sample data in CSV format that can be easily copied and pasted into Excel, Google Sheets, or any data analysis tool.

## Context
- **Run 1**: Successful complete run (2025-08-07 00:00:00 UTC)
- **Run 2**: Successful complete run (2025-08-08 00:00:00 UTC)  
- **Run 3**: Partial run with network failure (2025-08-09 00:00:00 UTC)
- **Run 4**: Incomplete ERA5 scoring (2025-08-10 00:00:00 UTC, still in progress)

## Table: weather_forecast_stats

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

```csv
miner_uid,miner_hotkey,miner_rank,avg_forecast_score,successful_forecasts,failed_forecasts,forecast_success_ratio,hosting_successes,hosting_failures,host_reliability_ratio,avg_hosting_latency_ms,avg_day1_score,avg_era5_score,avg_era5_completeness,best_forecast_score,worst_forecast_score,score_std_dev,last_successful_forecast,last_failed_forecast,consecutive_successes,consecutive_failures,total_inference_time,avg_inference_time,first_seen,last_active,updated_at
42,5CqXdfAn...xyz,3,0.498,2,1,0.667,3,1,0.75,240,0.725,0.421,0.70,0.556,0.165,0.165,2025-08-10 00:15:00,2025-08-09 00:15:00,1,0,715,178.75,2025-08-07 00:00:00,2025-08-14 06:00:00,2025-08-14 06:00:00
87,5DmNbfRt...abc,1,0.571,2,1,0.667,3,1,0.75,181.25,0.850,0.456,0.70,0.710,0.202,0.223,2025-08-10 00:15:00,2025-08-09 00:15:00,1,0,659,164.75,2025-08-07 00:00:00,2025-08-14 06:00:00,2025-08-14 06:00:00
156,5EpQrYst...def,8,0.368,2,1,0.667,2,2,0.50,315,0.590,0.290,0.70,0.445,0.136,0.133,2025-08-10 00:15:00,2025-08-09 00:15:00,1,0,585,195,2025-08-07 00:00:00,2025-08-14 06:00:00,2025-08-14 06:00:00
```

## Table: weather_forecast_component_scores
*Showing subset of records for Run 4 (incomplete ERA5) - Miner 87 only, first 3 lead times*

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

## How to Use This Data

1. **Copy the CSV data**: Select all text within a CSV code block (including the header row)
2. **Paste into your tool**:
   - **Excel**: Paste directly into cell A1
   - **Google Sheets**: Paste directly or use File > Import
   - **Python Pandas**: Save as .csv file and use `pd.read_csv()`
   - **Database**: Use COPY command or import wizard

## Notes
- Empty cells in CSV represent NULL values
- Boolean values are represented as `true`/`false`
- Timestamps are in ISO 8601 format (YYYY-MM-DD HH:MM:SS)
- Decimal numbers use period (.) as decimal separator
