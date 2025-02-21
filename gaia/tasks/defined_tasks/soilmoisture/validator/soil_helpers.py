from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple, Any
import json
import math
import traceback
import asyncio
import glob
import os
from pathlib import Path
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

async def get_todays_regions(self, target_time: datetime) -> List[Dict]:
    """Get regions already selected for today."""
    try:
        query = """
            SELECT * FROM soil_moisture_regions
            WHERE region_date = :target_date
            AND status = 'pending'
        """
        result = await self.db_manager.fetch_all(query, {"target_date": target_time.date()})
        return result
    except Exception as e:
        logger.error(f"Error getting today's regions: {str(e)}")
        return []

async def validator_prepare_subtasks(self):
    """Prepare the subtasks for execution.

    Returns:
        List[Dict]: List of subtasks
    """
    pass

async def add_task_to_queue(
    self, responses: Dict[str, Any], metadata: Dict[str, Any]
):
    """Add predictions to the database queue."""
    try:
        logger.info(f"Starting add_task_to_queue with metadata: {metadata}")
        logger.info(f"Adding predictions to queue for {len(responses)} miners")
        if not self.db_manager:
            raise RuntimeError("Database manager not initialized")

        # Update region status
        update_query = """
            UPDATE soil_moisture_regions 
            SET status = 'sent_to_miners' 
            WHERE id = :region_id
        """
        await self.db_manager.execute(update_query, {"region_id": metadata["region_id"]})

        for miner_hotkey, response_data in responses.items():
            try:
                logger.info(f"Raw response data for miner {miner_hotkey}: {response_data.keys() if isinstance(response_data, dict) else 'not dict'}")
                logger.info(f"Processing prediction from miner {miner_hotkey}")
                
                # Get miner UID
                query = "SELECT uid FROM node_table WHERE hotkey = :miner_hotkey"
                result = await self.db_manager.fetch_one(query, {"miner_hotkey": miner_hotkey})
                if not result:
                    logger.warning(f"No UID found for hotkey {miner_hotkey}")
                    continue
                miner_uid = str(result["uid"])

                if isinstance(response_data, dict) and "text" in response_data:
                    try:
                        response_data = json.loads(response_data["text"])
                        logger.info(f"Parsed response data for miner {miner_hotkey}: {response_data.keys()}")
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse response text for {miner_hotkey}: {e}")
                        continue

                prediction_data = {
                    "surface_sm": response_data.get("surface_sm", []),
                    "rootzone_sm": response_data.get("rootzone_sm", []),
                    "uncertainty_surface": response_data.get("uncertainty_surface"),
                    "uncertainty_rootzone": response_data.get("uncertainty_rootzone"),
                    "sentinel_bounds": response_data.get("sentinel_bounds", metadata.get("sentinel_bounds")),
                    "sentinel_crs": response_data.get("sentinel_crs", metadata.get("sentinel_crs")),
                    "target_time": metadata["target_time"]
                }

                # Validate that returned bounds and CRS match the original request
                original_bounds = metadata.get("sentinel_bounds")
                original_crs = metadata.get("sentinel_crs")
                returned_bounds = response_data.get("sentinel_bounds")
                returned_crs = response_data.get("sentinel_crs")
                if returned_bounds != original_bounds:
                    logger.warning(f"Miner {miner_hotkey} returned different bounds than requested. Rejecting prediction.")
                    logger.warning(f"Original: {original_bounds}")
                    logger.warning(f"Returned: {returned_bounds}")
                    continue
                if returned_crs != original_crs:
                    logger.warning(f"Miner {miner_hotkey} returned different CRS than requested. Rejecting prediction.")
                    logger.warning(f"Original: {original_crs}")
                    logger.warning(f"Returned: {returned_crs}")
                    continue

                if not SoilMoisturePrediction.validate_prediction(prediction_data):
                    logger.warning(f"Skipping invalid prediction from miner {miner_hotkey}")
                    continue

                db_prediction_data = {
                    "region_id": metadata["region_id"],
                    "miner_uid": miner_uid,
                    "miner_hotkey": miner_hotkey,
                    "target_time": metadata["target_time"],
                    "surface_sm": prediction_data["surface_sm"],
                    "rootzone_sm": prediction_data["rootzone_sm"],
                    "uncertainty_surface": prediction_data["uncertainty_surface"],
                    "uncertainty_rootzone": prediction_data["uncertainty_rootzone"],
                    "sentinel_bounds": prediction_data["sentinel_bounds"],
                    "sentinel_crs": prediction_data["sentinel_crs"],
                    "status": "sent_to_miner",
                }

                logger.info(f"About to insert prediction_data for miner {miner_hotkey}: {db_prediction_data}")

                insert_query = """
                    INSERT INTO soil_moisture_predictions 
                    (region_id, miner_uid, miner_hotkey, target_time, surface_sm, rootzone_sm, 
                    uncertainty_surface, uncertainty_rootzone, sentinel_bounds, 
                    sentinel_crs, status)
                    VALUES 
                    (:region_id, :miner_uid, :miner_hotkey, :target_time, 
                    :surface_sm, :rootzone_sm,
                    :uncertainty_surface, :uncertainty_rootzone, :sentinel_bounds,
                    :sentinel_crs, :status)
                """
                await self.db_manager.execute(insert_query, db_prediction_data)

                # Verify insertion
                verify_query = """
                    SELECT COUNT(*) as count 
                    FROM soil_moisture_predictions 
                    WHERE miner_hotkey = :hotkey 
                    AND target_time = :target_time
                """
                verify_params = {
                    "hotkey": db_prediction_data["miner_hotkey"], 
                    "target_time": db_prediction_data["target_time"]
                }
                result = await self.db_manager.fetch_one(verify_query, verify_params)
                logger.info(f"Verification found {result['count']} matching records")

                logger.info(f"Successfully stored prediction for miner {miner_hotkey} (UID: {miner_uid}) for region {metadata['region_id']}")

            except Exception as e:
                logger.error(f"Error processing response from miner {miner_hotkey}: {str(e)}")
                logger.error(traceback.format_exc())
                continue

    except Exception as e:
        logger.error(f"Error storing predictions: {str(e)}")
        logger.error(traceback.format_exc())
        raise

async def get_pending_tasks(self):
    """Get tasks that are ready for scoring and haven't been scored yet."""

    if self.test_mode: # Force scoring to use old data in test mode
        scoring_time = datetime.now(timezone.utc)
        scoring_time = scoring_time.replace(hour=19, minute=30, second=0, microsecond=0)
    else:
        scoring_time = datetime.now(timezone.utc) - self.scoring_delay
    
    try:
        debug_query = """
            SELECT p.status, COUNT(*) as count, MIN(r.target_time) as earliest, MAX(r.target_time) as latest
            FROM soil_moisture_predictions p
            JOIN soil_moisture_regions r ON p.region_id = r.id
            GROUP BY p.status
        """
        debug_result = await self.db_manager.fetch_all(debug_query)
        for row in debug_result:
            logger.info(f"Status: {row['status']}, Count: {row['count']}, Time Range: {row['earliest']} to {row['latest']}")

        pending_query = """
            SELECT 
                r.*,
                json_agg(json_build_object(
                    'miner_id', p.miner_uid,
                    'miner_hotkey', p.miner_hotkey,
                    'surface_sm', p.surface_sm,
                    'rootzone_sm', p.rootzone_sm,
                    'uncertainty_surface', p.uncertainty_surface,
                    'uncertainty_rootzone', p.uncertainty_rootzone
                )) as predictions
            FROM soil_moisture_regions r
            JOIN soil_moisture_predictions p ON p.region_id = r.id
            WHERE p.status = 'sent_to_miner'
            AND (
                -- Normal case: Past scoring delay and no retry
                (
                    r.target_time <= :scoring_time 
                    AND p.next_retry_time IS NULL
                )
                OR 
                -- Retry case: Has retry time and it's in the past
                (
                    p.next_retry_time IS NOT NULL 
                    AND p.next_retry_time <= :current_time
                    AND p.retry_count < 5
                )
            )
            GROUP BY r.id, r.target_time, r.sentinel_bounds, r.sentinel_crs, r.status
            ORDER BY r.target_time ASC
        """
        params = {
            "scoring_time": scoring_time,
            "current_time": datetime.now(timezone.utc)
        }
        result = await self.db_manager.fetch_all(pending_query, params)
        if not result:
            await asyncio.sleep(60)  # Sleep for 60 seconds when no tasks are found
        return result

    except Exception as e:
        logger.error(f"Error fetching pending tasks: {str(e)}")
        return []

async def move_task_to_history(
    self, region: Dict, predictions: Dict, ground_truth: Dict, scores: Dict
):
    """Move completed task data to history tables."""
    try:
        logger.info(f"Final scores for region {region['id']}:")
        logger.info(f"Surface RMSE: {scores['metrics'].get('surface_rmse'):.4f}")
        logger.info(f"Surface SSIM: {scores['metrics'].get('surface_ssim', 0):.4f}")
        logger.info(f"Rootzone RMSE: {scores['metrics'].get('rootzone_rmse'):.4f}")
        logger.info(f"Rootzone SSIM: {scores['metrics'].get('rootzone_ssim', 0):.4f}")
        logger.info(f"Total Score: {scores.get('total_score', 0):.4f}")

        for prediction in predictions:
            try:
                miner_id = prediction["miner_id"]
                params = {
                    "region_id": region["id"],
                    "miner_uid": miner_id,
                    "miner_hotkey": prediction.get("miner_hotkey", ""),
                    "target_time": region["target_time"],
                    "surface_sm_pred": prediction["surface_sm"],
                    "rootzone_sm_pred": prediction["rootzone_sm"],
                    "surface_sm_truth": ground_truth["surface_sm"] if ground_truth else None,
                    "rootzone_sm_truth": ground_truth["rootzone_sm"] if ground_truth else None,
                    "surface_rmse": scores["metrics"].get("surface_rmse"),
                    "rootzone_rmse": scores["metrics"].get("rootzone_rmse"),
                    "surface_structure_score": scores["metrics"].get("surface_ssim", 0),
                    "rootzone_structure_score": scores["metrics"].get("rootzone_ssim", 0),
                }

                insert_query = """
                    INSERT INTO soil_moisture_history 
                    (region_id, miner_uid, miner_hotkey, target_time,
                        surface_sm_pred, rootzone_sm_pred,
                        surface_sm_truth, rootzone_sm_truth,
                        surface_rmse, rootzone_rmse,
                        surface_structure_score, rootzone_structure_score)
                    VALUES 
                    (:region_id, :miner_uid, :miner_hotkey, :target_time,
                        :surface_sm_pred, :rootzone_sm_pred,
                        :surface_sm_truth, :rootzone_sm_truth,
                        :surface_rmse, :rootzone_rmse,
                        :surface_structure_score, :rootzone_structure_score)
                """
                await self.db_manager.execute(insert_query, params)

                update_query = """
                    UPDATE soil_moisture_predictions 
                    SET status = 'scored'
                    WHERE region_id = :region_id 
                    AND miner_uid = :miner_uid
                    AND status = 'sent_to_miner'
                """
                await self.db_manager.execute(update_query, {
                    "region_id": region["id"],
                    "miner_uid": miner_id
                })

            except Exception as e:
                logger.error(f"Error processing prediction for miner {miner_id}: {str(e)}")
                continue

        logger.info(f"Moved {len(predictions)} tasks to history for region {region['id']}")

        await self.cleanup_predictions(
            bounds=region["sentinel_bounds"],
            target_time=region["target_time"],
            miner_uid=miner_id
        )

        return True

    except Exception as e:
        logger.error(f"Failed to move task to history: {str(e)}")
        logger.error(traceback.format_exc())
        return False
        


def get_ifs_time_for_smap(self, smap_time: datetime) -> datetime:
    """Get corresponding IFS forecast time for SMAP target time."""
    smap_to_ifs = {
        1: 0,  # 01:30 uses 00:00 forecast
        7: 6,  # 07:30 uses 06:00 forecast
        13: 12,  # 13:30 uses 12:00 forecast
        19: 18,  # 19:30 uses 18:00 forecast
    }

    ifs_hour = smap_to_ifs.get(smap_time.hour)
    if ifs_hour is None:
        raise ValueError(f"Invalid SMAP time: {smap_time.hour}:30")

    return smap_time.replace(hour=ifs_hour, minute=0, second=0, microsecond=0)

def get_smap_time_for_validator(self, current_time: datetime) -> datetime:
    """Get SMAP time based on validator execution time."""
    if self.test_mode:
        smap_hours = [1, 7, 13, 19]
        current_hour = current_time.hour
        closest_hour = min(smap_hours, key=lambda x: abs(x - current_hour))
        return current_time.replace(
            hour=1, minute=30, second=0, microsecond=0
        )

    validator_to_smap = {
        1: 1,    # 1:30 prep → 1:30 SMAP
        2: 1,    # 2:00 execution → 1:30 SMAP
        9: 7,    # 9:30 prep → 7:30 SMAP
        10: 7,   # 10:00 execution → 7:30 SMAP
        13: 13,  # 13:30 prep → 13:30 SMAP
        14: 13,  # 14:00 execution → 13:30 SMAP
        19: 19,  # 19:30 prep → 19:30 SMAP
        20: 19,  # 20:00 execution → 19:30 SMAP
    }
    smap_hour = validator_to_smap.get(current_time.hour)
    if smap_hour is None:
        raise ValueError(f"No SMAP time mapping for validator hour {current_time.hour}")
    return current_time.replace(hour=smap_hour, minute=30, second=0, microsecond=0)

def get_validator_windows(self, current_time: datetime) -> Tuple[int, int, int, int]:
    """Get all validator windows (hour_start, min_start, hour_end, min_end)."""
    windows = [
        (1, 30, 2, 0),  # Prep window for 1:30 SMAP time
        (2, 0, 2, 30),  # Execution window for 1:30 SMAP time
        (9, 30, 10, 0),  # Prep window for 7:30 SMAP time
        (10, 0, 10, 30),  # Execution window for 7:30 SMAP time
        (13, 30, 14, 0),  # Prep window for 13:30 SMAP time
        (14, 0, 14, 30),  # Execution window for 13:30 SMAP time
        (19, 30, 20, 0),  # Prep window for 19:30 SMAP time
        (20, 0, 20, 30),  # Execution window for 19:30 SMAP time
    ]
    return next(
        (w for w in windows if self.is_in_window(current_time, w)), None
    )

def is_in_window(
    self, current_time: datetime, window: Tuple[int, int, int, int]
) -> bool:
    """Check if current time is within a specific window."""
    start_hr, start_min, end_hr, end_min = window
    current_mins = current_time.hour * 60 + current_time.minute
    window_start_mins = start_hr * 60 + start_min
    window_end_mins = end_hr * 60 + end_min
    return window_start_mins <= current_mins < window_end_mins

async def build_score_row(self, target_time, recent_tasks=None):
    """Build score row for global scoring mechanism."""
    try:
        current_time = datetime.now(timezone.utc)
        scores = [float("nan")] * 256

        # Get miner mappings
        miner_query = """
            SELECT uid, hotkey FROM node_table 
            WHERE hotkey IS NOT NULL
        """
        miner_mappings = await self.db_manager.fetch_all(miner_query)
        hotkey_to_uid = {row["hotkey"]: row["uid"] for row in miner_mappings}
        logger.info(f"Found {len(hotkey_to_uid)} miner mappings: {hotkey_to_uid}")

        scores = [float("nan")] * 256
        current_datetime = datetime.fromisoformat(str(target_time))

        if recent_tasks:
            logger.info(f"Processing {len(recent_tasks)} recent tasks")
            miner_scores = {}
            
            for task in recent_tasks:
                task_score = task.get("score", {})
                for prediction in task.get("predictions", []):
                    miner_id = prediction.get("miner_id")
                    if miner_id not in miner_scores:
                        miner_scores[miner_id] = []
                    if isinstance(task_score.get("total_score"), (int, float)):
                        miner_scores[miner_id].append(float(task_score["total_score"]))
                        logger.info(f"Added score {task_score['total_score']} for miner_id {miner_id} in region {task['id']}")

            for miner_id, scores_list in miner_scores.items():
                if scores_list:
                    scores[int(miner_id)] = sum(scores_list) / len(scores_list)
                    logger.info(f"Final average score for miner {miner_id}: {scores[int(miner_id)]} across {len(scores_list)} regions")

        score_row = {
            "task_name": "soil_moisture",
            "task_id": str(current_datetime.timestamp()),
            "score": scores,
            "status": "completed"
        }
        
        logger.info(f"Raw score row being inserted: {json.dumps({**score_row, 'score': [f'{s:.4f}' if not math.isnan(s) else 'nan' for s in score_row['score']]})}")

        return [score_row]

    except Exception as e:
        logger.error(f"Error building score row: {e}")
        logger.error(traceback.format_exc())
        return []


async def cleanup_predictions(self, bounds, target_time=None, miner_uid=None):
    """Clean up predictions after they've been processed and moved to history."""
    try:
        delete_query = """
            DELETE FROM soil_moisture_predictions p
            USING soil_moisture_regions r
            WHERE p.region_id = r.id 
            AND r.sentinel_bounds = :bounds
            AND r.target_time = :target_time
            AND p.miner_uid = :miner_uid
            AND p.status = 'scored'
        """
        params = {
            "bounds": bounds,
            "target_time": target_time,
            "miner_uid": miner_uid
        }
        await self.db_manager.execute(delete_query, params)
        
        logger.info(
            f"Cleaned up predictions for bounds {bounds}"
            f"{f', time {target_time}' if target_time else ''}"
            f"{f', miner {miner_uid}' if miner_uid else ''}"
        )

    except Exception as e:
        logger.error(f"Failed to cleanup predictions: {str(e)}")
        logger.error(traceback.format_exc())

async def ensure_retry_columns_exist(self):
    """Ensure retry-related columns exist in soil_moisture_predictions table."""
    try:
        # Check if columns exist
        check_query = """
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.columns 
                WHERE table_name = 'soil_moisture_predictions' 
                AND column_name = 'retry_count'
            )
        """
        result = await self.db_manager.fetch_one(check_query)
        columns_exist = result["exists"] if result else False
        
        if not columns_exist:
            logger.info("Adding retry columns to soil_moisture_predictions table")
            alter_query = """
                ALTER TABLE soil_moisture_predictions 
                ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0,
                ADD COLUMN IF NOT EXISTS next_retry_time TIMESTAMP WITH TIME ZONE,
                ADD COLUMN IF NOT EXISTS last_retry_at TIMESTAMP WITH TIME ZONE;
                
                -- Add index on status for faster cleanup queries
                CREATE INDEX IF NOT EXISTS idx_soil_predictions_status 
                ON soil_moisture_predictions(status);
                
                -- Add index on region table status
                CREATE INDEX IF NOT EXISTS idx_region_status 
                ON region_table(status);
                
                -- Add index on score table status and task_name
                CREATE INDEX IF NOT EXISTS idx_score_status_task 
                ON score_table(status, task_name);
            """
            await self.db_manager.execute(alter_query)
            logger.info("Successfully added retry columns and indices")
        
    except Exception as e:
        logger.error(f"Error ensuring retry columns exist: {e}")
        logger.error(traceback.format_exc())
        raise

async def cleanup_resources(self):
    """Clean up any resources used by the task during recovery."""
    try:
        current_time = datetime.now(timezone.utc)
        cutoff_time = current_time - timedelta(hours=1)  # Only clean items older than 1 hour
        
        # Use a single transaction for all cleanup operations
        async with self.db_manager.transaction() as transaction:  # Changed from with_transaction()
            # Reset predictions status in batch
            await asyncio.wait_for(
                self.db_manager.execute(
                    """
                    UPDATE soil_predictions 
                    SET status = 'pending', retry_count = 0
                    WHERE status = 'processing'
                    AND created_at < :cutoff_time
                    """,
                    {"cutoff_time": cutoff_time},
                    transaction=transaction
                ),
                timeout=10  # 10 second timeout
            )
            logger.info("Reset in-progress prediction statuses")
            
            # Clean up incomplete scoring operations
            await asyncio.wait_for(
                self.db_manager.execute(
                    """
                    DELETE FROM score_table 
                    WHERE task_name = 'soil_moisture' 
                    AND status = 'processing'
                    AND created_at < :cutoff_time
                    """,
                    {"cutoff_time": cutoff_time},
                    transaction=transaction
                ),
                timeout=10  # 10 second timeout
            )
            logger.info("Cleaned up incomplete scoring operations")
            
            # Clean up temporary region states
            await asyncio.wait_for(
                self.db_manager.execute(
                    """
                    UPDATE region_table 
                    SET status = 'pending', retry_count = 0
                    WHERE status IN ('processing', 'failed')
                    AND updated_at < :cutoff_time
                    """,
                    {"cutoff_time": cutoff_time},
                    transaction=transaction
                ),
                timeout=10  # 10 second timeout
            )
            logger.info("Reset region processing states")
        
        # Clean up any temporary files older than 1 hour
        try:
            import glob
            import os
            from pathlib import Path
            
            for pattern in ['*.h5', '*.nc', '*.tif']:
                for f in glob.glob(f"/tmp/*{pattern}"):
                    try:
                        path = Path(f)
                        # Only remove files older than cutoff time
                        if path.stat().st_mtime < cutoff_time.timestamp():
                            os.unlink(f)
                            logger.debug(f"Cleaned up old temp file: {f}")
                    except Exception as e:
                        logger.error(f"Failed to remove temp file {f}: {e}")
        except Exception as e:
            logger.error(f"Error during temp file cleanup: {e}")
            # Don't raise here to allow database cleanup to succeed
        
        logger.info("Completed soil moisture task cleanup")
        
    except asyncio.TimeoutError:
        logger.error("Cleanup operation timed out")
        # Don't raise here to allow partial cleanup
    except Exception as e:
        logger.error(f"Error during soil moisture task cleanup: {e}")
        logger.error(traceback.format_exc())
        raise