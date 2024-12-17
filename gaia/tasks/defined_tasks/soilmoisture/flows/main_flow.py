from datetime import datetime, timezone
from prefect import flow, task, get_run_logger
from typing import Dict, List, Optional, Tuple, Any, Union
import asyncio
from .data_collection_flow import collect_soil_data_flow
from .scoring_flow import soil_scoring_flow
from ..soil_task import SoilMoistureTask

logger = get_run_logger()

@task(
    name="prepare_regions_task",
    retries=2,
    tags=["preprocessing"],
    description="Prepare regions for processing"
)
async def prepare_regions_task(
    task: SoilMoistureTask,
    target_time: datetime,
    ifs_forecast_time: datetime
) -> Optional[List[Dict[str, Any]]]:
    """
    Prepare regions for processing with Prefect task management.
    """
    try:
        logger.info(f"Preparing regions for target time: {target_time}")
        regions = await task.get_daily_regions(target_time, ifs_forecast_time)
        if regions:
            logger.info(f"Successfully prepared {len(regions)} regions")
            return regions
        logger.error("No regions prepared")
        return None
    except Exception as e:
        logger.error(f"Error preparing regions: {str(e)}")
        raise

@task(
    name="process_region_task",
    retries=3,
    tags=["processing"],
    description="Process a single region"
)
async def process_region_task(
    task: SoilMoistureTask,
    region: Dict[str, Any],
    target_time: datetime,
    ifs_forecast_time: datetime,
    current_time: datetime,
    validator: Any
) -> Optional[Dict[str, Any]]:
    """
    Process a single region with Prefect task management.
    """
    try:
        logger.info(f"Processing region {region['id']}")
        result = await task.process_region(
            region=region,
            target_time=target_time,
            ifs_forecast_time=ifs_forecast_time,
            current_time=current_time,
            validator=validator
        )
        if result:
            logger.info(f"Successfully processed region {region['id']}")
            return result
        logger.error(f"Failed to process region {region['id']}")
        return None
    except Exception as e:
        logger.error(f"Error processing region: {str(e)}")
        raise

@task(
    name="add_to_queue_task",
    retries=2,
    tags=["database"],
    description="Add processed results to task queue"
)
async def add_to_queue_task(
    task: SoilMoistureTask,
    responses: Dict[str, Any],
    metadata: Dict[str, Any]
) -> bool:
    """
    Add processed results to task queue with Prefect task management.
    """
    try:
        logger.info(f"Adding results to queue for region {metadata['region_id']}")
        await task.add_task_to_queue(responses, metadata)
        logger.info("Successfully added to queue")
        return True
    except Exception as e:
        logger.error(f"Error adding to queue: {str(e)}")
        raise

@flow(
    name="soil_moisture_main_flow",
    retries=3,
    description="Main soil moisture prediction and validation flow"
)
async def soil_moisture_main_flow(
    task: SoilMoistureTask,
    validator: Any,
    test_mode: bool = False
) -> Dict[str, Any]:
    """
    Main flow for soil moisture prediction and validation.
    """
    try:
        flow_state = {
            "status": "running",
            "processed_regions": [],
            "failed_regions": [],
            "scores": [],
            "start_time": datetime.now(timezone.utc).isoformat()
        }
        
        current_time = datetime.now(timezone.utc)
        target_time = task.get_smap_time_for_validator(current_time)
        ifs_forecast_time = task.get_ifs_time_for_smap(target_time)
        
        logger.info(f"Starting soil moisture flow for target time: {target_time}")
        
        # Prepare regions
        regions = await prepare_regions_task(task, target_time, ifs_forecast_time)
        if not regions:
            flow_state["status"] = "failed"
            flow_state["error"] = "No regions prepared"
            return flow_state
            
        # Process each region
        for region in regions:
            try:
                # Collect data
                data_result = await collect_soil_data_flow(
                    region["bbox"],
                    target_time,
                    region["sentinel_bounds"],
                    region["sentinel_crs"],
                    (222, 222)  # Default shape for soil moisture
                )
                
                if not data_result:
                    logger.error(f"Failed to collect data for region {region['id']}")
                    flow_state["failed_regions"].append({
                        "region_id": region["id"],
                        "error": "Data collection failed"
                    })
                    continue
                    
                # Process region
                process_result = await process_region_task(
                    task,
                    region,
                    target_time,
                    ifs_forecast_time,
                    current_time,
                    validator
                )
                
                if not process_result:
                    logger.error(f"Failed to process region {region['id']}")
                    flow_state["failed_regions"].append({
                        "region_id": region["id"],
                        "error": "Processing failed"
                    })
                    continue
                    
                # Score predictions if available
                if "predictions" in process_result:
                    score_result = await soil_scoring_flow(
                        process_result["predictions"],
                        region["sentinel_bounds"],
                        region["sentinel_crs"],
                        target_time
                    )
                    
                    if score_result:
                        flow_state["scores"].append(score_result)
                        
                # Add to queue
                if "responses" in process_result and "metadata" in process_result:
                    queue_result = await add_to_queue_task(
                        task,
                        process_result["responses"],
                        process_result["metadata"]
                    )
                    
                    if queue_result:
                        flow_state["processed_regions"].append(region["id"])
                        
            except Exception as e:
                logger.error(f"Error processing region {region['id']}: {str(e)}")
                flow_state["failed_regions"].append({
                    "region_id": region["id"],
                    "error": str(e)
                })
                continue
                
        # Update flow state
        flow_state["end_time"] = datetime.now(timezone.utc).isoformat()
        flow_state["status"] = "completed"
        if flow_state["failed_regions"]:
            flow_state["status"] = "completed_with_errors"
            
        logger.info(f"Flow completed with state: {flow_state}")
        return flow_state
        
    except Exception as e:
        logger.error(f"Error in main flow: {str(e)}")
        flow_state["status"] = "failed"
        flow_state["error"] = str(e)
        return flow_state 