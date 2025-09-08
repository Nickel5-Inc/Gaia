#!/usr/bin/env python3
"""
Script to backfill weather statistics from existing weather_miner_scores data.

This is a one-time migration script to populate the new weather_forecast_stats
and miner_stats tables from existing scoring data.

Usage:
    python scripts/backfill_weather_stats.py [--days 7] [--batch-size 100]
"""

import argparse
import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone

from loguru import logger

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gaia.validator.database.validator_database_manager import \
    ValidatorDatabaseManager
from gaia.validator.stats.weather_stats_hooks import WeatherStatsHooks


async def backfill_stats(days: int = 7, batch_size: int = 100, validator_hotkey: str = None):
    """
    Backfill weather statistics from existing data.
    
    Args:
        days: Number of days to look back
        batch_size: Number of runs to process at once
        validator_hotkey: Validator hotkey (optional, for tracking)
    """
    logger.info(f"Starting backfill for last {days} days")
    
    # Initialize database connection
    db_manager = ValidatorDatabaseManager(
        server="localhost",
        port=5432,
        database="validator_db",
        username="postgres",
        password="postgres"
    )
    
    try:
        await db_manager.initialize_database()
        
        # Get validator hotkey if not provided
        if not validator_hotkey:
            # Try to get from node_table or use placeholder
            query = "SELECT hotkey FROM node_table WHERE uid = 252 LIMIT 1"
            result = await db_manager.fetch_one(query)
            validator_hotkey = result["hotkey"] if result else "validator_unknown"
        
        # Initialize stats hooks
        stats_hooks = WeatherStatsHooks(
            database_manager=db_manager,
            validator_hotkey=validator_hotkey
        )
        
        # Get all runs from the specified time period
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        
        runs_query = """
            SELECT DISTINCT 
                wfr.id as run_id,
                wfr.gfs_init_time_utc,
                wfr.run_initiation_time,
                COUNT(DISTINCT wms.miner_uid) as miner_count,
                COUNT(wms.id) as score_count
            FROM weather_forecast_runs wfr
            LEFT JOIN weather_miner_scores wms ON wms.run_id = wfr.id
            WHERE wfr.run_initiation_time > :cutoff
            GROUP BY wfr.id, wfr.gfs_init_time_utc, wfr.run_initiation_time
            ORDER BY wfr.run_initiation_time DESC
        """
        
        runs = await db_manager.fetch_all(runs_query, {"cutoff": cutoff_date})
        
        if not runs:
            logger.warning("No runs found in the specified time period")
            return
        
        logger.info(f"Found {len(runs)} runs to process")
        
        total_scores_processed = 0
        runs_processed = 0
        
        for i, run in enumerate(runs):
            run_id = run["run_id"]
            score_count = run["score_count"] or 0
            miner_count = run["miner_count"] or 0
            
            if score_count == 0:
                logger.debug(f"Run {run_id}: No scores to process")
                continue
            
            logger.info(
                f"Processing run {run_id} ({i+1}/{len(runs)}): "
                f"{score_count} scores from {miner_count} miners"
            )
            
            # Extract all scores for this run
            scores_query = """
                SELECT 
                    wms.miner_uid,
                    wms.miner_hotkey,
                    wms.score_type,
                    wms.score,
                    wms.lead_hours,
                    wms.calculation_time,
                    wmr.verification_passed,
                    wmr.status as response_status
                FROM weather_miner_scores wms
                JOIN weather_miner_responses wmr ON wmr.id = wms.response_id
                WHERE wms.run_id = :run_id
                ORDER BY wms.miner_uid, wms.score_type, wms.lead_hours
            """
            
            scores = await db_manager.fetch_all(scores_query, {"run_id": run_id})
            
            if scores:
                # Group scores by miner for efficient processing
                miner_groups = {}
                for score in scores:
                    miner_uid = score["miner_uid"]
                    if miner_uid not in miner_groups:
                        miner_groups[miner_uid] = []
                    miner_groups[miner_uid].append(dict(score))
                
                # Process each miner's scores
                for miner_uid, miner_scores in miner_groups.items():
                    # Convert to format expected by hooks
                    await stats_hooks.on_scores_inserted(
                        run_id=run_id,
                        scores_batch=miner_scores
                    )
                
                total_scores_processed += len(scores)
                
                # Mark run as complete to trigger aggregation
                await stats_hooks.on_run_scoring_complete(run_id)
                
                runs_processed += 1
                logger.info(
                    f"Processed {len(scores)} scores for run {run_id}. "
                    f"Total processed: {total_scores_processed}"
                )
            
            # Process in batches to avoid overwhelming the system
            if (i + 1) % batch_size == 0:
                logger.info(f"Completed batch of {batch_size} runs, pausing briefly...")
                await asyncio.sleep(2)
                # Clear cache to free memory
                stats_hooks.clear_cache()
        
        # Final aggregation
        logger.info("Running final aggregation...")
        from gaia.validator.stats.weather_stats_manager import \
            WeatherStatsManager
        stats_manager = WeatherStatsManager(db_manager, validator_hotkey)
        
        # Update overall miner stats
        await stats_manager.aggregate_miner_stats()
        await stats_manager.get_overall_miner_ranks()
        
        logger.info(
            f"✅ Backfill complete! Processed {runs_processed} runs with "
            f"{total_scores_processed} total scores"
        )
        
        # Show summary
        summary_query = """
            SELECT 
                COUNT(DISTINCT miner_uid) as total_miners,
                COUNT(DISTINCT forecast_run_id) as total_runs,
                AVG(era5_combined_score) as avg_score,
                MAX(updated_at) as last_update
            FROM weather_forecast_stats
        """
        summary = await db_manager.fetch_one(summary_query)
        
        if summary:
            logger.info(
                f"📊 Summary:\n"
                f"  - Total miners tracked: {summary['total_miners']}\n"
                f"  - Total forecast runs: {summary['total_runs']}\n"
                f"  - Average combined score: {summary['avg_score']:.4f if summary['avg_score'] else 0}\n"
                f"  - Last update: {summary['last_update']}"
            )
        
    except Exception as e:
        logger.error(f"Error during backfill: {e}", exc_info=True)
        raise
    finally:
        await db_manager.close_all_connections()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Backfill weather statistics from existing data"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to look back (default: 7)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of runs to process at once (default: 100)"
    )
    parser.add_argument(
        "--validator-hotkey",
        type=str,
        help="Validator hotkey for tracking (optional)"
    )
    
    args = parser.parse_args()
    
    # Configure logging
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
        level="INFO"
    )
    
    # Run the backfill
    asyncio.run(
        backfill_stats(
            days=args.days,
            batch_size=args.batch_size,
            validator_hotkey=args.validator_hotkey
        )
    )


if __name__ == "__main__":
    main()
