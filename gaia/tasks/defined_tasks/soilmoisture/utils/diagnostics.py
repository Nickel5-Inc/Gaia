from datetime import datetime, timedelta, timezone
import logging
from typing import Dict, List, Optional
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager

logger = logging.getLogger(__name__)

async def analyze_soil_moisture_tables(db_manager: ValidatorDatabaseManager) -> Dict:
    """
    Analyze soil moisture prediction and history tables to identify potential issues.
    
    Args:
        db_manager: Database manager instance for querying
        
    Returns:
        Dict containing analysis results
    """
    try:
        results = {}
        
        # 1. Count predictions by status
        status_query = """
            SELECT status, COUNT(*) as count 
            FROM soil_moisture_predictions 
            GROUP BY status
        """
        status_counts = await db_manager.fetch_all(status_query)
        results["prediction_status_counts"] = {
            row["status"]: row["count"] for row in status_counts
        }

        # 2. Check for scored predictions missing from history
        missing_history_query = """
            SELECT p.region_id, p.miner_uid, p.target_time
            FROM soil_moisture_predictions p
            LEFT JOIN soil_moisture_history h 
                ON p.region_id = h.region_id 
                AND p.miner_uid = h.miner_uid
            WHERE p.status = 'scored'
            AND h.region_id IS NULL
            LIMIT 100
        """
        missing_history = await db_manager.fetch_all(missing_history_query)
        results["missing_history_count"] = len(missing_history)
        results["missing_history_samples"] = missing_history[:5]  # First 5 examples

        # 3. Analyze prediction age distribution
        age_query = """
            SELECT 
                CASE 
                    WHEN age < interval '1 day' THEN 'under_1_day'
                    WHEN age < interval '3 days' THEN '1_to_3_days'
                    WHEN age < interval '7 days' THEN '3_to_7_days'
                    ELSE 'over_7_days'
                END as age_bucket,
                COUNT(*) as count
            FROM (
                SELECT 
                    NOW() - target_time as age
                FROM soil_moisture_predictions
                WHERE status != 'scored'
            ) subquery
            GROUP BY age_bucket
        """
        age_distribution = await db_manager.fetch_all(age_query)
        results["prediction_age_distribution"] = {
            row["age_bucket"]: row["count"] for row in age_distribution
        }

        # 4. Check for orphaned predictions (no matching region)
        orphaned_query = """
            SELECT p.region_id, p.miner_uid, p.target_time
            FROM soil_moisture_predictions p
            LEFT JOIN soil_moisture_regions r ON p.region_id = r.id
            WHERE r.id IS NULL
            LIMIT 100
        """
        orphaned_predictions = await db_manager.fetch_all(orphaned_query)
        results["orphaned_predictions_count"] = len(orphaned_predictions)
        results["orphaned_predictions_samples"] = orphaned_predictions[:5]

        # 5. History table statistics
        history_stats_query = """
            SELECT 
                COUNT(*) as total_records,
                MIN(target_time) as earliest_record,
                MAX(target_time) as latest_record,
                COUNT(DISTINCT miner_uid) as unique_miners
            FROM soil_moisture_history
        """
        history_stats = await db_manager.fetch_one(history_stats_query)
        results["history_stats"] = dict(history_stats)

        return results

    except Exception as e:
        logger.error(f"Error analyzing soil moisture tables: {e}")
        raise

async def print_analysis_results(results: Dict) -> None:
    """
    Print analysis results in a readable format.
    
    Args:
        results: Dictionary containing analysis results
    """
    print("\n=== Soil Moisture Task Analysis ===\n")
    
    print("Prediction Status Distribution:")
    for status, count in results["prediction_status_counts"].items():
        print(f"  {status}: {count}")
    
    print("\nMissing History Records:")
    print(f"  Total count: {results['missing_history_count']}")
    if results['missing_history_samples']:
        print("  Sample entries:")
        for sample in results['missing_history_samples']:
            print(f"    Region {sample['region_id']}, Miner {sample['miner_uid']}, Time {sample['target_time']}")
    
    print("\nPrediction Age Distribution:")
    for bucket, count in results["prediction_age_distribution"].items():
        print(f"  {bucket}: {count}")
    
    print("\nOrphaned Predictions:")
    print(f"  Total count: {results['orphaned_predictions_count']}")
    if results['orphaned_predictions_samples']:
        print("  Sample entries:")
        for sample in results['orphaned_predictions_samples']:
            print(f"    Region {sample['region_id']}, Miner {sample['miner_uid']}, Time {sample['target_time']}")
    
    print("\nHistory Table Statistics:")
    stats = results["history_stats"]
    print(f"  Total records: {stats['total_records']}")
    print(f"  Date range: {stats['earliest_record']} to {stats['latest_record']}")
    print(f"  Unique miners: {stats['unique_miners']}")

async def main():
    """
    Main function to run diagnostics.
    """
    try:
        db_manager = ValidatorDatabaseManager()
        results = await analyze_soil_moisture_tables(db_manager)
        await print_analysis_results(results)
    except Exception as e:
        logger.error(f"Error running diagnostics: {e}")
        raise
    finally:
        if db_manager:
            await db_manager.close()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main()) 