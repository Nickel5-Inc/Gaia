import asyncio
import math
import time
import traceback
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any

import numpy as np
from fiber.logging_utils import get_logger
from fiber.validator.weights import FiberWeightSetter
import gaia.validator.weights as w

logger = get_logger(__name__)


class ScoringEngine:
    """
    Handles all weight calculation and scoring logic for the validator.
    Extracted from the monolithic validator to improve maintainability.
    """
    
    def __init__(self, validator):
        self.validator = validator
        
    def get_current_task_weights(self) -> Dict[str, float]:
        """
        Get current task weights based on the stepped schedule.
        Returns the weights that should be applied at the current time.
        """
        current_time = datetime.now(datetime.now().astimezone().tzinfo)
        
        # Find the appropriate weights for the current time
        applicable_weights = None
        for threshold_time, weights_dict in self.validator.task_weight_schedule:
            if current_time >= threshold_time:
                applicable_weights = weights_dict
            else:
                break  # Schedule is in ascending order, so we can stop here
        
        if applicable_weights is None:
            # Fallback to the first entry if current time is before all thresholds
            applicable_weights = self.validator.task_weight_schedule[0][1]
            logger.warning(f"Current time {current_time.isoformat()} is before all scheduled thresholds. Using first weights: {applicable_weights}")
        
        logger.debug(f"Current task weights at {current_time.isoformat()}: {applicable_weights}")
        return applicable_weights

    def perform_weight_calculations_sync(self, weather_results, geomagnetic_results, soil_results, now, validator_nodes_by_uid_list):
        """
        Synchronous helper to perform CPU-bound weight calculations.
        Memory-optimized version with explicit cleanup.
        """
        logger.info("Synchronous weight calculation: Processing fetched scores...")
        
        try:
            # Initialize score arrays
            weather_scores = np.full(256, np.nan)
            geomagnetic_scores = np.full(256, np.nan)
            soil_scores = np.full(256, np.nan)

            # Count raw scores per UID - use numpy for better memory efficiency
            weather_counts = np.zeros(256, dtype=int)
            geo_counts = np.zeros(256, dtype=int)
            soil_counts = np.zeros(256, dtype=int)
        
            if weather_results:
                for result in weather_results:
                    # Ensure 'score' key exists and is a list of appropriate length
                    scores = result.get('score', [np.nan]*256)
                    if not isinstance(scores, list) or len(scores) != 256: 
                        scores = [np.nan]*256  # Defensive
                    for uid in range(256):
                        if not isinstance(scores[uid], str) and not np.isnan(scores[uid]) and scores[uid] != 0.0:
                            weather_counts[uid] += 1
            
            if geomagnetic_results:
                for result in geomagnetic_results:
                    scores = result.get('score', [np.nan]*256)
                    if not isinstance(scores, list) or len(scores) != 256: 
                        scores = [np.nan]*256  # Defensive
                    for uid in range(256):
                        if not isinstance(scores[uid], str) and not np.isnan(scores[uid]) and scores[uid] != 0.0:
                            geo_counts[uid] += 1

            if soil_results:
                for result in soil_results:
                    scores = result.get('score', [np.nan]*256)
                    if not isinstance(scores, list) or len(scores) != 256: 
                        scores = [np.nan]*256  # Defensive
                    for uid in range(256):
                        if not isinstance(scores[uid], str) and not np.isnan(scores[uid]) and scores[uid] != 0.0:
                            soil_counts[uid] += 1

            # Process geomagnetic scores with memory-efficient approach
            if geomagnetic_results:
                zero_scores_count = 0
                for uid in range(256):
                    uid_scores = []
                    uid_weights = []
                    
                    # Collect scores for this UID across all results
                    for result in geomagnetic_results:
                        age_days = (now - result['created_at']).total_seconds() / (24 * 3600)
                        decay = np.exp(-age_days * np.log(2))
                        scores = result.get('score', [np.nan]*256)
                        if not isinstance(scores, list) or len(scores) != 256: 
                            scores = [np.nan]*256  # Defensive
                        
                        score_val = scores[uid]
                        if isinstance(score_val, str) or np.isnan(score_val): 
                            score_val = 0.0
                        if score_val == 0.0: 
                            zero_scores_count += 1
                        
                        uid_scores.append(score_val)
                        uid_weights.append(decay)
                        geo_counts[uid] += (score_val != 0.0)
                    
                    # Calculate weighted average for this UID
                    if uid_scores:
                        s_arr = np.array(uid_scores)
                        w_arr = np.array(uid_weights)
                        non_zero_mask = s_arr != 0.0
                        
                        if np.any(non_zero_mask):
                            masked_s = s_arr[non_zero_mask]
                            masked_w = w_arr[non_zero_mask]
                            weight_sum = np.sum(masked_w)
                            if weight_sum > 0:
                                geomagnetic_scores[uid] = np.sum(masked_s * masked_w) / weight_sum
                            else:
                                geomagnetic_scores[uid] = 0.0
                        else:
                            geomagnetic_scores[uid] = 0.0
                    
                    # Clean up temporary arrays immediately
                    del uid_scores, uid_weights
                
                logger.info(f"Processed {len(geomagnetic_results)} geomagnetic records with {zero_scores_count} zero scores")

            # Process weather scores
            if weather_results:
                latest_result = weather_results[0]
                scores = latest_result.get('score', [np.nan]*256)
                if not isinstance(scores, list) or len(scores) != 256: 
                    scores = [np.nan]*256  # Defensive
                score_age_days = (now - latest_result['created_at']).total_seconds() / (24 * 3600)
                logger.info(f"Using latest weather score from {latest_result['created_at']} ({score_age_days:.1f} days ago)")
                for uid in range(256):
                    if isinstance(scores[uid], str) or np.isnan(scores[uid]): 
                        weather_scores[uid] = 0.0
                    else: 
                        weather_scores[uid] = scores[uid]
                        weather_counts[uid] += (scores[uid] != 0.0)
                logger.info(f"Weather scores: {sum(1 for s in weather_scores if s == 0.0)} UIDs have zero score")

            # Process soil scores with memory-efficient approach
            if soil_results:
                zero_soil_scores = 0
                for uid in range(256):
                    uid_scores = []
                    uid_weights = []
                    
                    # Collect scores for this UID across all results
                    for result in soil_results:
                        age_days = (now - result['created_at']).total_seconds() / (24 * 3600)
                        decay = np.exp(-age_days * np.log(2))
                        scores = result.get('score', [np.nan]*256)
                        if not isinstance(scores, list) or len(scores) != 256: 
                            scores = [np.nan]*256  # Defensive
                        
                        score_val = scores[uid]
                        if isinstance(score_val, str) or np.isnan(score_val): 
                            score_val = 0.0
                        if score_val == 0.0: 
                            zero_soil_scores += 1
                        
                        uid_scores.append(score_val)
                        uid_weights.append(decay)
                        soil_counts[uid] += (score_val != 0.0)
                    
                    # Calculate weighted average for this UID
                    if uid_scores:
                        s_arr = np.array(uid_scores)
                        w_arr = np.array(uid_weights)
                        non_zero_mask = s_arr != 0.0
                        
                        if np.any(non_zero_mask):
                            masked_s = s_arr[non_zero_mask]
                            masked_w = w_arr[non_zero_mask]
                            weight_sum = np.sum(masked_w)
                            if weight_sum > 0:
                                soil_scores[uid] = np.sum(masked_s * masked_w) / weight_sum
                            else:
                                soil_scores[uid] = 0.0
                        else:
                            soil_scores[uid] = 0.0
                    
                    # Clean up temporary arrays immediately
                    del uid_scores, uid_weights
                
                logger.info(f"Processed {len(soil_results)} soil records with {zero_soil_scores} zero scores")

            logger.info("Aggregate scores calculated. Proceeding to weight normalization...")
            
            # Get current task weights from the stepped schedule
            current_task_weights = self.get_current_task_weights()
            
            def sigmoid(x, k=20, x0=0.93): 
                return 1 / (1 + math.exp(-k * (x - x0)))
            
            weights_final = np.zeros(256)
            for idx in range(256):
                w_s, g_s, sm_s = weather_scores[idx], geomagnetic_scores[idx], soil_scores[idx]
                if np.isnan(w_s) or w_s == 0: 
                    w_s = np.nan
                if np.isnan(g_s) or g_s == 0: 
                    g_s = np.nan
                if np.isnan(sm_s) or sm_s == 0: 
                    sm_s = np.nan
                    
                node_obj, hk_chain = validator_nodes_by_uid_list[idx] if idx < len(validator_nodes_by_uid_list) else None, "N/A"
                if node_obj: 
                    hk_chain = node_obj.get('hotkey', 'N/A')
                    
                if np.isnan(w_s) and np.isnan(g_s) and np.isnan(sm_s): 
                    weights_final[idx] = 0.0
                else:
                    wc, gc, sc, total_w_avail = 0.0, 0.0, 0.0, 0.0
                    
                    # Use dynamic task weights from the stepped schedule
                    weather_weight = current_task_weights.get("weather", 0.70)
                    geomagnetic_weight = current_task_weights.get("geomagnetic", 0.15)
                    soil_weight = current_task_weights.get("soil", 0.15)
                    
                    if not np.isnan(w_s): 
                        wc, total_w_avail = weather_weight * w_s, total_w_avail + weather_weight
                    if not np.isnan(g_s): 
                        gc, total_w_avail = geomagnetic_weight * sigmoid(g_s), total_w_avail + geomagnetic_weight
                    if not np.isnan(sm_s): 
                        sc, total_w_avail = soil_weight * sm_s, total_w_avail + soil_weight
                        
                    weights_final[idx] = (wc + gc + sc) / total_w_avail if total_w_avail > 0 else 0.0
                    
                # Reduce logging to prevent memory pressure - only log every 32nd UID or non-zero weights
                if idx % 32 == 0 or weights_final[idx] > 0.0:
                    logger.debug(f"UID {idx} (HK: {hk_chain}): Wea={w_s if not np.isnan(w_s) else '-'} ({weather_counts[idx]} scores), Geo={g_s if not np.isnan(g_s) else '-'} ({geo_counts[idx]} scores), Soil={sm_s if not np.isnan(sm_s) else '-'} ({soil_counts[idx]} scores), AggW={weights_final[idx]:.4f}")
                    
            logger.info(f"Weights before normalization: Min={np.min(weights_final):.4f}, Max={np.max(weights_final):.4f}, Mean={np.mean(weights_final):.4f}")
            logger.info(f"Task weight distribution: Weather={current_task_weights.get('weather', 0.70):.2f}, Geomagnetic={current_task_weights.get('geomagnetic', 0.15):.2f}, Soil={current_task_weights.get('soil', 0.15):.2f}")
            
            non_zero_mask = weights_final != 0.0
            if not np.any(non_zero_mask): 
                logger.warning("No non-zero weights to normalize!")
                return None
            
            nz_weights = weights_final[non_zero_mask]
            max_w_val = np.max(nz_weights)
            if max_w_val == 0: 
                logger.warning("Max weight is 0, cannot normalize.")
                return None
            
            # Normalize non-zero weights to [0, 1] range
            weights_final[non_zero_mask] = nz_weights / max_w_val
            
            logger.info(f"Weights after normalization: Min={np.min(weights_final):.4f}, Max={np.max(weights_final):.4f}, Mean={np.mean(weights_final):.4f}")
            logger.info(f"Non-zero weights: {np.sum(non_zero_mask)} out of 256 UIDs")
            
            return weights_final.tolist()
            
        except Exception as e:
            logger.error(f"Error in weight calculations: {e}", exc_info=True)
            return None
    
    async def run_main_scoring(self):
        """Run scoring every subnet tempo blocks."""
        weight_setter = FiberWeightSetter(
            netuid=self.validator.netuid,
            wallet_name=self.validator.wallet_name,
            hotkey_name=self.validator.hotkey_name,
            network=self.validator.subtensor_network,
            substrate_manager=self.validator.substrate_manager,
        )

        while True:
            try:
                await self.validator.update_task_status('scoring', 'active')
                
                async def scoring_cycle():
                    try:
                        validator_uid = self.validator.validator_uid
                        
                        if validator_uid is None:
                            try:
                                self.validator.validator_uid = self.validator.substrate.query(
                                    "SubtensorModule", 
                                    "Uids", 
                                    [self.validator.netuid, self.validator.keypair.ss58_address]
                                ).value
                                validator_uid = int(self.validator.validator_uid)
                            except Exception as e:
                                logger.error(f"Error getting validator UID: {e}")
                                logger.error(traceback.format_exc())
                                await self.validator.update_task_status('scoring', 'error')
                                return False
                            

                        validator_uid = int(validator_uid)
                        last_updated_value = self.validator.substrate.query(
                            "SubtensorModule",
                            "LastUpdate",
                            [self.validator.netuid]
                        ).value
                        if last_updated_value is not None and validator_uid < len(last_updated_value):
                            last_updated = int(last_updated_value[validator_uid])
                            resp = self.validator.substrate.rpc_request("chain_getHeader", [])  
                            hex_num = resp["result"]["number"]
                            current_block = int(hex_num, 16)
                            blocks_since_update = current_block - last_updated
                            logger.info(f"Calculated blocks since update: {blocks_since_update} (current: {current_block}, last: {last_updated})")
                        else:
                            blocks_since_update = None
                            logger.warning("Could not determine last update value")

                        min_interval = w.min_interval_to_set_weights(
                            self.validator.substrate, 
                            self.validator.netuid
                        )
                        if min_interval is not None:
                            min_interval = int(min_interval)
                        resp = self.validator.substrate.rpc_request("chain_getHeader", [])  
                        hex_num = resp["result"]["number"]
                        current_block = int(hex_num, 16)                     
                        if current_block - self.validator.last_set_weights_block < min_interval:
                            logger.info(f"Recently set weights {current_block - self.validator.last_set_weights_block} blocks ago")
                            await self.validator.update_task_status('scoring', 'idle', 'waiting')
                            await asyncio.sleep(60)
                            return True

                        # Only enter weight_setting state when actually setting weights
                        if (min_interval is None or 
                            (blocks_since_update is not None and blocks_since_update >= min_interval)):
                            logger.info(f"Setting weights: {blocks_since_update}/{min_interval} blocks")
                            can_set = w.can_set_weights(
                                self.validator.substrate, 
                                self.validator.netuid, 
                                validator_uid
                            )
                            
                            if can_set:
                                await self.validator.update_task_status('scoring', 'processing', 'weight_setting')
                                
                                # Calculate weights with timeout
                                normalized_weights = await asyncio.wait_for(
                                    self._calc_task_weights(),
                                    timeout=120
                                )
                                
                                if normalized_weights:
                                    # Set weights with timeout
                                    success = await asyncio.wait_for(
                                        weight_setter.set_weights(normalized_weights),
                                        timeout=480
                                    )
                                    
                                    if success:
                                        await self._update_last_weights_block()
                                        self.validator.last_successful_weight_set = time.time()
                                        logger.info("✅ Successfully set weights")
                                        await self.validator.update_task_status('scoring', 'idle')
                                        
                                        # Clean up any stale operations
                                        await self.validator.database_manager.cleanup_stale_operations('score_table')
                        else:
                            logger.info(
                                f"Waiting for weight setting: {blocks_since_update}/{min_interval} blocks"
                            )
                            await self.validator.update_task_status('scoring', 'idle', 'waiting')

                        return True
                        
                    except asyncio.TimeoutError as e:
                        logger.error(f"Timeout in scoring cycle: {str(e)}")
                        return False
                    except Exception as e:
                        logger.error(f"Error in scoring cycle: {str(e)}")
                        logger.error(traceback.format_exc())
                        return False
                    finally:
                        # Sleep removed - now handled in main loop for consistent timing
                        pass

                # Run scoring cycle with overall timeout
                await asyncio.wait_for(scoring_cycle(), timeout=900)

            except asyncio.TimeoutError:
                logger.error("Weight setting operation timed out - restarting cycle")
                await self.validator.update_task_status('scoring', 'error')
                try:
                    self.validator.substrate = self.validator.substrate_manager.force_reconnect()
                except Exception as e:
                    logger.error(f"Failed to reconnect to substrate: {e}")
                await asyncio.sleep(12)
                continue
            except Exception as e:
                logger.error(f"Error in main_scoring: {e}")
                logger.error(traceback.format_exc())
                await self.validator.update_task_status('scoring', 'error')
                await asyncio.sleep(12)
                continue
            
            # Add sleep to prevent rapid cycling when scoring completes quickly
            # This ensures consistent timing regardless of scoring outcome
            await asyncio.sleep(60)

    async def _calc_task_weights(self):
        """Calculate weights based on recent task scores. Async part fetches data."""
        try:
            # Log memory before large database operations
            self.validator._log_memory_usage("calc_weights_start")
            
            now = datetime.now(timezone.utc)
            one_day_ago = now - timedelta(days=1)
            
            query = """
            SELECT score, created_at 
            FROM score_table 
            WHERE task_name = :task_name AND created_at >= :start_time ORDER BY created_at DESC
            """
            weather_query = """
            SELECT score, created_at 
            FROM score_table 
            WHERE task_name = 'weather' AND created_at >= :start_time ORDER BY created_at DESC LIMIT 50
            """
            geomagnetic_query = """
            SELECT score, created_at 
            FROM score_table 
            WHERE task_name = 'geomagnetic' AND created_at >= :start_time ORDER BY created_at DESC LIMIT 50
            """
            soil_query = """
            SELECT score, created_at 
            FROM score_table 
            WHERE task_name LIKE 'soil_moisture_region_%' AND created_at >= :start_time ORDER BY created_at DESC LIMIT 200
            """

            # Query node table for validator nodes with chunking to manage memory
            validator_nodes_query = """
            SELECT uid, hotkey, ip, port, incentive 
            FROM node_table 
            WHERE uid IS NOT NULL 
            ORDER BY uid
            """

            params = {"start_time": one_day_ago}
            
            # Fetch all data concurrently using regular async approach
            try:
                self.validator._log_memory_usage("calc_weights_before_db_fetch")
                weather_results, geomagnetic_results, soil_results, validator_nodes_list = await asyncio.gather(
                    self.validator.database_manager.fetch_all(weather_query, params),
                    self.validator.database_manager.fetch_all(geomagnetic_query, params),
                    self.validator.database_manager.fetch_all(soil_query, params),
                    self.validator.database_manager.fetch_all(validator_nodes_query),
                    return_exceptions=True
                )
                self.validator._log_memory_usage("calc_weights_after_db_fetch")
                
                # Log individual dataset sizes
                if weather_results and not isinstance(weather_results, Exception):
                    logger.info(f"Weather dataset: {len(weather_results)} records")
                if geomagnetic_results and not isinstance(geomagnetic_results, Exception):
                    logger.info(f"Geomagnetic dataset: {len(geomagnetic_results)} records")
                if soil_results and not isinstance(soil_results, Exception):
                    logger.info(f"Soil dataset: {len(soil_results)} records")
                    # Check soil record size (they contain large score arrays)
                    if soil_results:
                        sample_soil = soil_results[0]
                        if 'score' in sample_soil and isinstance(sample_soil['score'], list):
                            logger.info(f"Soil score array size per record: {len(sample_soil['score'])} elements")
                if validator_nodes_list and not isinstance(validator_nodes_list, Exception):
                    logger.info(f"Validator nodes dataset: {len(validator_nodes_list)} records")
                
                # Check for exceptions in results
                for i, result in enumerate([weather_results, geomagnetic_results, soil_results, validator_nodes_list]):
                    if isinstance(result, Exception):
                        task_names = ['weather', 'geomagnetic', 'soil', 'validator_nodes']
                        logger.error(f"Error fetching {task_names[i]} data: {result}")
                        return None
                
                # Log memory after database queries
                self.validator._log_memory_usage("calc_weights_after_db_queries")

                # Defensive fallbacks for failed queries
                if not weather_results:
                    weather_results = []
                if not geomagnetic_results:
                    geomagnetic_results = []
                if not soil_results:
                    soil_results = []
                if not validator_nodes_list:
                    logger.error("Failed to fetch validator nodes - cannot calculate weights")
                    return None

                logger.info(f"Fetched scores: Weather={len(weather_results)}, Geo={len(geomagnetic_results)}, Soil={len(soil_results)}")
                
                # Convert to list for the sync calculation
                self.validator._log_memory_usage("calc_weights_before_node_conversion")
                validator_nodes_by_uid_list = [None] * 256
                for node_dict in validator_nodes_list:
                    uid = node_dict.get('uid')
                    if uid is not None and 0 <= uid < 256:
                        validator_nodes_by_uid_list[uid] = node_dict
                self.validator._log_memory_usage("calc_weights_after_node_conversion")

                # Perform CPU-bound weight calculation in thread pool to avoid blocking
                self.validator._log_memory_usage("calc_weights_before_sync_calc")
                loop = asyncio.get_event_loop()
                final_weights_list = await loop.run_in_executor(
                    None,
                    self.perform_weight_calculations_sync,
                    weather_results,
                    geomagnetic_results,
                    soil_results,
                    now,
                    validator_nodes_by_uid_list
                )
                self.validator._log_memory_usage("calc_weights_after_sync_calc")

                # Aggressive memory cleanup after weight calculation
                try:
                    # Clear all large data structures
                    del weather_results
                    del geomagnetic_results
                    del soil_results
                    del validator_nodes_list
                    del validator_nodes_by_uid_list
                    
                    # Force comprehensive cleanup for weight calculation (if fully initialized)
                    if (hasattr(self.validator, 'substrate_manager') and 
                        self.validator.substrate_manager is not None and
                        hasattr(self.validator, 'last_metagraph_sync')):
                        memory_freed = self.validator._comprehensive_memory_cleanup("weight_calculation")
                    else:
                        # Fallback to basic cleanup during startup
                        import gc
                        collected = gc.collect()
                        logger.info(f"Basic GC cleanup during startup: collected {collected} objects")
                        memory_freed = 0
                    
                    # Log memory after cleanup
                    self.validator._log_memory_usage("calc_weights_after_cleanup")
                    
                except Exception as cleanup_err:
                    logger.warning(f"Error during weight calculation cleanup: {cleanup_err}")

                return final_weights_list

            except Exception as query_error:
                logger.error(f"Error during database queries for weight calculation: {query_error}")
                return None

        except Exception as e:
            logger.error(f"Error calculating task weights: {e}")
            logger.error(traceback.format_exc())
            return None

    async def _update_last_weights_block(self):
        """Update the last weights block number."""
        try:
            block_number = await self.validator.neuron.get_current_block()
            self.validator.last_set_weights_block = block_number
        except Exception as e:
            logger.error(f"Error updating last weights block: {e}") 