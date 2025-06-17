"""
Scoring Engine Core Module

Handles weight calculations, scoring algorithms, score aggregation,
and weight normalization for the validator.
"""

import math
import time
import asyncio
import traceback
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import numpy as np

from fiber.logging_utils import get_logger
from fiber.chain import weights as w
from gaia.validator.weights.set_weights import FiberWeightSetter

logger = get_logger(__name__)

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    psutil = None 
    PSUTIL_AVAILABLE = False


class ScoringEngine:
    """
    Handles weight calculations, scoring algorithms, and normalization
    for validator scoring operations.
    """
    
    def __init__(self, args, database_manager, substrate_manager):
        """Initialize the scoring engine."""
        self.args = args
        self.database_manager = database_manager
        self.substrate_manager = substrate_manager
        self.netuid = None
        self.wallet_name = None
        self.hotkey_name = None
        self.subtensor_network = None
        self.weight_setter = None
        self.last_successful_weight_set = time.time()
        
        # Task weight schedule configuration
        self.task_weight_schedule = [
            (datetime(2025, 5, 28, 0, 0, 0, tzinfo=timezone.utc), 
             {"weather": 0.50, "geomagnetic": 0.25, "soil": 0.25}),
            (datetime(2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc), 
             {"weather": 0.65, "geomagnetic": 0.175, "soil": 0.175}), 
            (datetime(2025, 6, 5, 0, 0, 0, tzinfo=timezone.utc), 
             {"weather": 0.80, "geomagnetic": 0.10, "soil": 0.10})
        ]
        
        # Validate task weight schedule
        for dt_thresh, weights_dict in self.task_weight_schedule:
            if not math.isclose(sum(weights_dict.values()), 1.0):
                logger.error(f"Task weights for threshold {dt_thresh.isoformat()} do not sum to 1.0! Sum: {sum(weights_dict.values())}")

    def setup(self, netuid: int, wallet_name: str, hotkey_name: str, subtensor_network: str):
        """Set up the scoring engine with network configuration."""
        self.netuid = netuid
        self.wallet_name = wallet_name
        self.hotkey_name = hotkey_name
        self.subtensor_network = subtensor_network
        
        # Initialize weight setter
        self.weight_setter = FiberWeightSetter(
            netuid=self.netuid,
            wallet_name=self.wallet_name,
            hotkey_name=self.hotkey_name,
            network=self.subtensor_network,
            substrate_manager=self.substrate_manager,
        )

    async def main_scoring_cycle(self, neuron, task_health):
        """Run a single scoring cycle."""
        try:
            validator_uid = neuron.validator_uid
            
            if validator_uid is None:
                try:
                    neuron.validator_uid = neuron.substrate.query(
                        "SubtensorModule", 
                        "Uids", 
                        [neuron.netuid, neuron.keypair.ss58_address]
                    ).value
                    validator_uid = int(neuron.validator_uid)
                except Exception as e:
                    logger.error(f"Error getting validator UID: {e}")
                    return False

            validator_uid = int(validator_uid)
            last_updated_value = neuron.substrate.query(
                "SubtensorModule",
                "LastUpdate",
                [neuron.netuid]
            ).value
            
            if last_updated_value is not None and validator_uid < len(last_updated_value):
                last_updated = int(last_updated_value[validator_uid])
                resp = neuron.substrate.rpc_request("chain_getHeader", [])  
                hex_num = resp["result"]["number"]
                current_block = int(hex_num, 16)
                blocks_since_update = current_block - last_updated
                logger.info(f"Calculated blocks since update: {blocks_since_update} (current: {current_block}, last: {last_updated})")
            else:
                blocks_since_update = None
                logger.warning("Could not determine last update value")

            min_interval = w.min_interval_to_set_weights(
                neuron.substrate, 
                neuron.netuid
            )
            if min_interval is not None:
                min_interval = int(min_interval)
                
            resp = neuron.substrate.rpc_request("chain_getHeader", [])  
            hex_num = resp["result"]["number"]
            current_block = int(hex_num, 16)
            
            if current_block - neuron.last_set_weights_block < min_interval:
                logger.info(f"Recently set weights {current_block - neuron.last_set_weights_block} blocks ago")
                return True

            # Check if we can set weights
            if (min_interval is None or 
                (blocks_since_update is not None and blocks_since_update >= min_interval)):
                logger.info(f"Setting weights: {blocks_since_update}/{min_interval} blocks")
                can_set = w.can_set_weights(
                    neuron.substrate, 
                    neuron.netuid, 
                    validator_uid
                )
                
                if can_set:
                    # Calculate weights with timeout
                    normalized_weights = await asyncio.wait_for(
                        self.calc_task_weights(),
                        timeout=120
                    )
                    
                    if normalized_weights:
                        # Set weights with timeout
                        success = await asyncio.wait_for(
                            self.weight_setter.set_weights(normalized_weights),
                            timeout=480
                        )
                        
                        if success:
                            await neuron.update_last_weights_block()
                            self.last_successful_weight_set = time.time()
                            logger.info("✅ Successfully set weights")
                            
                            # Clean up any stale operations
                            await self.database_manager.cleanup_stale_operations('score_table')
                            return True
            else:
                logger.info(f"Waiting for weight setting: {blocks_since_update}/{min_interval} blocks")

            return True
            
        except asyncio.TimeoutError as e:
            logger.error(f"Timeout in scoring cycle: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error in scoring cycle: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    async def calc_task_weights(self):
        """Calculate weights based on recent task scores."""
        try:
            # Log memory before large database operations
            self._log_memory_usage("calc_weights_start")
            
            now = datetime.now(timezone.utc)
            one_day_ago = now - timedelta(days=1)
            
            # Database queries for different task types
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
            validator_nodes_query = """
            SELECT uid, hotkey, ip, port, incentive 
            FROM node_table 
            WHERE uid IS NOT NULL 
            ORDER BY uid
            """

            params = {"start_time": one_day_ago}
            
            # Fetch all data concurrently
            try:
                self._log_memory_usage("calc_weights_before_db_fetch")
                weather_results, geomagnetic_results, soil_results, validator_nodes_list = await asyncio.gather(
                    self.database_manager.fetch_all(weather_query, params),
                    self.database_manager.fetch_all(geomagnetic_query, params),
                    self.database_manager.fetch_all(soil_query, params),
                    self.database_manager.fetch_all(validator_nodes_query),
                    return_exceptions=True
                )
                self._log_memory_usage("calc_weights_after_db_fetch")
                
                # Check for exceptions in results
                for i, result in enumerate([weather_results, geomagnetic_results, soil_results, validator_nodes_list]):
                    if isinstance(result, Exception):
                        task_names = ['weather', 'geomagnetic', 'soil', 'validator_nodes']
                        logger.error(f"Error fetching {task_names[i]} data: {result}")
                        return None
                
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
                
                # Convert to list for the calculation
                self._log_memory_usage("calc_weights_before_node_conversion")
                validator_nodes_by_uid_list = [None] * 256
                for node_dict in validator_nodes_list:
                    uid = node_dict.get('uid')
                    if uid is not None and 0 <= uid < 256:
                        validator_nodes_by_uid_list[uid] = node_dict
                self._log_memory_usage("calc_weights_after_node_conversion")

                # Perform CPU-bound weight calculation in thread pool
                self._log_memory_usage("calc_weights_before_sync_calc")
                loop = asyncio.get_event_loop()
                final_weights_list = await loop.run_in_executor(
                    None,
                    self._perform_weight_calculations_sync,
                    weather_results,
                    geomagnetic_results,
                    soil_results,
                    now,
                    validator_nodes_by_uid_list
                )
                self._log_memory_usage("calc_weights_after_sync_calc")

                # Aggressive memory cleanup after weight calculation
                try:
                    del weather_results
                    del geomagnetic_results
                    del soil_results
                    del validator_nodes_list
                    del validator_nodes_by_uid_list
                    
                    import gc
                    collected = gc.collect()
                    if collected > 100:
                        logger.info(f"Weight calculation cleanup: collected {collected} objects")
                    
                    self._log_memory_usage("calc_weights_after_cleanup")
                    
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

    def _perform_weight_calculations_sync(self, weather_results, geomagnetic_results, soil_results, now, validator_nodes_by_uid_list):
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

            # Count raw scores per UID
            weather_counts = np.zeros(256, dtype=int)
            geo_counts = np.zeros(256, dtype=int)
            soil_counts = np.zeros(256, dtype=int)
        
            # Process weather scores (latest only)
            if weather_results:
                latest_result = weather_results[0]
                scores = latest_result.get('score', [np.nan]*256)
                if not isinstance(scores, list) or len(scores) != 256: 
                    scores = [np.nan]*256
                score_age_days = (now - latest_result['created_at']).total_seconds() / (24 * 3600)
                logger.info(f"Using latest weather score from {latest_result['created_at']} ({score_age_days:.1f} days ago)")
                for uid in range(256):
                    if isinstance(scores[uid], str) or np.isnan(scores[uid]): 
                        weather_scores[uid] = 0.0
                    else: 
                        weather_scores[uid] = scores[uid]
                        weather_counts[uid] += (scores[uid] != 0.0)

            # Process geomagnetic scores with time decay
            if geomagnetic_results:
                zero_scores_count = 0
                for uid in range(256):
                    uid_scores = []
                    uid_weights = []
                    
                    for result in geomagnetic_results:
                        age_days = (now - result['created_at']).total_seconds() / (24 * 3600)
                        decay = np.exp(-age_days * np.log(2))
                        scores = result.get('score', [np.nan]*256)
                        if not isinstance(scores, list) or len(scores) != 256: 
                            scores = [np.nan]*256
                        
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

            # Process soil scores with time decay
            if soil_results:
                zero_soil_scores = 0
                for uid in range(256):
                    uid_scores = []
                    uid_weights = []
                    
                    for result in soil_results:
                        age_days = (now - result['created_at']).total_seconds() / (24 * 3600)
                        decay = np.exp(-age_days * np.log(2))
                        scores = result.get('score', [np.nan]*256)
                        if not isinstance(scores, list) or len(scores) != 256: 
                            scores = [np.nan]*256
                        
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
            
            # Apply task weighting and normalization
            def sigmoid(x, k=20, x0=0.93): 
                return 1 / (1 + math.exp(-k * (x - x0)))
            
            weights_final = np.zeros(256)
            for idx in range(256):
                w_s, g_s, sm_s = weather_scores[idx], geomagnetic_scores[idx], soil_scores[idx]
                if np.isnan(w_s) or w_s==0: w_s=np.nan
                if np.isnan(g_s) or g_s==0: g_s=np.nan
                if np.isnan(sm_s) or sm_s==0: sm_s=np.nan
                
                node_obj = validator_nodes_by_uid_list[idx] if idx < len(validator_nodes_by_uid_list) else None
                hk_chain = node_obj.get('hotkey', 'N/A') if node_obj else 'N/A'
                
                if np.isnan(w_s) and np.isnan(g_s) and np.isnan(sm_s): 
                    weights_final[idx] = 0.0
                else:
                    wc, gc, sc, total_w_avail = 0.0, 0.0, 0.0, 0.0
                    if not np.isnan(w_s): 
                        wc, total_w_avail = 0.70*w_s, total_w_avail+0.70
                    if not np.isnan(g_s): 
                        gc, total_w_avail = 0.15*sigmoid(g_s), total_w_avail+0.15
                    if not np.isnan(sm_s): 
                        sc, total_w_avail = 0.15*sm_s, total_w_avail+0.15
                    weights_final[idx] = (wc+gc+sc)/total_w_avail if total_w_avail>0 else 0.0
                
                # Reduce logging to prevent memory pressure
                if idx % 32 == 0 or weights_final[idx] > 0.0:
                    logger.debug(f"UID {idx} (HK: {hk_chain}): Weight={weights_final[idx]:.4f}")
            
            logger.info(f"Weights before normalization: Min={np.min(weights_final):.4f}, Max={np.max(weights_final):.4f}, Mean={np.mean(weights_final):.4f}")
            
            # Normalize weights
            non_zero_mask = weights_final != 0.0
            if not np.any(non_zero_mask): 
                logger.warning("No non-zero weights to normalize!")
                return None
            
            nz_weights = weights_final[non_zero_mask]
            max_w_val = np.max(nz_weights)
            if max_w_val == 0: 
                logger.warning("Max weight is 0, cannot normalize.")
                return None
            
            norm_weights = np.copy(weights_final)
            norm_weights[non_zero_mask] /= max_w_val
            positives = norm_weights[norm_weights > 0]
            if not positives.any(): 
                logger.warning("No positive weights after initial normalization!")
                return None
            
            # Apply sigmoid transformation
            M = np.percentile(positives, 80)
            logger.info(f"Using 80th percentile ({M:.8f}) as curve midpoint")
            b, Q, v, k, a, slope = 70, 8, 0.3, 0.98, 0.0, 0.01
            transformed_w = np.zeros_like(weights_final)
            nz_indices = np.where(weights_final > 0.0)[0]
            
            if not nz_indices.any(): 
                logger.warning("No positive weight indices for transformation!")
                return None

            for idx in nz_indices:
                sig_p = a+(k-a)/np.power(1+Q*np.exp(-b*(norm_weights[idx]-M)),1/v)
                transformed_w[idx] = sig_p + slope*norm_weights[idx]
            
            trans_nz = transformed_w[transformed_w > 0]
            if trans_nz.any(): 
                logger.info(f"Transformed weights: Min={np.min(trans_nz):.4f}, Max={np.max(trans_nz):.4f}, Mean={np.mean(trans_nz):.4f}")

            # Check for uniform weights and apply rank-based fallback if needed
            if len(trans_nz) > 1 and np.std(trans_nz) < 0.01:
                logger.warning(f"Transformed weights too uniform (std={np.std(trans_nz):.4f}), switching to rank-based.")
                sorted_indices = np.argsort(-weights_final)
                transformed_w = np.zeros_like(weights_final)
                pos_count = np.sum(weights_final > 0)
                for i, idx_val in enumerate(sorted_indices[:pos_count]): 
                    transformed_w[idx_val] = 1.0/((i+1)**1.2)
                rank_nz = transformed_w[transformed_w > 0]
                if rank_nz.any(): 
                    logger.info(f"Rank-based: Min={np.min(rank_nz):.4f}, Max={np.max(rank_nz):.4f}, Mean={np.mean(rank_nz):.4f}")
            
            # Final normalization
            final_sum = np.sum(transformed_w)
            final_weights_list = None
            if final_sum > 0:
                transformed_w /= final_sum
                final_nz_vals = transformed_w[transformed_w > 0]
                if final_nz_vals.any():
                    logger.info(f"Final Norm Weights: Count={len(final_nz_vals)}, Min={np.min(final_nz_vals):.4f}, Max={np.max(final_nz_vals):.4f}")
                final_weights_list = transformed_w.tolist()
                logger.info("Final normalized weights calculated.")
            else: 
                logger.warning("Sum of weights is zero, cannot normalize! Returning None.")
                final_weights_list = None
            
            # Clean up all intermediate arrays
            try:
                del weather_scores, geomagnetic_scores, soil_scores
                del weather_counts, geo_counts, soil_counts
                del weights_final, transformed_w
                if 'nz_weights' in locals(): del nz_weights
                if 'norm_weights' in locals(): del norm_weights
                if 'positives' in locals(): del positives
                if 'trans_nz' in locals(): del trans_nz
                if 'sorted_indices' in locals(): del sorted_indices
            except Exception as cleanup_e:
                logger.warning(f"Error during sync weight calculation cleanup: {cleanup_e}")
            
            return final_weights_list
        
        except Exception as calc_error:
            logger.error(f"Error in sync weight calculation: {calc_error}")
            return None

    def get_current_task_weights(self) -> Dict[str, float]:
        """Get current task weights based on the schedule."""
        now_utc = datetime.now(timezone.utc)
        
        # Default to the first set of weights
        active_weights = self.task_weight_schedule[0][1] 
        
        # Find the latest applicable weights
        for dt_threshold, weights_at_threshold in self.task_weight_schedule:
            if now_utc >= dt_threshold:
                active_weights = weights_at_threshold
            else:
                break 
                
        return active_weights.copy()

    def _log_memory_usage(self, context: str, threshold_mb: float = 100.0):
        """Enhanced memory logging for scoring operations."""
        if not PSUTIL_AVAILABLE:
            return
            
        try:
            process = psutil.Process()
            current_memory = process.memory_info().rss / (1024 * 1024)
            
            if not hasattr(self, '_last_memory'):
                self._last_memory = current_memory
                memory_change = 0
            else:
                memory_change = current_memory - self._last_memory
                self._last_memory = current_memory
            
            if abs(memory_change) > threshold_mb or context in ['calc_weights_start', 'calc_weights_after_cleanup']:
                logger.info(f"Memory usage [{context}]: {current_memory:.1f}MB ({'+' if memory_change > 0 else ''}{memory_change:.1f}MB)")
            else:
                logger.debug(f"Memory usage [{context}]: {current_memory:.1f}MB ({'+' if memory_change > 0 else ''}{memory_change:.1f}MB)")
                
        except Exception as e:
            logger.debug(f"Error logging memory usage for {context}: {e}")

    async def cleanup(self):
        """Clean up scoring engine resources."""
        try:
            # Clean up any stale operations
            if self.database_manager:
                await self.database_manager.cleanup_stale_operations('score_table')
            logger.info("Scoring engine cleanup completed")
        except Exception as e:
            logger.error(f"Error during scoring engine cleanup: {e}")