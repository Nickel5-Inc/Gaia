import gc
import logging
import sys
from datetime import datetime, timezone, timedelta
import os
import time
import threading
import concurrent.futures
import glob
import signal
import tracemalloc
import memray
import asyncio
import traceback
import math

from gaia.database.database_manager import DatabaseTimeout
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    psutil = None 
    PSUTIL_AVAILABLE = False
    print("psutil not found, memory logging will be skipped.")

import ssl
import random
from typing import Any, Optional, List, Dict, Set
from dotenv import load_dotenv
from cryptography.fernet import Fernet
import httpx
from fiber.chain import chain_utils, interface
from fiber.chain import weights as w
from fiber.chain.fetch_nodes import get_nodes_for_netuid
from fiber.chain.chain_utils import query_substrate
from fiber.logging_utils import get_logger
from fiber.encrypted.validator import client as vali_client, handshake
from fiber.chain.metagraph import Metagraph
from fiber.chain.interface import get_substrate
from substrateinterface import SubstrateInterface
from argparse import ArgumentParser
import pandas as pd
import json
import base64
import numpy as np
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

from fiber.validator.weight_setter import FiberWeightSetter

logger = get_logger(__name__)


class WeightCalculationsManager:
    """Handles weight calculations and scoring operations for the validator."""
    
    def __init__(self):
        """Initialize weight calculations manager independently."""
        self.netuid = None
        self.wallet_name = None
        self.hotkey_name = None
        self.subtensor_network = None
        self.substrate_manager = None
        self.substrate = None
        self.keypair = None
        self.validator_uid = None
        self.last_set_weights_block = None
        self.last_successful_weight_set = None
        self.database_manager = None
        self.memory_manager = None
        self.main_execution_manager = None
        self._shutdown_event = None
        logger.info("Weight Calculations Manager initialized")
    
    async def initialize(self, neuron_manager, database_manager, memory_manager, main_execution_manager):
        """Initialize weight calculations with required components."""
        try:
            logger.info("Initializing weight calculations manager...")
            
            # Get configuration from neuron manager
            self.netuid = neuron_manager.netuid
            self.wallet_name = neuron_manager.wallet_name
            self.hotkey_name = neuron_manager.hotkey_name
            self.subtensor_network = neuron_manager.subtensor_network
            self.substrate_manager = neuron_manager.substrate_manager
            self.substrate = neuron_manager.substrate
            self.keypair = neuron_manager.keypair
            self.validator_uid = neuron_manager.validator_uid
            self.last_set_weights_block = getattr(neuron_manager, 'last_set_weights_block', 0)
            self.last_successful_weight_set = getattr(neuron_manager, 'last_successful_weight_set', 0)
            
            # Store references to other managers
            self.database_manager = database_manager
            self.memory_manager = memory_manager
            self.main_execution_manager = main_execution_manager
            self._shutdown_event = getattr(main_execution_manager, '_shutdown_event', asyncio.Event())
            
            logger.info("Weight calculations manager initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing weight calculations manager: {e}")
            raise

    async def main_scoring(self):
        """Run scoring every subnet tempo blocks."""
        weight_setter = FiberWeightSetter(
            netuid=self.netuid,
            wallet_name=self.wallet_name,
            hotkey_name=self.hotkey_name,
            network=self.subtensor_network,
            substrate_manager=self.substrate_manager,  # Pass the substrate manager
        )

        while True:
            try:
                await self.main_execution_manager.update_task_status('scoring', 'active')
                
                async def scoring_cycle():
                    try:
                        validator_uid = self.validator_uid
                        
                        if validator_uid is None:
                            try:
                                self.validator_uid = self.substrate.query(
                                    "SubtensorModule", 
                                    "Uids", 
                                    [self.netuid, self.keypair.ss58_address]
                                ).value
                                validator_uid = int(self.validator_uid)
                            except Exception as e:
                                logger.error(f"Error getting validator UID: {e}")
                                logger.error(traceback.format_exc())
                                await self.main_execution_manager.update_task_status('scoring', 'error')
                                return False
                            

                        validator_uid = int(validator_uid)
                        last_updated_value = self.substrate.query(
                            "SubtensorModule",
                            "LastUpdate",
                            [self.netuid]
                        ).value
                        if last_updated_value is not None and validator_uid < len(last_updated_value):
                            last_updated = int(last_updated_value[validator_uid])
                            resp = self.substrate.rpc_request("chain_getHeader", [])  
                            hex_num = resp["result"]["number"]
                            current_block = int(hex_num, 16)
                            blocks_since_update = current_block - last_updated
                            logger.info(f"Calculated blocks since update: {blocks_since_update} (current: {current_block}, last: {last_updated})")
                        else:
                            blocks_since_update = None
                            logger.warning("Could not determine last update value")

                        min_interval = w.min_interval_to_set_weights(
                            self.substrate, 
                            self.netuid
                        )
                        if min_interval is not None:
                            min_interval = int(min_interval)
                        resp = self.substrate.rpc_request("chain_getHeader", [])  
                        hex_num = resp["result"]["number"]
                        current_block = int(hex_num, 16)                     
                        if current_block - self.last_set_weights_block < min_interval:
                            logger.info(f"Recently set weights {current_block - self.last_set_weights_block} blocks ago")
                            await self.main_execution_manager.update_task_status('scoring', 'idle', 'waiting')
                            await asyncio.sleep(60)
                            return True

                        # Only enter weight_setting state when actually setting weights
                        if (min_interval is None or 
                            (blocks_since_update is not None and blocks_since_update >= min_interval)):
                            logger.info(f"Setting weights: {blocks_since_update}/{min_interval} blocks")
                            can_set = w.can_set_weights(
                                self.substrate, 
                                self.netuid, 
                                validator_uid
                            )
                            
                            if can_set:
                                await self.main_execution_manager.update_task_status('scoring', 'processing', 'weight_setting')
                                
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
                                        await self.update_last_weights_block()
                                        self.last_successful_weight_set = time.time()
                                        logger.info("✅ Successfully set weights")
                                        
                                        # MEMORY LEAK FIX: Aggressive substrate cleanup after successful weight setting
                                        try:
                                            substrate_cleanup_count = self.memory_manager._aggressive_substrate_cleanup("post_weight_setting")
                                            if substrate_cleanup_count > 0:
                                                logger.info(f"Post-weight-setting cleanup: cleared {substrate_cleanup_count} substrate cache objects")
                                        except Exception as substrate_cleanup_err:
                                            logger.debug(f"Error during post-weight-setting substrate cleanup: {substrate_cleanup_err}")
                                        
                                        await self.main_execution_manager.update_task_status('scoring', 'idle')
                                        
                                        # Clean up any stale operations
                                        await self.database_manager.cleanup_stale_operations('score_table')
                        else:
                            logger.info(
                                f"Waiting for weight setting: {blocks_since_update}/{min_interval} blocks"
                            )
                            await self.main_execution_manager.update_task_status('scoring', 'idle', 'waiting')

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
                await self.main_execution_manager.update_task_status('scoring', 'error')
                try:
                    # Use managed substrate connection for error recovery
                    self.substrate = self.substrate_manager.force_reconnect()
                    logger.info("🔄 Force reconnect - using managed fresh substrate connection")
                    
                    # Clear scalecodec caches after reconnection
                    try:
                        import sys
                        import gc
                        cleared_count = 0
                        
                        for module_name in list(sys.modules.keys()):
                            if 'scalecodec' in module_name.lower() or 'substrate' in module_name.lower():
                                module = sys.modules.get(module_name)
                                if hasattr(module, '__dict__'):
                                    for attr_name in list(module.__dict__.keys()):
                                        if 'cache' in attr_name.lower() or 'registry' in attr_name.lower():
                                            try:
                                                cache_obj = getattr(module, attr_name)
                                                if hasattr(cache_obj, 'clear') and callable(cache_obj.clear):
                                                    cache_obj.clear()
                                                    cleared_count += 1
                                                elif isinstance(cache_obj, (dict, list, set)):
                                                    cache_obj.clear()
                                                    cleared_count += 1
                                            except Exception:
                                                pass
                        
                        if cleared_count > 0:
                            collected = gc.collect()
                            logger.debug(f"Substrate reconnect cleanup: cleared {cleared_count} cache objects, GC collected {collected}")
                            
                    except Exception:
                        pass
                        
                except Exception as e:
                    logger.error(f"Failed to reconnect to substrate: {e}")
                await asyncio.sleep(12)
                continue
            except Exception as e:
                logger.error(f"Error in main_scoring: {e}")
                logger.error(traceback.format_exc())
                await self.main_execution_manager.update_task_status('scoring', 'error')
                await asyncio.sleep(12)
                continue
            
            # Add sleep to prevent rapid cycling when scoring completes quickly
            # This ensures consistent timing regardless of scoring outcome
            await asyncio.sleep(60)

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

            # Count raw scores per UID - use numpy for better memory efficiency
            weather_counts = np.zeros(256, dtype=int)
            geo_counts = np.zeros(256, dtype=int)
            soil_counts = np.zeros(256, dtype=int)
        
            if weather_results:
                for result in weather_results:
                    # Ensure 'score' key exists and is a list of appropriate length
                    scores = result.get('score', [np.nan]*256)
                    if not isinstance(scores, list) or len(scores) != 256: scores = [np.nan]*256 # Defensive
                    for uid in range(256):
                        if not isinstance(scores[uid], str) and not np.isnan(scores[uid]) and scores[uid] != 0.0:
                            weather_counts[uid] += 1
            
            if geomagnetic_results:
                for result in geomagnetic_results:
                    scores = result.get('score', [np.nan]*256)
                    if not isinstance(scores, list) or len(scores) != 256: scores = [np.nan]*256 # Defensive
                    for uid in range(256):
                        if not isinstance(scores[uid], str) and not np.isnan(scores[uid]) and scores[uid] != 0.0:
                            geo_counts[uid] += 1

            if soil_results:
                for result in soil_results:
                    scores = result.get('score', [np.nan]*256)
                    if not isinstance(scores, list) or len(scores) != 256: scores = [np.nan]*256 # Defensive
                    for uid in range(256):
                        if not isinstance(scores[uid], str) and not np.isnan(scores[uid]) and scores[uid] != 0.0:
                            soil_counts[uid] += 1

            if geomagnetic_results:
                # MEMORY LEAK FIX: Process geomagnetic scores with vectorized operations
                logger.info(f"Processing {len(geomagnetic_results)} geomagnetic records with enhanced memory management")
                zero_scores_count = 0
                
                # Pre-allocate arrays to reduce memory allocations
                num_results = len(geomagnetic_results)
                scores_matrix = np.full((256, num_results), np.nan, dtype=np.float32)  # Use float32 to save memory
                weights_matrix = np.full((256, num_results), 0.0, dtype=np.float32)
                
                # Process all results in one pass to build the matrix
                for result_idx, result in enumerate(geomagnetic_results):
                    age_days = (now - result['created_at']).total_seconds() / (24 * 3600)
                    decay = np.exp(-age_days * np.log(2))
                    scores = result.get('score', [np.nan]*256)
                    if not isinstance(scores, list) or len(scores) != 256: 
                        scores = [np.nan]*256 # Defensive
                    
                    # Process all UIDs for this result
                    for uid in range(256):
                        score_val = scores[uid]
                        if isinstance(score_val, str) or np.isnan(score_val): 
                            score_val = 0.0
                        if score_val == 0.0: 
                            zero_scores_count += 1
                        
                        scores_matrix[uid, result_idx] = score_val
                        if score_val != 0.0:
                            weights_matrix[uid, result_idx] = decay
                            geo_counts[uid] += 1
                
                # Vectorized calculation for all UIDs at once
                for uid in range(256):
                    uid_scores = scores_matrix[uid, :]
                    uid_weights = weights_matrix[uid, :]
                    
                    # Find non-zero scores
                    non_zero_mask = uid_scores != 0.0
                    
                    if np.any(non_zero_mask):
                        masked_s = uid_scores[non_zero_mask]
                        masked_w = uid_weights[non_zero_mask]
                        weight_sum = np.sum(masked_w)
                        if weight_sum > 0:
                            geomagnetic_scores[uid] = np.sum(masked_s * masked_w) / weight_sum
                        else:
                            geomagnetic_scores[uid] = 0.0
                    else:
                        geomagnetic_scores[uid] = 0.0
                
                # IMMEDIATE cleanup of large matrices
                del scores_matrix, weights_matrix
                import gc
                gc.collect()
                
                logger.info(f"Processed {len(geomagnetic_results)} geomagnetic records with {zero_scores_count} zero scores (memory-optimized)")

            if weather_results:
                latest_result = weather_results[0]
                scores = latest_result.get('score', [np.nan]*256)
                if not isinstance(scores, list) or len(scores) != 256: scores = [np.nan]*256 # Defensive
                score_age_days = (now - latest_result['created_at']).total_seconds() / (24 * 3600)
                logger.info(f"Using latest weather score from {latest_result['created_at']} ({score_age_days:.1f} days ago)")
                for uid in range(256):
                    if isinstance(scores[uid], str) or np.isnan(scores[uid]): weather_scores[uid] = 0.0
                    else: weather_scores[uid] = scores[uid]; weather_counts[uid] += (scores[uid] != 0.0)
                logger.info(f"Weather scores: {sum(1 for s in weather_scores if s == 0.0)} UIDs have zero score")

            if soil_results:
                # MEMORY LEAK FIX: Process soil scores with aggressive memory management
                logger.info(f"Processing {len(soil_results)} soil records with enhanced memory management")
                zero_soil_scores = 0
                
                # Pre-allocate arrays to reduce memory allocations
                num_results = len(soil_results)
                scores_matrix = np.full((256, num_results), np.nan, dtype=np.float32)  # Use float32 to save memory
                weights_matrix = np.full((256, num_results), 0.0, dtype=np.float32)
                
                # Process all results in one pass to build the matrix
                for result_idx, result in enumerate(soil_results):
                    age_days = (now - result['created_at']).total_seconds() / (24 * 3600)
                    decay = np.exp(-age_days * np.log(2))
                    scores = result.get('score', [np.nan]*256)
                    if not isinstance(scores, list) or len(scores) != 256: 
                        scores = [np.nan]*256 # Defensive
                    
                    # Process all UIDs for this result
                    for uid in range(256):
                        score_val = scores[uid]
                        if isinstance(score_val, str) or np.isnan(score_val): 
                            score_val = 0.0
                        if score_val == 0.0: 
                            zero_soil_scores += 1
                        
                        scores_matrix[uid, result_idx] = score_val
                        if score_val != 0.0:
                            weights_matrix[uid, result_idx] = decay
                            soil_counts[uid] += 1
                
                # Vectorized calculation for all UIDs at once
                for uid in range(256):
                    uid_scores = scores_matrix[uid, :]
                    uid_weights = weights_matrix[uid, :]
                    
                    # Find non-zero scores
                    non_zero_mask = uid_scores != 0.0
                    
                    if np.any(non_zero_mask):
                        masked_s = uid_scores[non_zero_mask]
                        masked_w = uid_weights[non_zero_mask]
                        weight_sum = np.sum(masked_w)
                        if weight_sum > 0:
                            soil_scores[uid] = np.sum(masked_s * masked_w) / weight_sum
                        else:
                            soil_scores[uid] = 0.0
                    else:
                        soil_scores[uid] = 0.0
                
                # IMMEDIATE cleanup of large matrices
                del scores_matrix, weights_matrix
                import gc
                gc.collect()
                
                logger.info(f"Processed {len(soil_results)} soil records with {zero_soil_scores} zero scores (memory-optimized)")

            logger.info("Aggregate scores calculated. Proceeding to weight normalization...")
            def sigmoid(x, k=20, x0=0.93): return 1 / (1 + math.exp(-k * (x - x0)))
            weights_final = np.zeros(256)
            for idx in range(256):
                w_s, g_s, sm_s = weather_scores[idx], geomagnetic_scores[idx], soil_scores[idx]
                if np.isnan(w_s) or w_s==0: w_s=np.nan
                if np.isnan(g_s) or g_s==0: g_s=np.nan
                if np.isnan(sm_s) or sm_s==0: sm_s=np.nan
                node_obj, hk_chain = validator_nodes_by_uid_list[idx] if idx < len(validator_nodes_by_uid_list) else None, "N/A"
                if node_obj: hk_chain = node_obj.get('hotkey', 'N/A')
                if np.isnan(w_s) and np.isnan(g_s) and np.isnan(sm_s): weights_final[idx] = 0.0
                else:
                    wc, gc, sc, total_w_avail = 0.0,0.0,0.0,0.0
                    if not np.isnan(w_s): wc,total_w_avail = 0.70*w_s, total_w_avail+0.70
                    if not np.isnan(g_s): gc,total_w_avail = 0.15*sigmoid(g_s), total_w_avail+0.15
                    if not np.isnan(sm_s): sc,total_w_avail = 0.15*sm_s, total_w_avail+0.15
                    weights_final[idx] = (wc+gc+sc)/total_w_avail if total_w_avail>0 else 0.0
                # Reduce logging to prevent memory pressure - only log every 32nd UID or non-zero weights
                if idx % 32 == 0 or weights_final[idx] > 0.0:
                    logger.debug(f"UID {idx} (HK: {hk_chain}): Wea={w_s if not np.isnan(w_s) else '-'} ({weather_counts[idx]} scores), Geo={g_s if not np.isnan(g_s) else '-'} ({geo_counts[idx]} scores), Soil={sm_s if not np.isnan(sm_s) else '-'} ({soil_counts[idx]} scores), AggW={weights_final[idx]:.4f}")
            logger.info(f"Weights before normalization: Min={np.min(weights_final):.4f}, Max={np.max(weights_final):.4f}, Mean={np.mean(weights_final):.4f}")
            
            non_zero_mask = weights_final != 0.0
            if not np.any(non_zero_mask): logger.warning("No non-zero weights to normalize!"); return None
            
            nz_weights = weights_final[non_zero_mask]
            max_w_val = np.max(nz_weights)
            if max_w_val == 0: logger.warning("Max weight is 0, cannot normalize."); return None
            
            norm_weights = np.copy(weights_final); norm_weights[non_zero_mask] /= max_w_val
            positives = norm_weights[norm_weights > 0]
            if not positives.any(): logger.warning("No positive weights after initial normalization!"); return None
            
            M = np.percentile(positives, 80); logger.info(f"Using 80th percentile ({M:.8f}) as curve midpoint")
            b,Q,v,k,a,slope = 70,8,0.3,0.98,0.0,0.01
            transformed_w = np.zeros_like(weights_final)
            nz_indices = np.where(weights_final > 0.0)[0]
            if not nz_indices.any(): logger.warning("No positive weight indices for transformation!"); return None

            for idx in nz_indices:
                sig_p = a+(k-a)/np.power(1+Q*np.exp(-b*(norm_weights[idx]-M)),1/v)
                transformed_w[idx] = sig_p + slope*norm_weights[idx]
            
            trans_nz = transformed_w[transformed_w > 0]
            if trans_nz.any(): logger.info(f"Transformed weights: Min={np.min(trans_nz):.4f}, Max={np.max(trans_nz):.4f}, Mean={np.mean(trans_nz):.4f}")
            else: logger.warning("No positive weights after sigmoid transformation!"); # Continue to rank-based if needed or return None

            if len(trans_nz) > 1 and np.std(trans_nz) < 0.01:
                logger.warning(f"Transformed weights too uniform (std={np.std(trans_nz):.4f}), switching to rank-based.")
                sorted_indices = np.argsort(-weights_final); transformed_w = np.zeros_like(weights_final)
                pos_count = np.sum(weights_final > 0)
                for i,idx_val in enumerate(sorted_indices[:pos_count]): transformed_w[idx_val] = 1.0/((i+1)**1.2)
                rank_nz = transformed_w[transformed_w > 0]
                if rank_nz.any(): logger.info(f"Rank-based: Min={np.min(rank_nz):.4f}, Max={np.max(rank_nz):.4f}, Mean={np.mean(rank_nz):.4f}")
            
            final_sum = np.sum(transformed_w); final_weights_list = None
            if final_sum > 0:
                transformed_w /= final_sum
                final_nz_vals = transformed_w[transformed_w > 0]
                if final_nz_vals.any():
                    logger.info(f"Final Norm Weights: Count={len(final_nz_vals)}, Min={np.min(final_nz_vals):.4f}, Max={np.max(final_nz_vals):.4f}, Std={np.std(final_nz_vals):.4f}")
                    if len(np.unique(final_nz_vals)) < len(final_nz_vals)/2 : logger.warning(f"Low unique weights! {len(np.unique(final_nz_vals))}/{len(final_nz_vals)}")
                    if final_nz_vals.max() > 0.90: logger.warning(f"Max weight {final_nz_vals.max():.4f} is very high!")
                final_weights_list = transformed_w.tolist()
                logger.info("Final normalized weights calculated.")
            else: 
                logger.warning("Sum of weights is zero, cannot normalize! Returning None.")
                final_weights_list = None
            
            # Clean up all intermediate arrays to prevent memory leaks
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

    async def _calc_task_weights(self):
        """Calculate weights based on recent task scores. Async part fetches data."""
        try:
            # Log memory before large database operations
            self.memory_manager._log_memory_usage("calc_weights_start")
            
            now = datetime.now(timezone.utc)
            one_day_ago = now - timedelta(days=1)
            
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
                self.memory_manager._log_memory_usage("calc_weights_before_db_fetch")
                weather_results, geomagnetic_results, soil_results, validator_nodes_list = await asyncio.gather(
                    self.database_manager.fetch_all(weather_query, params),
                    self.database_manager.fetch_all(geomagnetic_query, params),
                    self.database_manager.fetch_all(soil_query, params),
                    self.database_manager.fetch_all(validator_nodes_query),
                    return_exceptions=True
                )
                self.memory_manager._log_memory_usage("calc_weights_after_db_fetch")
                
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
                self.memory_manager._log_memory_usage("calc_weights_after_db_queries")

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
                self.memory_manager._log_memory_usage("calc_weights_before_node_conversion")
                validator_nodes_by_uid_list = [None] * 256
                for node_dict in validator_nodes_list:
                    uid = node_dict.get('uid')
                    if uid is not None and 0 <= uid < 256:
                        validator_nodes_by_uid_list[uid] = node_dict
                self.memory_manager._log_memory_usage("calc_weights_after_node_conversion")

                # IMMEDIATELY clear the original large list to save memory
                del validator_nodes_list
                
                # MEMORY LEAK FIX: Force immediate cleanup of database query results before sync calculation
                # The database results can be very large (200 soil records × 256 scores = 51,200 values)
                try:
                    import gc
                    # Calculate total memory footprint before cleanup
                    total_records = len(weather_results) + len(geomagnetic_results) + len(soil_results)
                    logger.info(f"Processing {total_records} total database records for weight calculation")
                    
                    # Force garbage collection before the heavy sync calculation
                    collected = gc.collect()
                    logger.debug(f"Pre-sync GC: collected {collected} objects before weight calculation")
                    
                    self.memory_manager._log_memory_usage("calc_weights_pre_sync_gc")
                except Exception as pre_sync_cleanup_err:
                    logger.debug(f"Error during pre-sync cleanup: {pre_sync_cleanup_err}")

                # Perform CPU-bound weight calculation in thread pool to avoid blocking
                self.memory_manager._log_memory_usage("calc_weights_before_sync_calc")
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
                self.memory_manager._log_memory_usage("calc_weights_after_sync_calc")

                # MEMORY CLEANUP after sync calculation (scope-aware)
                try:
                    # Clear all large data structures that exist in this scope
                    del weather_results
                    del geomagnetic_results  
                    del soil_results
                    del validator_nodes_by_uid_list
                    
                    # Force immediate garbage collection
                    import gc
                    collected = gc.collect()
                    logger.info(f"Weight calculation cleanup: collected {collected} objects after data deletion")
                    
                    # Force comprehensive cleanup for weight calculation (if fully initialized)
                    if hasattr(self.validator, 'last_metagraph_sync'):
                        memory_freed = self.memory_manager._comprehensive_memory_cleanup("weight_calculation")
                        logger.info(f"Weight calculation comprehensive cleanup: freed {memory_freed:.1f}MB")
                    else:
                        # Additional GC during startup
                        for _ in range(2):  # Multiple GC passes
                            collected += gc.collect()
                        logger.info(f"Basic GC cleanup during startup: collected {collected} objects")
                        memory_freed = 0
                    
                    # Log memory after cleanup
                    self.memory_manager._log_memory_usage("calc_weights_after_cleanup")
                    
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

    async def update_last_weights_block(self):
        try:
            resp = self.substrate.rpc_request("chain_getHeader", [])
            hex_num = resp["result"]["number"]
            block_number = int(hex_num, 16)
            self.last_set_weights_block = block_number
        except Exception as e:
            logger.error(f"Error updating last weights block: {e}")

    def get_current_task_weights(self) -> Dict[str, float]:
        """Get current task weights for display/monitoring purposes."""
        try:
            # Default weights if not configured
            return {
                "weather": 0.70,
                "geomagnetic": 0.15,
                "soil_moisture": 0.15
            }
        except Exception as e:
            logger.error(f"Error getting current task weights: {e}")
            return {} 