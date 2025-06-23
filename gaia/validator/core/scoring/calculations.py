"""
Weight Calculation Functions
===========================

Clean functions for calculating and setting weights based on task scores.
Extracted from the original WeightCalculationsManager to be functional.
"""

import asyncio
import time
import math
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple
from fiber.logging_utils import get_logger
from gaia.validator.weights.set_weights import FiberWeightSetter
from fiber.chain import weights as w

logger = get_logger(__name__)


async def calculate_and_set_weights(
    network: Any, 
    database: Any, 
    config: Dict[str, Any]
) -> bool:
    """
    Calculate weights based on recent task scores and set them on the blockchain.
    
    Args:
        network: Network components (substrate, metagraph, etc.)
        database: Database manager
        config: Validator configuration
        
    Returns:
        bool: True if weights were successfully set
    """
    try:
        logger.info("Starting weight calculation and setting process...")
        
        # Create weight setter
        weight_setter = FiberWeightSetter(
            netuid=network.netuid,
            wallet_name=config.get('wallet_name'),
            hotkey_name=config.get('hotkey_name'),
            network=config.get('subtensor_network'),
        )
        
        # Check if we can set weights
        validator_uid = network.validator_uid
        if validator_uid is None:
            logger.error("Validator UID not available for weight setting")
            return False
            
        # Check timing constraints
        can_set = w.can_set_weights(network.substrate, network.netuid, validator_uid)
        if not can_set:
            logger.info("Cannot set weights at this time due to timing constraints")
            return False
            
        # Calculate weights
        normalized_weights = await fetch_scoring_data_and_calculate_weights(database, network)
        if not normalized_weights:
            logger.warning("No weights calculated - skipping weight setting")
            return False
            
        # Set weights on blockchain
        success = await weight_setter.set_weights(normalized_weights)
        if success:
            logger.info("✅ Successfully set weights on blockchain")
            
            # Update last weights block
            try:
                resp = network.substrate.rpc_request("chain_getHeader", [])
                hex_num = resp["result"]["number"]
                block_number = int(hex_num, 16)
                # Update this in the network state or wherever it's tracked
                logger.info(f"Updated last weights block to: {block_number}")
            except Exception as e:
                logger.error(f"Error updating last weights block: {e}")
                
            return True
        else:
            logger.error("Failed to set weights on blockchain")
            return False
            
    except Exception as e:
        logger.error(f"Error in weight calculation and setting: {e}", exc_info=True)
        return False


async def fetch_scoring_data(database: Any) -> Dict[str, List[Dict]]:
    """
    Fetch recent scoring data from the database.
    
    Args:
        database: Database manager
        
    Returns:
        Dict containing weather, geomagnetic, and soil scoring data
    """
    try:
        now = datetime.now(timezone.utc)
        one_day_ago = now - timedelta(days=1)
        
        # Query definitions
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
        
        params = {"start_time": one_day_ago}
        
        # Fetch all data concurrently
        weather_results, geomagnetic_results, soil_results = await asyncio.gather(
            database.fetch_all(weather_query, params),
            database.fetch_all(geomagnetic_query, params),
            database.fetch_all(soil_query, params),
            return_exceptions=True
        )
        
        # Handle exceptions
        for i, result in enumerate([weather_results, geomagnetic_results, soil_results]):
            if isinstance(result, Exception):
                task_names = ['weather', 'geomagnetic', 'soil']
                logger.error(f"Error fetching {task_names[i]} data: {result}")
                
        # Ensure we have lists even if queries failed
        return {
            'weather': weather_results if not isinstance(weather_results, Exception) else [],
            'geomagnetic': geomagnetic_results if not isinstance(geomagnetic_results, Exception) else [],
            'soil': soil_results if not isinstance(soil_results, Exception) else []
        }
        
    except Exception as e:
        logger.error(f"Error fetching scoring data: {e}")
        return {'weather': [], 'geomagnetic': [], 'soil': []}


async def fetch_scoring_data_and_calculate_weights(database: Any, network: Any) -> Optional[List[float]]:
    """
    Fetch scoring data and calculate normalized weights.
    
    Args:
        database: Database manager
        network: Network components
        
    Returns:
        List of normalized weights or None if calculation failed
    """
    try:
        # Fetch scoring data
        scoring_data = await fetch_scoring_data(database)
        
        # Fetch validator nodes information
        validator_nodes_query = """
        SELECT uid, hotkey, ip, port, incentive 
        FROM node_table 
        WHERE uid IS NOT NULL 
        ORDER BY uid
        """
        
        validator_nodes_list = await database.fetch_all(validator_nodes_query)
        if not validator_nodes_list:
            logger.error("Failed to fetch validator nodes - cannot calculate weights")
            return None
            
        # Convert to list format for weight calculation
        validator_nodes_by_uid = [None] * 256
        for node_dict in validator_nodes_list:
            uid = node_dict.get('uid')
            if uid is not None and 0 <= uid < 256:
                validator_nodes_by_uid[uid] = node_dict
                
        # Perform weight calculation
        normalized_weights = await asyncio.to_thread(
            compute_final_weights,
            scoring_data['weather'],
            scoring_data['geomagnetic'], 
            scoring_data['soil'],
            validator_nodes_by_uid
        )
        
        return normalized_weights
        
    except Exception as e:
        logger.error(f"Error in scoring data fetch and weight calculation: {e}")
        return None


def compute_final_weights(
    weather_results: List[Dict],
    geomagnetic_results: List[Dict], 
    soil_results: List[Dict],
    validator_nodes_by_uid: List[Optional[Dict]]
) -> Optional[List[float]]:
    """
    Compute final normalized weights based on task scores.
    
    This is the core weight calculation logic extracted from the original validator.
    
    Args:
        weather_results: Weather scoring results
        geomagnetic_results: Geomagnetic scoring results  
        soil_results: Soil scoring results
        validator_nodes_by_uid: Validator node information by UID
        
    Returns:
        List of normalized weights or None if calculation failed
    """
    try:
        logger.info("Computing final weights from task scores...")
        now = datetime.now(timezone.utc)
        
        # Initialize score arrays
        weather_scores = np.full(256, np.nan)
        geomagnetic_scores = np.full(256, np.nan)
        soil_scores = np.full(256, np.nan)
        
        # Process weather scores (use latest)
        if weather_results:
            latest_result = weather_results[0]
            scores = latest_result.get('score', [np.nan]*256)
            if not isinstance(scores, list) or len(scores) != 256:
                scores = [np.nan]*256
                
            for uid in range(256):
                if isinstance(scores[uid], str) or np.isnan(scores[uid]):
                    weather_scores[uid] = 0.0
                else:
                    weather_scores[uid] = scores[uid]
                    
            score_age_days = (now - latest_result['created_at']).total_seconds() / (24 * 3600)
            logger.info(f"Using latest weather score from {latest_result['created_at']} ({score_age_days:.1f} days ago)")
        
        # Process geomagnetic scores (time-weighted average)
        if geomagnetic_results:
            logger.info(f"Processing {len(geomagnetic_results)} geomagnetic records")
            
            for uid in range(256):
                weighted_sum = 0.0
                weight_sum = 0.0
                
                for result in geomagnetic_results:
                    scores = result.get('score', [np.nan]*256)
                    if not isinstance(scores, list) or len(scores) != 256:
                        scores = [np.nan]*256
                        
                    score_val = scores[uid]
                    if isinstance(score_val, str) or np.isnan(score_val):
                        score_val = 0.0
                        
                    if score_val != 0.0:
                        age_days = (now - result['created_at']).total_seconds() / (24 * 3600)
                        decay = np.exp(-age_days * np.log(2))
                        weighted_sum += score_val * decay
                        weight_sum += decay
                        
                if weight_sum > 0:
                    geomagnetic_scores[uid] = weighted_sum / weight_sum
                else:
                    geomagnetic_scores[uid] = 0.0
        
        # Process soil scores (time-weighted average)
        if soil_results:
            logger.info(f"Processing {len(soil_results)} soil records")
            
            for uid in range(256):
                weighted_sum = 0.0
                weight_sum = 0.0
                
                for result in soil_results:
                    scores = result.get('score', [np.nan]*256)
                    if not isinstance(scores, list) or len(scores) != 256:
                        scores = [np.nan]*256
                        
                    score_val = scores[uid]
                    if isinstance(score_val, str) or np.isnan(score_val):
                        score_val = 0.0
                        
                    if score_val != 0.0:
                        age_days = (now - result['created_at']).total_seconds() / (24 * 3600)
                        decay = np.exp(-age_days * np.log(2))
                        weighted_sum += score_val * decay
                        weight_sum += decay
                        
                if weight_sum > 0:
                    soil_scores[uid] = weighted_sum / weight_sum
                else:
                    soil_scores[uid] = 0.0
        
        # Combine scores using task weights
        def sigmoid(x, k=20, x0=0.93):
            return 1 / (1 + math.exp(-k * (x - x0)))
            
        weights_final = np.zeros(256)
        
        for idx in range(256):
            w_s, g_s, sm_s = weather_scores[idx], geomagnetic_scores[idx], soil_scores[idx]
            
            # Handle NaN/zero values
            if np.isnan(w_s) or w_s == 0:
                w_s = np.nan
            if np.isnan(g_s) or g_s == 0:
                g_s = np.nan
            if np.isnan(sm_s) or sm_s == 0:
                sm_s = np.nan
                
            # If all scores are NaN/zero, weight is 0
            if np.isnan(w_s) and np.isnan(g_s) and np.isnan(sm_s):
                weights_final[idx] = 0.0
            else:
                # Apply task weights: weather=0.70, geomagnetic=0.15, soil=0.15
                wc, gc, sc, total_w_avail = 0.0, 0.0, 0.0, 0.0
                
                if not np.isnan(w_s):
                    wc = 0.70 * w_s
                    total_w_avail += 0.70
                    
                if not np.isnan(g_s):
                    gc = 0.15 * sigmoid(g_s)
                    total_w_avail += 0.15
                    
                if not np.isnan(sm_s):
                    sc = 0.15 * sm_s
                    total_w_avail += 0.15
                    
                weights_final[idx] = (wc + gc + sc) / total_w_avail if total_w_avail > 0 else 0.0
        
        logger.info(f"Raw weights: Min={np.min(weights_final):.4f}, Max={np.max(weights_final):.4f}, Mean={np.mean(weights_final):.4f}")
        
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
            
        # Initial normalization
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
            sig_p = a + (k - a) / np.power(1 + Q * np.exp(-b * (norm_weights[idx] - M)), 1/v)
            transformed_w[idx] = sig_p + slope * norm_weights[idx]
            
        # Check for uniformity
        trans_nz = transformed_w[transformed_w > 0]
        if trans_nz.any() and len(trans_nz) > 1 and np.std(trans_nz) < 0.01:
            logger.warning(f"Transformed weights too uniform (std={np.std(trans_nz):.4f}), switching to rank-based.")
            sorted_indices = np.argsort(-weights_final)
            transformed_w = np.zeros_like(weights_final)
            pos_count = np.sum(weights_final > 0)
            for i, idx_val in enumerate(sorted_indices[:pos_count]):
                transformed_w[idx_val] = 1.0 / ((i + 1) ** 1.2)
                
        # Final normalization
        final_sum = np.sum(transformed_w)
        if final_sum > 0:
            transformed_w /= final_sum
            final_nz_vals = transformed_w[transformed_w > 0]
            if final_nz_vals.any():
                logger.info(f"Final weights: Count={len(final_nz_vals)}, Min={np.min(final_nz_vals):.4f}, Max={np.max(final_nz_vals):.4f}")
            return transformed_w.tolist()
        else:
            logger.warning("Sum of weights is zero, cannot normalize!")
            return None
            
    except Exception as e:
        logger.error(f"Error in weight computation: {e}")
        return None 