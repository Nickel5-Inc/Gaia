import numpy as np
import xarray as xr
from typing import Dict, List, Tuple, Union, Optional, Any
import gc
import os
import asyncio
from fiber.logging_utils import get_logger
from gaia.tasks.defined_tasks.weather.weather_scoring.metrics import calculate_rmse, normalize_rmse
logger = get_logger(__name__)

"""
Utilities for ensemble forecast generation.
This module currently provides a function for creating a weighted average ensemble.
Functionality related to BMA weight calculation and specific ensemble pipelines
has been moved or refactored.
"""

async def create_weighted_ensemble(
    predictions: Dict[str, Dict],
    weights: Dict[str, float]
) -> Dict:
    """
    Create a weighted ensemble forecast from multiple predictions.
    Assumes predictions are dictionaries mapping variable names to numpy arrays.

    Args:
        predictions: Dictionary mapping miner_id -> prediction variables
                       (e.g., {'miner1': {'2t': array1, 'msl': array2}, ...})
        weights: Dictionary mapping miner_id -> weight

    Returns:
        Dictionary of ensemble variables (variable_name -> ensemble_array)
    """
    try:
        if not predictions:
            logger.warning("Received empty predictions dictionary.")
            return {}

        participating_miners = {m_id for m_id, pred in predictions.items() if pred}
        if not participating_miners:
            logger.warning("No participating miners with valid predictions found.")
            return {}

        valid_weights = {m_id: weights.get(m_id, 0.0) for m_id in participating_miners}

        total_weight = sum(valid_weights.values())
        if total_weight <= 1e-9:
            logger.warning(f"Total weight is zero or negligible ({total_weight}). Falling back to equal weights for {len(participating_miners)} miners.")
            num_miners = len(participating_miners)
            if num_miners == 0: return {}
            normalized_weights = {m: 1.0 / num_miners for m in participating_miners}
        else:
            normalized_weights = {m: w / total_weight for m, w in valid_weights.items()}

        all_vars = set()
        var_shapes = {}
        first_valid_pred = {}
        for m_id in participating_miners:
            for var, data in predictions[m_id].items():
                if data is not None and hasattr(data, 'shape'):
                    all_vars.add(var)
                    if var not in var_shapes:
                        var_shapes[var] = data.shape
                        first_valid_pred[var] = m_id
                    elif var_shapes[var] != data.shape:
                        logger.warning(
                            f"Shape mismatch for variable '{var}'. "
                            f"Expected {var_shapes[var]} (from miner {first_valid_pred[var]}), "
                            f"but got {data.shape} from miner {m_id}. Excluding {m_id} for this variable."
                        )
                        # Might exclude the mismatching miner for this var
                        # For now just skip the variable later if shapes vary wildly.

        ensemble = {}
        processed_vars_count = 0

        for var in sorted(list(all_vars)):
            forecasts_for_var = []
            weights_for_var = []
            consistent_miners = []

            target_shape = var_shapes.get(var)
            if target_shape is None:
                logger.warning(f"Could not determine target shape for variable '{var}'. Skipping.")
                continue

            for m_id in participating_miners:
                if var in predictions[m_id]:
                    pred_data = predictions[m_id][var]
                    if pred_data is not None and pred_data.shape == target_shape:
                        forecasts_for_var.append(pred_data.astype(np.float32))
                        weights_for_var.append(normalized_weights.get(m_id, 0.0))
                        consistent_miners.append(m_id)
                    elif pred_data is not None:
                         logger.warning(f"Miner {m_id} excluded for var '{var}' due to shape mismatch: {pred_data.shape} vs {target_shape}")

            if not forecasts_for_var:
                logger.warning(f"No valid forecasts found for variable '{var}' after shape checks.")
                continue

            weights_np = np.array(weights_for_var)
            sum_weights_np = np.sum(weights_np)

            if sum_weights_np <= 1e-9:
                 logger.warning(f"Sum of weights for variable '{var}' is zero. Using equal weights for {len(consistent_miners)} contributing miners.")
                 num_consistent = len(consistent_miners)
                 if num_consistent == 0: continue
                 final_var_weights = np.full(num_consistent, 1.0 / num_consistent)
            else:
                 final_var_weights = weights_np / sum_weights_np

            try:
                stacked_forecasts = np.stack(forecasts_for_var, axis=0)

                # Reshape weights for broadcasting: (num_miners, 1, 1, ...) depending on data ndim
                # Example: for (lat, lon) data (ndim=2), shape should be (num_miners, 1, 1)
                # Example: for (level, lat, lon) data (ndim=3), shape should be (num_miners, 1, 1, 1)
                weight_shape = (len(final_var_weights),) + (1,) * stacked_forecasts.ndim
                reshaped_weights = final_var_weights.reshape(weight_shape)

                ensemble_var_data = np.sum(stacked_forecasts * reshaped_weights, axis=0)
                ensemble[var] = ensemble_var_data.astype(np.float32) # Store as float32
                processed_vars_count += 1
            except ValueError as ve:
                 logger.error(f"ValueError during stacking/averaging for var '{var}': {ve}. Shapes: {[f.shape for f in forecasts_for_var]}")
            except Exception as stack_err:
                 logger.error(f"Error during stacking/averaging for var '{var}': {stack_err}")

        logger.info(f"Created weighted ensemble for {processed_vars_count} variables.")
        return ensemble
    except Exception as e:
        logger.error(f"Error in create_weighted_ensemble: {e}")
        logger.error(traceback.format_exc())
        return {}
