import asyncio
import xarray as xr
import fsspec
import numpy as np
from typing import Dict, List, Optional, Any
from fiber.logging_utils import get_logger
from metrics import calculate_all_metrics
from scoring import VARIABLE_WEIGHTS

logger = get_logger(__name__)

# Indices based on a 0-indexed list of 40 timesteps (10 days * 4 steps/day)
DEFAULT_SCORING_TIMESTEP_INDICES = [1, 4, 12, 20, 28]



async def open_remote_dataset_with_kerchunk(kerchunk_index_url: str) -> xr.Dataset:
    """
    Asynchronously opens a remote NetCDF dataset using a kerchunk index URL.

    Args:
        kerchunk_index_url: The HTTPS URL to the kerchunk JSON index file.

    Returns:
        An xarray Dataset object opened lazily using the kerchunk index.

    Raises:
        Exception: If the dataset cannot be opened.
    """
    try:
        # Use fsspec to open the remote JSON index via HTTPS
        # 'simplecache::' can optionally cache the index file locally for speed
        mapper = fsspec.get_mapper(kerchunk_index_url)
        # Open the dataset using the index, specifying consolidated=False and engine='zarr'
        # chunks={} or 'auto' allows dask to manage loading
        ds = xr.open_dataset(mapper, engine="zarr", consolidated=False, chunks={})
        logger.info(f"Successfully opened remote dataset using kerchunk index: {kerchunk_index_url}")
        return ds
    except Exception as e:
        logger.error(f"Failed to open dataset with kerchunk URL {kerchunk_index_url}: {e}")
        raise


async def get_ground_truth_subset(
    ground_truth_info: Dict[str, Any],
    timestep_indices: List[int],
    variables: List[str]
) -> Dict[str, np.ndarray]:
    """
    Fetches the relevant ground truth data subset for specified timesteps and variables.

    Args:
        ground_truth_info: Dictionary containing information needed to locate
                           and access the ground truth data (e.g., file path,
                           reference time, database query parameters).
        timestep_indices: List of integer indices for the timesteps required.
        variables: List of variable names required.

    Returns:
        A dictionary mapping variable names to NumPy arrays containing the
        ground truth data for the specified subset.

    Raises:
        NotImplementedError: This is a placeholder and needs specific implementation.
        Exception: If ground truth data cannot be accessed or processed.
    """
    # <<< Placeholder Implementation >>>
    # This function needs to be implemented based on how the validator
    # accesses ground truth data (e.g., reading from a local file store,
    # querying a database, accessing another remote source).
    # It should efficiently load only the required variables and timesteps.
    # Example: If ground truth is in a large NetCDF file, use isel() similarly.
    logger.warning("Placeholder function `get_ground_truth_subset` called. Needs implementation.")
    # Example structure:
    # gt_path = ground_truth_info.get("path")
    # with xr.open_dataset(gt_path) as ds_gt:
    #     ds_gt_subset = ds_gt[variables].isel(time=timestep_indices)
    #     ground_truth_subset = {var: ds_gt_subset[var].values for var in variables if var in ds_gt_subset}
    #     return ground_truth_subset

    # Returning dummy data for now
    dummy_shape_map = {
        '2t': (len(timestep_indices), 720, 1440),
        'msl': (len(timestep_indices), 720, 1440),
        '10u': (len(timestep_indices), 720, 1440),
        '10v': (len(timestep_indices), 720, 1440),
        't': (len(timestep_indices), 13, 720, 1440),
        'q': (len(timestep_indices), 13, 720, 1440),
        'z': (len(timestep_indices), 13, 720, 1440),
        'u': (len(timestep_indices), 13, 720, 1440),
        'v': (len(timestep_indices), 13, 720, 1440),
    }
    ground_truth_subset = {}
    for var in variables:
         if var in dummy_shape_map:
              ground_truth_subset[var] = np.zeros(dummy_shape_map[var], dtype=np.float32)

    if not ground_truth_subset:
         raise ValueError("Could not load or generate dummy ground truth data.")

    return ground_truth_subset



async def score_forecast_subset(
    kerchunk_index_url: str,
    ground_truth_info: Dict[str, Any],
    variables: List[str] = list(VARIABLE_WEIGHTS.keys()),
    timestep_indices: List[int] = DEFAULT_SCORING_TIMESTEP_INDICES,
) -> Dict[str, Dict[str, float]]:
    """
    Scores a single miner's forecast against ground truth using efficient remote
    access via kerchunk for a defined subset of timesteps.

    Args:
        kerchunk_index_url: URL to the forecast's kerchunk index file.
        ground_truth_info: Information needed to fetch the corresponding ground truth subset.
        variables: List of variables to score. Defaults to keys in VARIABLE_WEIGHTS.
        timestep_indices: List of timestep indices to score. Defaults to a standard subset.

    Returns:
        A dictionary containing detailed scores, structured as:
        {variable_name: {metric_name: score_value, ...}, ...}
        Returns an empty dictionary if scoring fails critically.
        Score values can be NaN if a specific metric calculation failed.
    """
    detailed_scores = {}
    ds_forecast = None

    try:
        ds_forecast = await open_remote_dataset_with_kerchunk(kerchunk_index_url)
        forecast_subset = ds_forecast.isel(time=timestep_indices)
        logger.info(f"Selected forecast subset for timesteps: {timestep_indices}")

        ground_truth_subset = await get_ground_truth_subset(
            ground_truth_info, timestep_indices, variables
        )
        logger.info(f"Fetched ground truth subset for {len(ground_truth_subset)} variables.")

        for var in variables:
            if var in forecast_subset.data_vars and var in ground_truth_subset:
                logger.debug(f"Scoring variable: {var}")
                try:
                    pred_data = forecast_subset[var].compute().astype(np.float32)
                    truth_data = ground_truth_subset[var].astype(np.float32)

                    if pred_data.shape != truth_data.shape:
                        logger.warning(
                            f"Shape mismatch for '{var}': "
                            f"Pred {pred_data.shape}, Truth {truth_data.shape}. Skipping variable."
                        )
                        continue

                    var_metrics = await calculate_all_metrics(
                        prediction=pred_data,
                        ground_truth=truth_data,
                        variable_name=var
                        # Pass prediction_quantiles=None for now
                    )
                    detailed_scores[var] = var_metrics
                    logger.debug(f"Metrics calculated for var '{var}'.")

                except Exception as var_error:
                    logger.error(f"Error scoring variable '{var}': {var_error}", exc_info=True)
                    detailed_scores[var] = {metric: np.nan for metric in ['rmse', 'normalized_rmse', 'mae', 'bias', 'correlation']}
            else:
                logger.warning(f"Variable '{var}' not found in forecast subset or ground truth subset. Skipping.")

    except Exception as e:
        logger.error(f"Failed to score forecast from {kerchunk_index_url}: {e}", exc_info=True)
        return {}
    finally:
        if ds_forecast is not None:
            try:
                ds_forecast.close()
                logger.info(f"Closed remote dataset connection for {kerchunk_index_url}")
            except Exception as close_err:
                logger.error(f"Error closing dataset for {kerchunk_index_url}: {close_err}")


    logger.info(f"Completed scoring subset for {kerchunk_index_url}. Found scores for {len(detailed_scores)} variables.")
    return detailed_scores



async def get_historical_weights_from_db() -> Dict[str, float]:
    """
    Placeholder function to retrieve the last known set of miner weights.
    Needs to interact with the validator's state/database manager.
    """
    # <<< Placeholder Implementation >>>
    logger.warning("Placeholder function `get_historical_weights_from_db` called. Needs implementation.")
    # Example: Query database, return dict {miner_hotkey: weight}
    # For now, return empty dict assuming no history
    return {}
    # <<< End Placeholder >>>

async def persist_historical_weights(weights: Dict[str, float]):
    """
    Placeholder function to save the newly calculated consensus weights.
    Needs to interact with the validator's state/database manager.
    """
    # <<< Placeholder Implementation >>>
    logger.warning("Placeholder function `persist_historical_weights` called. Needs implementation.")
    # Example: Update database table with new weights
    pass
    # <<< End Placeholder >>>


async def calculate_consensus_weights(
    all_miner_scores: Dict[str, Dict[str, Dict[str, float]]], # {miner_hotkey: {var: {metric: val}}}
    learning_rate: float = 0.1, # How quickly weights adapt
    min_weight_factor: float = 0.01 # Minimum relative weight (e.g., 1% of average)
) -> Dict[str, float]:
    """
    Calculates consensus BMA weights based on detailed scores from all miners
    for a specific scoring period.

    Uses a primary skill metric (e.g., 1 - normalized_rmse) aggregated across
    variables, weighted by VARIABLE_WEIGHTS.

    Args:
        all_miner_scores: Dictionary containing the detailed scores for all
                          miners that were successfully processed in this cycle.
                          Structure: {miner_hotkey: {var: {metric: val}, ...}, ...}
        learning_rate: Factor controlling how much new scores influence weights (0 to 1).
        min_weight_factor: Sets a minimum weight relative to an equal share,
                           ensuring some diversity. E.g., 0.01 means min weight
                           is 1% of what it would be with equal weighting.

    Returns:
        A dictionary mapping miner hotkeys to their new consensus weights (normalized to sum to 1).
    """
    logger.info(f"Calculating consensus weights for {len(all_miner_scores)} miners.")
    miner_overall_skill_metric = {}

    # 1. Calculate an overall skill metric for each miner from detailed scores
    for miner_id, var_scores_dict in all_miner_scores.items():
        if not var_scores_dict: # Handle cases where scoring failed entirely for a miner
            miner_overall_skill_metric[miner_id] = 0.0 # Assign minimum skill
            logger.debug(f"Miner {miner_id} had no valid scores, assigning skill 0.")
            continue

        weighted_skill_sum = 0.0
        total_weight = 0.0
        processed_vars = 0

        for var, metrics in var_scores_dict.items():
            var_weight = VARIABLE_WEIGHTS.get(var, 0.0)
            if var_weight <= 0:
                continue

            # Choose the primary metric for skill (e.g., normalized RMSE)
            # Convert error (lower=better) to skill (higher=better)
            norm_rmse = metrics.get('normalized_rmse', None)

            if norm_rmse is None or np.isnan(norm_rmse):
                skill = 0.0 # Assign 0 skill if metric is missing/NaN
                logger.debug(f"Miner {miner_id}, var {var}: Missing or NaN norm_rmse, skill set to 0.")
            else:
                # Simple skill: 1.0 is perfect, 0.0 is at threshold, <0 is worse
                # We can cap skill at 0, as negative skill is hard to interpret in BMA.
                # Using threshold of 1.0 for normalized RMSE (can be adjusted)
                skill_threshold = 1.0
                skill = max(0.0, 1.0 - (norm_rmse / skill_threshold))
                logger.debug(f"Miner {miner_id}, var {var}: norm_rmse={norm_rmse:.4f}, skill={skill:.4f}")


            weighted_skill_sum += skill * var_weight
            total_weight += var_weight
            processed_vars += 1

        if total_weight > 0:
             overall_skill = weighted_skill_sum / total_weight
        else:
             overall_skill = 0.0 # No relevant variables scored

        miner_overall_skill_metric[miner_id] = overall_skill
        logger.debug(f"Miner {miner_id}: Overall skill metric = {overall_skill:.4f} from {processed_vars} variables.")

    # 2. Retrieve historical weights (needs implementation)
    current_weights = await get_historical_weights_from_db()
    logger.info(f"Retrieved {len(current_weights)} historical weights.")

    # 3. Update weights using BMA-like logic based on skill
    updated_weights = {}
    all_processed_miners = set(miner_overall_skill_metric.keys())
    all_historical_miners = set(current_weights.keys())
    all_relevant_miners = all_processed_miners.union(all_historical_miners)

    for miner_id in all_relevant_miners:
        current_w = current_weights.get(miner_id, 0.0)
        # Get skill, default to 0 if miner wasn't scored this cycle
        skill = miner_overall_skill_metric.get(miner_id, 0.0)

        # Map skill (0-1+) to a positive weight factor (e.g., linear or exponential)
        # Exponential emphasizes high performers more
        weight_factor = np.exp((skill - 0.5) * 4) # Example: Centered around 0.5 skill, scale factor 4

        if miner_id in all_processed_miners:
            # Apply learning rate update for miners scored this cycle
            updated_weights[miner_id] = (
                (1 - learning_rate) * current_w +
                learning_rate * weight_factor
            )
            logger.debug(f"Updating weight for {miner_id}: "
                         f"current={current_w:.4f}, skill={skill:.4f}, factor={weight_factor:.4f} -> new={updated_weights[miner_id]:.4f}")
        elif miner_id in all_historical_miners:
            # Decay weights for miners not seen this cycle
            updated_weights[miner_id] = current_w * (1 - learning_rate) # Decay only based on LR
            logger.debug(f"Decaying weight for unseen miner {miner_id}: {current_w:.4f} -> {updated_weights[miner_id]:.4f}")


    final_weights = {}
    total_active_weight = 0.0
    num_active_miners = len(all_processed_miners)
    min_weight_abs = (1.0 / max(1, num_active_miners)) * min_weight_factor if num_active_miners > 0 else 0

    for miner_id in all_relevant_miners:
        raw_updated_w = updated_weights.get(miner_id, 0.0)
        if miner_id in all_processed_miners:
             final_weights[miner_id] = max(raw_updated_w, min_weight_abs)
        else:
             final_weights[miner_id] = raw_updated_w
        total_active_weight += final_weights[miner_id]

    if total_active_weight > 1e-9:
        normalized_weights = {m: w / total_active_weight for m, w in final_weights.items()}
        logger.info(f"Normalized final weights. Sum: {sum(normalized_weights.values()):.4f}")
    else:
        logger.warning("Total weight sum is zero after updates. Falling back to equal weights for active miners.")
        num_active = len(all_processed_miners)
        equal_weight = 1.0 / max(1, num_active)
        normalized_weights = {m: equal_weight if m in all_processed_miners else 0.0 for m in all_relevant_miners}

    # Persist the new weights (needs implementation)
    await persist_historical_weights(normalized_weights)
    logger.info(f"Calculated and persisted consensus weights for {len(normalized_weights)} miners.")

    return normalized_weights 