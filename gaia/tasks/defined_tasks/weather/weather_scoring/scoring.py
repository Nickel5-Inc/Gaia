import numpy as np
import xarray as xr
from typing import Dict, List, Tuple, Union, Optional
import gc
from fiber.logging_utils import get_logger
from utils.metrics import calculate_rmse, normalize_rmse, calculate_mean_abs_diff
logger = get_logger(__name__)

"""
Core scoring functions for weather model evaluation.

This module implements scoring functions for evaluating weather forecasts
from miners and ensemble contribution scoring.
"""

# Constants, weights are based on impact to the average person
# Subject to change
VARIABLE_WEIGHTS = {
    "2t": 0.20,   # 2m temperature 
    "msl": 0.15,  # Mean sea level pressure
    "10u": 0.10,  # 10m U wind
    "10v": 0.10,  # 10m V wind
    "t": 0.15,    # Temperature at pressure levels
    "q": 0.10,    # Specific humidity
    "z": 0.10,    # Geopotential
    "u": 0.05,    # U wind at pressure levels
    "v": 0.05     # V wind at pressure levels
}

DEFAULT_THRESHOLD_RMSE = {
    # Example structure - will be filled with actual values
    "2t": [1.0] * 40,  # 40 lead times (10 days x 4 steps per day)
    "msl": [100.0] * 40,
}

def calculate_rmse_vectorized(forecast: Dict, ground_truth: Dict, subsample_factor: int = 1) -> float:
    """
    Vectorized RMSE calculation with optional subsampling for efficiency.
    
    Args:
        forecast: Dictionary of predicted variables
        ground_truth: Dictionary of ground truth variables
        subsample_factor: Factor to subsample grid points (1 = use all points)
    
    Returns:
        Root Mean Square Error
    """
    total_se = 0.0
    total_count = 0
    
    for var in forecast:
        if var in ground_truth:
            pred = forecast[var].astype(np.float32)
            truth = ground_truth[var].astype(np.float32)
            
            if subsample_factor > 1:
                pred = pred[::subsample_factor, ::subsample_factor]
                truth = truth[::subsample_factor, ::subsample_factor]
                
            se = np.sum((pred - truth) ** 2)
            count = pred.size
            total_se += se
            total_count += count
    
    if total_count > 0:
        return np.sqrt(total_se / total_count)
    else:
        return float('inf')

def score_accuracy(prediction: Dict, ground_truth: Dict, lead_time: int, 
                  threshold_rmse: Optional[Dict] = None,
                  subsample_factor: int = 1) -> float:
    """
    Calculate the accuracy score for a weather prediction.
    
    Args:
        prediction: Dictionary of predicted variables (var_name -> array)
        ground_truth: Dictionary of ground truth variables
        lead_time: Integer index (0-39) representing the forecast step
        threshold_rmse: Optional dictionary of threshold RMSE values by variable and lead time
        subsample_factor: Factor to subsample grid points
    
    Returns:
        Accuracy score (0.0 to 1.0+)
    """
    if threshold_rmse is None:
        threshold_rmse = DEFAULT_THRESHOLD_RMSE
    
    total_score = 0.0
    total_weight = 0.0
    
    for var, weight in VARIABLE_WEIGHTS.items():
        if var not in prediction or var not in ground_truth:
            continue
            
        pred_data = prediction[var]
        truth_data = ground_truth[var]
        
        if subsample_factor > 1:
            pred_data = pred_data[::subsample_factor, ::subsample_factor]
            truth_data = truth_data[::subsample_factor, ::subsample_factor]
        
        rmse = normalize_rmse(pred_data, truth_data, var)
        var_threshold = threshold_rmse.get(var, [1.0] * 40)[min(lead_time, 39)]
        skill = max(0.0, 1.0 - rmse / var_threshold)
        total_score += skill * weight
        total_weight += weight
    
    if total_weight > 0:
        total_score = total_score / total_weight
    
    lead_time_factor = 1.0 + (lead_time / 40) * 0.5  # Up to 50% bonus for day 10
    
    return total_score * lead_time_factor

def preprocess_forecasts(miner_data: Dict, variables: List[str], lead_time: int = 72) -> Dict:
    """
    Extract and format relevant data from miner forecasts.

    Args:
        miner_data: Dictionary of all miner forecasts
        variables: List of variables to extract
        lead_time: Forecast lead time in hours
    
    Returns:
        Dictionary of processed forecast data by miner and variable
    """
    processed_data = {}
    timestep = lead_time // 12
    
    for miner_id, forecast in miner_data.items():
        processed_data[miner_id] = {}
        
        for var in variables:
            if var not in forecast:
                continue
                
            # Surface variables (time, lat, lon)
            if var.startswith('surf_') or var in ['2t', 'msl', '10u', '10v']:
                if timestep < len(forecast[var]):
                    processed_data[miner_id][var] = forecast[var][timestep]
            
            # Atmospheric variables (time, level, lat, lon)
            elif var in ['t', 'q', 'z', 'u', 'v']:
                processed_data[miner_id][var] = {}
                for level_idx, level in enumerate(forecast.get(f"{var}_levels", [])):
                    if timestep < len(forecast[var]):
                        processed_data[miner_id][var][level] = forecast[var][timestep, level_idx]
    
    return processed_data

def create_base_ensemble(processed_forecasts: Dict, variables: List[str]) -> Tuple[Dict, Dict]:
    """
    Create base ensemble using mean of all forecasts.
    
    Args:
        processed_forecasts: Preprocessed forecast data by miner
        variables: List of variables to include
    
    Returns:
        Tuple of (base_ensemble, variable_forecast_data)
    """
    base_ensemble = {}
    var_forecasts = {}
    
    for var in variables:
        # Surface variables
        if var.startswith('surf_') or var in ['2t', 'msl', '10u', '10v']:
            # Collect all forecasts for this variable
            forecasts_for_var = []
            miner_ids_for_var = []
            
            for miner_id, forecast in processed_forecasts.items():
                if var in forecast:
                    forecasts_for_var.append(forecast[var])
                    miner_ids_for_var.append(miner_id)
            
            if forecasts_for_var:
                stacked = np.stack(forecasts_for_var)
                base_ensemble[var] = np.mean(stacked, axis=0)
                var_forecasts[var] = {
                    'stacked': stacked,
                    'miner_ids': miner_ids_for_var,
                    'sum': np.sum(stacked, axis=0),
                    'count': len(forecasts_for_var)
                }
        
        # Atmospheric variables (by pressure level)
        elif var in ['t', 'q', 'z', 'u', 'v']:
            all_levels = set()
            for forecast in processed_forecasts.values():
                if var in forecast:
                    all_levels.update(forecast[var].keys())
            
            # Create ensemble for each level
            for level in sorted(all_levels):
                level_key = f"{var}_{level}"
                forecasts_for_level = []
                miner_ids_for_level = []
                
                for miner_id, forecast in processed_forecasts.items():
                    if var in forecast and level in forecast[var]:
                        forecasts_for_level.append(forecast[var][level])
                        miner_ids_for_level.append(miner_id)
                
                if forecasts_for_level:
                    stacked = np.stack(forecasts_for_level)
                    if level_key not in base_ensemble:
                        base_ensemble[level_key] = np.mean(stacked, axis=0)
                    
                    var_forecasts[level_key] = {
                        'stacked': stacked,
                        'miner_ids': miner_ids_for_level,
                        'sum': np.sum(stacked, axis=0),
                        'count': len(forecasts_for_level)
                    }
    
    return base_ensemble, var_forecasts

def calculate_contribution_scores(base_ensemble: Dict, var_forecasts: Dict, 
                                 ground_truth: Dict, subsample_factor: int = 1) -> Dict:
    """
    Calculate contribution scores using leave-one-out method.
    
    Args:
        base_ensemble: Base ensemble forecast
        var_forecasts: Dictionary of variable forecast data
        ground_truth: Ground truth data
        subsample_factor: Factor to subsample grid points
    
    Returns:
        Dictionary of contribution scores by miner
    """
    base_error = calculate_rmse_vectorized(base_ensemble, ground_truth, subsample_factor)
    all_contributions = {}
    miner_to_var_contributions = {}
    
    for var in var_forecasts:
        if var not in ground_truth:
            continue
            
        var_data = var_forecasts[var]
        var_count = var_data['count']
        
        if var_count <= 1:
            continue
            
        var_ground_truth = {var: ground_truth[var]}
        for i, miner_id in enumerate(var_data['miner_ids']):
            loo_ensemble_i = (var_data['sum'] - var_data['stacked'][i]) / (var_count - 1)
            loo_ensemble_dict = {var: loo_ensemble_i}
            loo_error = calculate_rmse_vectorized(loo_ensemble_dict, var_ground_truth, subsample_factor)
            
            # Positive means ensemble gets worse without this miner
            contribution = (loo_error - base_error) / max(base_error, 1e-8)
            if miner_id not in miner_to_var_contributions:
                miner_to_var_contributions[miner_id] = {}
            
            miner_to_var_contributions[miner_id][var] = contribution
    
    # Average contribution across variables for each miner
    for miner_id, var_contributions in miner_to_var_contributions.items():
        if var_contributions:
            all_contributions[miner_id] = np.mean(list(var_contributions.values()))
    
    return all_contributions

def detect_similar_forecasts(miner_forecasts: Dict, threshold: float = 0.01) -> Dict:
    """
    Detect groups of miners with highly similar forecasts, likely using the same model.
    
    Args:
        miner_forecasts: Dictionary of miner forecasts
        threshold: Similarity threshold (lower = more sensitive)
    
    Returns:
        Dictionary mapping miners to their uniqueness score
    """
    uniqueness_scores = {}
    n_miners = len(miner_forecasts)
    
    if n_miners < 3:
        return {m: 1.0 for m in miner_forecasts}
    
    pairwise_diffs = {}
    key_variables = ["2t", "msl", "z"]  # Variables to focus on
    
    for m1 in miner_forecasts:
        for m2 in miner_forecasts:
            if m1 >= m2:
                continue
                
            pair_key = f"{m1}_{m2}"
            diffs = []
            
            for var in key_variables:
                if var in miner_forecasts[m1] and var in miner_forecasts[m2]:
                    var_diff = calculate_mean_abs_diff(
                        miner_forecasts[m1][var], 
                        miner_forecasts[m2][var]
                    )
                    diffs.append(var_diff)
            
            if diffs:
                pairwise_diffs[pair_key] = np.mean(diffs)
    
    try:
        from sklearn.cluster import DBSCAN
        
        dist_matrix = np.zeros((n_miners, n_miners))
        miner_idx = {m: i for i, m in enumerate(miner_forecasts)}
        
        for pair, diff in pairwise_diffs.items():
            m1, m2 = pair.split('_')
            i, j = miner_idx[m1], miner_idx[m2]
            dist_matrix[i, j] = dist_matrix[j, i] = diff
        
        # Cluster miners by forecast similarity
        clustering = DBSCAN(eps=threshold, min_samples=2, metric='precomputed')
        clusters = clustering.fit_predict(dist_matrix)
        
        # Uniqueness scores
        cluster_counts = {}
        for c in clusters:
            if c >= 0:  # -1 means no cluster assigned
                cluster_counts[c] = cluster_counts.get(c, 0) + 1
        
        for miner, idx in miner_idx.items():
            cluster = clusters[idx]
            if cluster == -1:  # Unique model. Assign uniqueness score of 1.0  
                uniqueness_scores[miner] = 1.0
            else:
                # Penalty based on cluster size (larger clusters = more similar models)
                cluster_size = cluster_counts.get(cluster, 1)
                uniqueness_scores[miner] = max(0.2, 1.0 - (cluster_size / n_miners))
    
    except ImportError:
        for miner in miner_forecasts:
            similar_count = 0
            total_comparisons = 0
            
            for pair, diff in pairwise_diffs.items():
                if miner in pair.split('_'):
                    total_comparisons += 1
                    if diff < threshold:
                        similar_count += 1
            
            if total_comparisons > 0:
                uniqueness_scores[miner] = 1.0 - (similar_count / total_comparisons)
            else:
                uniqueness_scores[miner] = 1.0
    
    return uniqueness_scores

def update_bma_weights(current_weights: Dict[str, float], 
                      contribution_scores: Dict[str, float], 
                      learning_rate: float = 0.2) -> Dict[str, float]:
    """
    Update BMA weights based on contribution scores.
    
    Args:
        current_weights: Dictionary of current weights
        contribution_scores: Dictionary of contribution scores
        learning_rate: Rate of adaptation (0.0-1.0)
        
    Returns:
        Dictionary of updated weights
    """
    weight_factors = {
        m: np.exp(max(-5, min(5, score * 3))) 
        for m, score in contribution_scores.items()
    }
    
    updated_weights = {}
    
    for miner_id, current_weight in current_weights.items():
        if miner_id in weight_factors:
            updated_weights[miner_id] = (
                (1 - learning_rate) * current_weight + 
                learning_rate * weight_factors[miner_id]
            )
        else:
            updated_weights[miner_id] = current_weight * 0.5
    
    for miner_id in weight_factors:
        if miner_id not in current_weights:
            updated_weights[miner_id] = weight_factors[miner_id]
    
    total_weight = sum(updated_weights.values())
    if total_weight > 0:
        normalized_weights = {m: w/total_weight for m, w in updated_weights.items()}
    else:
        normalized_weights = {m: 1.0/len(updated_weights) for m in updated_weights}
    
    return normalized_weights

def compute_final_scores(
    accuracy_scores: Dict[str, float],
    contribution_scores: Dict[str, float],
    uniqueness_scores: Dict[str, float],
    weights: Dict[str, float] = {"accuracy": 0.6, "contribution": 0.3, "uniqueness": 0.1}
) -> Dict[str, float]:
    """
    Compute final scores as weighted combination of components.
    
    Args:
        accuracy_scores: Dictionary of accuracy scores
        contribution_scores: Dictionary of contribution scores
        uniqueness_scores: Dictionary of uniqueness scores
        weights: Dictionary of component weights
    
    Returns:
        Dictionary of final scores
    """
    final_scores = {}
    all_miners = set()
    all_miners.update(accuracy_scores.keys())
    all_miners.update(contribution_scores.keys())
    all_miners.update(uniqueness_scores.keys())
    
    for miner_id in all_miners:
        accuracy = accuracy_scores.get(miner_id, 0.0)
        contribution = contribution_scores.get(miner_id, 0.0)
        uniqueness = uniqueness_scores.get(miner_id, 0.0)
        
        final_scores[miner_id] = (
            accuracy * weights["accuracy"] +
            contribution * weights["contribution"] +
            uniqueness * weights["uniqueness"]
        )
    
    return final_scores