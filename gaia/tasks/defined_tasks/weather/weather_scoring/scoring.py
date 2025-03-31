"""
Core scoring functions for Aurora weather model evaluation.

This module implements deterministic scoring functions for evaluating weather forecasts
from Aurora models, including accuracy, improvement over baseline, extreme event 
prediction, and ensemble contribution.
"""

import numpy as np
import xarray as xr
from typing import Dict, List, Tuple, Union, Optional

from utils.metrics import calculate_rmse, normalize_rmse, calculate_mean_abs_diff
from utils.data import extract_variable

# Constants, weights are based on impact to the average person
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

# Default threshold RMSE values by variable and lead time
# These would be calibrated based on historical data
DEFAULT_THRESHOLD_RMSE = {
    # Example structure - would be filled with actual values
    "2t": [1.0] * 40,  # 40 lead times (10 days x 4 steps per day)
    "msl": [100.0] * 40,
    # ... other variables
}

# Innovation threshold for detecting unmodified baseline models
INNOVATION_THRESHOLD = 0.01  # Would be calibrated based on testing

def score_accuracy(prediction: Dict, ground_truth: Dict, lead_time: int, 
                  threshold_rmse: Optional[Dict] = None) -> float:
    """
    Calculate the accuracy score for a weather prediction.
    
    Args:
        prediction: Dictionary of predicted variables (var_name -> array)
        ground_truth: Dictionary of ground truth variables
        lead_time: Integer index (0-39) representing the forecast step
        threshold_rmse: Optional dictionary of threshold RMSE values by variable and lead time
    
    Returns:
        Accuracy score (0.0 to 1.0+)
    """
    if threshold_rmse is None:
        threshold_rmse = DEFAULT_THRESHOLD_RMSE
    
    total_score = 0.0
    
    for var, weight in VARIABLE_WEIGHTS.items():
        if var not in prediction or var not in ground_truth:
            continue
            
        # Extract the data for this variable
        pred_data = extract_variable(prediction, var)
        truth_data = extract_variable(ground_truth, var)
        
        # Calculate normalized RMSE
        rmse = normalize_rmse(pred_data, truth_data, var)
        
        # Convert to skill score (higher is better)
        var_threshold = threshold_rmse.get(var, [1.0] * 40)[min(lead_time, 39)]
        skill = max(0.0, 1.0 - rmse / var_threshold)
        
        # Add to total score
        total_score += skill * weight
    
    # Apply lead time difficulty bonus
    lead_time_factor = 1.0 + (lead_time / 40) * 0.5  # Up to 50% bonus for day 10
    
    return total_score * lead_time_factor

def score_improvement(prediction: Dict, baseline_prediction: Dict, ground_truth: Dict) -> float:
    """
    Calculate improvement score compared to a baseline prediction.
    
    Args:
        prediction: Dictionary of miner's predicted variables
        baseline_prediction: Dictionary of baseline predicted variables
        ground_truth: Dictionary of ground truth variables
    
    Returns:
        Improvement score (0.0 to 1.0)
    """
    # Calculate RMSE for baseline and miner predictions
    baseline_rmse = calculate_rmse(baseline_prediction, ground_truth)
    miner_rmse = calculate_rmse(prediction, ground_truth)
    
    # Calculate relative improvement
    if baseline_rmse > 0:
        improvement = (baseline_rmse - miner_rmse) / baseline_rmse
    else:
        improvement = 0.0
    
    # Apply non-linear scaling to reward meaningful improvements
    improvement_score = min(1.0, pow(max(0, improvement), 0.7) * 3.0)
    
    return improvement_score

def score_extreme_events(prediction: Dict, ground_truth: Dict, climatology: Dict) -> float:
    """
    Score prediction of extreme weather events.
    
    Args:
        prediction: Dictionary of predicted variables
        ground_truth: Dictionary of ground truth variables
        climatology: Dictionary of climatological data for thresholds
    
    Returns:
        Extreme event score (0.0 to 1.0)
    """
    # Define extreme event thresholds based on climatology
    thresholds = {
        "2t_hot": np.quantile(climatology["2t"], 0.95),
        "2t_cold": np.quantile(climatology["2t"], 0.05),
        "wind_strong": np.quantile(np.sqrt(climatology["10u"]**2 + climatology["10v"]**2), 0.95),
        "msl_low": np.quantile(climatology["msl"], 0.05)
    }
    
    # Calculate scores for each extreme event type
    event_scores = {}
    
    # Hot temperature extremes
    if "2t" in prediction and "2t" in ground_truth:
        truth_mask = ground_truth["2t"] > thresholds["2t_hot"]
        pred_mask = prediction["2t"] > thresholds["2t_hot"]
        event_scores["2t_hot"] = calculate_csi(truth_mask, pred_mask)
    
    # Cold temperature extremes
    if "2t" in prediction and "2t" in ground_truth:
        truth_mask = ground_truth["2t"] < thresholds["2t_cold"]
        pred_mask = prediction["2t"] < thresholds["2t_cold"]
        event_scores["2t_cold"] = calculate_csi(truth_mask, pred_mask)
    
    # Strong wind events
    if "10u" in prediction and "10v" in prediction and "10u" in ground_truth and "10v" in ground_truth:
        truth_wind = np.sqrt(ground_truth["10u"]**2 + ground_truth["10v"]**2)
        pred_wind = np.sqrt(prediction["10u"]**2 + prediction["10v"]**2)
        truth_mask = truth_wind > thresholds["wind_strong"]
        pred_mask = pred_wind > thresholds["wind_strong"]
        event_scores["wind_strong"] = calculate_csi(truth_mask, pred_mask)
    
    # Low pressure systems
    if "msl" in prediction and "msl" in ground_truth:
        truth_mask = ground_truth["msl"] < thresholds["msl_low"]
        pred_mask = prediction["msl"] < thresholds["msl_low"]
        event_scores["msl_low"] = calculate_csi(truth_mask, pred_mask)
    
    # Calculate weighted average score
    weights = {"2t_hot": 0.3, "2t_cold": 0.2, "wind_strong": 0.3, "msl_low": 0.2}
    total_score = 0.0
    total_weight = 0.0
    
    for event, score in event_scores.items():
        if event in weights:
            total_score += score * weights[event]
            total_weight += weights[event]
    
    # Return normalized score
    if total_weight > 0:
        return total_score / total_weight
    else:
        return 0.0

def calculate_csi(truth_mask: np.ndarray, pred_mask: np.ndarray) -> float:
    """
    Calculate Critical Success Index (CSI) for binary event prediction.
    
    Args:
        truth_mask: Boolean array of where events occurred
        pred_mask: Boolean array of where events were predicted
    
    Returns:
        CSI score (0.0 to 1.0)
    """
    hits = np.sum(truth_mask & pred_mask)
    misses = np.sum(truth_mask & ~pred_mask)
    false_alarms = np.sum(~truth_mask & pred_mask)
    
    denominator = hits + misses + false_alarms
    if denominator > 0:
        return hits / denominator
    else:
        return 0.0

def score_ensemble_contribution(
    miner_id: str, 
    predictions: Dict[str, Dict], 
    ground_truth: Dict,
    historical_weights: Optional[Dict[str, float]] = None
) -> float:
    """
    Score how much a miner's prediction improves the ensemble.
    
    Args:
        miner_id: ID of miner being evaluated
        predictions: Dictionary of all miners' predictions
        ground_truth: Dictionary of ground truth variables
        historical_weights: Optional dictionary of historical weights for each miner
    
    Returns:
        Contribution score (0.0 to 1.0)
    """
    from ensemble import create_weighted_ensemble
    
    # Default weights if none provided
    if historical_weights is None:
        historical_weights = {m: 1.0 for m in predictions.keys()}
    
    # Create ensemble without this miner
    other_miners = [m for m in predictions.keys() if m != miner_id]
    if not other_miners:
        return 0.5  # Neutral score if no other miners
    
    # Create weights for ensemble without this miner
    weights_without = {m: historical_weights.get(m, 0.0) for m in other_miners}
    total = sum(weights_without.values())
    if total > 0:
        weights_without = {m: w/total for m, w in weights_without.items()}
    
    # Create ensemble without this miner
    ensemble_without = create_weighted_ensemble(
        {m: predictions[m] for m in other_miners},
        weights_without
    )
    
    # Calculate RMSE of ensemble without this miner
    rmse_without = calculate_rmse(ensemble_without, ground_truth)
    
    # Create ensemble with all miners
    ensemble_with = create_weighted_ensemble(predictions, historical_weights)
    
    # Calculate RMSE of ensemble with this miner
    rmse_with = calculate_rmse(ensemble_with, ground_truth)
    
    # Calculate contribution
    if rmse_without > 0:
        contribution = (rmse_without - rmse_with) / rmse_without
    else:
        contribution = 0.0
    
    # Scale to 0.0-1.0 range
    contribution_score = min(1.0, max(0.0, contribution * 10.0))
    
    return contribution_score

def detect_baseline_model(
    prediction: Dict, 
    baseline_prediction: Dict,
    threshold: float = INNOVATION_THRESHOLD
) -> bool:
    """
    Detect if a prediction is from an unmodified baseline model.
    
    Args:
        prediction: Dictionary of predicted variables
        baseline_prediction: Dictionary of baseline predicted variables
        threshold: Threshold for detecting baseline models
        
    Returns:
        Boolean indicating if prediction is likely from baseline model
    """
    total_diff = 0.0
    count = 0
    
    for var in VARIABLE_WEIGHTS.keys():
        if var in prediction and var in baseline_prediction:
            diff = calculate_mean_abs_diff(prediction[var], baseline_prediction[var])
            total_diff += diff
            count += 1
    
    if count == 0:
        return True  # No comparable variables, assume baseline
    
    mean_diff = total_diff / count
    
    return mean_diff < threshold

def adjust_score_for_baseline(score: float, is_baseline: bool) -> float:
    """
    Adjust a miner's score based on whether they're using baseline model.
    
    Args:
        score: Original score (0.0 to 1.0+)
        is_baseline: Boolean indicating if prediction is from baseline model
        
    Returns:
        Adjusted score
    """
    if is_baseline:
        # Baseline models capped at 25% of potential reward
        return min(score, 0.25)
    else:
        # Full scoring potential for modified models
        return score

def calculate_total_score(
    prediction: Dict,
    baseline_prediction: Dict,
    ground_truth: Dict,
    climatology: Dict,
    lead_time: int,
    miner_id: str = None,
    all_predictions: Dict[str, Dict] = None,
    historical_weights: Dict[str, float] = None,
    threshold_rmse: Dict = None
) -> Dict[str, float]:
    """
    Calculate complete score for a weather prediction.
    
    Args:
        prediction: Dictionary of predicted variables
        baseline_prediction: Dictionary of baseline predictions
        ground_truth: Dictionary of ground truth variables
        climatology: Dictionary of climatological data
        lead_time: Integer lead time index (0-39)
        miner_id: Optional miner ID for ensemble contribution
        all_predictions: Optional dictionary of all miners' predictions
        historical_weights: Optional dictionary of historical weights
        threshold_rmse: Optional dictionary of threshold RMSE values
        
    Returns:
        Dictionary of score components and total
    """
    # Calculate base accuracy score (65%)
    accuracy = score_accuracy(prediction, ground_truth, lead_time, threshold_rmse)
    
    # Calculate improvement over baseline (20%)
    improvement = score_improvement(prediction, baseline_prediction, ground_truth)
    
    # Calculate extreme event score (15%)
    extreme = score_extreme_events(prediction, ground_truth, climatology)
    
    # Calculate ensemble contribution if possible (10%)
    contribution = 0.0
    if miner_id is not None and all_predictions is not None:
        contribution = score_ensemble_contribution(
            miner_id, all_predictions, ground_truth, historical_weights
        )
    
    # Check if this is a baseline model
    is_baseline = detect_baseline_model(prediction, baseline_prediction)
    
    # Calculate weighted total
    raw_total = (
        0.65 * accuracy +
        0.20 * improvement +
        0.15 * extreme
    )
    
    # Apply tiered scoring based on lead time
    forecast_day = lead_time / 4  # 4 steps per day
    if forecast_day <= 3:  # Days 1-3
        tier_weight = 0.3
    elif forecast_day <= 7:  # Days 4-7
        tier_weight = 0.3
    else:  # Days 8-10
        tier_weight = 0.4
    
    # Apply ensemble contribution if available
    if contribution > 0:
        # Add contribution score (not tier-weighted as it's a global property)
        raw_total = 0.9 * raw_total + 0.1 * contribution
    
    # Apply tier weight
    weighted_total = raw_total * tier_weight
    
    # Apply baseline adjustment
    adjusted_total = adjust_score_for_baseline(weighted_total, is_baseline)
    
    # Return all score components
    return {
        "accuracy": accuracy,
        "improvement": improvement,
        "extreme_events": extreme,
        "ensemble_contribution": contribution,
        "is_baseline": is_baseline,
        "raw_total": raw_total,
        "weighted_total": weighted_total,
        "adjusted_total": adjusted_total,
        "tier_weight": tier_weight
    }