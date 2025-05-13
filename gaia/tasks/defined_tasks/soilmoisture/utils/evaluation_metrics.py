import numpy as np
from scipy import stats
import logging

logger = logging.getLogger(__name__)

DROUGHT_THRESHOLD = 0.15
WET_THRESHOLD = 0.40

def _mask_arrays(prediction: np.ndarray, truth: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Applies NaN mask from truth array to both arrays and flattens."""
    if prediction.shape != truth.shape:
        raise ValueError("Prediction and truth arrays must have the same shape.")
    
    mask = ~np.isnan(truth)
    valid_pred = prediction[mask]
    valid_truth = truth[mask]
    
    if valid_pred.size == 0:
        return np.array([]), np.array([]) 
        
    return valid_pred, valid_truth

def calculate_mean_error(prediction: np.ndarray, truth: np.ndarray) -> float:
    """Calculate Mean Error (Bias)."""
    valid_pred, valid_truth = _mask_arrays(prediction, truth)
    if valid_pred.size == 0:
        return np.nan
    return np.mean(valid_pred - valid_truth)

def calculate_mae(prediction: np.ndarray, truth: np.ndarray) -> float:
    """Calculate Mean Absolute Error."""
    valid_pred, valid_truth = _mask_arrays(prediction, truth)
    if valid_pred.size == 0:
        return np.nan
    return np.mean(np.abs(valid_pred - valid_truth))

def calculate_pearson_correlation(prediction: np.ndarray, truth: np.ndarray) -> tuple[float, float]:
    """Calculate Pearson correlation coefficient and p-value."""
    valid_pred, valid_truth = _mask_arrays(prediction, truth)
    if valid_pred.size < 2 or np.all(valid_pred == valid_pred[0]) or np.all(valid_truth == valid_truth[0]):
        return np.nan, np.nan
    try:
        rho, p_value = stats.pearsonr(valid_pred, valid_truth)
        return rho, p_value
    except Exception as e:
        logger.warning(f"Could not calculate Pearson correlation: {e}")
        return np.nan, np.nan

def calculate_spearman_correlation(prediction: np.ndarray, truth: np.ndarray) -> tuple[float, float]:
    """Calculate Spearman rank correlation coefficient and p-value."""
    valid_pred, valid_truth = _mask_arrays(prediction, truth)
    if valid_pred.size < 2 or np.all(valid_pred == valid_pred[0]) or np.all(valid_truth == valid_truth[0]):
        return np.nan, np.nan
    try:
        rho, p_value = stats.spearmanr(valid_pred, valid_truth)
        return rho, p_value
    except Exception as e:
        logger.warning(f"Could not calculate Spearman correlation: {e}")
        return np.nan, np.nan

def calculate_ks_statistic(prediction: np.ndarray, truth: np.ndarray) -> tuple[float, float]:
    """Calculate Kolmogorov-Smirnov test statistic and p-value."""
    valid_pred, valid_truth = _mask_arrays(prediction, truth)
    if valid_pred.size == 0:
        return np.nan, np.nan
    try:
        statistic, p_value = stats.ks_2samp(valid_pred, valid_truth)
        return statistic, p_value
    except Exception as e:
        logger.warning(f"Could not calculate KS statistic: {e}")
        return np.nan, np.nan
        
def calculate_cv(array: np.ndarray) -> float:
    """Calculate Coefficient of Variation (handle zero mean)."""
    if array.size == 0:
        return np.nan
    mean = np.mean(array)
    std = np.std(array)
    if mean == 0:
        return np.inf if std > 0 else 0.0 # Return Inf if variable but mean is 0, else 0 if constant 0
    return std / mean

def calculate_histogram_intersection(prediction: np.ndarray, truth: np.ndarray, num_bins=10) -> float:
    """Calculate Histogram Intersection."""
    valid_pred, valid_truth = _mask_arrays(prediction, truth)
    if valid_pred.size == 0:
        return np.nan
        
    min_val = min(np.min(valid_pred), np.min(valid_truth))
    max_val = max(np.max(valid_pred), np.max(valid_truth))
    bin_edges = np.linspace(min_val, max_val, num_bins + 1)
    
    hist_pred, _ = np.histogram(valid_pred, bins=bin_edges)
    hist_truth, _ = np.histogram(valid_truth, bins=bin_edges)
    
    prob_pred = hist_pred / valid_pred.size
    prob_truth = hist_truth / valid_truth.size
    
    intersection = np.sum(np.minimum(prob_pred, prob_truth))
    return intersection

def calculate_contingency_table(prediction: np.ndarray, truth: np.ndarray, threshold: float) -> dict:
    """Calculate Hits, Misses, False Alarms, Correct Negatives for a threshold."""
    valid_pred, valid_truth = _mask_arrays(prediction, truth)
    if valid_pred.size == 0:
        return {'hits': 0, 'misses': 0, 'false_alarms': 0, 'correct_negatives': 0}

    pred_over_thresh = valid_pred >= threshold
    truth_over_thresh = valid_truth >= threshold

    hits = np.sum(pred_over_thresh & truth_over_thresh)
    misses = np.sum(~pred_over_thresh & truth_over_thresh)
    false_alarms = np.sum(pred_over_thresh & ~truth_over_thresh)
    correct_negatives = np.sum(~pred_over_thresh & ~truth_over_thresh)
    
    return {
        'hits': int(hits),
        'misses': int(misses),
        'false_alarms': int(false_alarms),
        'correct_negatives': int(correct_negatives)
    }

def calculate_csi(contingency: dict) -> float:
    """Calculate Critical Success Index (Threat Score)."""
    hits = contingency['hits']
    misses = contingency['misses']
    false_alarms = contingency['false_alarms']
    denominator = hits + misses + false_alarms
    return hits / denominator if denominator > 0 else 0.0

def calculate_pod(contingency: dict) -> float:
    """Calculate Probability of Detection (Hit Rate)."""
    hits = contingency['hits']
    misses = contingency['misses']
    denominator = hits + misses
    return hits / denominator if denominator > 0 else 0.0

def calculate_far(contingency: dict) -> float:
    """Calculate False Alarm Ratio."""
    hits = contingency['hits']
    false_alarms = contingency['false_alarms']
    denominator = hits + false_alarms
    return false_alarms / denominator if denominator > 0 else 0.0


def calculate_all_metrics(prediction: np.ndarray, truth: np.ndarray) -> dict:
    """Calculate a comprehensive set of metrics."""
    metrics = {}
    
    metrics['mean_error'] = calculate_mean_error(prediction, truth)
    metrics['mae'] = calculate_mae(prediction, truth)
    
    metrics['pearson_rho'], metrics['pearson_p'] = calculate_pearson_correlation(prediction, truth)
    metrics['spearman_rho'], metrics['spearman_p'] = calculate_spearman_correlation(prediction, truth)
    
    metrics['ks_stat'], metrics['ks_p'] = calculate_ks_statistic(prediction, truth)
    metrics['hist_intersection'] = calculate_histogram_intersection(prediction, truth)
    
    metrics['cv_pred'] = calculate_cv(prediction[~np.isnan(prediction)]) # CV on non-nan pred pixels
    metrics['cv_truth'] = calculate_cv(truth[~np.isnan(truth)])         # CV on non-nan truth pixels
    
    drought_contingency = calculate_contingency_table(prediction, truth, threshold=DROUGHT_THRESHOLD)
    metrics['drought_csi'] = calculate_csi(drought_contingency)
    metrics['drought_pod'] = calculate_pod(drought_contingency)
    metrics['drought_far'] = calculate_far(drought_contingency)

    wet_contingency = calculate_contingency_table(prediction, truth, threshold=WET_THRESHOLD)
    metrics['wet_csi'] = calculate_csi(wet_contingency)
    metrics['wet_pod'] = calculate_pod(wet_contingency)
    metrics['wet_far'] = calculate_far(wet_contingency)

    for key, value in metrics.items():
        if isinstance(value, (np.float32, np.float64)):
            metrics[key] = float(value) if not np.isnan(value) else None # Replace NaN with None for JSON/logging
        elif isinstance(value, np.bool_):
            metrics[key] = bool(value)
        elif value is None or np.isnan(value): # Catch potential NaNs missed
             metrics[key] = None

    return metrics

if __name__ == '__main__':
    np.random.seed(42)
    shape = (11, 11)
    truth_data = np.random.rand(*shape).astype(np.float32) * 0.5 # Scale to 0-0.5
    pred_data = truth_data + np.random.normal(0, 0.05, size=shape).astype(np.float32)
    pred_data = np.clip(pred_data, 0, 1) # Clip prediction to valid range
    
    truth_data[0, 0] = np.nan
    truth_data[5, 5] = np.nan

    print("--- Sample Data --- \n")
    print(f"Truth Shape: {truth_data.shape}, NaN count: {np.sum(np.isnan(truth_data))}")
    print(f"Pred Shape: {pred_data.shape}, NaN count: {np.sum(np.isnan(pred_data))}\n")
    
    print("--- Calculating All Metrics --- \n")
    all_metrics = calculate_all_metrics(pred_data, truth_data)
    
    print("--- Calculated Metrics --- \n")
    for key, value in all_metrics.items():
        print(f"{key}: {value}")
    
    print("\n--- Example Categorical (Drought) ---")
    drought_cont = calculate_contingency_table(pred_data, truth_data, DROUGHT_THRESHOLD)
    print(drought_cont)
    print(f"CSI: {calculate_csi(drought_cont)}")
