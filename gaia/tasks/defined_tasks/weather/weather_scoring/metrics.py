import numpy as np
import xarray as xr
import xskillscore as xs
import asyncio
from typing import Dict, Any, Union, Optional, Tuple, List
from gaia.utils.custom_logger import get_logger

logger = get_logger(__name__)

"""
Core metric calculations for weather forecast evaluation.
This module provides implementations of common metrics for
evaluating weather forecasts, including RMSE, MAE, bias, correlation,
"""


def _compute_metric_to_array(metric_func, *args, **kwargs):
    """
    Helper function to compute metrics and preserve array structure.
    Handles both numpy arrays and dask arrays properly.
    This function is synchronous and designed to be called via asyncio.to_thread().
    """
    result = metric_func(*args, **kwargs)

    # Force computation if it's a dask array
    if hasattr(result, "compute"):
        result = result.compute()

    return result


def _compute_metric_to_scalar_array(metric_func, *args, **kwargs):
    """
    Helper function to compute metrics and convert to scalar using sync processing.
    Handles both numpy arrays and dask arrays properly.
    This function is synchronous and designed to be called via asyncio.to_thread().
    """
    result = metric_func(*args, **kwargs)

    # Force computation if it's a dask array
    if hasattr(result, "compute"):
        result = result.compute()

    return result


def _get_metric_as_float(metric_fn, *args, **kwargs):
    """
    Synchronous helper that directly calls the metric function.
    This function is called via asyncio.to_thread() so it should be synchronous.
    Handles both numpy arrays and dask arrays properly.
    """
    computed_array = metric_fn(*args, **kwargs)

    # Force computation if it's a dask array
    if hasattr(computed_array, "compute"):
        computed_array = computed_array.compute()

    if computed_array.ndim == 0:
        return float(computed_array.item())
    elif computed_array.ndim == 1:
        logger.debug(
            f"Metric returned 1D array (shape: {computed_array.shape}), taking mean for scalar result."
        )
        mean_val = computed_array.mean()
        # Force computation again if mean result is also dask
        if hasattr(mean_val, "compute"):
            mean_val = mean_val.compute()
        return float(mean_val.item())
    else:
        raise ValueError(
            f"Metric calculation resulted in an array with unexpected dimensions: {computed_array.shape}. Cannot convert to single float."
        )


def _calculate_latitude_weights(lat_da: xr.DataArray) -> xr.DataArray:
    return np.cos(np.deg2rad(lat_da)).where(np.isfinite(lat_da), 0)


async def calculate_bias_corrected_forecast(
    forecast_da: xr.DataArray, truth_da: xr.DataArray
) -> xr.DataArray:
    """Calculates a bias-corrected forecast by subtracting the spatial mean error."""
    try:
        if not isinstance(forecast_da, xr.DataArray) or not isinstance(
            truth_da, xr.DataArray
        ):
            logger.error(
                "Inputs to calculate_bias_corrected_forecast must be xr.DataArray"
            )
            raise TypeError("Inputs must be xr.DataArray")

        spatial_dims = [
            d
            for d in forecast_da.dims
            if d.lower() in ("latitude", "longitude", "lat", "lon")
        ]
        if not spatial_dims:
            logger.error("No spatial dimensions (lat/lon) found for bias correction.")
            return forecast_da

        error = forecast_da - truth_da
        bias_uncomputed = error.mean(dim=spatial_dims)
        if hasattr(bias_uncomputed, "compute"):
            bias = await asyncio.to_thread(bias_uncomputed.compute)
        else:
            bias = bias_uncomputed

        forecast_bc_da = forecast_da - bias
        logger.info(f"Bias correction calculated. Mean bias: {bias.values}")
        return forecast_bc_da
    except Exception as e:
        logger.error(f"Error in calculate_bias_corrected_forecast: {e}", exc_info=True)
        return forecast_da


async def calculate_mse_skill_score_by_pressure_level(
    forecast_bc_da: xr.DataArray,
    truth_da: xr.DataArray,
    reference_da: xr.DataArray,
    lat_weights: xr.DataArray,
) -> Dict[int, float]:
    """
    Calculate MSE skill score for each pressure level from vectorized calculations.
    Returns a dict mapping pressure_level -> skill_score.
    """
    try:
        spatial_dims = [
            d for d in forecast_bc_da.dims
            if d.lower() in ("latitude", "longitude", "lat", "lon")
        ]
        if not spatial_dims:
            logger.error("No spatial dimensions (lat/lon) found for MSE skill score.")
            return {}

        # Strict weights validation: must match spatial dims exactly
        validated_weights = lat_weights
        if lat_weights is not None:
            weights_dims_set = set(lat_weights.dims)
            spatial_dims_set = set(spatial_dims)
            if weights_dims_set != spatial_dims_set:
                raise ValueError(
                    f"Weights dims must match spatial dims exactly. "
                    f"Spatial dims: {spatial_dims}, Weights dims: {list(lat_weights.dims)}"
                )
            # Basic broadcast compatibility
            _ = truth_da.isel({d: 0 for d in truth_da.dims if d not in spatial_dims}) * lat_weights
            logger.debug(
                f"MSE Skill Score by Pressure Level: Weights validation passed - dims: {lat_weights.dims}"
            )

        # Single vectorized MSE calculations (preserve pressure_level dimension)
        mse_forecast_result = await asyncio.to_thread(
            _compute_metric_to_array,  # Use array-preserving helper
            xs.mse,
            forecast_bc_da,
            truth_da,
            dim=spatial_dims,  # Only reduce lat/lon
            weights=validated_weights,
            skipna=True,
        )
        
        mse_reference_result = await asyncio.to_thread(
            _compute_metric_to_array,  # Use array-preserving helper
            xs.mse,
            reference_da,
            truth_da,
            dim=spatial_dims,  # Only reduce lat/lon
            weights=validated_weights,
            skipna=True,
        )

        # Extract per-pressure-level skill scores
        per_level_scores = {}
        if "pressure_level" in mse_forecast_result.dims:
            # Atmospheric variable - extract each pressure level
            for level in mse_forecast_result.coords["pressure_level"]:
                level_val = int(level.item())
                
                mse_forecast_val = float(mse_forecast_result.sel(pressure_level=level).item())
                mse_reference_val = float(mse_reference_result.sel(pressure_level=level).item())
                
                if mse_reference_val == 0:
                    skill_score = 1.0 if mse_forecast_val == 0 else -np.inf
                else:
                    skill_score = 1 - (mse_forecast_val / mse_reference_val)
                
                per_level_scores[level_val] = skill_score
        else:
            # Surface variable - single score
            mse_forecast_val = float(mse_forecast_result.item())
            mse_reference_val = float(mse_reference_result.item())
            
            if mse_reference_val == 0:
                skill_score = 1.0 if mse_forecast_val == 0 else -np.inf
            else:
                skill_score = 1 - (mse_forecast_val / mse_reference_val)
            
            per_level_scores[None] = skill_score
        
        return per_level_scores

    except Exception as e:
        logger.error(f"Error calculating MSE skill score by pressure level: {e}")
        return {}


async def calculate_mse_skill_score(
    forecast_bc_da: xr.DataArray,
    truth_da: xr.DataArray,
    reference_da: xr.DataArray,
    lat_weights: xr.DataArray,
    variable_name: Optional[str] = None,
) -> float:
    """
    Calculates the MSE-based skill score: 1 - (MSE_forecast / MSE_reference).
    Enhanced with async processing for large datasets.
    """
    try:
        spatial_dims = [
            d
            for d in forecast_bc_da.dims
            if d.lower() in ("latitude", "longitude", "lat", "lon")
        ]
        if not spatial_dims:
            logger.error("No spatial dimensions (lat/lon) found for MSE skill score.")
            return -np.inf

        # Strictly use xskillscore with validated dimensions (no fallbacks)
        # Validate dims equality and expected structure
        if truth_da.dims != forecast_bc_da.dims or reference_da.dims != truth_da.dims:
            raise ValueError(
                f"Forecast, truth, and reference dims must match exactly. "
                f"Forecast: {forecast_bc_da.dims}, Truth: {truth_da.dims}, Reference: {reference_da.dims}"
            )
        # CRITICAL FIX: Validate weights dimensions before passing to xskillscore
        validated_weights = lat_weights
        if lat_weights is not None:
            weights_dims_set = set(lat_weights.dims)
            spatial_dims_set = set(spatial_dims)
            if weights_dims_set != spatial_dims_set:
                raise ValueError(
                    f"Weights dims must match spatial dims exactly. "
                    f"Spatial dims: {spatial_dims}, Weights dims: {list(lat_weights.dims)}"
                )
            _ = truth_da.isel({d: 0 for d in truth_da.dims if d not in spatial_dims}) * lat_weights
            logger.debug(f"MSE Skill Score: Weights validation passed - dims: {lat_weights.dims}")
        
        # Add diagnostics before MSE calculation
        variable_name = getattr(forecast_bc_da, 'name', 'unknown')
        logger.debug(f"MSE Skill Score: About to calculate MSE for variable '{variable_name}'")
        logger.debug(f"  Forecast dims: {forecast_bc_da.dims}, shape: {forecast_bc_da.shape}")
        logger.debug(f"  Truth dims: {truth_da.dims}, shape: {truth_da.shape}")
        logger.debug(f"  Reference dims: {reference_da.dims}, shape: {reference_da.shape}")
        logger.debug(f"  Spatial dims for reduction: {spatial_dims}")
        logger.debug(f"  Using weights: {validated_weights is not None}")
        
        # Coordinate consistency already enforced; additional debug only
        
        # Compute MSE results
        mse_forecast_result = await asyncio.to_thread(
            _compute_metric_to_array,
            xs.mse,
            forecast_bc_da,
            truth_da,
            dim=spatial_dims,
            weights=validated_weights,
            skipna=True,
        )
        mse_reference_result = await asyncio.to_thread(
            _compute_metric_to_array,
            xs.mse,
            reference_da,
            truth_da,
            dim=spatial_dims,
            weights=validated_weights,
            skipna=True,
        )

        # Aggregate across pressure levels using mass-weighted thickness if needed
        if "pressure_level" in mse_forecast_result.dims:
            levels = mse_forecast_result["pressure_level"].values.astype(float)
            levels_sorted = np.sort(levels)
            # compute half-level thickness weights in hPa
            thickness = np.zeros_like(levels_sorted)
            for i in range(len(levels_sorted)):
                if i == 0:
                    thickness[i] = (levels_sorted[i + 1] - levels_sorted[i]) / 2.0
                elif i == len(levels_sorted) - 1:
                    thickness[i] = (levels_sorted[i] - levels_sorted[i - 1]) / 2.0
                else:
                    thickness[i] = (levels_sorted[i + 1] - levels_sorted[i - 1]) / 2.0
            # map thickness to the original level order
            thickness_map = {lvl: th for lvl, th in zip(levels_sorted, thickness)}
            weights_pl = np.array([thickness_map[lvl] for lvl in levels])
            weights_pl = weights_pl / np.sum(weights_pl)

            # Ensure we weight in the same order as DataArray levels
            mse_forecast_vals = np.array([float(mse_forecast_result.sel(pressure_level=l).item()) for l in levels])
            mse_reference_vals = np.array([float(mse_reference_result.sel(pressure_level=l).item()) for l in levels])

            mse_forecast_val = float(np.sum(weights_pl * mse_forecast_vals))
            mse_reference_val = float(np.sum(weights_pl * mse_reference_vals))
        else:
            mse_forecast_val = float(mse_forecast_result.item())
            mse_reference_val = float(mse_reference_result.item())
        
        # ENHANCED DIAGNOSTIC: Check for NaN/inf values in MSE calculations
        var_label = variable_name if variable_name is not None else "unknown"
        logger.info(f"MSE Skill Score Diagnostics for '{var_label}' - Forecast MSE: {mse_forecast_val}, Reference MSE: {mse_reference_val}")
        
        # Additional diagnostics for NaN investigation
        if not np.isfinite(mse_forecast_val) or not np.isfinite(mse_reference_val):
            logger.warning(f"MSE Skill Score: Non-finite MSE detected for variable '{variable_name}'!")
            logger.warning(f"  Forecast MSE: {mse_forecast_val} (finite: {np.isfinite(mse_forecast_val)})")
            logger.warning(f"  Reference MSE: {mse_reference_val} (finite: {np.isfinite(mse_reference_val)})")
            
            # Check input data for NaN/inf values
            forecast_has_nan = np.isnan(forecast_bc_da.values).any()
            truth_has_nan = np.isnan(truth_da.values).any()
            reference_has_nan = np.isnan(reference_da.values).any()
            
            logger.warning(f"  Input data NaN check:")
            logger.warning(f"    Forecast has NaN: {forecast_has_nan}")
            logger.warning(f"    Truth has NaN: {truth_has_nan}")
            logger.warning(f"    Reference has NaN: {reference_has_nan}")
            
            # Check data ranges
            if not forecast_has_nan:
                logger.warning(f"    Forecast range: [{np.nanmin(forecast_bc_da.values):.6f}, {np.nanmax(forecast_bc_da.values):.6f}]")
            if not truth_has_nan:
                logger.warning(f"    Truth range: [{np.nanmin(truth_da.values):.6f}, {np.nanmax(truth_da.values):.6f}]")
            if not reference_has_nan:
                logger.warning(f"    Reference range: [{np.nanmin(reference_da.values):.6f}, {np.nanmax(reference_da.values):.6f}]")
            
            return np.nan
        
        if not np.isfinite(mse_forecast_val):
            logger.warning(f"MSE Skill Score: Forecast MSE is non-finite: {mse_forecast_val}")
            return np.nan
        
        if not np.isfinite(mse_reference_val):
            logger.warning(f"MSE Skill Score: Reference MSE is non-finite: {mse_reference_val}")
            return np.nan

        if mse_reference_val == 0:
            skill_score = 1.0 if mse_forecast_val == 0 else -np.inf
        else:
            skill_score = 1 - (mse_forecast_val / mse_reference_val)

        logger.info(f"MSE Skill Score (xskillscore): {skill_score}")
        return skill_score

    except Exception as e:
        logger.error(f"Error calculating MSE skill score: {e}")
        return -np.inf


async def calculate_acc_by_pressure_level(
    forecast_da: xr.DataArray,
    truth_da: xr.DataArray,
    climatology_da: xr.DataArray,
    lat_weights: xr.DataArray,
) -> Dict[int, float]:
    """
    Calculate ACC for each pressure level from a single vectorized calculation.
    Returns a dict mapping pressure_level -> acc_score.
    """
    try:
        spatial_dims = [
            d for d in forecast_da.dims
            if d.lower() in ("latitude", "longitude", "lat", "lon", "lat_lon")
        ]
        if not spatial_dims:
            logger.error("No spatial dimensions (lat/lon) found for ACC.")
            return {}

        # Calculate anomalies once
        forecast_anom = forecast_da - climatology_da
        truth_anom = truth_da - climatology_da

        # Rechunk guard for Dask core-dimension errors (lat, lon, or combined lat_lon)
        def _rechunk_spatial(da: xr.DataArray) -> xr.DataArray:
            try:
                dims = getattr(da, "dims", ())
                chunk_map = {}
                if "lat" in dims:
                    chunk_map["lat"] = -1
                if "lon" in dims:
                    chunk_map["lon"] = -1
                if "lat_lon" in dims:
                    chunk_map["lat_lon"] = -1
                if chunk_map:
                    return da.chunk(chunk_map)
            except Exception:
                pass
            return da

        forecast_anom = _rechunk_spatial(forecast_anom)
        truth_anom = _rechunk_spatial(truth_anom)

        # Debug logging
        logger.debug(f"ACC calculation - Data dims: {forecast_da.dims}, spatial_dims: {spatial_dims}")
        logger.debug(f"ACC calculation - Weights dims: {lat_weights.dims if lat_weights is not None else None}")
        
        # Strict weights validation
        validated_weights = lat_weights
        if lat_weights is not None:
            weights_dims_set = set(lat_weights.dims)
            spatial_dims_set = set(spatial_dims)
            if not spatial_dims_set.issubset(weights_dims_set) and weights_dims_set != spatial_dims_set:
                raise ValueError(
                    f"Weights dims must match spatial dims exactly or be broadcastable. "
                    f"Spatial dims: {spatial_dims}, Weights dims: {list(lat_weights.dims)}"
                )

        # Debug the input data dimensions before calling xskillscore
        logger.debug(f"ACC Input Debug:")
        logger.debug(f"  forecast_anom dims: {forecast_anom.dims}, shape: {forecast_anom.shape}")
        logger.debug(f"  truth_anom dims: {truth_anom.dims}, shape: {truth_anom.shape}")
        logger.debug(f"  spatial_dims for reduction: {spatial_dims}")
        
        # Single vectorized ACC calculation across all pressure levels
        acc_result = await asyncio.to_thread(
            _compute_metric_to_array,  # Use array-preserving helper
            xs.pearson_r,
            forecast_anom,
            truth_anom,
            dim=spatial_dims,  # Only reduce spatial dims, preserve pressure_level
            weights=validated_weights,
            skipna=True,
        )
        
        logger.debug(f"ACC result dims: {acc_result.dims}, shape: {acc_result.shape}")
        
        # Rigor: result must have single 'pressure_level' dimension
        if hasattr(acc_result, 'dims') and (len(acc_result.dims) != 1 or 'pressure_level' not in acc_result.dims):
            raise ValueError(
                f"ACC result must have a single 'pressure_level' dimension after reducing spatial dims. "
                f"Got dims: {acc_result.dims} with sizes {getattr(acc_result, 'sizes', {})}"
            )

        # Degeneracy guards: if anomalies have near-zero variance, ACC is undefined
        try:
            eps = 1e-12
            std_f = await asyncio.to_thread(_compute_metric_to_array, xr.apply_ufunc, np.std, forecast_anom, input_core_dims=[spatial_dims], kwargs={"keepdims": False})
            std_t = await asyncio.to_thread(_compute_metric_to_array, xr.apply_ufunc, np.std, truth_anom, input_core_dims=[spatial_dims], kwargs={"keepdims": False})
        except Exception:
            std_f = None
            std_t = None

        # Extract per-pressure-level scores
        per_level_scores = {}
        if "pressure_level" in acc_result.dims:
            for level in acc_result.coords["pressure_level"]:
                level_val = int(level.item())
                level_result = acc_result.sel(pressure_level=level)
                # Degenerate anomalies -> NaN
                try:
                    if std_f is not None and std_t is not None:
                        sf = float(std_f.sel(pressure_level=level).item()) if "pressure_level" in getattr(std_f, 'dims', ()) else float(std_f.item())
                        st = float(std_t.sel(pressure_level=level).item()) if "pressure_level" in getattr(std_t, 'dims', ()) else float(std_t.item())
                        if (sf < eps) or (st < eps):
                            score_val = float('nan')
                        else:
                            score_val = float(level_result.mean().item()) if level_result.size > 1 else float(level_result.item())
                    else:
                        score_val = float(level_result.mean().item()) if level_result.size > 1 else float(level_result.item())
                except Exception:
                    score_val = float(level_result.mean().item()) if level_result.size > 1 else float(level_result.item())
                per_level_scores[level_val] = score_val
        else:
            if acc_result.size != 1:
                raise ValueError(
                    f"Surface ACC result must be scalar. Got size={acc_result.size}"
                )
            # Surface degeneracy check
            try:
                eps = 1e-12
                std_f_s = await asyncio.to_thread(_compute_metric_to_array, xr.apply_ufunc, np.std, forecast_anom, input_core_dims=[spatial_dims], kwargs={"keepdims": False})
                std_t_s = await asyncio.to_thread(_compute_metric_to_array, xr.apply_ufunc, np.std, truth_anom, input_core_dims=[spatial_dims], kwargs={"keepdims": False})
                sf = float(std_f_s.item())
                st = float(std_t_s.item())
                if (sf < eps) or (st < eps):
                    per_level_scores[None] = float('nan')
                else:
                    per_level_scores[None] = float(acc_result.item())
            except Exception:
                per_level_scores[None] = float(acc_result.item())
        
        return per_level_scores

    except Exception as e:
        logger.error(f"Error calculating ACC by pressure level: {e}")
        return {}


async def calculate_acc(
    forecast_da: xr.DataArray,
    truth_da: xr.DataArray,
    climatology_da: xr.DataArray,
    lat_weights: xr.DataArray,
) -> float:
    """
    Calculates the Anomaly Correlation Coefficient (ACC).
    Enhanced with async processing for large datasets.
    """
    try:
        spatial_dims = [
            d
            for d in forecast_da.dims
            if d.lower() in ("latitude", "longitude", "lat", "lon")
        ]
        if not spatial_dims:
            logger.error("No spatial dimensions (lat/lon) found for ACC.")
            return -np.inf

        # Strictly use xskillscore (no fallbacks)
        forecast_anom = forecast_da - climatology_da
        truth_anom = truth_da - climatology_da

        logger.info(
            f"[calculate_acc] forecast_anom shape: {forecast_anom.shape}, dims: {forecast_anom.dims}, type: {type(forecast_anom.data)}"
        )
        logger.info(
            f"[calculate_acc] truth_anom shape: {truth_anom.shape}, dims: {truth_anom.dims}, type: {type(truth_anom.data)}"
        )

        acc_scalar = await asyncio.to_thread(
            _compute_metric_to_scalar_array,
            xs.pearson_r,
            forecast_anom,
            truth_anom,
            dim=spatial_dims,
            weights=lat_weights,
            skipna=True,
        )

        # Degeneracy guard: anomalies must have non-zero variance across spatial dims
        try:
            eps = 1e-12
            std_f = await asyncio.to_thread(_compute_metric_to_array, xr.apply_ufunc, np.std, forecast_anom, input_core_dims=[spatial_dims], kwargs={"keepdims": False})
            std_t = await asyncio.to_thread(_compute_metric_to_array, xr.apply_ufunc, np.std, truth_anom, input_core_dims=[spatial_dims], kwargs={"keepdims": False})
            if float(std_f.item()) < eps or float(std_t.item()) < eps:
                return float('nan')
        except Exception:
            pass

        acc_float = acc_scalar.item()
        logger.info(f"ACC calculated (xskillscore): {acc_float:.4f}")
        return acc_float

    except Exception as e:
        logger.error(f"Error calculating ACC: {e}")
        return -np.inf


async def perform_sanity_checks(
    forecast_da: xr.DataArray,
    reference_da_for_corr: xr.DataArray,
    variable_name: str,
    climatology_bounds_config: Dict[str, Tuple[float, float]],
    pattern_corr_threshold: float,
    lat_weights: xr.DataArray,
) -> Dict[str, any]:
    """
    Performs climatological bounds check and pattern correlation against a reference.
    Returns a dictionary with check statuses and values.
    """
    results = {
        "climatology_passed": None,
        "climatology_min_actual": None,
        "climatology_max_actual": None,
        "pattern_correlation_passed": None,
        "pattern_correlation_value": None,
    }
    spatial_dims = [
        d
        for d in forecast_da.dims
        if d.lower() in ("latitude", "longitude", "lat", "lon")
    ]

    try:
        min_val_uncomputed = forecast_da.min()
        if hasattr(min_val_uncomputed, "compute"):
            actual_min_scalar = await asyncio.to_thread(min_val_uncomputed.compute)
        else:
            actual_min_scalar = min_val_uncomputed
        actual_min = float(actual_min_scalar.item())

        max_val_uncomputed = forecast_da.max()
        if hasattr(max_val_uncomputed, "compute"):
            actual_max_scalar = await asyncio.to_thread(max_val_uncomputed.compute)
        else:
            actual_max_scalar = max_val_uncomputed
        actual_max = float(actual_max_scalar.item())

        results["climatology_min_actual"] = actual_min
        results["climatology_max_actual"] = actual_max

        passed_general_bounds = True
        bounds = climatology_bounds_config.get(variable_name)
        if bounds:
            min_bound, max_bound = bounds
            if not (actual_min >= min_bound and actual_max <= max_bound):
                passed_general_bounds = False
                logger.warning(
                    f"Climatology check FAILED for {variable_name}. Expected: {bounds}, Got: ({actual_min:.2f}, {actual_max:.2f})"
                )
        else:
            logger.info(
                f"No general climatology bounds configured for {variable_name}. Skipping general bounds part of check."
            )

        passed_specific_physical_checks = True
        if variable_name.startswith("q"):
            if actual_min < 0.0:
                passed_specific_physical_checks = False
                logger.warning(
                    f"Physicality check FAILED for humidity {variable_name}: min value {actual_min:.4f} is negative."
                )

        results["climatology_passed"] = (
            passed_general_bounds and passed_specific_physical_checks
        )
        if (
            not results["climatology_passed"]
            and passed_general_bounds
            and not passed_specific_physical_checks
        ):
            logger.warning(
                f"Climatology check FAILED for {variable_name} due to specific physical constraint violation."
            )

        if not spatial_dims:
            logger.warning(
                f"No spatial dimensions for pattern correlation on {variable_name}. Skipping."
            )
            results["pattern_correlation_passed"] = True
        else:
            correlation_float = await asyncio.to_thread(
                _get_metric_as_float,
                xs.pearson_r,
                forecast_da,
                reference_da_for_corr,
                dim=spatial_dims,
                weights=lat_weights,
                skipna=True,
            )
            results["pattern_correlation_value"] = correlation_float
            if correlation_float >= pattern_corr_threshold:
                results["pattern_correlation_passed"] = True
            else:
                results["pattern_correlation_passed"] = False
                logger.warning(
                    f"Pattern correlation FAILED for {variable_name}. Expected >= {pattern_corr_threshold}, Got: {results['pattern_correlation_value']:.2f}"
                )

    except Exception as e:
        logger.error(
            f"Error during sanity checks for {variable_name}: {e}", exc_info=True
        )
        if results["climatology_passed"] is None:
            results["climatology_passed"] = False
        if results["pattern_correlation_passed"] is None:
            results["pattern_correlation_passed"] = False

    logger.info(f"Sanity check results for {variable_name}: {results}")
    return results


async def calculate_rmse_by_pressure_level(
    forecast_da: xr.DataArray, truth_da: xr.DataArray, lat_weights: xr.DataArray
) -> Dict[int, float]:
    """
    Calculate RMSE for each pressure level from a single vectorized calculation.
    Returns a dict mapping pressure_level -> rmse_score.
    """
    try:
        spatial_dims = [
            d for d in forecast_da.dims 
            if d.lower() in ("latitude", "longitude", "lat", "lon")
        ]
        if not spatial_dims:
            logger.error("No spatial dimensions (lat/lon) found for RMSE.")
            return {}

        # Single vectorized calculation across all pressure levels
        rmse_result = await asyncio.to_thread(
            _compute_metric_to_array,  # Use array-preserving helper
            xs.rmse,
            forecast_da,
            truth_da,
            dim=spatial_dims,  # Only reduce lat/lon, preserve pressure_level
            weights=lat_weights,
            skipna=True,
        )

        # Extract per-pressure-level scores
        per_level_scores = {}
        if "pressure_level" in rmse_result.dims:
            # Atmospheric variable - extract each pressure level
            for level in rmse_result.coords["pressure_level"]:
                level_val = int(level.item())
                level_result = rmse_result.sel(pressure_level=level)
                
                # Handle case where result might still have extra dimensions
                if level_result.size == 1:
                    score_val = float(level_result.item())
                else:
                    raise ValueError(
                        f"RMSE result for level {level_val} has {level_result.size} elements; expected scalar after spatial reduction"
                    )
                
                per_level_scores[level_val] = score_val
        else:
            # Surface variable - single score must be scalar
            if rmse_result.size != 1:
                raise ValueError(
                    f"Surface RMSE result must be scalar. Got size={rmse_result.size}"
                )
            per_level_scores[None] = float(rmse_result.item())
        
        return per_level_scores

    except Exception as e:
        logger.error(f"Error calculating RMSE by pressure level: {e}")
        return {}


async def calculate_rmse(
    forecast_da: xr.DataArray, truth_da: xr.DataArray, lat_weights: xr.DataArray
) -> float:
    """
    Calculates the Root Mean Square Error (RMSE) using async processing.
    """
    try:
        spatial_dims = [
            d
            for d in forecast_da.dims
            if d.lower() in ("latitude", "longitude", "lat", "lon")
        ]
        if not spatial_dims:
            logger.error("No spatial dimensions (lat/lon) found for RMSE.")
            return np.inf

        # Strictly use xskillscore (no async alt)
        rmse_scalar = await asyncio.to_thread(
            _compute_metric_to_scalar_array,
            xs.rmse,
            forecast_da,
            truth_da,
            dim=spatial_dims,
            weights=lat_weights,
            skipna=True,
        )
        rmse_value = rmse_scalar.item()
        logger.info(f"RMSE calculated (xskillscore): {rmse_value:.4f}")
        return rmse_value

    except Exception as e:
        logger.error(f"Error calculating RMSE: {e}")
        return np.inf


async def calculate_bias(
    forecast_da: xr.DataArray, truth_da: xr.DataArray, lat_weights: xr.DataArray
) -> float:
    """
    Calculates the bias (mean error) using async processing.
    """
    try:
        spatial_dims = [
            d
            for d in forecast_da.dims
            if d.lower() in ("latitude", "longitude", "lat", "lon")
        ]
        if not spatial_dims:
            logger.error("No spatial dimensions (lat/lon) found for bias.")
            return np.nan

        # Strict manual calculation (no async alt)
        diff = forecast_da - truth_da
        if lat_weights is not None:
            weighted_diff = diff * lat_weights
            weighted_sum = weighted_diff.sum()
            weights_sum = lat_weights.sum()

            # Compute dask arrays if needed
            if hasattr(weighted_sum, "compute"):
                weighted_sum = await asyncio.to_thread(weighted_sum.compute)
            if hasattr(weights_sum, "compute"):
                weights_sum = await asyncio.to_thread(weights_sum.compute)

            bias_value = float(weighted_sum / weights_sum)
        else:
            mean_diff = diff.mean()

            # Compute dask array if needed
            if hasattr(mean_diff, "compute"):
                mean_diff = await asyncio.to_thread(mean_diff.compute)

            bias_value = float(mean_diff)

        logger.info(f"Bias calculated (manual): {bias_value:.4f}")
        return bias_value

    except Exception as e:
        logger.error(f"Error calculating bias: {e}")
        return np.nan
