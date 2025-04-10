import asyncio
import gc
import traceback
from datetime import datetime, timezone
from typing import Dict, List, Optional

import fsspec
import metpy.calc as mpcalc
import numpy as np
import xarray as xr
import xskillscore as xs
from fiber.logging_utils import get_logger
from metpy.units import units

logger = get_logger(__name__)

AURORA_FUNDAMENTAL_SURFACE_VARIABLES = ["2t", "msl", "10u", "10v"]
AURORA_FUNDAMENTAL_ATMOS_VARIABLES = ["t", "q", "u", "v", "z"] # Geopotential (z) is m2 s-2
AURORA_DERIVED_VARIABLES = ["rh", "td", "ws", "wdir"]

ALL_EXPECTED_VARIABLES = (
    AURORA_FUNDAMENTAL_SURFACE_VARIABLES
    + AURORA_FUNDAMENTAL_ATMOS_VARIABLES
    + AURORA_DERIVED_VARIABLES
)


async def _open_dataset_lazily(reference_spec: Dict) -> Optional[xr.Dataset]:
    """Asynchronously open an xarray dataset lazily using fsspec and zarr."""
    url = reference_spec.get("url", "unknown URL")
    try:
        fs = fsspec.filesystem(
            "reference",
            fo=url,
            remote_protocol=reference_spec.get("protocol", "http"),
            remote_options=reference_spec.get("options", {}),
        )
        mapper = fs.get_mapper("")
        ds = xr.open_dataset(mapper, engine="zarr", consolidated=False, chunks={})
        logger.debug(f"Successfully opened lazy dataset: {url}")
        return ds
    except FileNotFoundError:
        logger.warning(f"Kerchunk JSON file not found: {url}")
        return None
    except Exception as e:
        logger.error(f"Failed to open lazy dataset from {url}: {e}")
        logger.debug(traceback.format_exc())
        return None


async def create_physics_aware_ensemble(
    miner_forecast_refs: Dict[str, Dict],
    miner_weights: Dict[str, float],
    top_k: Optional[int] = None,
    variables_to_process: Optional[List[str]] = None,
    consistency_checks: Optional[Dict[str, bool]] = None,
) -> Optional[xr.Dataset]:
    """
    Creates a weighted ensemble forecast.

    Processes data timestep-by-timestep.
    Applies MetPy calculations to enforce thermodynamic and wind consistency
    between related variables in the final ensemble product.
    Not perfect, much more work to implement a SPPT based method, if possible. 

    Args:
        miner_forecast_refs: Dict mapping miner_hotkey to its forecast reference spec.
                             Each spec should be a dict like:
                             {'url': 'http://...', 'protocol': 'http', 'options': {'headers': {'Authorization': 'Bearer ...'}}}
        miner_weights: Dict mapping miner_hotkey to its numerical weight/score.
        top_k: Max number of top-weighted miners to include (None uses all valid).
        variables_to_process: Specific variables to include in the ensemble. If None,
                              uses all variables found in the template dataset.
                              Derived variables (rh, td, ws, wdir) will be added if
                              consistency checks are enabled and dependencies exist.
        consistency_checks: Dict specifying which checks to apply, defaults to
                              {'thermo': True, 'wind': True}.

    Returns:
        An xarray.Dataset containing the ensemble forecast, or None if creation failed.
    """
    start_time = datetime.now(timezone.utc)
    logger.info(f"Starting ensemble creation (top_k={top_k})...")

    if not miner_forecast_refs or not miner_weights:
        logger.warning("Ensemble creation failed: No miner forecasts or weights provided.")
        return None

    valid_miners = [m for m in miner_forecast_refs if m in miner_weights and miner_weights[m] > 0]
    if not valid_miners:
        logger.warning("Ensemble creation failed: No miners with positive weights found.")
        return None

    if top_k is not None and len(valid_miners) > top_k:
        sorted_miners = sorted(valid_miners, key=lambda m: miner_weights[m], reverse=True)
        selected_miner_ids = sorted_miners[:top_k]
        logger.info(f"Selected top {top_k} miners for ensemble: {selected_miner_ids}")
    else:
        selected_miner_ids = valid_miners
        logger.info(f"Using all {len(selected_miner_ids)} valid miners for ensemble.")

    if not selected_miner_ids:
         logger.warning("Ensemble creation failed: No miners selected after filtering.")
         return None

    selected_refs = {m: miner_forecast_refs[m] for m in selected_miner_ids}
    selected_weights = {m: miner_weights[m] for m in selected_miner_ids}

    total_weight = sum(selected_weights.values())
    if total_weight <= 1e-9:
        logger.warning(f"Total weight is near zero ({total_weight}). Using equal weights for {len(selected_miner_ids)} miners.")
        num_selected = len(selected_miner_ids)
        normalized_weights = {m: 1.0 / num_selected for m in selected_miner_ids}
    else:
        normalized_weights = {m: w / total_weight for m, w in selected_weights.items()}

    template_miner_id = selected_miner_ids[0]
    template_ds = await _open_dataset_lazily(selected_refs[template_miner_id])
    if template_ds is None:
        logger.error(f"Failed to open template dataset from miner {template_miner_id}. Aborting ensemble.")
        return None

    if variables_to_process is None:
        target_variables = list(template_ds.data_vars)
        logger.info(f"Processing all variables found in template: {target_variables}")
    else:
         target_variables = [v for v in variables_to_process if v in template_ds.data_vars]
         logger.info(f"Processing specified variables found in template: {target_variables}")

    active_checks = consistency_checks if consistency_checks is not None else {'thermo': True, 'wind': True}
    if active_checks.get('thermo') and all(v in target_variables for v in ['2t', 'q', 'msl']) or all(v in target_variables for v in ['t', 'q']):
        if 'rh' not in target_variables: target_variables.append('rh')
        if 'td' not in target_variables: target_variables.append('td')
    if active_checks.get('wind') and all(v in target_variables for v in ['10u', '10v']) or all(v in target_variables for v in ['u', 'v']):
        if 'ws' not in target_variables: target_variables.append('ws')
        if 'wdir' not in target_variables: target_variables.append('wdir')

    target_variables = sorted(list(set(target_variables)))
    logger.info(f"Final list of variables to compute in ensemble: {target_variables}")

    if not target_variables:
        logger.error("No valid variables identified for processing in the ensemble.")
        template_ds.close()
        return None

    ensemble_ds = xr.Dataset(coords={c: template_ds[c].copy(deep=True) for c in template_ds.coords})
    for var in target_variables:
        if var in template_ds:
            ensemble_ds[var] = xr.full_like(template_ds[var].variable, np.nan, dtype=template_ds[var].dtype)
        else:
            example_dependency = None
            if var in ['rh', 'td']:
                 example_dependency = '2t' if '2t' in template_ds else 't'
            elif var in ['ws', 'wdir']:
                 example_dependency = '10u' if '10u' in template_ds else 'u'

            if example_dependency and example_dependency in template_ds:
                 ensemble_ds[var] = xr.full_like(template_ds[example_dependency].variable, np.nan, dtype=np.float32)
                 logger.info(f"Initialized derived variable '{var}' structure based on '{example_dependency}'.")
            else:
                 logger.error(f"Cannot initialize structure for derived variable '{var}'. Missing dependencies in template.")
                 template_ds.close()
                 return None

    time_coords = ensemble_ds.time.values
    template_ds.close()
    gc.collect()

    num_timesteps = len(time_coords)
    logger.info(f"Processing {num_timesteps} timesteps...")
    processed_timesteps = 0
    for t_idx, t_val in enumerate(time_coords):
        logger.debug(f"Processing timestep {t_idx+1}/{num_timesteps} ({t_val})")
        timestep_start_time = datetime.now(timezone.utc)
        raw_ensemble_slices = {}  

        fundamental_vars_in_target = [ 
            v for v in target_variables 
            if v in AURORA_FUNDAMENTAL_SURFACE_VARIABLES + AURORA_FUNDAMENTAL_ATMOS_VARIABLES
        ]

        for var in fundamental_vars_in_target:
            data_slices = []
            current_weights_for_var = []
            participating_miner_ids_for_var = []

            load_tasks = []
            miners_to_load_from = []

            async def load_slice(miner_id, ref):
                miner_ds_local = None
                try:
                    miner_ds_local = await _open_dataset_lazily(ref)
                    if miner_ds_local is None: return None, miner_id, f"Failed to open dataset"

                    if var in miner_ds_local and 'time' in miner_ds_local[var].coords:
                        slice_lazy = miner_ds_local[var].sel(time=t_val, method="nearest")
                        slice_data = await asyncio.to_thread(slice_lazy.load)
                        return slice_data, miner_id, None
                    else:
                        return None, miner_id, f"Variable '{var}' or time coord not found"
                except Exception as e_load:
                    return None, miner_id, f"Exception loading slice: {e_load}"
                finally:
                    if miner_ds_local:
                        miner_ds_local.close()

            for miner_id in selected_miner_ids:
                 load_tasks.append(load_slice(miner_id, selected_refs[miner_id]))

            load_results = await asyncio.gather(*load_tasks)

            for slice_data, miner_id, error_msg in load_results:
                if error_msg:
                    logger.warning(f"Failed load for {miner_id}, var '{var}', time {t_val}: {error_msg}")
                elif slice_data is not None:
                    if isinstance(slice_data, xr.DataArray):
                        data_slices.append(slice_data)
                        current_weights_for_var.append(normalized_weights[miner_id])
                        participating_miner_ids_for_var.append(miner_id)
                    else:
                        logger.warning(f"Loaded data for {miner_id}, var '{var}', time {t_val} is not DataArray ({type(slice_data)}). Skipping.")

            if not data_slices:
                logger.warning(f"No valid data slices found for fundamental variable '{var}' at timestep {t_val}. Ensemble for this var/time will be NaN.")
                continue 

            try:
                stacked_data = xr.concat(data_slices, dim='member')
                stacked_data['member'] = participating_miner_ids_for_var 
            except Exception as e:
                logger.error(f"Failed to stack data for var '{var}' at timestep {t_val}: {e}. Shapes: {[s.shape for s in data_slices]}")
                continue

            weights_xr = xr.DataArray(current_weights_for_var, dims='member', coords={'member': participating_miner_ids_for_var})

            try:
                ensemble_slice = xs.weighted_mean(stacked_data, weights=weights_xr, dim='member', keep_attrs=True)
                raw_ensemble_slices[var] = ensemble_slice 
            except Exception as e:
                logger.error(f"xskillscore weighted_mean failed for var '{var}' at timestep {t_val}: {e}")
                continue

            del data_slices, current_weights_for_var, stacked_data, weights_xr
            gc.collect()


        logger.debug(f"Applying consistency checks for timestep {t_idx+1}")
        final_ensemble_slices = raw_ensemble_slices.copy()

        def _add_units(var_name, data_slice):
            unit_val = None
            if var_name in ['2t', 't', 'td']: unit_val = units.kelvin
            elif var_name == 'msl': unit_val = units.pascal
            elif var_name in ['10u', '10v', 'u', 'v', 'ws']: unit_val = units('m/s')
            elif var_name == 'q': unit_val = units('kg/kg') 
            elif var_name == 'z': unit_val = units('m**2 / s**2')
            elif var_name == 'rh': unit_val = units.percent
            elif var_name == 'wdir': unit_val = units.degrees
            return data_slice * unit_val if unit_val else data_slice

        if active_checks.get('thermo', False):
            try:
                temp_var, hum_var, press_var, press_lev = None, None, None, None
                if all(v in raw_ensemble_slices for v in ['2t', 'q', 'msl']):
                    temp_var, hum_var, press_var = '2t', 'q', 'msl'
                elif all(v in raw_ensemble_slices for v in ['t', 'q']) and 'level' in ensemble_ds.coords:
                    temp_var, hum_var = 't', 'q'
                    press_lev = ensemble_ds['level'] * units.hPa

                if temp_var and hum_var and (press_var or press_lev is not None):
                    t_slice = _add_units(temp_var, raw_ensemble_slices[temp_var])
                    q_slice = _add_units(hum_var, raw_ensemble_slices[hum_var])
                    p_slice = _add_units(press_var, raw_ensemble_slices[press_var]) if press_var else press_lev
                    
                    if hasattr(t_slice, 'metpy') and hasattr(q_slice, 'metpy') and hasattr(p_slice, 'metpy'):
                        rh_derived = mpcalc.relative_humidity_from_specific_humidity(p_slice, t_slice, q_slice)
                        if 'rh' in target_variables: 
                            final_ensemble_slices['rh'] = rh_derived.to(units.percent).magnitude
                            logger.debug(f"Updated 'rh' using derived value for timestep {t_idx+1}")

                        td_derived = mpcalc.dewpoint_from_specific_humidity(p_slice, t_slice, q_slice)
                        if 'td' in target_variables: 
                            final_ensemble_slices['td'] = td_derived.to(units.kelvin).magnitude
                            logger.debug(f"Updated 'td' using derived value for timestep {t_idx+1}")
                    else:
                         logger.warning(f"Skipping thermo consistency: Input slices lack units for timestep {t_idx+1}")

            except Exception as e:
                 logger.warning(f"MetPy thermodynamic consistency check failed for timestep {t_idx+1}: {e}")
                 logger.debug(traceback.format_exc())

        if active_checks.get('wind', False):
            try:
                u_var, v_var = None, None
                if all(v in raw_ensemble_slices for v in ['10u', '10v']):
                    u_var, v_var = '10u', '10v'
                elif all(v in raw_ensemble_slices for v in ['u', 'v']):
                    u_var, v_var = 'u', 'v'

                if u_var and v_var:
                    u_slice = _add_units(u_var, raw_ensemble_slices[u_var])
                    v_slice = _add_units(v_var, raw_ensemble_slices[v_var])
                    
                    if hasattr(u_slice, 'metpy') and hasattr(v_slice, 'metpy'):
                        # Wind Speed
                        ws_derived = mpcalc.wind_speed(u_slice, v_slice)
                        if 'ws' in target_variables: 
                            final_ensemble_slices['ws'] = ws_derived.to(units('m/s')).magnitude
                            logger.debug(f"Updated 'ws' using derived value for timestep {t_idx+1}")

                        # Wind Direction
                        wdir_derived = mpcalc.wind_direction(u_slice, v_slice, convention='from')
                        if 'wdir' in target_variables: 
                            final_ensemble_slices['wdir'] = wdir_derived.to(units.degrees).magnitude
                            logger.debug(f"Updated 'wdir' using derived value for timestep {t_idx+1}")
                    else:
                         logger.warning(f"Skipping wind consistency: Input slices lack units for timestep {t_idx+1}")
                            
            except Exception as e:
                 logger.warning(f"MetPy wind consistency check failed for timestep {t_idx+1}: {e}")
                 logger.debug(traceback.format_exc())

        for var, data_slice in final_ensemble_slices.items():
            if var in ensemble_ds:
                try:
                    ensemble_ds[var].loc[dict(time=t_val)] = data_slice
                except Exception as assign_err:
                     logger.error(f"Failed final assign for var '{var}' at time {t_val}: {assign_err}")
                     logger.debug(f"  Slice Type: {type(data_slice)}, Slice Shape: {getattr(data_slice, 'shape', 'N/A')}")
                     logger.debug(f"  Target Var Dims: {ensemble_ds[var].dims}, Target Var Shape: {ensemble_ds[var].shape}")
                     logger.debug(f"  Target Slice Shape via .loc: {ensemble_ds[var].loc[dict(time=t_val)].shape}")
            else:
                 logger.warning(f"Variable '{var}' calculated but not found in ensemble_ds structure. Skipping assignment.")

        del raw_ensemble_slices, final_ensemble_slices
        gc.collect()

        timestep_duration = (datetime.now(timezone.utc) - timestep_start_time).total_seconds()
        processed_timesteps += 1
        logger.debug(f"Finished timestep {t_idx+1}/{num_timesteps} in {timestep_duration:.2f}s")


    end_time = datetime.now(timezone.utc)
    total_duration = (end_time - start_time).total_seconds()
    logger.info(f"Ensemble creation finished. Processed {processed_timesteps}/{num_timesteps} timesteps in {total_duration:.2f}s.")

    ensemble_ds.attrs['title'] = "Physics-Aware Weighted Ensemble Forecast"
    ensemble_ds.attrs['institution'] = "Distributed Gaia Network"
    ensemble_ds.attrs['history'] = f"Created {start_time.isoformat()} using create_physics_aware_ensemble."
    ensemble_ds.attrs['ensemble_creation_time'] = start_time.isoformat()
    ensemble_ds.attrs['ensemble_processing_duration_seconds'] = round(total_duration, 2)
    ensemble_ds.attrs['ensemble_method'] = "weighted_mean_xskillscore"
    ensemble_ds.attrs['top_k_applied'] = str(top_k) if top_k is not None else 'all_valid'
    ensemble_ds.attrs['number_of_members_used'] = len(selected_miner_ids)
    ensemble_ds.attrs['member_hotkeys'] = ",".join(selected_miner_ids)
    ensemble_ds.attrs['consistency_checks_applied'] = str(active_checks)

    return ensemble_ds 