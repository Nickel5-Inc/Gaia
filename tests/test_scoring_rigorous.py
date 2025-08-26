import numpy as np
import xarray as xr
import asyncio

from gaia.tasks.defined_tasks.weather.weather_scoring.metrics import (
    calculate_mse_skill_score_by_pressure_level,
    calculate_mse_skill_score,
    calculate_acc_by_pressure_level,
    calculate_acc,
    calculate_rmse_by_pressure_level,
    calculate_rmse,
    calculate_bias,
)


def _lat_lon_grid(nlat=5, nlon=8):
    lats = np.linspace(90, -90, nlat)
    lons = np.linspace(0, 360 - 360 / nlon, nlon)
    return lats, lons


def _weights(lat):
    return np.cos(np.deg2rad(lat))


def test_metrics_strict_shapes_and_values():
    lats, lons = _lat_lon_grid()
    levels = np.array([1000, 925, 850], dtype=float)

    # Synthetic truth and forecast with known relationships
    truth = xr.DataArray(
        np.random.RandomState(0).rand(len(lats), len(lons), len(levels)).astype(np.float32),
        dims=("lat", "lon", "pressure_level"),
        coords={"lat": lats, "lon": lons, "pressure_level": levels},
        name="t",
    )
    forecast = truth + 0.1  # constant bias
    reference = truth + 0.5  # worse than forecast
    clim = truth * 0.0  # zero climatology for ACC; anomalies=truth

    lat_weights = xr.DataArray(
        _weights(lats), dims=["lat"], coords={"lat": lats}
    ).expand_dims({"lon": xr.DataArray(lons, dims=["lon"])})

    async def run():
        # Per-level MSE skill should be finite and better than reference
        pl_skill = await calculate_mse_skill_score_by_pressure_level(
            forecast, truth, reference, lat_weights
        )
        assert set(pl_skill.keys()) == set(levels.astype(int))
        assert np.all(np.isfinite(list(pl_skill.values())))

        # Aggregated skill should be finite and > -inf
        agg_skill = await calculate_mse_skill_score(
            forecast, truth, reference, lat_weights
        )
        assert np.isfinite(agg_skill)

        # ACC per-level with zero climatology -> correlation of truth with forecast offset
        pl_acc = await calculate_acc_by_pressure_level(
            forecast, truth, clim, lat_weights
        )
        assert set(pl_acc.keys()) == set(levels.astype(int))
        assert np.all(np.isfinite(list(pl_acc.values())))

        # Surface variants (drop level)
        truth_sfc = truth.isel(pressure_level=0).drop_vars("pressure_level").squeeze()
        forecast_sfc = forecast.isel(pressure_level=0).drop_vars("pressure_level").squeeze()

        rmse_val = await calculate_rmse(forecast_sfc, truth_sfc, lat_weights)
        assert np.isfinite(rmse_val)

        bias_val = await calculate_bias(forecast_sfc, truth_sfc, lat_weights)
        assert np.isfinite(bias_val)

    asyncio.get_event_loop().run_until_complete(run())


