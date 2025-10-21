import os
from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask


def test_strict_time_level_config_defaults_loaded():
    # Access config via loader without constructing full WeatherTask
    cfg = WeatherTask._load_config(WeatherTask)
    assert cfg.get("era5_strict_time_tolerance_hours") in (0, 0.0)
    assert cfg.get("era5_strict_level_tolerance_hpa") in (0, 0.0)


def test_strict_metadata_env_flag_defaults_true(monkeypatch):
    from gaia.tasks.defined_tasks.weather.utils.hashing import VerifyingChunkMapper
    import fsspec

    monkeypatch.delenv("WEATHER_STRICT_ZARR_METADATA", raising=False)
    fs = fsspec.filesystem("memory")
    mapper = VerifyingChunkMapper(
        root="memory://test.zarr/",
        fs=fs,
        trusted_manifest={"files": {}},
        job_id_for_logging="test",
    )
    assert mapper.strict_metadata is True


