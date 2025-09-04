from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

from gaia.utils.custom_logger import get_logger

from .cache import DeltaCache
from .http_client import WebApiClient
from .mappers import build_predictions_payload, map_recent_runs_to_api, map_weather_stats_to_api

logger = get_logger(__name__)


class WebSyncWorker:
    def __init__(self, database_manager, api_client: Optional[WebApiClient] = None, poll_interval_seconds: int = 60) -> None:
        self.db = database_manager
        self.api = api_client or WebApiClient()
        self.cache = DeltaCache()
        self.poll_interval_seconds = poll_interval_seconds
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        logger.info("WebSyncWorker: starting")
        try:
            while not self._stop_event.is_set():
                await self._tick_once()
                await asyncio.sleep(self.poll_interval_seconds)
        finally:
            await self.api.aclose()
            logger.info("WebSyncWorker: stopped")

    async def stop(self) -> None:
        self._stop_event.set()

    async def _tick_once(self) -> None:
        try:
            # Fetch miners snapshot
            miners = await self._fetch_miners()
            if not miners:
                logger.debug("WebSyncWorker: no miners found")
                await self.cache.mark_flushed()
                return

            # Compute deltas per miner based on weather_forecast_stats.updated_at high-water mark
            hwm_key = "weather_forecast_stats.updated_at"
            last_hwm = await self.cache.get_hwm(hwm_key)

            # Pull recent stats updates; if no HWM, get last 5 per miner as warm start
            recent_rows_by_miner = await self._fetch_recent_forecast_stats_since(last_hwm)

            # For each miner, assemble payloads; batch sends with concurrency limits
            send_tasks: List[asyncio.Task] = []
            for miner in miners:
                miner_uid = miner["uid"]
                miner_rows = recent_rows_by_miner.get(miner_uid, [])
                if not miner_rows:
                    continue

                stats_row = await self._fetch_miner_aggregate_stats(miner_uid)
                payload = build_predictions_payload(
                    miner=miner,
                    weather_stats=map_weather_stats_to_api(stats_row),
                    recent_runs=map_recent_runs_to_api(miner_rows),
                )
                send_tasks.append(asyncio.create_task(self.api.post_predictions(payload)))

            if send_tasks:
                results = await asyncio.gather(*send_tasks, return_exceptions=True)
                failures = sum(1 for r in results if isinstance(r, Exception))
                if failures:
                    logger.warning(f"WebSyncWorker: {failures} send failures in batch")

            # Advance HWM to max updated_at seen
            max_updated_at = None
            for rows in recent_rows_by_miner.values():
                for r in rows:
                    ts = r.get("updated_at")
                    if ts is not None and (max_updated_at is None or ts > max_updated_at):
                        max_updated_at = ts
            if max_updated_at is not None:
                await self.cache.set_hwm(hwm_key, max_updated_at)
            await self.cache.mark_flushed()
        except Exception as e:
            logger.error(f"WebSyncWorker: tick error: {e}")

    async def _fetch_miners(self) -> List[Dict[str, Any]]:
        query = "SELECT uid, hotkey, coldkey FROM node_table WHERE hotkey IS NOT NULL"
        rows = await self.db.fetch_all(query)
        return [{"uid": r["uid"], "hotkey": r["hotkey"], "coldkey": r.get("coldkey") if isinstance(r, dict) else r["coldkey"]} for r in rows]

    async def _fetch_miner_aggregate_stats(self, miner_uid: int) -> Optional[Dict[str, Any]]:
        query = (
            "SELECT miner_rank, avg_forecast_score, successful_forecasts, failed_forecasts, "
            "forecast_success_ratio, host_reliability_ratio, avg_day1_score, avg_era5_score, "
            "avg_era5_completeness, consecutive_successes, consecutive_failures "
            "FROM miner_stats WHERE miner_uid = :uid"
        )
        return await self.db.fetch_one(query, {"uid": miner_uid})

    async def _fetch_recent_forecast_stats_since(self, since_ts) -> Dict[int, List[Dict[str, Any]]]:
        # Group by miner_uid and take up to N recent rows per miner since HWM
        if since_ts is None:
            query = (
                "SELECT miner_uid, forecast_run_id, forecast_init_time, forecast_status, forecast_score_initial, "
                "era5_combined_score, era5_completeness, hosting_status, hosting_latency_ms, updated_at "
                "FROM weather_forecast_stats "
                "WHERE updated_at > NOW() - INTERVAL '2 hours' "
                "ORDER BY miner_uid, updated_at DESC"
            )
            rows = await self.db.fetch_all(query)
        else:
            query = (
                "SELECT miner_uid, forecast_run_id, forecast_init_time, forecast_status, forecast_score_initial, "
                "era5_combined_score, era5_completeness, hosting_status, hosting_latency_ms, updated_at "
                "FROM weather_forecast_stats "
                "WHERE updated_at > :since "
                "ORDER BY miner_uid, updated_at DESC"
            )
            rows = await self.db.fetch_all(query, {"since": since_ts})

        by_miner: Dict[int, List[Dict[str, Any]]] = {}
        for r in rows:
            uid = r["miner_uid"]
            by_miner.setdefault(uid, [])
            if len(by_miner[uid]) < 5:
                by_miner[uid].append(r)
        return by_miner


async def start_websync_worker(database_manager, poll_interval_seconds: int = 60) -> WebSyncWorker:
    worker = WebSyncWorker(database_manager=database_manager, poll_interval_seconds=poll_interval_seconds)
    asyncio.create_task(worker.run())
    return worker


