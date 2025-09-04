import asyncio
import os
from typing import Any, Dict, Optional

import httpx

from gaia.utils.custom_logger import get_logger

logger = get_logger(__name__)


class WebApiClient:
    def __init__(self, base_url: str = "https://dev-gaia-api.azurewebsites.net", client: Optional[httpx.AsyncClient] = None):
        self.base_url = base_url.rstrip("/")
        if client is not None:
            self.client = client
            self._should_close = False
        else:
            self.client = httpx.AsyncClient(
                timeout=30.0,
                limits=httpx.Limits(max_connections=100, max_keepalive_connections=20, keepalive_expiry=30),
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "User-Agent": "GaiaValidator-WebSync/1.0",
                },
            )
            self._should_close = True

        self._semaphore = asyncio.Semaphore(16)
        self._retry_delays = [1, 2, 4, 8, 16]
        try:
            self._strict_api = os.getenv("WEBSYNC_STRICT_API", "false").lower() == "true"
        except Exception:
            self._strict_api = False

    @staticmethod
    def _prune_predictions_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
        allowed_top = {"minerHotKey", "minerColdKey", "geomagneticPredictions", "soilMoisturePredictions"}
        return {k: payload.get(k) for k in allowed_top if k in payload}

    async def aclose(self) -> None:
        if self._should_close:
            await self.client.aclose()

    async def post_predictions(self, payload: Dict[str, Any]) -> None:
        url = f"{self.base_url}/Predictions"
        if self._strict_api:
            payload = self._prune_predictions_payload(payload)
        async with self._semaphore:
            for attempt, delay in enumerate(self._retry_delays):
                try:
                    resp = await self.client.post(url, json=payload)
                    if resp.is_success:
                        logger.debug("WebSync: posted /Predictions successfully")
                        return
                    if resp.status_code == 429 and attempt < len(self._retry_delays) - 1:
                        logger.warning(f"WebSync: rate limited, retrying in {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    body = None
                    try:
                        body = resp.json()
                    except Exception:
                        body = resp.text
                    logger.error(f"WebSync: POST /Predictions failed {resp.status_code}: {body}")
                    if attempt < len(self._retry_delays) - 1:
                        await asyncio.sleep(delay)
                        continue
                    break
                except httpx.RequestError as e:
                    logger.warning(f"WebSync: request error {e}")
                    if attempt < len(self._retry_delays) - 1:
                        await asyncio.sleep(delay)
                        continue
                    raise


