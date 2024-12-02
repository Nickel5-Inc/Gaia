import torch
import torch.nn.functional as F
import numpy as np
import rasterio
from typing import Dict


class SoilMoistureInferencePreprocessor:
    def __init__(self, patch_size: int = 220):
        self.patch_size = patch_size

    def _normalize_channelwise(self, tensor: torch.Tensor) -> torch.Tensor:
        """Channel-wise min-max normalization"""
        if tensor.dim() == 2:
            tensor = tensor.unsqueeze(0)

        normalized = torch.zeros_like(tensor)
        for c in range(tensor.shape[0]):
            channel = tensor[c]
            channel_min = channel.min()
            channel_max = channel.max()
            normalized[c] = (channel - channel_min) / (channel_max - channel_min + 1e-8)

        return normalized

    def preprocess(self, tiff_path: str) -> Dict[str, torch.Tensor]:
        try:
            with rasterio.open(tiff_path) as src:
                sentinel_data = src.read([1, 2])  # B8, B4
                ifs_data = src.read(list(range(3, 20)))  # IFS variables
                elevation = src.read([20])  # SRTM
                ndvi = src.read([21])  # NDVI

                sentinel_ndvi = torch.from_numpy(sentinel_data).float()
                elevation = torch.from_numpy(elevation).float()
                ifs = torch.from_numpy(ifs_data).float()
                ndvi = torch.from_numpy(ndvi).float()
                elevation = self._handle_elevation_mask(elevation)

                sentinel_ndvi = self._normalize_channelwise(sentinel_ndvi)
                ifs = self._normalize_channelwise(ifs)
                elevation = self._normalize_channelwise(elevation)
                ndvi = self._normalize_channelwise(ndvi)

                sentinel_ndvi = self._pad_tensor(sentinel_ndvi)
                elevation = self._pad_tensor(elevation)
                ifs = self._pad_tensor(ifs)
                ndvi = self._pad_tensor(ndvi)

                sentinel_ndvi = torch.cat([sentinel_ndvi, ndvi], dim=0)

                return {
                    "sentinel_ndvi": sentinel_ndvi,
                    "elevation": elevation,
                    "era5": ifs,
                }

        except Exception as e:
            print(f"Error preprocessing file {tiff_path}: {str(e)}")
            return None

    def _handle_elevation_mask(self, elevation: torch.Tensor) -> torch.Tensor:
        """Handle elevation masking"""
        elevation_mask = torch.isnan(elevation)
        if elevation_mask.any():
            elevation_mean = torch.nanmean(elevation)
            elevation = torch.where(elevation_mask, elevation_mean, elevation)
        return elevation

    def _pad_tensor(self, tensor: torch.Tensor) -> torch.Tensor:
        """Pad tensor to target size"""
        current_size = tensor.shape[-2:]
        pad_h = self.patch_size - current_size[0]
        pad_w = self.patch_size - current_size[1]

        padding = (0, pad_w, 0, pad_h)

        return F.pad(tensor, padding, mode="constant", value=0)
