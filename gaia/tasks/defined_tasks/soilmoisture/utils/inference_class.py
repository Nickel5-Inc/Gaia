import torch
import torch.nn.functional as F
import numpy as np
import rasterio
from typing import Dict

class SoilMoistureInferencePreprocessor:
    def __init__(self, patch_size: int = 256):
        self.patch_size = patch_size

    def preprocess(self, tiff_path: str) -> Dict[str, torch.Tensor]:
        try:
            with rasterio.open(tiff_path) as src:
                sentinel_data = src.read([1, 2])  #sent2
                ifs_data = src.read(list(range(3, 21)))  #IFS 
                srtm = src.read([20])        #srtm
                ndvi = src.read([21])        #ndvi
                sentinel_mask = ~(np.isnan(sentinel_data).any(axis=0) | (sentinel_data == 0).all(axis=0))
                elevation_mask = ~(np.isnan(srtm) | (srtm == 0))
                ndvi_mask = ~(np.isnan(ndvi) | (ndvi == 0))
                ifs_mask = ~(np.isnan(ifs_data).any(axis=0))
                combined_mask = sentinel_mask & elevation_mask.squeeze() & ndvi_mask.squeeze() & ifs_mask
                sentinel_ndvi = np.concatenate([sentinel_data, ndvi], axis=0)
                elevation = srtm
                ifs_data = np.nan_to_num(ifs_data, nan=0.0, posinf=1e6, neginf=-1e6)
                sentinel_ndvi = torch.from_numpy(sentinel_ndvi).float()
                elevation = torch.from_numpy(elevation).float()
                ifs = torch.from_numpy(ifs_data).float()
                mask = torch.from_numpy(combined_mask).bool()

                if torch.isnan(elevation).any():
                    elevation_mean = torch.nanmean(elevation)
                    elevation = torch.where(torch.isnan(elevation), elevation_mean, elevation)

                def masked_normalize(x, mask):
                    valid_data = x.reshape(x.shape[0], -1)[:, mask.reshape(-1)]
                    mins = valid_data.min(dim=1, keepdim=True)[0]
                    maxs = valid_data.max(dim=1, keepdim=True)[0]
                    return (x - mins.reshape(-1, 1, 1)) / (maxs - mins + 1e-8).reshape(-1, 1, 1)

                sentinel_ndvi = masked_normalize(sentinel_ndvi, mask)
                ifs = masked_normalize(ifs, mask)
                elevation = masked_normalize(elevation, mask)
                target_size = (self.patch_size, self.patch_size)
                sentinel_ndvi = F.pad(sentinel_ndvi, (0,target_size[1] - sentinel_ndvi.shape[2], 0, target_size[0] - sentinel_ndvi.shape[1]))
                elevation = F.pad(elevation, (0, target_size[1] - elevation.shape[2], 0, target_size[0] - elevation.shape[1]))
                ifs = F.pad(ifs, (0, target_size[1] - ifs.shape[2], 0, target_size[0] - ifs.shape[1]))
                mask = F.pad(mask.unsqueeze(0), (0, target_size[1] - mask.shape[1], 0, target_size[0] - mask.shape[0])).squeeze(0)

                return {
                    'sentinel_ndvi': sentinel_ndvi,
                    'elevation': elevation,
                    'era5': ifs, 
                    'mask': mask
                }

        except Exception as e:
            print(f"Error preprocessing file {tiff_path}: {str(e)}")
            return None