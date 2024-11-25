import os
from typing import Optional, Dict, List, Tuple
from datetime import datetime
import torch
import pytorch_lightning as pl
from torch.utils.data import DataLoader, Dataset, random_split
import rasterio
import glob
import numpy as np
from torch.nn.utils.rnn import pad_sequence
import torch.nn.functional as F
import random


class SoilMoistureDataset(Dataset):
    def __init__(
        self,
        monthly_root: str,
        three_day_root: str,
        transforms: Optional[callable] = None,
        normalization_params: Optional[Dict] = None,
        patch_size: int = 256,
    ):
        self.monthly_root = monthly_root
        self.three_day_root = three_day_root
        self.transforms = transforms
        self.normalization_params = normalization_params
        self.patch_size = patch_size
        self.samples = self._align_data()
        self.samples = self._filter_invalid_files(self.samples)

    def _align_data(self) -> List[Dict]:
        aligned_samples = []
        regions = [
            d
            for d in os.listdir(self.monthly_root)
            if os.path.isdir(os.path.join(self.monthly_root, d))
        ]

        for region in regions:
            monthly_files = sorted(
                glob.glob(os.path.join(self.monthly_root, region, "*.tif"))
            )
            three_day_files = sorted(
                glob.glob(os.path.join(self.three_day_root, region, "*.tif"))
            )

            for i in range(len(three_day_files) - 1):
                current_file = three_day_files[i]
                future_file = three_day_files[i + 1]

                current_date = datetime.strptime(
                    os.path.basename(current_file).split("_")[-1].split(".")[0],
                    "%Y-%m-%d",
                )
                future_date = datetime.strptime(
                    os.path.basename(future_file).split("_")[-1].split(".")[0],
                    "%Y-%m-%d",
                )

                if (future_date - current_date).days != 3:
                    continue

                monthly_date = current_date.replace(day=1)
                monthly_file = next(
                    (f for f in monthly_files if monthly_date.strftime("%Y-%m") in f),
                    None,
                )

                if monthly_file:
                    aligned_samples.append(
                        {
                            "monthly": monthly_file,
                            "current_three_day": current_file,
                            "future_three_day": future_file,
                            "date": current_date,
                            "region": region,
                        }
                    )
        return aligned_samples

    def __getitem__(self, index: int) -> Dict[str, torch.Tensor]:
        sample = self.samples[index]

        try:
            monthly_file = sample["monthly"]
            with rasterio.open(monthly_file) as src:
                monthly_data = src.read([1, 2, 25, 24])  # B8, B4, NDVI, Elevation

            current_three_day_file = sample["current_three_day"]
            with rasterio.open(current_three_day_file) as src:
                era5_data = src.read()[2:]
                era5_data = np.nan_to_num(era5_data, nan=0.0, posinf=1e6, neginf=-1e6)

            future_three_day_file = sample["future_three_day"]
            with rasterio.open(future_three_day_file) as src:
                future_smap = src.read([1, 2])

            sentinel_ndvi = monthly_data[:3]  # B8, B4, NDVI
            elevation = monthly_data[3:4]

            sentinel_ndvi = torch.from_numpy(sentinel_ndvi).float()
            elevation = torch.from_numpy(elevation).float()
            era5 = torch.from_numpy(era5_data).float()
            future_smap = torch.from_numpy(future_smap).float()

            elevation_mask = torch.isnan(elevation)
            if elevation_mask.any():
                elevation_mean = torch.nanmean(elevation)
                elevation = torch.where(elevation_mask, elevation_mean, elevation)

            sentinel_ndvi = (sentinel_ndvi - sentinel_ndvi.min()) / (
                sentinel_ndvi.max() - sentinel_ndvi.min() + 1e-8
            )
            era5 = (era5 - era5.min()) / (era5.max() - era5.min() + 1e-8)
            elevation = (elevation - elevation.min()) / (
                elevation.max() - elevation.min() + 1e-8
            )

            target_size = (self.patch_size, self.patch_size)
            sentinel_ndvi = F.pad(
                sentinel_ndvi,
                (
                    0,
                    target_size[1] - sentinel_ndvi.shape[2],
                    0,
                    target_size[0] - sentinel_ndvi.shape[1],
                ),
            )
            elevation = F.pad(
                elevation,
                (
                    0,
                    target_size[1] - elevation.shape[2],
                    0,
                    target_size[0] - elevation.shape[1],
                ),
            )
            era5 = F.pad(
                era5,
                (0, target_size[1] - era5.shape[2], 0, target_size[0] - era5.shape[1]),
            )
            future_smap = F.pad(
                future_smap,
                (
                    0,
                    target_size[1] - future_smap.shape[2],
                    0,
                    target_size[0] - future_smap.shape[1],
                ),
            )

            mask = (future_smap != 0).any(dim=0)

            return {
                "sentinel_ndvi": sentinel_ndvi,
                "elevation": elevation,
                "era5": era5,
                "future_smap": future_smap,
                "mask": mask,
                "region": sample["region"],
                "date": sample["date"],
            }

        except rasterio.errors.RasterioIOError as e:
            print(
                f"Error loading sample {index} from region {sample['region']}, date {sample['date']}"
            )
            print(f"Monthly file: {monthly_file}")
            print(f"Current three-day file: {current_three_day_file}")
            print(f"Future three-day file: {future_three_day_file}")
            print(f"Error details: {str(e)}")
            return None
        except Exception as e:
            print(f"Error loading sample {index}: {str(e)}")
            return None

    def __len__(self) -> int:
        return len(self.samples)

    def set_normalization_params(self, params):
        self.normalization_params = params


def collate_fn(batch):
    batch = [item for item in batch if item is not None]

    sentinel_ndvi = torch.stack([item["sentinel_ndvi"] for item in batch])
    elevation = torch.stack([item["elevation"] for item in batch])
    era5 = torch.stack([item["era5"] for item in batch])
    future_smap = torch.stack([item["future_smap"] for item in batch])
    dates = [item["date"] for item in batch]
    regions = [item["region"] for item in batch]
    masks = torch.stack([item["mask"] for item in batch])

    return {
        "sentinel_ndvi": sentinel_ndvi,
        "elevation": elevation,
        "era5": era5,
        "future_smap": future_smap,
        "date": dates,
        "region": regions,
        "mask": masks,
    }


class SoilMoistureDataModule(pl.LightningDataModule):
    def __init__(
        self,
        monthly_root: str,
        three_day_root: str,
        batch_size: int = 32,
        num_workers: int = 4,
        patch_size: int = 256,
        train_val_test_split: Tuple[float, float, float] = (0.7, 0.15, 0.15),
        seed: int = 33,
    ):
        super().__init__()
        self.save_hyperparameters()
        self.monthly_root = monthly_root
        self.three_day_root = three_day_root
        self.batch_size = batch_size
        self.num_workers = num_workers
        self.patch_size = patch_size
        self.train_val_test_split = train_val_test_split
        self.seed = seed

    def setup(self, stage: Optional[str] = None):
        if not hasattr(self, "full_dataset"):
            self.full_dataset = SoilMoistureDataset(
                self.monthly_root, self.three_day_root, patch_size=self.patch_size
            )

        samples_by_region = {}
        for idx, sample in enumerate(self.full_dataset.samples):
            region = sample["region"]
            if region not in samples_by_region:
                samples_by_region[region] = []
            samples_by_region[region].append((idx, sample["date"]))

        for region in samples_by_region:
            samples_by_region[region].sort(key=lambda x: x[1])

        regions = list(samples_by_region.keys())
        random.Random(self.seed).shuffle(regions)
        n_regions = len(regions)
        n_train = int(n_regions * self.train_val_test_split[0])
        n_val = int(n_regions * self.train_val_test_split[1])

        train_regions = regions[:n_train]
        val_regions = regions[n_train : n_train + n_val]
        test_regions = regions[n_train + n_val :]
        train_indices = [
            idx for region in train_regions for idx, _ in samples_by_region[region]
        ]
        val_indices = [
            idx for region in val_regions for idx, _ in samples_by_region[region]
        ]
        test_indices = [
            idx for region in test_regions for idx, _ in samples_by_region[region]
        ]

        self.train_dataset = torch.utils.data.Subset(self.full_dataset, train_indices)
        self.val_dataset = torch.utils.data.Subset(self.full_dataset, val_indices)
        self.test_dataset = torch.utils.data.Subset(self.full_dataset, test_indices)

    def train_dataloader(self):
        return DataLoader(
            self.train_dataset,
            batch_size=self.batch_size,
            num_workers=self.num_workers,
            shuffle=True,
            pin_memory=True,
            collate_fn=collate_fn,
        )

    def val_dataloader(self):
        return DataLoader(
            self.val_dataset,
            batch_size=self.batch_size,
            num_workers=self.num_workers,
            shuffle=False,
            pin_memory=True,
            collate_fn=collate_fn,
        )

    def test_dataloader(self):
        return DataLoader(
            self.test_dataset,
            batch_size=self.batch_size,
            num_workers=self.num_workers,
            shuffle=False,
            pin_memory=True,
            collate_fn=collate_fn,
        )

    def print_dataset_regions(self):
        def get_regions(dataset):
            return set(
                [self.full_dataset.samples[idx]["region"] for idx in dataset.indices]
            )

        train_regions = get_regions(self.train_dataset)
        val_regions = get_regions(self.val_dataset)
        test_regions = get_regions(self.test_dataset)
        print("Train regions:", train_regions)
        print("Validation regions:", val_regions)
        print("Test regions:", test_regions)
        print("Overlap between train and val:", train_regions.intersection(val_regions))
        print(
            "Overlap between train and test:", train_regions.intersection(test_regions)
        )
        print("Overlap between val and test:", val_regions.intersection(test_regions))
