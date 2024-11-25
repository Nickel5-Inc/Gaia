import torch.nn as nn
import torch
import torch.nn.functional as F
import pytorch_lightning as pl


# TODO remove unused era5 bands, shift to ifs bands for loss/attention/consistency
# TODO 6hr/3hr prediction window
# TODO better handling of resolution
# TODO clean up method defns
# TODO clean unused args in methods
# TODO add HG package for loading
def spatial_gradient(x):
    """Calculate spatial gradients"""
    dy = x[:, :, 1:] - x[:, :, :-1]
    dx = x[:, :, :, 1:] - x[:, :, :, :-1]
    return torch.mean(torch.abs(dx)) + torch.mean(torch.abs(dy))


class ResidualBlock(nn.Module):
    def __init__(self, channels):
        super().__init__()
        self.conv1 = nn.Conv2d(channels, channels, kernel_size=3, padding=1)
        self.bn1 = nn.BatchNorm2d(channels)
        self.conv2 = nn.Conv2d(channels, channels, kernel_size=3, padding=1)
        self.bn2 = nn.BatchNorm2d(channels)

    def forward(self, x):
        residual = x
        out = F.relu(self.bn1(self.conv1(x)))
        out = self.bn2(self.conv2(out))
        out += residual
        return F.relu(out)


class PhysicalConsistencyLoss(nn.Module):
    """
    Enforces physical relationships between soil moisture and meteorological variables.
    Includes temporal consistency constraints based on physical limits of water movement.
    Based on recent soil moisture literature.
    """

    def __init__(self, max_hourly_change=0.05):
        super().__init__()
        self.max_hourly_change = max_hourly_change

    def forward(self, predictions, era5_features, era5_raw, prev_predictions=None):
        """
        Args:
            predictions: Current soil moisture predictions [B, 1, H, W]
            era5_features: Processed ERA5 features
            era5_raw: Raw ERA5 variables [B, C, H, W]
            prev_predictions: Previous timestep predictions (optional) [B, 1, H, W]
        """
        if era5_raw.shape[-1] != predictions.shape[-1]:
            era5_raw = F.interpolate(era5_raw, size=predictions.shape[-2:], mode="area")

        precip = era5_raw[:, 0:1]
        temp = era5_raw[:, 1:2]
        evap = era5_raw[:, 2:3]

        precip_loss = F.relu(-(predictions * F.relu(precip))).mean()
        temp_loss = F.relu(predictions * F.relu(temp - 298.15)).mean()
        evap_loss = F.relu(predictions * F.relu(evap)).mean()
        bound_loss = F.relu(predictions - 0.5).mean() + F.relu(-predictions).mean()
        physical_loss = (
            bound_loss + 0.1 * precip_loss + 0.1 * temp_loss + 0.1 * evap_loss
        )

        if prev_predictions is not None:
            temporal_loss = F.relu(
                torch.abs(predictions - prev_predictions) - self.max_hourly_change
            ).mean()
            physical_loss = physical_loss + 0.1 * temporal_loss

        return physical_loss


class SMAPStructureLoss(nn.Module):
    """
    Enforces SMAP spatial structure in predictions.
    """

    def __init__(self, smap_size=11):
        super().__init__()
        self.smap_size = smap_size

    def forward(self, predictions):
        dx = predictions[:, :, :, 1:] - predictions[:, :, :, :-1]
        dy = predictions[:, :, 1:, :] - predictions[:, :, :-1, :]
        gradient_loss = torch.mean(dx.pow(2)) + torch.mean(dy.pow(2))

        return gradient_loss


class UncertaintyHead(nn.Module):
    def __init__(self, num_era5_features, num_sentinel_features):
        super().__init__()
        total_channels = num_era5_features + num_sentinel_features + 2

        self.channel_reduction = nn.Sequential(
            nn.Conv2d(64, total_channels, kernel_size=1),
            nn.BatchNorm2d(total_channels),
            nn.ReLU(),
        )

        self.to_smap_resolution = nn.Sequential(
            nn.AdaptiveAvgPool2d((11, 11)),
            nn.Conv2d(total_channels, total_channels, kernel_size=1),
        )

        self.uncertainty_processor = nn.Sequential(
            nn.Conv2d(total_channels, total_channels, kernel_size=3, padding=1),
            nn.BatchNorm2d(total_channels),
            nn.ReLU(),
            ResidualBlock(total_channels),
            nn.Conv2d(total_channels, 32, kernel_size=3, padding=1),
            nn.BatchNorm2d(32),
            nn.ReLU(),
            nn.Conv2d(32, 2, kernel_size=1),
            nn.Sigmoid(),
        )

    def forward(self, x):
        x = self.channel_reduction(x)
        x = self.to_smap_resolution(x)
        uncertainty = self.uncertainty_processor(x)
        return uncertainty


class MeteorologicalAttention(nn.Module):
    """
    Attention mechanism specifically for meteorological (ERA5) features.
    Maintains SMAP resolution throughout processing.
    """

    def __init__(self, num_era5_features):
        super().__init__()
        self.feature_attention = nn.Sequential(
            nn.Conv2d(num_era5_features, 64, 1),
            nn.BatchNorm2d(64),
            nn.ReLU(),
            nn.Conv2d(64, num_era5_features, 1),
            nn.Sigmoid(),
        )

        self.physical_relationships = nn.Sequential(
            nn.Conv2d(num_era5_features, num_era5_features, 3, padding=1),
            nn.BatchNorm2d(num_era5_features),
            nn.ReLU(),
            nn.Conv2d(num_era5_features, num_era5_features, 1),
            nn.Sigmoid(),
        )

    def forward(self, era5_features):
        """
        Args:
            era5_features: ERA5 data at SMAP resolution [B, C, H, W]
        """
        attention_weights = self.feature_attention(era5_features)
        physical_weights = self.physical_relationships(era5_features)

        combined_attention = attention_weights * physical_weights

        return era5_features * combined_attention


def meteorological_consistency_loss(predictions, era5, indices, downsample):
    total_loss = 0.0
    scales = [220, 110, 55, 22, 11]

    for scale in scales:
        pred_at_scale = F.interpolate(predictions, size=(scale, scale), mode="area")
        era5_at_scale = {}

        for key, idx in indices.items():
            if key in [
                "tp",
                "snowfall",
                "snowmelt",
                "surface_runoff",
                "subsurface_runoff",
            ]:
                era5_at_scale[key] = (
                    F.interpolate(
                        era5[:, idx : idx + 1], size=(scale, scale), mode="area"
                    )
                    * (220 * 220)
                    / (scale * scale)
                )
            else:
                era5_at_scale[key] = F.interpolate(
                    era5[:, idx : idx + 1], size=(scale, scale), mode="area"
                )

        scale_loss = 0.0
        scale_loss += F.relu(
            pred_at_scale * F.relu(era5_at_scale["t2m"] - 298.15)
        ).mean()
        scale_loss += F.relu(
            pred_at_scale * F.relu(era5_at_scale["soil_temp_1"] - 298.15)
        ).mean()
        scale_loss += F.relu(-(pred_at_scale * F.relu(era5_at_scale["tp"]))).mean()
        scale_loss += F.relu(
            -(pred_at_scale * F.relu(era5_at_scale["snowmelt"]))
        ).mean()
        scale_loss += F.relu(pred_at_scale * F.relu(era5_at_scale["total_evap"])).mean()
        scale_loss += F.relu(
            pred_at_scale * F.relu(era5_at_scale["bare_soil_evap"])
        ).mean()

        scale_weight = 1.0 if scale == 11 else 0.2
        total_loss += scale_weight * scale_loss

    return total_loss


class TemporalPredictionBlock(nn.Module):
    """Current weather/sent2 conditions to predict soil moisture"""

    def __init__(self, era5_channels, sentinel_channels):
        super().__init__()

        self.era5_processor = nn.Sequential(
            nn.Conv2d(era5_channels, era5_channels, 3, padding=1),
            nn.BatchNorm2d(era5_channels),
            nn.ReLU(),
            nn.Conv2d(era5_channels, era5_channels, 1),
            nn.ReLU(),
        )
        self.sentinel_processor = nn.Sequential(
            nn.Conv2d(sentinel_channels, sentinel_channels, 3, padding=1),
            nn.BatchNorm2d(sentinel_channels),
            nn.ReLU(),
            nn.Conv2d(sentinel_channels, sentinel_channels, 1),
            nn.ReLU(),
        )
        self.future_attention = nn.Sequential(
            nn.Conv2d(era5_channels + sentinel_channels, 32, 3, padding=1),
            nn.ReLU(),
            nn.Conv2d(32, 1, 1),
            nn.Sigmoid(),
        )

    def forward(self, era5_current, sentinel_current):
        """
        Args:
            era5_current: Current ERA5 data [B, C, H, W]
            sentinel_current: Current Sentinel-2 data [B, C, H, W]
        """
        era5_features = self.era5_processor(era5_current)
        sentinel_features = self.sentinel_processor(sentinel_current)
        combined = torch.cat([era5_features, sentinel_features], dim=1)
        attention_weights = self.future_attention(combined)
        era5_features = era5_features * attention_weights
        sentinel_features = sentinel_features * attention_weights

        return era5_features, sentinel_features


class MultiTemporalFusion(nn.Module):
    def __init__(self, era5_channels=20, sentinel_channels=2, static_channels=2):
        super().__init__()
        self.era5_processor = nn.Sequential(
            nn.Conv2d(era5_channels, era5_channels, 3, padding=1),
            nn.BatchNorm2d(era5_channels),
            nn.ReLU(),
            MeteorologicalAttention(era5_channels),
        )
        self.sentinel_processor = nn.Sequential(
            nn.Conv2d(sentinel_channels, sentinel_channels, 3, padding=1),
            nn.BatchNorm2d(sentinel_channels),
            nn.ReLU(),
        )
        self.static_processor = nn.Sequential(
            nn.Conv2d(static_channels, static_channels, 3, padding=1),
            nn.BatchNorm2d(static_channels),
            nn.ReLU(),
        )
        self.scale_attention = nn.Sequential(
            nn.Conv2d(
                era5_channels + sentinel_channels + static_channels, 32, 3, padding=1
            ),
            nn.ReLU(),
            nn.Conv2d(32, 3, 1),
            nn.Sigmoid(),
        )
        self.downsample = nn.AvgPool2d(kernel_size=9)

    def forward(self, era5, sentinel, static_features):
        if era5.shape[-1] > 24:
            era5 = self.downsample(era5)
        if sentinel.shape[-1] > 24:
            sentinel = self.downsample(sentinel)
        if static_features.shape[-1] > 24:
            static_features = self.downsample(static_features)

        era5_features = self.era5_processor(era5)
        sentinel_features = self.sentinel_processor(sentinel)
        static_features = self.static_processor(static_features)
        combined = torch.cat([era5_features, sentinel_features, static_features], dim=1)

        scale_weights = self.scale_attention(combined)
        era5_weighted = era5_features * scale_weights[:, 0:1]
        sentinel_weighted = sentinel_features * scale_weights[:, 1:2]
        static_weighted = static_features * scale_weights[:, 2:3]

        return era5_weighted, sentinel_weighted, static_weighted


class CrossResolutionAttention(nn.Module):
    def __init__(self, channels):
        super().__init__()
        self.scale_attention = nn.Sequential(
            nn.Conv2d(channels * 4, channels, 1),
            nn.BatchNorm2d(channels),
            nn.ReLU(),
            nn.Conv2d(channels, 4, 1),
            nn.Softmax(dim=1),
        )

    def forward(self, smap_features, hr_features, mid_features, lr_features):
        smap_up = F.interpolate(
            smap_features, size=hr_features.shape[-2:], mode="bilinear"
        )
        mid_up = F.interpolate(
            mid_features, size=hr_features.shape[-2:], mode="bilinear"
        )
        lr_up = F.interpolate(lr_features, size=hr_features.shape[-2:], mode="bilinear")

        combined = torch.cat([smap_up, hr_features, mid_up, lr_up], dim=1)
        weights = self.scale_attention(combined)

        output = (
            weights[:, 0:1] * smap_up
            + weights[:, 1:2] * hr_features
            + weights[:, 2:3] * mid_up
            + weights[:, 3:4] * lr_up
        )

        return output


class ResolutionAwareProcessing(nn.Module):
    """
    Processing of features at different resolutions:
    - Sentinel-2: 220x220 (500m resolution)
    - SMAP: 11x11 (9km resolution covering same region)
    - ERA5: Same as SMAP
    """

    def __init__(self, input_size=220, smap_size=11):
        super().__init__()
        self.input_size = input_size
        self.smap_size = smap_size
        self.pool_size = input_size // smap_size
        self.to_smap_res = nn.AvgPool2d(
            kernel_size=self.pool_size,
            stride=self.pool_size,
            padding=self.pool_size // 2,
        )

        self.scale_aware_conv = nn.Sequential(
            nn.Conv2d(64, 64, kernel_size=3, padding=1, groups=64),
            nn.BatchNorm2d(64),
            nn.ReLU(),
        )

        self.downsampling_modes = {
            "soil_moisture": "area",
            "precipitation": "sum",
            "temperature": "mean",
            "radiation": "mean",
            "vegetation": "max",
        }

    def to_smap_resolution(self, x, mode="mean", scale=1):
        """
        Convert high-resolution input to SMAP resolution
        Args:
            x: Input tensor [B, C, H, W]
            mode: 'mean' for soil moisture, 'max' for precipitation
            scale: Scaling factor for intermediate resolutions (1 for SMAP, 2 for half-res, etc.)
        """
        if scale > 1:
            pool_size = self.pool_size // scale
            if mode == "mean":
                return nn.AvgPool2d(
                    kernel_size=pool_size, stride=pool_size, padding=pool_size // 2
                )(x)
            elif mode == "max":
                return F.max_pool2d(x, kernel_size=pool_size, stride=pool_size)
        else:
            if mode == "mean":
                return self.to_smap_res(x)
            elif mode == "max":
                return F.max_pool2d(
                    x, kernel_size=self.pool_size, stride=self.pool_size
                )

    def ensure_smap_structure(self, x):
        """Ensure output maintains SMAP pixel structure"""
        if x.shape[-1] != self.smap_size:
            x = F.interpolate(x, size=(self.smap_size, self.smap_size), mode="area")
        return x


class ParallelFeatureProcessing(nn.Module):
    """
    Parallel processing streams for different resolution inputs.
    Based on recent soil moisture literature and resolution-aware processing.
    """

    def __init__(
        self,
        num_era5_features=20,
        num_sentinel_features=3,
        num_static_features=2,
        smap_size=11,
    ):
        super().__init__()
        self.resolution_handler = ResolutionAwareProcessing(
            input_size=220, smap_size=11
        )

        self.intermediate_scales = [220, 110, 55, 22, 11]

        self.scale_processors = nn.ModuleDict(
            {f"scale_{scale}": ResidualBlock(64) for scale in self.intermediate_scales}
        )
        self.meteo_attention = MeteorologicalAttention(num_era5_features)
        self.era5_processor = nn.Sequential(
            nn.Conv2d(num_era5_features, 64, 3, padding=1),
            nn.BatchNorm2d(64),
            nn.ReLU(),
            ResidualBlock(64),
        )
        self.sentinel_processor = nn.Sequential(
            nn.Conv2d(num_sentinel_features, 32, 3, padding=1),
            nn.BatchNorm2d(32),
            nn.ReLU(),
            ResidualBlock(32),
            nn.Conv2d(32, 64, 3, padding=1),
            nn.BatchNorm2d(64),
            nn.ReLU(),
        )
        self.sentinel_hr_blocks = nn.Sequential(
            ResidualBlock(64), ResidualBlock(64), ResidualBlock(64)
        )
        self.static_processor = nn.Sequential(
            nn.Conv2d(num_static_features, 32, 3, padding=1),
            nn.BatchNorm2d(32),
            nn.ReLU(),
            nn.Conv2d(32, 64, 3, padding=1),
            nn.BatchNorm2d(64),
            nn.ReLU(),
        )
        self.cross_attention = CrossResolutionAttention(64)

        self.scale_fusion = nn.ModuleDict(
            {
                "full_res": nn.Conv2d(64, 64, 3, padding=1),
                "mid_res": nn.Conv2d(64, 64, 3, padding=1),
                "smap_res": nn.Conv2d(64, 64, 3, padding=1),
            }
        )

    def forward(self, sentinel, era5, elev_ndvi):
        """
        Args:
            sentinel: [B, C, 220, 220] - 500m resolution
            era5: [B, C, 11, 11] - Already at SMAP resolution
            elev_ndvi: [B, 2, 220, 220] - 500m resolution
        """
        sentinel_features = self.sentinel_processor(sentinel)
        sentinel_hr = self.sentinel_hr_blocks(sentinel_features)
        static_hr = self.static_processor(elev_ndvi)

        sentinel_scales = {
            "full_res": sentinel_hr,
            "mid_res": F.interpolate(
                sentinel_hr,
                size=(
                    self.resolution_handler.input_size // 2,
                    self.resolution_handler.input_size // 2,
                ),
                mode="bilinear",
                align_corners=False,
            ),
            "smap_res": self.resolution_handler.to_smap_resolution(sentinel_hr),
        }

        static_scales = {
            "full_res": static_hr,
            "mid_res": F.interpolate(
                static_hr,
                size=(
                    self.resolution_handler.input_size // 2,
                    self.resolution_handler.input_size // 2,
                ),
                mode="bilinear",
                align_corners=False,
            ),
            "smap_res": self.resolution_handler.to_smap_resolution(static_hr),
        }

        era5_features = self.era5_processor(era5)

        processed_scales = {
            scale: self.scale_fusion[scale](features)
            for scale, features in sentinel_scales.items()
        }
        fused_features = self.cross_attention(
            smap_features=era5_features,
            hr_features=processed_scales["full_res"],
            mid_features=processed_scales["mid_res"],
            lr_features=processed_scales["smap_res"],
        )

        return {
            "fused": fused_features,
            "era5_features": era5_features,
            "sentinel_features": processed_scales,
            "static_features": static_scales,
        }


class SoilMoistureModel(pl.LightningModule):
    """
    Main soil moisture prediction model.
    """

    def __init__(
        self,
        num_era5_features: int,
        num_sentinel_features: int,
        learning_rate: float,
        smap_size: int = 11,
        temporal_window: int = 8,
    ):
        super().__init__()
        self.save_hyperparameters()

        self.uncertainty_head = UncertaintyHead(
            num_era5_features=num_era5_features,
            num_sentinel_features=num_sentinel_features,
        )
        self.feature_processor = ParallelFeatureProcessing(
            num_era5_features=num_era5_features,
            num_sentinel_features=num_sentinel_features,
            num_static_features=2,
            smap_size=smap_size,
        )
        self.temporal_window = temporal_window
        self.prediction_heads = nn.ModuleDict(
            {
                str(scale): nn.Sequential(
                    nn.Conv2d(64, 32, 3, padding=1),
                    nn.BatchNorm2d(32),
                    nn.ReLU(),
                    ResidualBlock(32),
                    nn.Conv2d(32, 2, 1),
                    nn.Sigmoid(),
                )
                for scale in [220, 110, 55, 22, 11]
            }
        )
        self.physical_loss = PhysicalConsistencyLoss()
        self.spatial_loss = SMAPStructureLoss(smap_size)
        self.meteo_loss = meteorological_consistency_loss
        self.era5_indices = {
            "t2m": 2,  # temperature_2m
            "tp": 3,  # total_precipitation
            "radiation": 4,  # surface_solar_radiation_downwards
            "soil_temp_1": 5,  # soil_temperature_level_1
            "soil_temp_2": 6,  # soil_temperature_level_2
            "soil_temp_3": 7,  # soil_temperature_level_3
            "bare_soil_evap": 8,  # evaporation_from_bare_soil
            "total_evap": 9,  # total_evaporation
            "potential_evap": 10,  # potential_evaporation
            "surface_pressure": 11,  # surface_pressure
            "dewpoint": 12,  # dewpoint_temperature_2m
            "u_wind": 13,  # u_component_of_wind_10m
            "v_wind": 14,  # v_component_of_wind_10m
            "snow_depth": 15,  # snow_depth
            "snowfall": 16,  # snowfall
            "snowmelt": 17,  # snowmelt
            "surface_runoff": 18,  # surface_runoff
            "subsurface_runoff": 19,  # sub_surface_runoff
            "lai_high": 20,  # leaf_area_index_high_vegetation
            "lai_low": 21,  # leaf_area_index_low_vegetation
        }

    def forward(self, sentinel, era5, elev_ndvi):
        features = self.feature_processor(sentinel, era5, elev_ndvi)
        predictions = {}
        for scale in [220, 110, 55, 22, 11]:
            scale_features = F.interpolate(
                features["fused"],
                size=(scale, scale),
                mode="bilinear",
                align_corners=False,
            )
            predictions[str(scale)] = self.prediction_heads[str(scale)](scale_features)
        uncertainty = self.uncertainty_head(features["fused"])

        return {
            "predictions": predictions["11"],
            "multi_scale_predictions": predictions,
            "uncertainties": uncertainty,
            "features": features,
        }

    def training_step(self, batch, batch_idx):
        sentinel = batch["sentinel_ndvi"]
        era5 = batch["era5"]
        elev_ndvi = torch.cat(
            [batch["elevation"], batch["sentinel_ndvi"][:, 2:3]], dim=1
        )
        target = batch["future_smap"]
        if target.dim() == 3:
            target = target.unsqueeze(1)

        outputs = self(sentinel, era5, elev_ndvi)
        mse_loss = 0
        for scale in [220, 110, 55, 22, 11]:
            target_at_scale = F.interpolate(target, size=(scale, scale), mode="area")
            pred_at_scale = outputs["multi_scale_predictions"][str(scale)]
            scale_weight = 1.0 if scale == 11 else 0.2
            mse_loss += scale_weight * F.mse_loss(pred_at_scale, target_at_scale)

        physical_loss = self.physical_loss(
            outputs["predictions"], outputs["features"]["era5_features"], era5
        )
        spatial_loss = self.spatial_loss(outputs["predictions"])

        meteo_loss = self.meteo_loss(
            outputs["predictions"],
            era5,
            self.era5_indices,
            self.feature_processor.resolution_handler.to_smap_resolution,
        )

        predictions = outputs["predictions"]
        uncertainty = outputs["uncertainties"]
        target_smap = F.adaptive_avg_pool2d(target, (11, 11))
        pred_error = torch.abs(predictions - target_smap).detach()
        uncertainty_loss = F.mse_loss(uncertainty, pred_error)
        total_loss = (
            mse_loss
            + 0.1 * physical_loss
            + 0.1 * spatial_loss
            + 0.1 * meteo_loss
            + 0.1 * uncertainty_loss
        )

        self.log_dict(
            {
                "train_loss": total_loss,
                "train_mse": mse_loss,
                "train_physical": physical_loss,
                "train_spatial": spatial_loss,
                "train_meteo": meteo_loss,
                "train_uncertainty": uncertainty_loss,
            },
            prog_bar=True,
            on_step=False,
            on_epoch=True,
        )

        return total_loss

    def validation_step(self, batch, batch_idx):
        sentinel = batch["sentinel_ndvi"]
        era5 = batch["era5"]
        elev_ndvi = torch.cat(
            [batch["elevation"], batch["sentinel_ndvi"][:, 2:3]], dim=1
        )
        target = batch["future_smap"]
        if target.dim() == 3:
            target = target.unsqueeze(1)

        outputs = self(sentinel, era5, elev_ndvi)

        mse_loss = 0
        for scale in [220, 110, 55, 22, 11]:
            target_at_scale = F.interpolate(target, size=(scale, scale), mode="area")
            scale_weight = 1.0 if scale == 11 else 0.2
            mse_loss += scale_weight * F.mse_loss(
                outputs["multi_scale_predictions"][str(scale)], target_at_scale
            )

        physical_loss = self.physical_loss(
            outputs["predictions"], outputs["features"]["era5_features"], era5
        )
        spatial_loss = self.spatial_loss(outputs["predictions"])
        meteo_loss = self.meteo_loss(
            outputs["predictions"],
            era5,
            self.era5_indices,
            self.feature_processor.resolution_handler.to_smap_resolution,
        )

        r2_scores = self.calculate_r2(outputs["predictions"], target)

        self.log_dict(
            {
                "val_loss": mse_loss
                + 0.1 * physical_loss
                + 0.1 * spatial_loss
                + 0.1 * meteo_loss,
                "val_mse": mse_loss,
                "val_physical": physical_loss,
                "val_spatial": spatial_loss,
                "val_meteo": meteo_loss,
                "val_surface_r2": r2_scores["surface_r2"],
                "val_rootzone_r2": r2_scores["rootzone_r2"],
                "val_mean_r2": r2_scores["mean_r2"],
            },
            prog_bar=True,
            on_epoch=True,
        )

        if batch_idx == 0:
            self.validation_step_outputs = {
                "predictions": outputs["predictions"].detach(),
                "targets": target.detach(),
                "mse_loss": mse_loss.detach(),
                "r2": r2_scores["mean_r2"].detach(),
            }

        return {
            "val_loss": mse_loss
            + 0.1 * physical_loss
            + 0.1 * spatial_loss
            + 0.1 * meteo_loss,
            "predictions": outputs["predictions"].detach(),
            "targets": target.detach(),
        }

    def test_step(self, batch, batch_idx):
        sentinel = batch["sentinel_ndvi"]
        era5 = batch["era5"]
        elev_ndvi = torch.cat(
            [batch["elevation"], batch["sentinel_ndvi"][:, 2:3]], dim=1
        )
        target = batch["future_smap"]
        if target.dim() == 3:
            target = target.unsqueeze(1)

        outputs = self(sentinel, era5, elev_ndvi)

        mse_loss = 0
        for scale in [220, 110, 55, 22, 11]:
            target_at_scale = F.interpolate(target, size=(scale, scale), mode="area")
            pred_at_scale = outputs["multi_scale_predictions"][str(scale)]
            scale_weight = 1.0 if scale == 11 else 0.2
            mse_loss += scale_weight * F.mse_loss(pred_at_scale, target_at_scale)

        predictions = outputs["predictions"]
        mae_loss = F.l1_loss(predictions, target_at_scale)
        rmse = torch.sqrt(mse_loss)
        r2_scores = self.calculate_r2(predictions, target_at_scale)

        physical_loss = self.physical_loss(
            predictions, outputs["features"]["era5_features"], era5
        )
        spatial_loss = self.spatial_loss(predictions)
        meteo_loss = self.meteo_loss(
            predictions,
            era5,
            self.era5_indices,
            self.feature_processor.resolution_handler.to_smap_resolution,
        )

        self.test_step_outputs.append(
            {
                "predictions": predictions.detach(),
                "target": target_at_scale.detach(),
                "metrics": {
                    "mse": mse_loss.item(),
                    "rmse": rmse.item(),
                    "r2": r2_scores["mean_r2"].item(),
                    "physical_loss": physical_loss.item(),
                    "spatial_loss": spatial_loss.item(),
                    "meteo_loss": meteo_loss.item(),
                },
            }
        )

        r2_scores = self.calculate_r2(predictions, target)

        self.log_dict(
            {
                "test_mse": mse_loss,
                "test_mae": mae_loss,
                "test_rmse": rmse,
                "test_surface_r2": r2_scores["surface_r2"],
                "test_rootzone_r2": r2_scores["rootzone_r2"],
                "test_mean_r2": r2_scores["mean_r2"],
                "test_physical_loss": physical_loss,
                "test_spatial_loss": spatial_loss,
                "test_correlation": self.calculate_correlation(predictions, target),
                "test_meteo_loss": meteo_loss,
                "preds_min": predictions.min(),
                "preds_max": predictions.max(),
                "preds_mean": predictions.mean(),
                "targets_min": target.min(),
                "targets_max": target.max(),
                "targets_mean": target.mean(),
            },
            on_epoch=True,
        )

        return mse_loss

    def calculate_r2(self, pred, target):
        """
        Calculate R² score for both surface and root zone soil moisture.
        """
        if target.dim() == 3:
            target = target.unsqueeze(1)
        if pred.dim() == 3:
            pred = pred.unsqueeze(1)

        if target.shape[1] == 1:
            raise ValueError("Target should have 2 channels (surface and root zone)")

        if pred.shape[1] != 2:
            raise ValueError(
                "Predictions should have 2 channels (surface and root zone)"
            )

        if target.shape[-2:] != (11, 11):
            target = F.interpolate(target, size=(11, 11), mode="area")

        surface_r2 = self._calculate_single_r2(pred[:, 0:1], target[:, 0:1])
        rootzone_r2 = self._calculate_single_r2(pred[:, 1:2], target[:, 1:2])

        return {
            "surface_r2": surface_r2,
            "rootzone_r2": rootzone_r2,
            "mean_r2": (surface_r2 + rootzone_r2) / 2,
        }

    def _calculate_single_r2(self, pred, target):
        """Helper method to calculate R² for a single soil moisture layer"""
        if pred.shape != target.shape:
            raise ValueError(
                f"Shape mismatch: pred {pred.shape} vs target {target.shape}"
            )

        target_flat = target.reshape(target.shape[0], -1)
        pred_flat = pred.reshape(pred.shape[0], -1)

        target_mean = torch.mean(target_flat, dim=1, keepdim=True)
        ss_tot = torch.sum((target_flat - target_mean) ** 2, dim=1)
        ss_res = torch.sum((target_flat - pred_flat) ** 2, dim=1)

        r2 = torch.where(ss_tot > 0, 1 - (ss_res / ss_tot), torch.zeros_like(ss_tot))

        return r2.mean()

    def calculate_correlation(self, pred, target):
        """
        Calculate Pearson correlation coefficient.
        Ensures both inputs are at the same resolution (SMAP resolution).

        Args:
            pred: Predictions tensor [B, 1, H, W]
            target: Target tensor [B, 1, H, W]
        """
        if target.shape[-1] != 11:
            target = F.interpolate(
                target.unsqueeze(1) if target.dim() == 3 else target,
                size=(11, 11),
                mode="area",
            )

        if pred.shape[-1] != 11:
            pred = F.interpolate(
                pred.unsqueeze(1) if pred.dim() == 3 else pred,
                size=(11, 11),
                mode="area",
            )

        pred_flat = pred.view(-1)
        target_flat = target.view(-1)

        pred_centered = pred_flat - torch.mean(pred_flat)
        target_centered = target_flat - torch.mean(target_flat)

        numerator = torch.sum(pred_centered * target_centered)
        denominator = torch.sqrt(
            torch.sum(pred_centered**2) * torch.sum(target_centered**2)
        )

        return numerator / (denominator + 1e-8)

    def on_test_epoch_start(self):
        self.test_step_outputs = []

    def on_test_epoch_end(self):
        """Process stored test predictions at epoch end"""
        if hasattr(self, "test_step_outputs"):
            all_preds = torch.cat([x["predictions"] for x in self.test_step_outputs])
            all_preds = torch.cat([x["predictions"] for x in self.test_step_outputs])
            all_targets = torch.cat([x["target"] for x in self.test_step_outputs])

            final_r2_scores = self.calculate_r2(all_preds, all_targets)
            final_mse = F.mse_loss(all_preds, all_targets)
            final_rmse = torch.sqrt(final_mse)
            final_correlation = self.calculate_correlation(all_preds, all_targets)

            self.log_dict(
                {
                    "test_final_mse": final_mse,
                    "test_final_rmse": final_rmse,
                    "test_final_surface_r2": final_r2_scores["surface_r2"],
                    "test_final_rootzone_r2": final_r2_scores["rootzone_r2"],
                    "test_final_mean_r2": final_r2_scores["mean_r2"],
                    "test_final_correlation": final_correlation,
                }
            )

            if self.trainer.is_global_zero:
                self.test_results = {
                    "predictions": all_preds.cpu().numpy(),
                    "targets": all_targets.cpu().numpy(),
                    "metrics": {
                        "mse": final_mse.item(),
                        "rmse": final_rmse.item(),
                        "surface_r2": final_r2_scores["surface_r2"].item(),
                        "rootzone_r2": final_r2_scores["rootzone_r2"].item(),
                        "mean_r2": final_r2_scores["mean_r2"].item(),
                        "correlation": final_correlation.item(),
                    },
                }

            self.test_step_outputs.clear()

    def configure_optimizers(self):
        optimizer = torch.optim.AdamW(
            self.parameters(), lr=self.hparams.learning_rate, weight_decay=0.01
        )

        scheduler = {
            "scheduler": torch.optim.lr_scheduler.ReduceLROnPlateau(
                optimizer, mode="min", factor=0.5, patience=5, min_lr=1e-6, verbose=True
            ),
            "monitor": "val_loss",
            "interval": "epoch",
            "frequency": 1,
        }

        return [optimizer], [scheduler]

    def on_train_epoch_end(self):  # leftover method
        pass

    def on_validation_epoch_end(self):  # leftover method
        pass

    def uncertainty_decorrelation_loss(self, uncertainty, features):
        """decorrelation loss between uncertainty and input features"""
        losses = []

        features = F.interpolate(features, size=uncertainty.shape[-2:], mode="bilinear")

        for scale in [1, 2, 4]:
            if scale > 1:
                unc_scaled = F.avg_pool2d(uncertainty, scale)
                feat_scaled = F.avg_pool2d(features, scale)
            else:
                unc_scaled = uncertainty
                feat_scaled = features

            unc_flat = unc_scaled.view(unc_scaled.size(0), -1)
            feat_flat = feat_scaled.view(feat_scaled.size(0), feat_scaled.size(1), -1)

            for i in range(feat_flat.size(1)):
                corr = torch.abs(self.batch_correlation(unc_flat, feat_flat[:, i, :]))
                losses.append(corr)

        return torch.mean(torch.stack(losses))

    def batch_correlation(self, x, y):
        """Calculate correlation coefficient for batched data"""
        x_centered = x - x.mean(dim=1, keepdim=True)
        y_centered = y - y.mean(dim=1, keepdim=True)
        x_normalized = F.normalize(x_centered, p=2, dim=1)
        y_normalized = F.normalize(y_centered, p=2, dim=1)

        return torch.abs(torch.sum(x_normalized * y_normalized, dim=1)).mean()
