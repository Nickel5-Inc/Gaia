import torch.nn as nn
import torch
import torch.nn.functional as F
import pytorch_lightning as pl
from huggingface_hub import PyTorchModelHubMixin
from torchmetrics import R2Score
from pytorch_msssim import ssim


class SoilModel(pl.LightningModule, PyTorchModelHubMixin):
    def __init__(
        self,
        num_era5_features: int = 17,
        num_sentinel_features: int = 2,
        learning_rate: float = 1e-4,
        smap_size: int = 11,
        temporal_window: int = 8,
    ):
        super().__init__()
        self.save_hyperparameters()
        self.sentinel_encoder = nn.Sequential(
            nn.Conv2d(2, 16, 3, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(4),
            nn.Conv2d(16, 32, 3, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(5),
        )
        self.era5_encoder = nn.Sequential(
            nn.Conv2d(num_era5_features, 32, 3, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(4),
            nn.Conv2d(32, 64, 3, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(5),
        )
        self.static_encoder = nn.Sequential(
            nn.Conv2d(2, 16, 3, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(4),
            nn.Conv2d(16, 32, 3, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(5),
        )

        self.fusion_layers = nn.Sequential(
            nn.Conv2d(128, 64, 3, padding=1),
            nn.ReLU(),
            nn.Conv2d(64, 32, 3, padding=1),
            nn.ReLU(),
        )

        self.pixel_predictor = nn.Sequential(
            nn.Conv2d(32, 16, 1),
            nn.ReLU(),
            nn.Conv2d(16, 8, 1),
            nn.ReLU(),
            nn.Conv2d(8, 2, 1),
            nn.Sigmoid(),
        )

    def forward(self, sentinel, era5, elev_ndvi):
        sentinel_features = self.sentinel_encoder(sentinel)
        era5_features = self.era5_encoder(era5)
        static_features = self.static_encoder(elev_ndvi)
        combined = torch.cat([sentinel_features, era5_features, static_features], dim=1)
        fused = self.fusion_layers(combined)
        predictions = self.pixel_predictor(fused)
        return predictions

    def training_step(self, batch, batch_idx):
        sentinel = batch["sentinel_ndvi"][:, :2]
        era5 = batch["era5"]
        elev_ndvi = torch.cat(
            [batch["elevation"], batch["sentinel_ndvi"][:, 2:3]], dim=1
        )
        target = batch["future_smap"]
        if target.dim() == 3:
            target = target.unsqueeze(1)

        predictions = self(sentinel, era5, elev_ndvi)
        target = F.interpolate(target, size=(11, 11), mode="area")
        rmse_loss = torch.sqrt(F.mse_loss(predictions, target))
        mse_loss = F.mse_loss(predictions, target)
        self.log("train_rmse", rmse_loss)
        self.log("train_mse", mse_loss)

        return rmse_loss

    def validation_step(self, batch, batch_idx):
        sentinel = batch["sentinel_ndvi"][:, :2]
        era5 = batch["era5"]
        elev_ndvi = torch.cat(
            [batch["elevation"], batch["sentinel_ndvi"][:, 2:3]], dim=1
        )
        target = batch["future_smap"]

        if target.dim() == 3:
            target = target.unsqueeze(1)

        predictions = self(sentinel, era5, elev_ndvi)
        target = F.interpolate(target, size=(11, 11), mode="area")
        rmse_loss = torch.sqrt(F.mse_loss(predictions, target))
        mse_loss = F.mse_loss(predictions, target)
        self.log("val_rmse", rmse_loss)
        self.log("val_mse", mse_loss)

        return {"val_loss": rmse_loss, "predictions": predictions, "targets": target}

    def test_step(self, batch, batch_idx):
        sentinel = batch["sentinel_ndvi"][:, :2]
        era5 = batch["era5"]
        elev_ndvi = torch.cat(
            [batch["elevation"], batch["sentinel_ndvi"][:, 2:3]], dim=1
        )
        target = batch["future_smap"]
        batch_size = target.size(0)
        device = target.device

        if target.dim() == 3:
            target = target.unsqueeze(1)

        predictions = self(sentinel, era5, elev_ndvi)
        target = F.interpolate(target, size=(11, 11), mode="area")

        if batch_idx == 0:
            print("\nInitial Value Ranges:")
            print(f"Predictions: [{predictions.min():.3f}, {predictions.max():.3f}]")
            print(f"Targets: [{target.min():.3f}, {target.max():.3f}]")
            print(
                f"Surface - Predictions: [{predictions[:, 0].min():.3f}, {predictions[:, 0].max():.3f}]"
            )
            print(
                f"Surface - Targets: [{target[:, 0].min():.3f}, {target[:, 0].max():.3f}]"
            )
            print(
                f"Rootzone - Predictions: [{predictions[:, 1].min():.3f}, {predictions[:, 1].max():.3f}]"
            )
            print(
                f"Rootzone - Targets: [{target[:, 1].min():.3f}, {target[:, 1].max():.3f}]"
            )

        pred_reshaped = predictions.permute(0, 2, 3, 1).reshape(-1, 2)
        target_reshaped = target.permute(0, 2, 3, 1).reshape(-1, 2)
        pred_min, pred_max = pred_reshaped.min(dim=0)[0], pred_reshaped.max(dim=0)[0]
        target_min, target_max = (
            target_reshaped.min(dim=0)[0],
            target_reshaped.max(dim=0)[0],
        )
        pred_scaled = (pred_reshaped - pred_min) / (pred_max - pred_min)
        target_scaled = (target_reshaped - target_min) / (target_max - target_min)

        r2_metric = R2Score(num_outputs=2).to(device)
        r2_surface_metric = R2Score().to(device)
        r2_rootzone_metric = R2Score().to(device)
        r2_score = r2_metric(pred_scaled, target_scaled)
        r2_surface = r2_surface_metric(pred_scaled[:, 0], target_scaled[:, 0])
        r2_rootzone = r2_rootzone_metric(pred_scaled[:, 1], target_scaled[:, 1])

        pred_norm = (predictions - predictions.min()) / (
            predictions.max() - predictions.min()
        )
        target_norm = (target - target.min()) / (target.max() - target.min())
        ssim_score = ssim(pred_norm, target_norm, data_range=1.0)
        rmse = torch.sqrt(F.mse_loss(predictions, target))
        mse = F.mse_loss(predictions, target)

        surface_metrics = {
            "rmse_surface": torch.sqrt(F.mse_loss(predictions[:, 0], target[:, 0])),
            "r2_surface": r2_surface,
        }
        rootzone_metrics = {
            "rmse_rootzone": torch.sqrt(F.mse_loss(predictions[:, 1], target[:, 1])),
            "r2_rootzone": r2_rootzone,
        }
        surface_stats = {
            "preds_surface_min": predictions[:, 0].min().item(),
            "preds_surface_max": predictions[:, 0].max().item(),
            "preds_surface_mean": predictions[:, 0].mean().item(),
            "preds_surface_std": predictions[:, 0].std().item(),
            "targets_surface_min": target[:, 0].min().item(),
            "targets_surface_max": target[:, 0].max().item(),
            "targets_surface_mean": target[:, 0].mean().item(),
            "targets_surface_std": target[:, 0].std().item(),
        }
        rootzone_stats = {
            "preds_rootzone_min": predictions[:, 1].min().item(),
            "preds_rootzone_max": predictions[:, 1].max().item(),
            "preds_rootzone_mean": predictions[:, 1].mean().item(),
            "preds_rootzone_std": predictions[:, 1].std().item(),
            "targets_rootzone_min": target[:, 1].min().item(),
            "targets_rootzone_max": target[:, 1].max().item(),
            "targets_rootzone_mean": target[:, 1].mean().item(),
            "targets_rootzone_std": target[:, 1].std().item(),
        }
        self.log_dict(
            {
                "test_rmse": rmse,
                "test_mse": mse,
                "test_r2": r2_score,
                "test_ssim": ssim_score,
                **surface_metrics,
                **rootzone_metrics,
                **surface_stats,
                **rootzone_stats,
            },
            batch_size=batch_size,
        )
        self.test_step_outputs = getattr(self, "test_step_outputs", [])
        self.test_step_outputs.append(
            {
                "predictions": predictions.detach().cpu(),
                "targets": target.detach().cpu(),
                "batch_size": batch_size,
                "metrics": {
                    "rmse": rmse.item(),
                    "mse": mse.item(),
                    "r2": r2_score.item(),
                    "ssim": ssim_score.item(),
                    **surface_metrics,
                    **rootzone_metrics,
                    **surface_stats,
                    **rootzone_stats,
                },
            }
        )
        return {
            "rmse": rmse,
            "r2": r2_score,
            "ssim": ssim_score,
            "predictions": predictions,
            "targets": target,
        }

    def on_test_epoch_end(self):
        outputs = self.test_step_outputs
        all_preds = torch.cat([x["predictions"] for x in outputs])
        all_targets = torch.cat([x["targets"] for x in outputs])
        weighted_metrics = {
            "test_final_rmse": torch.sqrt(F.mse_loss(all_preds, all_targets)),
            "test_final_mse": F.mse_loss(all_preds, all_targets),
            "test_final_rmse_surface": torch.sqrt(
                F.mse_loss(all_preds[:, 0], all_targets[:, 0])
            ),
            "test_final_rmse_rootzone": torch.sqrt(
                F.mse_loss(all_preds[:, 1], all_targets[:, 1])
            ),
        }
        pred_reshaped = all_preds.reshape(-1, 2)
        target_reshaped = all_targets.reshape(-1, 2)
        pred_min, pred_max = pred_reshaped.min(dim=0)[0], pred_reshaped.max(dim=0)[0]
        target_min, target_max = (
            target_reshaped.min(dim=0)[0],
            target_reshaped.max(dim=0)[0],
        )
        pred_scaled = (pred_reshaped - pred_min.unsqueeze(0)) / (
            pred_max - pred_min
        ).unsqueeze(0)
        target_scaled = (target_reshaped - target_min.unsqueeze(0)) / (
            target_max - target_min
        ).unsqueeze(0)

        r2_metric = R2Score(num_outputs=2).to(all_preds.device)
        r2_surface_metric = R2Score().to(all_preds.device)
        r2_rootzone_metric = R2Score().to(all_preds.device)

        weighted_metrics.update(
            {
                "test_final_r2": r2_metric(pred_scaled, target_scaled),
                "test_final_r2_surface": r2_surface_metric(
                    pred_scaled[:, 0], target_scaled[:, 0]
                ),
                "test_final_r2_rootzone": r2_rootzone_metric(
                    pred_scaled[:, 1], target_scaled[:, 1]
                ),
            }
        )

        pred_normalized = (all_preds - all_preds.min()) / (
            all_preds.max() - all_preds.min()
        )
        target_normalized = (all_targets - all_targets.min()) / (
            all_targets.max() - all_targets.min()
        )
        weighted_metrics["test_final_ssim"] = ssim(
            pred_normalized, target_normalized, data_range=1.0
        )

        final_stats = {
            "predictions_surface": {
                "min": all_preds[:, 0].min().item(),
                "max": all_preds[:, 0].max().item(),
                "mean": all_preds[:, 0].mean().item(),
                "std": all_preds[:, 0].std().item(),
            },
            "predictions_rootzone": {
                "min": all_preds[:, 1].min().item(),
                "max": all_preds[:, 1].max().item(),
                "mean": all_preds[:, 1].mean().item(),
                "std": all_preds[:, 1].std().item(),
            },
            "targets_surface": {
                "min": all_targets[:, 0].min().item(),
                "max": all_targets[:, 0].max().item(),
                "mean": all_targets[:, 0].mean().item(),
                "std": all_targets[:, 0].std().item(),
            },
            "targets_rootzone": {
                "min": all_targets[:, 1].min().item(),
                "max": all_targets[:, 1].max().item(),
                "mean": all_targets[:, 1].mean().item(),
                "std": all_targets[:, 1].std().item(),
            },
        }

        print("\n" + "=" * 50)
        print("FINAL TEST RESULTS")
        print("=" * 50)
        print("\nOverall Metrics:")
        print(f"RMSE: {weighted_metrics['test_final_rmse']:.4f}")
        print(f"MSE: {weighted_metrics['test_final_mse']:.4f}")
        print(f"R²: {weighted_metrics['test_final_r2']:.4f}")
        print(f"SSIM: {weighted_metrics['test_final_ssim']:.4f}")
        print("\nLayer-Specific Metrics:")
        print("Surface Layer:")
        print(f"RMSE: {weighted_metrics['test_final_rmse_surface']:.4f}")
        print(f"R²: {weighted_metrics['test_final_r2_surface']:.4f}")
        print("Rootzone Layer:")
        print(f"RMSE: {weighted_metrics['test_final_rmse_rootzone']:.4f}")
        print(f"R²: {weighted_metrics['test_final_r2_rootzone']:.4f}")
        print("\nValue Distributions:")
        for layer in ["surface", "rootzone"]:
            print(f"\n{layer.capitalize()} Layer:")
            print(
                f"Predictions: min={final_stats[f'predictions_{layer}']['min']:.3f}, "
                f"max={final_stats[f'predictions_{layer}']['max']:.3f}, "
                f"mean={final_stats[f'predictions_{layer}']['mean']:.3f}, "
                f"std={final_stats[f'predictions_{layer}']['std']:.3f}"
            )
            print(
                f"Targets: min={final_stats[f'targets_{layer}']['min']:.3f}, "
                f"max={final_stats[f'targets_{layer}']['max']:.3f}, "
                f"mean={final_stats[f'targets_{layer}']['mean']:.3f}, "
                f"std={final_stats[f'targets_{layer}']['std']:.3f}"
            )
        print("\n" + "=" * 50)
        self.log_dict(weighted_metrics)
        self.test_step_outputs.clear()
        return weighted_metrics

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=self.hparams.learning_rate)
