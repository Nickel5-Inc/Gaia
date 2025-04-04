import torch
from aurora import Aurora, Batch, rollout
from typing import List
from fiber.logging_utils import get_logger
import os

logger = get_logger(__name__)

class WeatherInferenceRunner:
    def __init__(self, model_repo="microsoft/aurora", checkpoint="aurora-0.25-pretrained.ckpt", device="cuda", use_lora=False):
        """
        Initializes the inference runner by loading the Aurora model.

        Args:
            model_repo: HuggingFace repository name.
            checkpoint: Checkpoint file name within the repository.
            device: Device to run inference on ('cuda' or 'cpu').
            use_lora: Whether the model uses LoRA (False for pretrained).
        """
        logger.info(f"Initializing WeatherInferenceRunner with device: {device}")
        self.device = torch.device(device if torch.cuda.is_available() and device == "cuda" else "cpu")
        logger.info(f"Using device: {self.device}")

        try:
            self.model = Aurora(use_lora=use_lora)
            logger.info(f"Loading checkpoint {checkpoint} from {model_repo}...")
            self.model.load_checkpoint(model_repo, checkpoint)
            self.model.eval()
            self.model = self.model.to(self.device)
            logger.info("Model loaded successfully and moved to device.")
        except Exception as e:
            logger.error(f"Failed to load Aurora model: {e}", exc_info=True)
            raise RuntimeError(f"Could not initialize Aurora model: {e}") from e

    def run_multistep_inference(self, initial_batch: Batch, steps: int) -> List[Batch]:
        """
        Runs multi-step inference using rollout and returns selected steps (every other).

        Args:
            initial_batch: The initial aurora.Batch object (should be on CPU).
            steps: The total number of 6-hour steps to simulate (e.g., 40 for 10 days).

        Returns:
            A list containing aurora.Batch objects for every other prediction step
            (T+12h, T+24h, T+36h, etc.), moved to CPU.
        """
        if self.model is None:
             logger.error("Model is not loaded, cannot run inference.")
             raise RuntimeError("Inference runner model not initialized.")

        logger.info(f"Starting multi-step inference for {steps} total steps...")
        selected_predictions: List[Batch] = []

        batch_on_device = initial_batch.to(self.device)

        try:
            with torch.inference_mode():
                for step_index, pred_batch_device in enumerate(rollout(self.model, batch_on_device, steps=steps)):
                    if step_index % 2 != 0:
                        logger.debug(f"Keeping prediction for step {step_index} (T+{(step_index + 1) * 6}h)")
                        selected_predictions.append(pred_batch_device.to("cpu"))

            logger.info(f"Finished multi-step inference. Selected {len(selected_predictions)} predictions.")

        except Exception as e:
             logger.error(f"Error during rollout inference: {e}", exc_info=True)
             raise

        finally:
             del batch_on_device
             if self.device == torch.device("cuda"):
                 torch.cuda.empty_cache()


        return selected_predictions

    def cleanup(self):
        """Release model resources if needed."""
        logger.info("Cleaning up WeatherInferenceRunner resources.")
        del self.model
        if self.device == torch.device("cuda"):
            torch.cuda.empty_cache()
