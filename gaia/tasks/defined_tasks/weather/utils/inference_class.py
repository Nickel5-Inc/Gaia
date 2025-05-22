import torch
from aurora import Aurora, Batch, rollout
from typing import List
from fiber.logging_utils import get_logger
import os
from aurora.foundry import FoundryClient, BlobStorageChannel, submit
import traceback

logger = get_logger(__name__)

class WeatherInferenceRunner:
    def __init__(self, model_repo="microsoft/aurora", checkpoint="aurora-0.25-pretrained.ckpt", device="cuda", use_lora=False, load_local_model=True):
        """
        Initializes the inference runner.

        Args:
            model_repo: HuggingFace repository name.
            checkpoint: Checkpoint file name within the repository.
            device: Device to run inference on ('cuda' or 'cpu').
            use_lora: Whether the model uses LoRA (False for pretrained).
            load_local_model: If True, loads the model into memory. If False, prepares for other modes (e.g., Foundry).
        """
        logger.info(f"Initializing WeatherInferenceRunner with device: {device}, load_local_model: {load_local_model}")
        self.device = torch.device(device if torch.cuda.is_available() and device == "cuda" else "cpu")
        logger.info(f"Using device: {self.device}")
        self.model = None

        if load_local_model:
            try:
                self.model = Aurora(use_lora=use_lora)
                logger.info(f"Loading checkpoint {checkpoint} from {model_repo}...")
                self.model.load_checkpoint(model_repo, checkpoint)
                self.model.eval()
                self.model = self.model.to(self.device)
                logger.info("Local model loaded successfully and moved to device.")
            except Exception as e:
                logger.error(f"Failed to load Aurora model locally: {e}", exc_info=True)
                self.model = None 
        else:
            logger.info("Local model loading skipped by configuration (e.g., for Azure Foundry mode).")

    def run_multistep_inference(self, initial_batch: Batch, steps: int) -> List[Batch]:
        """
        Runs multi-step inference using rollout and returns selected steps.

        Args:
            initial_batch: The initial aurora.Batch object (should be on CPU).
            steps: The total number of 6-hour steps to simulate (e.g., 40 for 10 days).

        Returns:
            A list containing aurora.Batch objects for every prediction step
            (T+6h, T+12h, T+18h, etc.), moved to CPU.
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
                    logger.debug(f"Keeping prediction for step {step_index+1} (T+{(step_index + 1) * 6}h)")
                    selected_predictions.append(pred_batch_device.to("cpu"))

            logger.info(f"Finished multi-step inference. Selected {len(selected_predictions)} predictions (all steps).")

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

    async def run_foundry_inference(self, initial_batch: Batch, steps: int) -> List[Batch]:
        """
        Runs multi-step inference using the Azure AI Foundry endpoint.

        Args:
            initial_batch: The initial aurora.Batch object (should be on CPU).
            steps: The total number of 6-hour steps to simulate (e.g., 40 for 10 days).

        Returns:
            A list containing aurora.Batch objects for each prediction step, moved to CPU.
            Returns an empty list if Foundry components are unavailable or an error occurs.

        Raises:
            RuntimeError: If required environment variables are not set.
        """
        logger.info(f"Starting Azure AI Foundry inference for {steps} steps...")

        if not FoundryClient or not BlobStorageChannel or not submit:
            logger.error("Aurora Foundry components are not available. Cannot run Foundry inference.")
            return []

        endpoint_url = os.getenv("FOUNDRY_ENDPOINT_URL")
        access_token = os.getenv("FOUNDRY_ACCESS_TOKEN")
        blob_sas_url = os.getenv("BLOB_URL_WITH_RW_SAS")

        if not all([endpoint_url, access_token, blob_sas_url]):
            missing = [var for var, val in [("FOUNDRY_ENDPOINT_URL", endpoint_url),
                                            ("FOUNDRY_ACCESS_TOKEN", access_token),
                                            ("BLOB_URL_WITH_RW_SAS", blob_sas_url)] if not val]
            msg = f"Missing required environment variables for Azure AI Foundry: {', '.join(missing)}"
            logger.error(msg)
            raise RuntimeError(msg)

        predictions_list: List[Batch] = []
        try:
            logger.debug("Initializing FoundryClient...")
            foundry_client = FoundryClient(
                endpoint=endpoint_url,
                token=access_token,
            )
            logger.debug("Initializing BlobStorageChannel...")
            channel = BlobStorageChannel(blob_sas_url)

            model_name = "aurora-0.25-finetuned"
            logger.info(f"Submitting inference job to Foundry endpoint for model '{model_name}'...")

            prediction_iterator = submit(
                batch=initial_batch,
                model_name=model_name,
                num_steps=steps,
                foundry_client=foundry_client,
                channel=channel,
            )

            logger.info("Waiting for prediction results from Foundry...")
            for step_idx, pred_batch in enumerate(prediction_iterator):
                logger.debug(f"Retrieved prediction step {step_idx + 1}/{steps} from Foundry.")
                logger.info(f"Keeping prediction step {step_idx + 1}/{steps} (T+{(step_idx + 1) * 6}h)")
                predictions_list.append(pred_batch)

            logger.info(f"Successfully retrieved and selected {len(predictions_list)} prediction steps (all steps) from Azure AI Foundry.")

        except ImportError as e:
            logger.error(f"ImportError during Foundry inference: {e}. Is aurora.foundry installed?")
            return []
        except Exception as e:
            logger.error(f"Error during Azure AI Foundry inference: {e}", exc_info=True)
            logger.error(traceback.format_exc())
            return []

        return predictions_list