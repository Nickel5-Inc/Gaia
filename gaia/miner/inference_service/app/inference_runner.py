import asyncio
import traceback
from typing import Any, Dict, Optional, List

import torch
# The following try-except block handles conditional import of Aurora and Batch
# This is important if the aurora SDK might not be present in all environments
# where this code is type-checked or partially run.

# Define LoggerPlaceholder early if it's to be used in except blocks below
class LoggerPlaceholder:
    def info(self, msg): print(f"INFO: {msg}")
    def error(self, msg): print(f"ERROR: {msg}")
    def warning(self, msg): print(f"WARNING: {msg}")

logger = LoggerPlaceholder() # Instantiate it

_AURORA_AVAILABLE = False
Aurora: Any # Pre-declare so the name exists; will be Any if import fails
Batch: Any  # Pre-declare so the name exists; will be Any if import fails

try:
    from aurora import Aurora as AuroraActual, Batch as BatchActual
    Aurora = AuroraActual
    Batch = BatchActual
    _AURORA_AVAILABLE = True
    logger.info("Successfully imported Aurora and Batch from aurora SDK.")
except ImportError:
    logger.warning("Aurora SDK (aurora) not found. Using 'Any' for Aurora and Batch types. Full functionality may be affected.")
    # Aurora and Batch remain typing.Any as pre-declared
    _AURORA_AVAILABLE = False

class InferenceModel:
    """Placeholder for your actual inference model wrapper."""
    def __init__(self, config: Dict[str, Any]):
        self.config = config.get('model', {}) # Get the 'model' sub-config
        self.model: Optional['Aurora'] = None # Changed to string literal
        self.device: Optional[torch.device] = None
        self.load_model()

    def load_model(self):
        model_repo = self.config.get('model_repo', "microsoft/aurora")
        checkpoint = self.config.get('checkpoint', "aurora-0.25-pretrained.ckpt")
        # use_lora = self.config.get('use_lora', False) # Assuming not used for pretrained, can be added to config
        use_lora = False # Defaulting to False as per WeatherInferenceRunner for pretrained
        
        # Determine device from config, defaulting to cuda if available, else cpu
        config_device_str = self.config.get('device', 'auto').lower()
        if config_device_str == "cuda" and torch.cuda.is_available():
            self.device = torch.device("cuda")
        elif config_device_str == "cpu":
            self.device = torch.device("cpu")
        else: # auto or invalid value
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        logger.info(f"Initializing InferenceModel. Attempting to use device: {self.device}")

        try:
            logger.info(f"Creating Aurora model instance (use_lora={use_lora})...")
            self.model = Aurora(use_lora=use_lora)
            logger.info(f"Loading checkpoint '{checkpoint}' from repository '{model_repo}'...")
            # The Aurora model internally handles downloading from HuggingFace Hub if not found locally
            # or loading from a local path if model_repo is a path.
            self.model.load_checkpoint(model_repo, checkpoint)
            self.model.eval() # Set to evaluation mode
            self.model = self.model.to(self.device) # Move model to the selected device
            logger.info(f"Aurora model '{checkpoint}' loaded successfully from '{model_repo}' and moved to {self.device}.")

        except FileNotFoundError as fnf_error:
            logger.error(f"Checkpoint file not found for model {model_repo}/{checkpoint}. Error: {fnf_error}", exc_info=True)
            logger.error("Please ensure the model_repo and checkpoint are correct in settings.yaml, ")
            logger.error("or that the checkpoint exists at the specified path if model_repo is a local directory.")
            self.model = None
        except Exception as e:
            logger.error(f"Failed to load Aurora model: {e}", exc_info=True)
            self.model = None

    async def run_inference(self, input_batch: 'Batch', steps: int) -> Optional[List['Batch']]: # Changed to string literals
        """
        Runs multi-step inference.
        Adapts logic from WeatherInferenceRunner.run_multistep_inference.
        """
        if self.model is None:
            logger.error("Model not loaded. Cannot run inference.")
            return None
        if not _AURORA_AVAILABLE and input_batch is None:
            logger.warning("Aurora not available and input_batch is None, cannot run placeholder inference.")
            return None
        if _AURORA_AVAILABLE and not isinstance(input_batch, BatchActual if _AURORA_AVAILABLE else Any):
             logger.error(f"Expected Aurora Batch, got {type(input_batch)}")
             return None 

        logger.info(f"Running inference for {steps} steps on device {self.device}...")
        
        selected_predictions: List['Batch'] = [] # Changed to string literal
        # Ensure input_batch is on the correct device before passing to the model
        batch_on_device = input_batch.to(self.device)

        try:
            # Mimic torch.inference_mode() for safety, though Aurora's rollout might handle it.
            # Actual rollout function is imported from aurora package
            # from aurora import rollout # This should be at the top of the file if not already
            
            # The original code uses `asyncio.to_thread` for `run_multistep_inference` 
            # because `rollout` can be blocking. We should do the same here.
            def _blocking_rollout():
                results = []
                with torch.inference_mode(): 
                    for step_index, pred_batch_device in enumerate(torch.rollout(self.model, batch_on_device, steps=steps)):
                        logger.debug(f"Prediction for step {step_index+1} (T+{(step_index + 1) * self.config.get('forecast_step_hours', 6)}h)")
                        results.append(pred_batch_device.to("cpu")) # Move to CPU before storing
                return results

            selected_predictions = await asyncio.to_thread(_blocking_rollout)
            logger.info(f"Finished multi-step inference. Generated {len(selected_predictions)} prediction steps.")

        except RuntimeError as e:
            if "out of memory" in str(e).lower() and self.device == torch.device("cuda"):
                logger.error(f"CUDA out of memory during inference: {e}. Try reducing batch size or model size if possible.", exc_info=True)
            else:
                logger.error(f"Runtime error during inference: {e}", exc_info=True)
            raise # Re-raise to be caught by the main handler
        except Exception as e:
             logger.error(f"Error during rollout inference: {e}", exc_info=True)
             raise # Re-raise
        finally:
             del batch_on_device # Free memory for the batch on device
             if self.device == torch.device("cuda"):
                 torch.cuda.empty_cache() # Clear CUDA cache

        return selected_predictions

# Global inference runner instance (or manage it within FastAPI app state)
# This instance will be created when the module is loaded.
# Ensure your config loading is handled before this, or pass it during app startup.

# To be initialized in main.py after loading config
INFERENCE_RUNNER: Optional[InferenceModel] = None

async def initialize_inference_runner(app_config: Dict[str, Any]):
    global INFERENCE_RUNNER
    if INFERENCE_RUNNER is None:
        logger.info("Initializing inference runner...")
        INFERENCE_RUNNER = InferenceModel(config=app_config)
        # You might want to run a dummy inference pass here to ensure the model is truly ready
        logger.info("Inference runner initialized.")
    else:
        logger.info("Inference runner already initialized.")

async def get_inference_runner() -> Optional[InferenceModel]:
    if INFERENCE_RUNNER is None:
        logger.error("Inference runner accessed before initialization.")
        # In a real scenario, you might want to raise an error or wait for initialization
    return INFERENCE_RUNNER

async def run_model_inference(
    prepared_batch: 'Batch',  
    config: Dict[str, Any] # Pass the main APP_CONFIG here
) -> Optional[List[Any]]: # Return type here is List[Any] which is fine, or could be List['Batch']
    """
    Main function to trigger inference using the global runner.
    """
    runner = await get_inference_runner()
    if not runner:
        logger.error("Inference runner not available.")
        return None
    if runner.model is None: # Added check here for more direct feedback before calling run_inference
        logger.error("Inference runner's model is not loaded. Cannot run inference.")
        return None

    try:
        inference_steps = config.get('model', {}).get('inference_steps', 40)
        forecast_step_hours = config.get('model', {}).get('forecast_step_hours', 6) # For logging in run_inference
        runner.config['forecast_step_hours'] = forecast_step_hours # Pass to runner for its logging
        
        predictions = await runner.run_inference(
            input_batch=prepared_batch, 
            steps=inference_steps
        )
        
        if predictions is None or not predictions:
            logger.warning("Inference did not return any predictions.")
            return None
        
        logger.info(f"Inference successful, received {len(predictions)} steps.")
        return predictions

    except Exception as e:
        logger.error(f"Error during model inference: {e}")
        logger.error(traceback.format_exc())
        return None 