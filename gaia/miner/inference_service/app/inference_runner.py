import asyncio
import traceback
import logging
from typing import Any, Dict, Optional, List

import torch

# --- Aurora and Batch Type Handling ---
# Attempt to import Aurora-specific types for type checking and runtime validation.
# If Aurora SDK is not available, use 'Any' and log a warning.
_AURORA_AVAILABLE = False
AuroraModelType: Any
BatchType: Any

try:
    from aurora import Aurora as AuroraActual, Batch as BatchActual
    AuroraModelType = AuroraActual
    BatchType = BatchActual
    _AURORA_AVAILABLE = True
    logging.info("Successfully imported Aurora and Batch from aurora SDK for type hinting.")
except ImportError:
    AuroraModelType = Any
    BatchType = Any
    logging.warning(
        "Aurora SDK (aurora.py) not found. Using 'Any' for Aurora and Batch types. "
        "Full model loading and type checking for these objects will be bypassed. "
        "Ensure the SDK is available in the environment for full functionality."
    )

logger = logging.getLogger(__name__)

class InferenceModel:
    """
    Manages the lifecycle and execution of the Aurora weather forecasting model.
    """
    def __init__(self, config: Dict[str, Any]):
        self.model_config = config.get('model', {}) # Get the 'model' sub-config
        self.model: Optional[AuroraModelType] = None
        self.device: Optional[torch.device] = None
        _model_repo = self.model_config.get('model_repo', "microsoft/aurora")
        _checkpoint = self.model_config.get('checkpoint', "aurora-0.25-pretrained.ckpt")
        self.model_name: str = f"{_model_repo}/{_checkpoint}"
        self._load_model_and_device()

    def _load_model_and_device(self):
        """
        Loads the Aurora model onto the appropriate device (CPU or GPU)
        based on the provided configuration and system availability.
        """
        model_repo = self.model_config.get('model_repo', "microsoft/aurora")
        checkpoint = self.model_config.get('checkpoint', "aurora-0.25-pretrained.ckpt")
        # use_lora = self.model_config.get('use_lora', False) # Not currently used for pretrained Aurora
        use_lora = False

        # Determine device from config, defaulting to cuda if available, else cpu
        config_device_str = self.model_config.get('device', 'auto').lower()
        if config_device_str == "cuda":
            if torch.cuda.is_available():
                self.device = torch.device("cuda")
            else:
                logger.warning("CUDA specified in config but not available. Falling back to CPU.")
                self.device = torch.device("cpu")
        elif config_device_str == "cpu":
            self.device = torch.device("cpu")
        else: # 'auto' or invalid value
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        logger.info(f"Initializing InferenceModel. Attempting to use device: {self.device}")

        if not _AURORA_AVAILABLE:
            logger.error(
                "Aurora SDK is not available. Cannot load the Aurora model. "
                "Inference functionality will be disabled."
            )
            self.model = None
            return

        try:
            logger.info(f"Creating Aurora model instance (use_lora={use_lora})...")
            # Ensure AuroraModelType is the actual class if available
            self.model = AuroraModelType(use_lora=use_lora) # type: ignore
            logger.info(f"Loading checkpoint '{checkpoint}' from repository '{model_repo}'...")
            # The Aurora model internally handles downloading from HuggingFace Hub
            # or loading from a local path if model_repo is a local directory path.
            self.model.load_checkpoint(model_repo, checkpoint) # type: ignore
            self.model.eval() # type: ignore # Set to evaluation mode
            self.model = self.model.to(self.device) # type: ignore # Move model to the selected device
            logger.info(f"Aurora model '{checkpoint}' loaded successfully from '{model_repo}' and moved to {self.device}.")

        except FileNotFoundError as fnf_error:
            logger.error(f"Checkpoint file not found for model {model_repo}/{checkpoint}. Error: {fnf_error}", exc_info=True)
            logger.error("Please ensure the model_repo and checkpoint are correct in settings.yaml, ")
            logger.error("or that the checkpoint exists at the specified path if model_repo is a local directory (and copied into the container).")
            self.model = None # Ensure model is None if loading fails
        except Exception as e:
            logger.error(f"Failed to load Aurora model: {e}", exc_info=True)
            self.model = None # Ensure model is None if loading fails

    async def run_inference(self, input_batch: BatchType, steps: int) -> Optional[List[BatchType]]:
        """
        Runs multi-step inference using the loaded Aurora model.
        This method is designed to be non-blocking by offloading the potentially
        CPU/GPU-bound rollout operation to a separate thread.
        """
        if self.model is None:
            logger.error("Model not loaded. Cannot run inference.")
            return None
        if not _AURORA_AVAILABLE:
            logger.error("Aurora SDK not available. Cannot run inference.")
            return None
        if not isinstance(input_batch, BatchType if _AURORA_AVAILABLE else Any): # type: ignore
             logger.error(f"Expected Aurora Batch, got {type(input_batch)}")
             return None

        logger.info(f"Running inference for {steps} steps on device {self.device}...")
        
        # Ensure input_batch is on the correct device before passing to the model
        try:
            batch_on_device = input_batch.to(self.device) # type: ignore
        except Exception as e:
            logger.error(f"Failed to move input batch to device {self.device}: {e}", exc_info=True)
            return None

        def _blocking_rollout_operation() -> List[BatchType]:
            """Synchronous part of the inference to be run in a thread."""
            results: List[BatchType] = []
            with torch.inference_mode(): # Essential for performance and correct behavior
                # torch.rollout is assumed to be available if Aurora is
                for step_index, pred_batch_device in enumerate(torch.rollout(self.model, batch_on_device, steps=steps)):
                    logger.debug(f"Prediction for step {step_index+1} (T+{(step_index + 1) * self.model_config.get('forecast_step_hours', 6)}h)")
                    # Move predictions to CPU before storing/returning to avoid GPU memory accumulation
                    # and to make them accessible for CPU-based serialization later.
                    results.append(pred_batch_device.to("cpu"))
            return results

        selected_predictions: Optional[List[BatchType]] = None
        try:
            selected_predictions = await asyncio.to_thread(_blocking_rollout_operation)
            logger.info(f"Finished multi-step inference. Generated {len(selected_predictions) if selected_predictions else 0} prediction steps.")
        except RuntimeError as e:
            if "out of memory" in str(e).lower() and self.device and self.device.type == "cuda":
                logger.error(f"CUDA out of memory during inference: {e}. Try reducing batch size or model size if possible.", exc_info=True)
            else:
                logger.error(f"Runtime error during inference: {e}", exc_info=True)
            # Do not re-raise here, allow finally block to run, return None will indicate failure
        except Exception as e:
             logger.error(f"Error during rollout inference: {e}", exc_info=True)
             # Do not re-raise here, allow finally block to run, return None will indicate failure
        finally:
            # Explicitly delete the on-device batch to free GPU memory as soon as possible.
            del batch_on_device
            if self.device and self.device.type == "cuda":
                torch.cuda.empty_cache() # Attempt to clear CUDA cache

        return selected_predictions

# --- Global Inference Runner Instance ---
# This instance will be initialized during FastAPI application startup.
INFERENCE_RUNNER: Optional[InferenceModel] = None

async def initialize_inference_runner(app_config: Dict[str, Any]):
    """
    Initializes the global INFERENCE_RUNNER instance.
    This should be called during application startup (e.g., FastAPI lifespan event).
    """
    global INFERENCE_RUNNER # MOVED TO THE TOP

    # ---- Print module and global ID from inference_runner.py's perspective ----
    import sys
    current_module = sys.modules[__name__]
    print(f"[IR_PY_DEBUG_ID] id(current_module i.e. app.inference_runner): {id(current_module)}", flush=True)
    print(f"[IR_PY_DEBUG_ID] id(INFERENCE_RUNNER global var in ir_module before assignment): {id(INFERENCE_RUNNER)}", flush=True)
    print(f"[IR_PY_DEBUG_ID] Value of INFERENCE_RUNNER in ir_module before assignment: {INFERENCE_RUNNER}", flush=True)

    print("[INFERENCE_RUNNER_PY_DEBUG] initialize_inference_runner function CALLED.", flush=True)
    if INFERENCE_RUNNER is None:
        logger.info("[INIT_RUNNER_DEBUG] Current INFERENCE_RUNNER is None. Attempting to create InferenceModel...")
        print("[INIT_RUNNER_DEBUG_PRINT] Current INFERENCE_RUNNER is None. Attempting to create InferenceModel...", flush=True)
        try:
            # Temporary variable for clarity during debugging
            temp_runner_instance = InferenceModel(config=app_config)
            logger.info(f"[INIT_RUNNER_DEBUG] InferenceModel() call completed. Instance: {temp_runner_instance}")
            print(f"[INIT_RUNNER_DEBUG_PRINT] InferenceModel() call completed. Instance: {temp_runner_instance}", flush=True)
            
            INFERENCE_RUNNER = temp_runner_instance # Assign to global in this module
            logger.info(f"[INIT_RUNNER_DEBUG] Global INFERENCE_RUNNER assigned. Checking model state...")
            print(f"[INIT_RUNNER_DEBUG_PRINT] Global INFERENCE_RUNNER assigned. Checking model state...", flush=True)
            # ---- Print ID of INFERENCE_RUNNER global from ir_module AFTER assignment ----
            print(f"[IR_PY_DEBUG_ID] id(INFERENCE_RUNNER global var in ir_module AFTER assignment): {id(INFERENCE_RUNNER)}", flush=True)
            print(f"[IR_PY_DEBUG_ID] Value of INFERENCE_RUNNER in ir_module AFTER assignment: {INFERENCE_RUNNER}", flush=True)

            if INFERENCE_RUNNER.model is None:
                logger.error("[INIT_RUNNER_DEBUG] Inference runner initialized (global var is set), but INFERENCE_RUNNER.model FAILED to load. Inference will not be available.")
                print("[INIT_RUNNER_DEBUG_PRINT] Inference runner initialized (global var is set), but INFERENCE_RUNNER.model FAILED to load. Inference will not be available.", flush=True)
            else:
                logger.info("[INIT_RUNNER_DEBUG] Global inference runner initialized successfully and model is loaded.")
                print("[INIT_RUNNER_DEBUG_PRINT] Global inference runner initialized successfully and model is loaded.", flush=True)
                logger.info(f"[INIT_RUNNER_DEBUG] Model: {INFERENCE_RUNNER.model_name}, Device: {INFERENCE_RUNNER.device}")
                print(f"[INIT_RUNNER_DEBUG_PRINT] Model: {INFERENCE_RUNNER.model_name}, Device: {INFERENCE_RUNNER.device}", flush=True)
        
        except ImportError as e_import:
            logger.error(f"[INIT_RUNNER_DEBUG] ImportError during InferenceModel instantiation or its setup: {e_import}", exc_info=True)
            print(f"[INIT_RUNNER_DEBUG_PRINT] ImportError during InferenceModel instantiation or its setup: {e_import}", flush=True)
            INFERENCE_RUNNER = None # Ensure it's None if init fails
        except Exception as e:
            logger.error(f"[INIT_RUNNER_DEBUG] Exception during InferenceModel instantiation: {e}", exc_info=True)
            print(f"[INIT_RUNNER_DEBUG_PRINT] Exception during InferenceModel instantiation: {e}", flush=True)
            INFERENCE_RUNNER = None # Ensure it's None if init fails
    else:
        logger.info(f"[INIT_RUNNER_DEBUG] Inference runner already initialized. Model: {INFERENCE_RUNNER.model_name}, Device: {INFERENCE_RUNNER.device}")
        print(f"[INIT_RUNNER_DEBUG_PRINT] Inference runner already initialized. Model: {INFERENCE_RUNNER.model_name}, Device: {INFERENCE_RUNNER.device}", flush=True)

async def get_inference_runner() -> Optional[InferenceModel]:
    """
    Provides access to the global INFERENCE_RUNNER instance.
    """
    if INFERENCE_RUNNER is None:
        logger.error("Inference runner accessed before initialization.")
        # In a robust application, you might raise an error or have a retry mechanism.
    return INFERENCE_RUNNER

async def run_model_inference(
    prepared_batch: BatchType,
    config: Dict[str, Any] # Main application config
) -> Optional[List[BatchType]]:
    """
    High-level function to execute model inference using the global runner.
    """
    runner = await get_inference_runner()
    if not runner:
        logger.error("Inference runner not available. Cannot run inference.")
        return None
    if runner.model is None:
        logger.error("Inference runner's model is not loaded. Cannot run inference.")
        return None

    try:
        model_settings = config.get('model', {})
        inference_steps = model_settings.get('inference_steps', 40)
        
        # The forecast_step_hours is already part of runner.model_config,
        # but ensuring it's clear what's being used.
        
        predictions = await runner.run_inference(
            input_batch=prepared_batch,
            steps=inference_steps
        )
        
        if predictions is None: # run_inference itself will log errors
            logger.warning("Inference execution returned None (likely due to an error).")
            return None
        if not predictions: # Empty list of predictions
            logger.warning("Inference execution completed but returned no prediction steps.")
            return [] # Return empty list rather than None if inference ran but produced nothing
        
        logger.info(f"Inference processing successful, received {len(predictions)} steps.")
        return predictions

    except Exception as e:
        logger.error(f"Unexpected error during run_model_inference: {e}")
        logger.error(traceback.format_exc())
        return None 