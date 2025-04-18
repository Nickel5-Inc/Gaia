import torch
from aurora import Aurora
from fiber.logging_utils import get_logger
import traceback
logger = get_logger(__name__)

def load_base_weather_model(device="cuda", model_repo="microsoft/aurora", checkpoint="aurora-0.25-pretrained.ckpt", use_lora=False):
    """
    Loads and configures the base Aurora weather model.

    Args:
        device (str): The target device ('cuda' or 'cpu'). Defaults to 'cuda'.
        model_repo (str): The HuggingFace repository containing the model checkpoint.
        checkpoint (str): The name of the checkpoint file within the repository.
        use_lora (bool): Whether the model checkpoint uses LoRA. Defaults to False for pretrained.

    Returns:
        aurora.Aurora: The loaded and configured model instance.

    Raises:
        RuntimeError: If the model fails to load or configure.
        ImportError: If the 'aurora' library is not installed.
    """
    logger.info(f"Attempting to load base Aurora model: {model_repo}/{checkpoint}")

    target_device = torch.device(device if torch.cuda.is_available() and device == "cuda" else "cpu")
    logger.info(f"Target device for base model: {target_device}")

    try:
        model = Aurora(use_lora=use_lora)
        logger.debug(f"Aurora model instantiated (use_lora={use_lora})")

        logger.info(f"Loading checkpoint {checkpoint} from {model_repo}...")
        model.load_checkpoint(model_repo, checkpoint)
        logger.info("Checkpoint loaded successfully.")

        model.eval()
        logger.debug("Model set to evaluation mode.")

        model = model.to(target_device)
        logger.info(f"Model moved to device: {target_device}")

        logger.info("Base Aurora model loaded and configured successfully.")
        return model

    except ImportError as e:
        logger.error("The 'microsoft-aurora' library is required but not installed.")
        logger.error("Please install it, e.g., using 'pip install microsoft-aurora'")
        raise e
    except Exception as e:
        logger.error(f"Failed to load base Aurora model: {e}")
        logger.error(traceback.format_exc())
        raise RuntimeError(f"Could not initialize or load base Aurora model: {e}") from e
