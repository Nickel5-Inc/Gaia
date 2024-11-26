import torch
import torch.nn as nn
import pytorch_lightning as pl
from huggingface_hub import hf_hub_download
import importlib.util
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


class FallbackGeoMagModel(pl.LightningModule):
    """
    A simple fallback model to use when the Hugging Face model isn't available.
    """

    def __init__(self):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=1, hidden_size=32, num_layers=1, batch_first=True
        )
        self.linear = nn.Linear(32, 1)

    def forward(self, x):
        # A simple persistence model: predict the same as the last input value
        out, _ = self.lstm(x)
        return self.linear(out[:, -1, :])  # Return the last time step


class GeoMagBaseModel:
    """
    A wrapper class for downloading, initializing, and managing the GeoMagModel.
    """

    def __init__(self, repo_id="Nickel5HF/geomagmodel", filename="BaseModel.py"):
        """
        Initialize the GeoMagBaseModel by attempting to download and load the Hugging Face model.
        Fallback to a default model if downloading or loading fails.

        Args:
            repo_id (str): The Hugging Face repository identifier.
            filename (str): The filename in the repository to download and use as the model.
        """
        self.model = None
        try:
            # Attempt to download and load the GeoMagModel from Hugging Face
            logger.info(
                f"Attempting to download model from repo: {repo_id}, file: {filename}"
            )
            model_path = hf_hub_download(repo_id=repo_id, filename=filename)
            spec = importlib.util.spec_from_file_location("BaseModel", model_path)
            model_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(model_module)
            self.model = model_module.GeoMagModel()
            logger.info("Successfully loaded GeoMagModel from Hugging Face.")
        except Exception as e:
            # Fall back to the default model if Hugging Face model fails
            logger.warning(f"Failed to load Hugging Face model: {e}")
            logger.info("Falling back to the default GeoMagModel.")
            self.model = FallbackGeoMagModel()

    def predict(self, x: torch.Tensor) -> torch.Tensor:
        """
        Perform predictions using the loaded model.

        Args:
            x (torch.Tensor): Input tensor for the model.

        Returns:
            torch.Tensor: Predicted output.
        """
        try:
            self.model.eval()  # Set the model to evaluation mode
            with torch.no_grad():
                return self.model(x)
        except Exception as e:
            logger.error(f"Error during prediction: {e}")
            return torch.tensor([])  # Return an empty tensor on failure

    @property
    def is_fallback(self) -> bool:
        """
        Check if the fallback model is being used.

        Returns:
            bool: True if the fallback model is used, False otherwise.
        """
        return isinstance(self.model, FallbackGeoMagModel)
