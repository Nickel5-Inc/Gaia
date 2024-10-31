from huggingface_hub import hf_hub_download
import importlib.util


class GeoMagBaseModel:
    """
    A wrapper class for downloading and initializing the GeoMagModel from Hugging Face.
    """

    def __init__(self, repo_id="vValentine7/geomagindex", filename="BaseModel.py"):
        """
        Initialize by downloading and loading the GeoMagModel class from Hugging Face.

        Args:
            repo_id (str): The Hugging Face repository identifier.
            filename (str): The filename in the repository to download and use as the model.
        """
        # Download the model script file from Hugging Face
        model_path = hf_hub_download(repo_id=repo_id, filename=filename)

        # Dynamically load the GeoMagModel class from the downloaded file
        spec = importlib.util.spec_from_file_location("BaseModel", model_path)
        model_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(model_module)

        # Initialize GeoMagModel instance from the loaded module
        self.model = model_module.GeoMagModel()  # Use the downloaded class directly
