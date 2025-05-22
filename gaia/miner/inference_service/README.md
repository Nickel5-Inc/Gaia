# Weather Inference Service

This service provides an API endpoint to run weather model inference using the Aurora model.
It's designed to be packaged as a Docker container and run in a serverless-style environment or as a standalone service.

## Features

*   Runs Aurora weather model inference.
*   Accepts serialized Aurora Batch objects as input.
*   Returns serialized forecast data.
*   Configurable for Hugging Face Hub models or local custom models/weights.
*   Simple HTTP API with health check and inference endpoints.

## Setup and Running

**Prerequisites:**
*   Docker installed.
*   If using a GPU, NVIDIA drivers and NVIDIA Container Toolkit installed on the host machine.

**1. Configuration (`config/settings.yaml`):**

   Before building the image, review and customize `config/settings.yaml`:

   *   **Model Source:**
       *   **Using Hugging Face Hub (Default):**
           ```yaml
           model:
             model_repo: "microsoft/aurora"
             checkpoint: "aurora-0.25-pretrained.ckpt"
             device: "auto" # or "cuda", "cpu"
             # ... other model params
           ```
       *   **Using a Local Custom Model/Weights:**
           1.  Create a directory for your model within the `inference_service` project, e.g., `inference_service/local_models/my_custom_aurora/`.
           2.  Place your Aurora model checkpoint (e.g., `my_weights.ckpt`) and any other necessary files (like `model_config.json` if your model requires it) into this directory.
           3.  Update `config/settings.yaml`:
               ```yaml
               model:
                 model_repo: "/app/local_models/my_custom_aurora" # Path INSIDE the container
                 checkpoint: "my_weights.ckpt"
                 device: "auto" # or "cuda", "cpu"
                 # ... other model params
               ```
   *   **Other Parameters:** Adjust `inference_steps`, `forecast_step_hours`, API port, logging level, etc., as needed.

**2. Update Dockerfile for Local Models (If Applicable):**

   If you are using a local custom model, you need to ensure it's copied into the Docker image.
   Open the `Dockerfile` and uncomment/edit the `COPY` instruction:

   ```dockerfile
   # ... (other Dockerfile content)
   COPY ./app /app/app
   COPY ./config /app/config

   # --- Optional: For local custom Aurora models ---
   # ... (explanatory comments)
   # COPY ./local_models/my_custom_aurora /app/local_models/my_custom_aurora # <--- UNCOMMENT AND ADJUST THIS LINE

   EXPOSE 8000
   # ... (rest of Dockerfile)
   ```
   Make sure the source path (`./local_models/my_custom_aurora`) matches the directory you created in step 1.3.1, and the destination path (`/app/local_models/my_custom_aurora`) matches what you set for `model_repo` in `settings.yaml`.

**3. Build the Docker Image:**

   Navigate to the `inference_service` directory (e.g., `gaia/miner/inference_service/`) in your terminal.

   *   **For CPU inference:**
       ```bash
       docker build -t weather-inference-service .
       ```
   *   **For GPU inference:**
       Ensure your `Dockerfile`'s base image supports CUDA (e.g., `FROM nvidia/cuda:11.8.0-cudnn8-runtime-ubuntu22.04` instead of `FROM python:3.10-slim`) and that PyTorch is installed with CUDA support in `requirements.txt`.
       Then build:
       ```bash
       docker build -t weather-inference-service .
       ```

**4. Run the Docker Container:**

   *   **CPU:**
       ```bash
       docker run -p 8000:8000 weather-inference-service
       ```
   *   **GPU:**
       ```bash
       docker run --gpus all -p 8000:8000 weather-inference-service
       ```

   To use a custom configuration file without rebuilding the image, you can mount it as a volume:
   ```bash
   docker run -p 8000:8000 -v /path/to/your/custom_settings.yaml:/app/config/settings.yaml weather-inference-service
   ```

## API Endpoints

### `GET /health`

*   **Description:** Returns the health status of the service.
*   **Response (Success 200):**
    ```json
    {
        "status": "ok"
    }
    ```

### `POST /run_inference`

*   **Description:** Runs the weather model inference using a pre-prepared Aurora Batch.
*   **Request Body (application/json):**
    ```json
    {
        "serialized_aurora_batch": "base64_encoded_pickled_aurora_batch_object"
    }
    ```
    (The `serialized_aurora_batch` is the Aurora `Batch` object that has been pickled and then base64 encoded.)

*   **Response (Success 200):** (Example - actual structure of `forecast_data` depends on `serialize_forecast_data` implementation)
    ```json
    {
        "service_job_id": "some_unique_uuid",
        "status": "completed",
        "forecast_data": {
            "message": "Forecast generated, but serialization of Batch list is a placeholder.",
            "num_steps": 40 
            // Or, e.g.: "forecast_dataset_b64": "base64_encoded_pickled_xarray_dataset"
        }
    }
    ```
*   **Response (Error 400/500):**
    ```json
    {
        "service_job_id": "some_unique_uuid",
        "status": "error",
        "error": "Error message detailing what went wrong."
    }
    ```
    Or, for Pydantic validation errors or some direct FastAPI errors:
    ```json
    {
        "detail": "Error message or validation error details"
    }
    ```

## Development

*   **Dependencies:** Listed in `requirements.txt`. Install with `pip install -r requirements.txt`.
*   **Local Execution (without Docker):**
    ```bash
    # Ensure you have the necessary environment variables or config/settings.yaml configured
    python app/main.py 
    ```
    The service will run on `http://localhost:8000` by default (or as configured).

## Important Notes for Implementation:

*   **`create_aurora_batch_from_gfs` (Miner Side):** The logic for creating the initial Aurora Batch (from GFS data or other sources) is now expected to reside on the client-side (e.g., the miner application). The miner should prepare the `Batch` object, pickle it, base64 encode it, and then send it in the `serialized_aurora_batch` field of the `/run_inference` request.
*   **`serialize_forecast_data`:** The function `app/data_preprocessor.py::serialize_forecast_data` is a placeholder. You MUST implement the actual serialization of your model's output (which is a list of Aurora `Batch` objects) into a JSON-serializable format that the client (miner) expects to receive.
*   **GPU Support:** For GPU inference, ensure the Docker base image includes CUDA, PyTorch is installed with GPU support, and the host machine has NVIDIA drivers and the NVIDIA Container Toolkit.
*   **Logging:** Configure logging levels and format in `config/settings.yaml` or directly in `app/main.py`.
*   **Error Handling:** The service includes basic error handling. Extend as needed for more specific error conditions. 
``` 