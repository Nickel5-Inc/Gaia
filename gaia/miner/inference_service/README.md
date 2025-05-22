# Weather Inference Service

This service provides an API endpoint to run weather model inference. 
It's designed to be packaged as a Docker container and run in a serverless-style environment.

## Setup

1.  **Build the Docker image:**
    ```bash
    docker build -t weather-inference-service .
    ```

2.  **Run the Docker container:**
    ```bash
    docker run -p 8000:8000 weather-inference-service
    ```

## API

### `/health`

*   **Method:** GET
*   **Description:** Returns the health status of the service.
*   **Response:**
    ```json
    {
        "status": "ok"
    }
    ```

### `/run_inference`

*   **Method:** POST
*   **Description:** Runs the weather model inference.
*   **Request Body:**
    ```json
    {
        "gfs_timestep_1": "base64_encoded_pickle_xarray_dataset_1",
        "gfs_timestep_2": "base64_encoded_pickle_xarray_dataset_2"
    }
    ```
*   **Response:** (Example - actual structure TBD based on forecast output)
    ```json
    {
        "job_id": "some_unique_id",
        "status": "completed",
        "forecast_data": {
            // Structure of your forecast data
        }
    }
    ```
    or if an error occurs:
    ```json
    {
        "error": "Error message"
    }
    ```

## Configuration

Configuration is managed via `config/settings.yaml`.
You can modify model paths, inference parameters, etc., in this file.
To use a custom configuration when running the Docker container, you can mount it as a volume:
```bash
docker run -p 8000:8000 -v /path/to/your/custom_settings.yaml:/app/config/settings.yaml weather-inference-service
``` 