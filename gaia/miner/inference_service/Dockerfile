FROM python:3.10-slim

WORKDIR /app

# Install system dependencies that might be needed by some Python packages
# For example, if xarray or its dependencies need them. Adjust as necessary.
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     gcc \
#     g++ \
#     make \
#     && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY ./app /app/app
COPY ./config /app/config

# --- Optional: For local custom Aurora models ---
# 1. Create a directory in your inference_service (e.g., `local_models/my_aurora_model`)
# 2. Place your model checkpoint (e.g., `my_custom_weights.ckpt`) and any other necessary files (e.g., `model_config.json`) there.
# 3. Uncomment and adjust the COPY line below to copy them into the image.
# 4. Update `config/settings.yaml` to point `model_repo` to the path inside the container (e.g., "/app/local_models/my_aurora_model")
#    and `checkpoint` to your checkpoint file name.
# COPY ./local_models/my_aurora_model /app/local_models/my_aurora_model

# Expose the port the app runs on
EXPOSE 8000

# Command to run the application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"] 