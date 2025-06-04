FROM python:3.10-slim

WORKDIR /app

# Install system dependencies that might be needed by some Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    make \
    libnetcdf-dev \
    libhdf5-dev \
    && rm -rf /var/lib/apt/lists/*

# Set PYTHONPATH to include the root directory where your 'app' package structure begins
# Since your code is copied to /app/app, and you want to import 'app.main',
# /app needs to be in PYTHONPATH so it finds the outer 'app' directory as a potential namespace for packages.
ENV PYTHONPATH="/app:${PYTHONPATH}"

# Copy requirements first to leverage Docker cache
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
# Your main application package 'app' (containing main.py, inference_runner.py, etc.)
# will be located at /app/app within the container.
COPY ./app /app/app
COPY ./config /app/config

# --- Optional: For local custom Aurora models ---
# 1. Create a directory in your inference_service (e.g., `local_models/my_aurora_model`)
# 2. Place your model checkpoint (e.g., `my_custom_weights.ckpt`) and any other necessary files (e.g., `model_config.json`) there.
# 3. Uncomment and adjust the COPY line below to copy them into the image.
# 4. Update `config/settings.yaml` to point `model_repo` to the path inside the container (e.g., "/app/local_models/my_aurora_model")
#    and `checkpoint` to your checkpoint file name.
# COPY ./local_models/my_aurora_model /app/local_models/my_aurora_model

ENV HF_HOME=/root/.cache/huggingface
# Ensure the directory exists and is writable
RUN mkdir -p /root/.cache/huggingface && chmod 777 /root/.cache/huggingface

# Expose the port the app runs on (RunPod typically uses 8000 or injects its own)
EXPOSE 8000

# Command to run the application as a module
# Python will look for a package named 'app' (which is /app/app)
# and then run 'main.py' within that package.
CMD ["python", "-u", "-m", "app.main"] 