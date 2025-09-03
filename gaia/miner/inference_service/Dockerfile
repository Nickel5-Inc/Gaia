# Build stage - includes build dependencies
FROM python:3.10-slim AS builder

WORKDIR /app

# Use uv (Astral) for fast, reliable installs
ENV PATH="/root/.local/bin:${PATH}"

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    make \
    libnetcdf-dev \
    libhdf5-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install to a local directory
COPY requirements.txt requirements.txt
# Install uv and dependencies
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    uv pip install --no-cache-dir --user -r requirements.txt && \
    uv pip install --no-cache-dir --user huggingface-hub boto3 botocore

# Download Aurora model in builder stage for better layer caching
ARG AURORA_MODEL_REPO="microsoft/aurora"
ARG AURORA_CHECKPOINT_NAME="aurora-0.25-pretrained.ckpt"

RUN mkdir -p /app/models/aurora_local && \
    echo "Downloading model ${AURORA_MODEL_REPO} checkpoint ${AURORA_CHECKPOINT_NAME}" && \
    python -c "import os; os.environ['HF_HUB_CACHE']='/tmp/hf_cache'; \
from huggingface_hub import snapshot_download; \
snapshot_download(repo_id='${AURORA_MODEL_REPO}', \
                  local_dir='/app/models/aurora_local', \
                  allow_patterns=['${AURORA_CHECKPOINT_NAME}', 'config.json', '*.pickle'])" && \
    rm -rf /tmp/hf_cache && \
    echo "Model download complete" && \
    ls -la /app/models/aurora_local

# Runtime stage - minimal dependencies only
FROM python:3.10-slim

WORKDIR /app

# Install only runtime system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnetcdf-dev \
    libhdf5-dev \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy Python packages and Aurora model from builder stage
COPY --from=builder /root/.local /root/.local
COPY --from=builder /app/models /app/models

# Set PYTHONPATH and PATH
ENV PYTHONPATH="/app${PYTHONPATH:+:${PYTHONPATH}}"
ENV PATH="/root/.local/bin:${PATH}"

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