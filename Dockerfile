# ---- Builder Stage ----
FROM python:3.9-slim as builder

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install system dependencies needed for building
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    libc6-dev \
    libffi-dev \
    libssl-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory for builder
WORKDIR /install

# Copy requirements first for better caching
COPY requirements.txt requirements-test.txt ./

# Install Python dependencies with optimizations
RUN pip install --no-cache-dir --prefix="/install" \
    --no-deps --compile \
    -r requirements.txt

# Install test dependencies for development builds (optional)
ARG BUILD_TYPE=production
RUN if [ "$BUILD_TYPE" = "development" ] ; then \
    pip install --no-cache-dir --prefix="/install" \
    --no-deps --compile \
    -r requirements-test.txt ; \
    fi

# ---- Final Stage ----
FROM python:3.9-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PATH="/usr/local/bin:$PATH"

# Create non-root user for security
RUN groupadd -r gaia && useradd -r -g gaia gaia

# Install only runtime system dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy installed Python packages from builder
COPY --from=builder /install /usr/local

# Copy application code
COPY . .

# Create required directories and set permissions
RUN mkdir -p /app/logs /app/data/cache /app/monitoring && \
    chown -R gaia:gaia /app

# Compile Python files for faster startup
RUN python -m compileall . || true

# Switch to non-root user
USER gaia

# Expose Prometheus metrics port
EXPOSE 9090

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:9090/health || exit 1

# Default entrypoint
ENTRYPOINT ["python", "-m", "gaia.validator.validator"]

# Default command (can be overridden)
CMD ["--config-file", "/app/config/production.yaml"]

# Build metadata
ARG BUILD_VERSION=unknown
ARG BUILD_COMMIT=unknown
ARG BUILD_DATE=unknown

LABEL maintainer="Gaia Validator Team" \
      version="$BUILD_VERSION" \
      commit="$BUILD_COMMIT" \
      build-date="$BUILD_DATE" \
      description="Gaia Validator v4.0 - Multi-process weather forecast validation"