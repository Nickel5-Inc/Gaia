"""Redis configuration with environment variable support."""

import os
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Redis configuration with defaults
REDIS_CONFIG = {
    "host": os.getenv("REDIS_HOST", "localhost"),
    "port": int(os.getenv("REDIS_PORT", "6379")),
    "db": int(os.getenv("REDIS_DB", "0")),
    "password": os.getenv("REDIS_PASSWORD", None),
    "max_connections": int(os.getenv("REDIS_MAX_CONNECTIONS", "10")),
    "socket_timeout": int(os.getenv("REDIS_SOCKET_TIMEOUT", "5")),
    "socket_connect_timeout": int(os.getenv("REDIS_CONNECT_TIMEOUT", "5")),
    "retry_on_timeout": bool(os.getenv("REDIS_RETRY_ON_TIMEOUT", "True")),
    "health_check_interval": int(os.getenv("REDIS_HEALTH_CHECK_INTERVAL", "30")),
}

# Cache TTL settings (in seconds)
CACHE_TTL = {
    "default": int(os.getenv("CACHE_TTL_DEFAULT", "300")),  # 5 minutes
    "miner_info": int(os.getenv("CACHE_TTL_MINER_INFO", "300")),  # 5 minutes
    "recent_scores": int(os.getenv("CACHE_TTL_RECENT_SCORES", "60")),  # 1 minute
    "active_miners": int(os.getenv("CACHE_TTL_ACTIVE_MINERS", "60")),  # 1 minute
}

def get_redis_url() -> str:
    """Get Redis URL from configuration."""
    config = REDIS_CONFIG
    auth = f":{config['password']}@" if config["password"] else ""
    return f"redis://{auth}{config['host']}:{config['port']}/{config['db']}"

def get_redis_config() -> Dict[str, Any]:
    """Get Redis configuration dictionary."""
    return REDIS_CONFIG

def get_cache_ttl(key_type: str) -> int:
    """Get cache TTL for specific key type."""
    return CACHE_TTL.get(key_type, CACHE_TTL["default"]) 