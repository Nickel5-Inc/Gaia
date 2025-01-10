import os
import json
import yaml
from typing import Dict, Any, Optional
from pathlib import Path

from fiber.logging_utils import get_logger
from gaia.validator.core.constants import (
    DEFAULT_CHAIN_ENDPOINT,
    DEFAULT_NETWORK,
    DEFAULT_WALLET,
    DEFAULT_HOTKEY,
    DEFAULT_NETUID
)

logger = get_logger(__name__)

class ConfigManager:
    """
    Manages validator configuration and settings.
    
    This class handles:
    1. Configuration loading
    2. Settings validation
    3. Environment variables
    4. Default values
    5. Configuration persistence
    """
    
    def __init__(
        self,
        config_path: Optional[str] = None,
        env_prefix: str = "GAIA_"
    ):
        self.config_path = config_path or self._get_default_config_path()
        self.env_prefix = env_prefix
        self.config = {}
        
        # Load configuration
        self._load_config()
        
    def _get_default_config_path(self) -> str:
        """Get default configuration file path."""
        home = str(Path.home())
        return os.path.join(home, ".gaia", "config.yaml")
        
    def _load_config(self):
        """Load configuration from file and environment."""
        try:
            # Load from file if exists
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as f:
                    if self.config_path.endswith('.json'):
                        self.config = json.load(f)
                    else:
                        self.config = yaml.safe_load(f)
                logger.info(f"Loaded config from {self.config_path}")
            else:
                logger.info("No config file found, using defaults")
                
            # Override with environment variables
            self._load_env_vars()
            
            # Set defaults for missing values
            self._set_defaults()
            
            # Validate configuration
            self._validate_config()
            
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            # Use defaults on error
            self._set_defaults()
            
    def _load_env_vars(self):
        """Load configuration from environment variables."""
        try:
            for key, value in os.environ.items():
                if key.startswith(self.env_prefix):
                    # Convert GAIA_CHAIN_ENDPOINT to chain.endpoint
                    config_key = key[len(self.env_prefix):].lower()
                    config_key = config_key.replace('_', '.')
                    
                    # Parse value
                    try:
                        # Try to parse as JSON for complex values
                        value = json.loads(value)
                    except json.JSONDecodeError:
                        # Keep as string if not valid JSON
                        pass
                        
                    self._set_nested_value(config_key, value)
                    
        except Exception as e:
            logger.error(f"Error loading environment variables: {e}")
            
    def _set_nested_value(self, key: str, value: Any):
        """Set nested dictionary value using dot notation."""
        keys = key.split('.')
        current = self.config
        
        for k in keys[:-1]:
            if k not in current:
                current[k] = {}
            current = current[k]
            
        current[keys[-1]] = value
        
    def _set_defaults(self):
        """Set default values for missing configuration."""
        defaults = {
            'chain': {
                'endpoint': DEFAULT_CHAIN_ENDPOINT,
                'network': DEFAULT_NETWORK,
                'netuid': DEFAULT_NETUID
            },
            'wallet': {
                'name': DEFAULT_WALLET,
                'hotkey': DEFAULT_HOTKEY,
                'path': os.path.expanduser('~/.gaia/wallet')
            },
            'database': {
                'host': 'localhost',
                'port': 5432,
                'name': 'gaia',
                'user': 'gaia',
                'password': None,
                'pool_size': 10
            },
            'logging': {
                'level': 'INFO',
                'file': os.path.expanduser('~/.gaia/validator.log'),
                'max_size': 10485760,  # 10MB
                'backup_count': 5
            },
            'monitoring': {
                'enabled': True,
                'port': 8000,
                'metrics_interval': 60
            },
            'timeouts': {
                'network': 180,
                'database': 60,
                'scoring': 300
            }
        }
        
        # Recursively update missing values
        self._update_recursive(self.config, defaults)
        
    def _update_recursive(self, current: Dict, defaults: Dict):
        """Recursively update dictionary with defaults."""
        for key, value in defaults.items():
            if key not in current:
                current[key] = value
            elif isinstance(value, dict) and isinstance(current[key], dict):
                self._update_recursive(current[key], value)
                
    def _validate_config(self):
        """Validate configuration values."""
        try:
            # Validate required values
            required = [
                'chain.endpoint',
                'chain.network',
                'chain.netuid',
                'wallet.name',
                'wallet.hotkey'
            ]
            
            for key in required:
                if not self.get(key):
                    raise ValueError(f"Missing required config: {key}")
                    
            # Validate types
            validations = [
                ('chain.netuid', int),
                ('database.port', int),
                ('database.pool_size', int),
                ('monitoring.port', int),
                ('monitoring.metrics_interval', int),
                ('timeouts.network', int),
                ('timeouts.database', int),
                ('timeouts.scoring', int)
            ]
            
            for key, expected_type in validations:
                value = self.get(key)
                if value is not None and not isinstance(value, expected_type):
                    raise TypeError(
                        f"Invalid type for {key}: "
                        f"expected {expected_type.__name__}, "
                        f"got {type(value).__name__}"
                    )
                    
            logger.info("Configuration validated successfully")
            
        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            raise
            
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.
        
        Args:
            key: Configuration key (e.g. 'database.host')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        try:
            current = self.config
            for k in key.split('.'):
                current = current[k]
            return current
        except (KeyError, TypeError):
            return default
            
    def set(self, key: str, value: Any):
        """
        Set configuration value using dot notation.
        
        Args:
            key: Configuration key (e.g. 'database.host')
            value: Value to set
        """
        self._set_nested_value(key, value)
        
    def save(self):
        """Save configuration to file."""
        try:
            # Create directory if needed
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            
            # Save configuration
            with open(self.config_path, 'w') as f:
                if self.config_path.endswith('.json'):
                    json.dump(self.config, f, indent=2)
                else:
                    yaml.safe_dump(self.config, f, indent=2)
                    
            logger.info(f"Saved config to {self.config_path}")
            
        except Exception as e:
            logger.error(f"Error saving config: {e}")
            
    def get_all(self) -> Dict:
        """Get complete configuration dictionary."""
        return self.config.copy()
        
    def reset(self):
        """Reset configuration to defaults."""
        self.config = {}
        self._set_defaults()
        
    def update(self, updates: Dict):
        """
        Update configuration with new values.
        
        Args:
            updates: Dictionary of updates
        """
        self._update_recursive(self.config, updates)
        self._validate_config() 