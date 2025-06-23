"""
Validator Configuration Settings
===============================

Clean configuration loading and validation functions.
All configuration is loaded from environment variables with sensible defaults.
"""

import os
from typing import Dict, Any, NamedTuple
from dotenv import load_dotenv


class ValidatorConfig(NamedTuple):
    """Complete validator configuration."""
    # Network settings
    netuid: int
    wallet_name: str
    hotkey_name: str
    subtensor_network: str
    subtensor_chain_endpoint: str
    
    # Database settings
    db_name: str
    db_user: str
    db_password: str
    db_host: str
    db_port: int
    
    # Memory settings
    memory_warning_threshold_mb: int
    memory_emergency_threshold_mb: int
    memory_critical_threshold_mb: int
    memory_monitor_enabled: bool
    
    # Operational settings
    test_mode: bool
    pm2_restart_enabled: bool
    
    # Task settings
    db_sync_interval_hours: int
    watchdog_timeout: int
    
    # DB sync settings
    is_source_validator_for_db_sync: bool


def load_validator_config(args) -> ValidatorConfig:
    """
    Load complete validator configuration from environment and args.
    
    Args:
        args: Command line arguments
        
    Returns:
        ValidatorConfig: Complete configuration object
    """
    load_dotenv(".env")
    
    return ValidatorConfig(
        # Network settings
        netuid=args.netuid if args.netuid else int(os.getenv("NETUID", 237)),
        wallet_name=args.wallet if args.wallet else os.getenv("WALLET_NAME", "default"),
        hotkey_name=args.hotkey if args.hotkey else os.getenv("HOTKEY_NAME", "default"),
        subtensor_network=(
            args.subtensor.network
            if hasattr(args, "subtensor") and hasattr(args.subtensor, "network")
            else os.getenv("SUBTENSOR_NETWORK", "test")
        ),
        subtensor_chain_endpoint=(
            args.subtensor.chain_endpoint
            if hasattr(args, "subtensor") and hasattr(args.subtensor, "chain_endpoint")
            else os.getenv("SUBTENSOR_ADDRESS", "wss://test.finney.opentensor.ai:443/")
        ),
        
        # Database settings
        db_name=os.getenv("DB_NAME", "gaia_validator"),
        db_user=os.getenv("DB_USER", "postgres"),
        db_password=os.getenv("DB_PASSWORD", "postgres"),
        db_host=os.getenv("DB_HOST", "localhost"),
        db_port=int(os.getenv("DB_PORT", "5432")),
        
        # Memory settings
        memory_warning_threshold_mb=int(os.getenv('VALIDATOR_MEMORY_WARNING_THRESHOLD_MB', '8000')),
        memory_emergency_threshold_mb=int(os.getenv('VALIDATOR_MEMORY_EMERGENCY_THRESHOLD_MB', '10000')),
        memory_critical_threshold_mb=int(os.getenv('VALIDATOR_MEMORY_CRITICAL_THRESHOLD_MB', '12000')),
        memory_monitor_enabled=os.getenv('VALIDATOR_MEMORY_MONITORING_ENABLED', 'true').lower() in ['true', '1', 'yes'],
        
        # Operational settings
        test_mode=getattr(args, 'test', False),
        pm2_restart_enabled=os.getenv('VALIDATOR_PM2_RESTART_ENABLED', 'true').lower() in ['true', '1', 'yes'],
        
        # Task settings
        db_sync_interval_hours=int(os.getenv("DB_SYNC_INTERVAL_HOURS", "1")) if not getattr(args, 'test', False) else 0.25,
        watchdog_timeout=3600,  # 1 hour default timeout
        
        # DB sync settings
        is_source_validator_for_db_sync=os.getenv("IS_SOURCE_VALIDATOR_FOR_DB_SYNC", "False").lower() == "true",
    )


def get_network_config(config: ValidatorConfig) -> Dict[str, Any]:
    """Extract network-specific configuration."""
    return {
        'netuid': config.netuid,
        'wallet_name': config.wallet_name,
        'hotkey_name': config.hotkey_name,
        'subtensor_network': config.subtensor_network,
        'subtensor_chain_endpoint': config.subtensor_chain_endpoint,
    }


def get_database_config(config: ValidatorConfig) -> Dict[str, Any]:
    """Extract database-specific configuration."""
    return {
        'db_name': config.db_name,
        'db_user': config.db_user,
        'db_password': config.db_password,
        'db_host': config.db_host,
        'db_port': config.db_port,
    }


def get_memory_config(config: ValidatorConfig) -> Dict[str, Any]:
    """Extract memory management configuration."""
    return {
        'memory_warning_threshold_mb': config.memory_warning_threshold_mb,
        'memory_emergency_threshold_mb': config.memory_emergency_threshold_mb,
        'memory_critical_threshold_mb': config.memory_critical_threshold_mb,
        'memory_monitor_enabled': config.memory_monitor_enabled,
        'pm2_restart_enabled': config.pm2_restart_enabled,
    }


def get_task_config(config: ValidatorConfig) -> Dict[str, Any]:
    """Extract task-specific configuration."""
    return {
        'test_mode': config.test_mode,
        'watchdog_timeout': config.watchdog_timeout,
        'db_sync_interval_hours': config.db_sync_interval_hours,
        'is_source_validator_for_db_sync': config.is_source_validator_for_db_sync,
    }
    pm2_restart_enabled: bool
    
    # Task settings
    test_mode: bool
    db_sync_enabled: bool
    score_sender_enabled: bool
    
    # Sync settings
    metagraph_sync_interval: int
    db_check_interval: int
    memory_log_interval: int


def load_validator_config(args=None) -> ValidatorConfig:
    """
    Load complete validator configuration from environment and args.
    
    Args:
        args: Command line arguments (optional)
        
    Returns:
        ValidatorConfig: Complete configuration object
    """
    load_dotenv(".env")
    
    return ValidatorConfig(
        # Network settings
        netuid=getattr(args, 'netuid', None) or int(os.getenv("NETUID", 237)),
        wallet_name=getattr(args, 'wallet', None) or os.getenv("WALLET_NAME", "default"),
        hotkey_name=getattr(args, 'hotkey', None) or os.getenv("HOTKEY_NAME", "default"),
        subtensor_network=_get_subtensor_network(args),
        subtensor_chain_endpoint=_get_subtensor_endpoint(args),
        
        # Database settings
        db_name=os.getenv("DB_NAME", "gaia_validator"),
        db_user=os.getenv("DB_USER", "postgres"),
        db_password=os.getenv("DB_PASSWORD", "postgres"),
        db_host=os.getenv("DB_HOST", "localhost"),
        db_port=int(os.getenv("DB_PORT", "5432")),
        
        # Memory settings
        memory_warning_threshold_mb=int(os.getenv('VALIDATOR_MEMORY_WARNING_THRESHOLD_MB', '8000')),
        memory_emergency_threshold_mb=int(os.getenv('VALIDATOR_MEMORY_EMERGENCY_THRESHOLD_MB', '10000')),
        memory_critical_threshold_mb=int(os.getenv('VALIDATOR_MEMORY_CRITICAL_THRESHOLD_MB', '12000')),
        memory_monitor_enabled=os.getenv('VALIDATOR_MEMORY_MONITORING_ENABLED', 'true').lower() in ['true', '1', 'yes'],
        pm2_restart_enabled=os.getenv('VALIDATOR_PM2_RESTART_ENABLED', 'true').lower() in ['true', '1', 'yes'],
        
        # Task settings
        test_mode=getattr(args, 'test', False),
        db_sync_enabled=os.getenv("DB_SYNC_ENABLED", "True").lower() == "true",
        score_sender_enabled=os.getenv("SCORE_SENDER_ON", "False").lower() == "true",
        
        # Sync settings
        metagraph_sync_interval=300,  # 5 minutes
        db_check_interval=300,        # 5 minutes
        memory_log_interval=300,      # 5 minutes
    )


def get_network_config(config: ValidatorConfig) -> Dict[str, Any]:
    """Extract network-specific configuration."""
    return {
        'netuid': config.netuid,
        'wallet_name': config.wallet_name,
        'hotkey_name': config.hotkey_name,
        'subtensor_network': config.subtensor_network,
        'subtensor_chain_endpoint': config.subtensor_chain_endpoint,
        'metagraph_sync_interval': config.metagraph_sync_interval
    }


def get_database_config(config: ValidatorConfig) -> Dict[str, Any]:
    """Extract database-specific configuration."""
    return {
        'db_name': config.db_name,
        'db_user': config.db_user,
        'db_password': config.db_password,
        'db_host': config.db_host,
        'db_port': config.db_port,
        'db_check_interval': config.db_check_interval
    }


def get_memory_config(config: ValidatorConfig) -> Dict[str, Any]:
    """Extract memory management configuration."""
    return {
        'memory_warning_threshold_mb': config.memory_warning_threshold_mb,
        'memory_emergency_threshold_mb': config.memory_emergency_threshold_mb,
        'memory_critical_threshold_mb': config.memory_critical_threshold_mb,
        'memory_monitor_enabled': config.memory_monitor_enabled,
        'pm2_restart_enabled': config.pm2_restart_enabled,
        'memory_log_interval': config.memory_log_interval
    }


def get_task_config(config: ValidatorConfig) -> Dict[str, Any]:
    """Extract task-specific configuration."""
    return {
        'test_mode': config.test_mode,
        'db_sync_enabled': config.db_sync_enabled,
        'score_sender_enabled': config.score_sender_enabled
    }


def _get_subtensor_network(args) -> str:
    """Get subtensor network from args or environment."""
    if (hasattr(args, "subtensor") and 
        hasattr(args.subtensor, "network")):
        return args.subtensor.network
    return os.getenv("SUBTENSOR_NETWORK", "test")


def _get_subtensor_endpoint(args) -> str:
    """Get subtensor chain endpoint from args or environment."""
    if (hasattr(args, "subtensor") and 
        hasattr(args.subtensor, "chain_endpoint")):
        return args.subtensor.chain_endpoint
    return os.getenv("SUBTENSOR_ADDRESS", "wss://test.finney.opentensor.ai:443/") 