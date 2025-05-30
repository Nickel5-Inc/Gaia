"""
pgBackRest Integration for Gaia Validator Database Synchronization

This module provides integration between the Gaia validator and pgBackRest
for database synchronization across validator nodes.
"""

import os
import subprocess
import logging
import asyncio
import configparser
from pathlib import Path
from typing import Optional, Dict, Any, Tuple

logger = logging.getLogger(__name__)


class PgBackRestIntegration:
    """Handles pgBackRest integration for database synchronization"""
    
    def __init__(self):
        self.config = {}
        self.is_available = False
        self.stanza_name = "gaia"
        self._load_config()
    
    def _load_config(self) -> None:
        """Load pgBackRest configuration from environment variables"""
        try:
            # Load configuration from environment variables with PGBACKREST_ prefix
            env_mapping = {
                'AZURE_STORAGE_ACCOUNT': 'PGBACKREST_AZURE_STORAGE_ACCOUNT',
                'AZURE_STORAGE_KEY': 'PGBACKREST_AZURE_STORAGE_KEY', 
                'AZURE_CONTAINER': 'PGBACKREST_AZURE_CONTAINER',
                'STANZA_NAME': 'PGBACKREST_STANZA_NAME',
                'PGDATA': 'PGBACKREST_PGDATA',
                'PGPORT': 'PGBACKREST_PGPORT',
                'PGUSER': 'PGBACKREST_PGUSER',
                'PRIMARY_HOST': 'PGBACKREST_PRIMARY_HOST',
                'REPLICA_HOSTS': 'PGBACKREST_REPLICA_HOSTS'
            }
            
            for internal_key, env_var in env_mapping.items():
                value = os.getenv(env_var)
                if value:
                    self.config[internal_key] = value
            
            # Set stanza name from config or default
            self.stanza_name = self.config.get('STANZA_NAME', 'gaia')
            
            # Check if pgBackRest is properly configured
            if self._check_pgbackrest_available():
                self.is_available = True
                logger.info("pgBackRest integration available and configured")
            else:
                logger.info("pgBackRest not properly configured or unavailable")
                
        except Exception as e:
            logger.warning(f"Failed to load pgBackRest configuration: {e}")
    
    def _check_pgbackrest_available(self) -> bool:
        """Check if pgBackRest is available and configured"""
        try:
            # Check if pgbackrest command exists
            result = subprocess.run(['which', 'pgbackrest'], 
                                  capture_output=True, text=True)
            if result.returncode != 0:
                logger.debug("pgBackRest command not found")
                return False
            
            # Check if configuration file exists OR we have environment variables
            config_file_exists = os.path.exists('/etc/pgbackrest/pgbackrest.conf')
            
            # Check if we have required Azure configuration in environment
            required_keys = ['AZURE_STORAGE_ACCOUNT', 'AZURE_STORAGE_KEY', 'AZURE_CONTAINER']
            env_config_complete = all(key in self.config for key in required_keys)
            
            if not config_file_exists and not env_config_complete:
                logger.debug("pgBackRest config file and environment variables both incomplete")
                return False
            
            return True
            
        except Exception as e:
            logger.debug(f"pgBackRest availability check failed: {e}")
            return False
    
    async def is_primary_node(self) -> bool:
        """Check if this node is the primary database node"""
        if not self.is_available:
            return False
        
        try:
            # Check if PostgreSQL is in recovery mode
            proc = await asyncio.create_subprocess_exec(
                'sudo', '-u', 'postgres', 'psql', '-t', '-c', 
                'SELECT pg_is_in_recovery();',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            
            if proc.returncode == 0:
                result = stdout.decode().strip()
                return result.lower() == 'f'  # 'f' means not in recovery (primary)
            
        except Exception as e:
            logger.warning(f"Failed to check primary node status: {e}")
        
        return False
    
    async def is_replica_node(self) -> bool:
        """Check if this node is a replica database node"""
        if not self.is_available:
            return False
        
        try:
            # Check if PostgreSQL is in recovery mode
            proc = await asyncio.create_subprocess_exec(
                'sudo', '-u', 'postgres', 'psql', '-t', '-c', 
                'SELECT pg_is_in_recovery();',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            
            if proc.returncode == 0:
                result = stdout.decode().strip()
                return result.lower() == 't'  # 't' means in recovery (replica)
            
        except Exception as e:
            logger.warning(f"Failed to check replica node status: {e}")
        
        return False
    
    async def get_replication_lag(self) -> Optional[float]:
        """Get replication lag in seconds (for replica nodes)"""
        if not self.is_available or not await self.is_replica_node():
            return None
        
        try:
            proc = await asyncio.create_subprocess_exec(
                'sudo', '-u', 'postgres', 'psql', '-t', '-c', 
                """SELECT EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())) 
                   AS lag_seconds;""",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            
            if proc.returncode == 0:
                result = stdout.decode().strip()
                if result and result != '':
                    return float(result)
            
        except Exception as e:
            logger.warning(f"Failed to get replication lag: {e}")
        
        return None
    
    async def check_backup_status(self) -> Dict[str, Any]:
        """Check pgBackRest backup status"""
        status = {
            'available': self.is_available,
            'last_backup': None,
            'backup_count': 0,
            'azure_connectivity': False,
            'wal_archiving': False
        }
        
        if not self.is_available:
            return status
        
        try:
            # Check backup info
            proc = await asyncio.create_subprocess_exec(
                'sudo', '-u', 'postgres', 'pgbackrest', 
                '--stanza', self.stanza_name, 'info',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            
            if proc.returncode == 0:
                status['azure_connectivity'] = True
                output = stdout.decode()
                # Parse backup information (simplified)
                if 'full backup:' in output.lower():
                    status['backup_count'] = output.lower().count('full backup:')
            
            # Check WAL archiving (for primary nodes)
            if await self.is_primary_node():
                proc = await asyncio.create_subprocess_exec(
                    'sudo', '-u', 'postgres', 'psql', '-t', '-c', 
                    'SELECT archived_count > 0 FROM pg_stat_archiver;',
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await proc.communicate()
                
                if proc.returncode == 0:
                    result = stdout.decode().strip()
                    status['wal_archiving'] = result.lower() == 't'
            
        except Exception as e:
            logger.warning(f"Failed to check backup status: {e}")
        
        return status
    
    async def should_disable_builtin_sync(self) -> bool:
        """Check if built-in database sync should be disabled"""
        return self.is_available and (await self.is_primary_node() or await self.is_replica_node())
    
    async def perform_manual_backup(self, backup_type: str = 'diff') -> bool:
        """Perform a manual backup (primary nodes only)"""
        if not self.is_available or not await self.is_primary_node():
            logger.warning("Manual backup not available (not a primary node or pgBackRest not configured)")
            return False
        
        try:
            logger.info(f"Starting manual {backup_type} backup...")
            
            proc = await asyncio.create_subprocess_exec(
                'sudo', '-u', 'postgres', 'pgbackrest', 
                '--stanza', self.stanza_name, 'backup', f'--type={backup_type}',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            
            if proc.returncode == 0:
                logger.info(f"Manual {backup_type} backup completed successfully")
                return True
            else:
                logger.error(f"Manual backup failed: {stderr.decode()}")
                return False
            
        except Exception as e:
            logger.error(f"Failed to perform manual backup: {e}")
            return False
    
    async def get_sync_status(self) -> Dict[str, Any]:
        """Get comprehensive synchronization status"""
        status = {
            'pgbackrest_available': self.is_available,
            'node_type': 'unknown',
            'replication_lag': None,
            'backup_status': {},
            'recommendations': []
        }
        
        if not self.is_available:
            status['recommendations'].append("pgBackRest not configured - using built-in sync")
            return status
        
        # Determine node type
        if await self.is_primary_node():
            status['node_type'] = 'primary'
        elif await self.is_replica_node():
            status['node_type'] = 'replica'
            status['replication_lag'] = await self.get_replication_lag()
        
        # Get backup status
        status['backup_status'] = await self.check_backup_status()
        
        # Generate recommendations
        if status['node_type'] == 'replica':
            lag = status['replication_lag']
            if lag is not None and lag > 300:  # 5 minutes
                status['recommendations'].append(f"High replication lag: {lag:.1f} seconds")
        
        if not status['backup_status'].get('azure_connectivity', False):
            status['recommendations'].append("Azure Storage connectivity issues detected")
        
        if status['node_type'] == 'primary' and not status['backup_status'].get('wal_archiving', False):
            status['recommendations'].append("WAL archiving not working properly")
        
        return status


# Global instance
pgbackrest_integration = PgBackRestIntegration()


async def initialize_pgbackrest_integration() -> None:
    """Initialize pgBackRest integration (call during validator startup)"""
    global pgbackrest_integration
    pgbackrest_integration = PgBackRestIntegration()
    
    if pgbackrest_integration.is_available:
        logger.info("pgBackRest integration initialized successfully")
        
        # Log current status
        status = await pgbackrest_integration.get_sync_status()
        logger.info(f"Database synchronization status: {status['node_type']} node")
        
        if status['recommendations']:
            for rec in status['recommendations']:
                logger.warning(f"pgBackRest recommendation: {rec}")
    else:
        logger.info("pgBackRest not available - using built-in database synchronization")


def is_pgbackrest_enabled() -> bool:
    """Check if pgBackRest is enabled and configured"""
    return pgbackrest_integration.is_available


async def should_skip_builtin_db_sync() -> bool:
    """Check if built-in database sync should be skipped"""
    return await pgbackrest_integration.should_disable_builtin_sync()


async def get_database_sync_status() -> Dict[str, Any]:
    """Get current database synchronization status"""
    return await pgbackrest_integration.get_sync_status() 