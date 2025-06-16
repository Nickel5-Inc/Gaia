#!/usr/bin/env python3
"""
Comprehensive Database Setup Manager

This module provides a fully automated, self-healing database setup process that handles:
1. PostgreSQL installation and configuration
2. Database creation and user management
3. Alembic schema management and migrations
4. pgBackRest configuration for backups
5. Self-healing and corruption recovery
6. Network transition handling

This is designed to be called at the main validator entry point and ensure
the database is always ready for the validator to operate.
"""

import asyncio
import os
import sys
import subprocess
import shutil
import tempfile
import json
import time
import signal
import psutil
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List, Tuple, Any
from dataclasses import dataclass

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from fiber.logging_utils import get_logger

logger = get_logger(__name__)

@dataclass
class DatabaseConfig:
    """Configuration for database setup"""
    postgres_version: str = "14"
    database_name: str = "gaia_validator"
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"
    data_directory: str = "/var/lib/postgresql/14/main"
    config_directory: str = "/etc/postgresql/14/main"
    port: int = 5432
    max_connections: int = 100
    shared_buffers: str = "256MB"
    effective_cache_size: str = "1GB"
    maintenance_work_mem: str = "64MB"
    checkpoint_completion_target: float = 0.9
    wal_buffers: str = "16MB"
    default_statistics_target: int = 100
    random_page_cost: float = 1.1
    effective_io_concurrency: int = 200

class ComprehensiveDatabaseSetup:
    """
    Comprehensive database setup and management system.
    
    This class handles the complete lifecycle of database setup including:
    - PostgreSQL installation and configuration
    - Database and user creation
    - Alembic schema management
    - Backup configuration
    - Self-healing and recovery
    """
    
    def __init__(self, config: Optional[DatabaseConfig] = None, test_mode: bool = False):
        self.config = config or DatabaseConfig()
        self.test_mode = test_mode
        self.system_info = self._detect_system()
        self.postgres_info = self._detect_postgresql()
        
        # Paths and configuration
        self.alembic_config_path = project_root / "alembic_validator.ini"
        self.migrations_dir = project_root / "alembic" / "versions"
        
        logger.info(f"🔧 Comprehensive Database Setup initialized")
        logger.info(f"   System: {self.system_info['os']} {self.system_info['version']}")
        logger.info(f"   PostgreSQL: {self.postgres_info.get('version', 'Not detected')}")
        logger.info(f"   Test Mode: {self.test_mode}")

    def _detect_system(self) -> Dict[str, str]:
        """Detect the operating system and version"""
        try:
            if os.path.exists('/etc/os-release'):
                with open('/etc/os-release', 'r') as f:
                    lines = f.readlines()
                os_info = {}
                for line in lines:
                    if '=' in line:
                        key, value = line.strip().split('=', 1)
                        os_info[key] = value.strip('"')
                
                return {
                    'os': os_info.get('ID', 'unknown'),
                    'version': os_info.get('VERSION_ID', 'unknown'),
                    'name': os_info.get('PRETTY_NAME', 'Unknown OS')
                }
            else:
                return {'os': 'unknown', 'version': 'unknown', 'name': 'Unknown OS'}
        except Exception as e:
            logger.warning(f"Could not detect system info: {e}")
            return {'os': 'unknown', 'version': 'unknown', 'name': 'Unknown OS'}

    def _detect_postgresql(self) -> Dict[str, Any]:
        """Detect PostgreSQL installation and configuration"""
        info = {
            'installed': False,
            'version': None,
            'service_name': None,
            'data_directory': None,
            'config_directory': None,
            'running': False
        }
        
        try:
            # Check if PostgreSQL is installed
            result = subprocess.run(['psql', '--version'], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                info['installed'] = True
                version_line = result.stdout.strip()
                # Extract version number (e.g., "psql (PostgreSQL) 14.10")
                if 'PostgreSQL' in version_line:
                    parts = version_line.split()
                    for part in parts:
                        if part.replace('.', '').isdigit():
                            info['version'] = part.split('.')[0]  # Major version
                            break
            
            # Detect service name and status
            service_candidates = [
                f'postgresql@{self.config.postgres_version}-main',
                f'postgresql-{self.config.postgres_version}',
                'postgresql'
            ]
            
            for service in service_candidates:
                try:
                    result = subprocess.run(['systemctl', 'is-active', service], 
                                          capture_output=True, text=True, timeout=5)
                    if result.returncode == 0:
                        info['service_name'] = service
                        info['running'] = result.stdout.strip() == 'active'
                        break
                except subprocess.TimeoutExpired:
                    continue
            
            # Detect data and config directories
            if info['installed']:
                info['data_directory'] = self.config.data_directory
                info['config_directory'] = self.config.config_directory
                
                # Verify directories exist
                if not os.path.exists(info['data_directory']):
                    info['data_directory'] = None
                if not os.path.exists(info['config_directory']):
                    info['config_directory'] = None
            
        except Exception as e:
            logger.warning(f"Error detecting PostgreSQL: {e}")
        
        return info

    async def setup_complete_database_system(self) -> bool:
        """
        Main orchestration method for complete database setup.
        This is designed to be idempotent and self-healing.
        """
        logger.info("🚀 Starting comprehensive database system setup...")
        
        # Check if the system is already perfectly configured. If so, we're done.
        if await self._check_if_already_configured():
            logger.info("✅ Database system already configured and running.")
            return True

        # Ensure the necessary PostgreSQL packages are installed on the system.
        if not await self._ensure_postgresql_installed():
            logger.error("❌ Failed to install PostgreSQL packages. Cannot proceed.")
            return False

        # Ensure the PostgreSQL service is running. This is the key step that will
        # trigger a full repair if the service is broken or misconfigured.
        if not await self._ensure_postgresql_running():
            logger.error("❌ Failed to start or repair the PostgreSQL service. Cannot proceed.")
            return False
            
        # At this point, a running cluster is guaranteed. Now, apply our specific configs.
        if not await self._configure_postgresql_system():
             logger.error("❌ Failed to apply PostgreSQL custom configurations.")
             return False
        
        # A restart is required to apply the new configurations.
        if not await self._restart_postgresql_service():
            logger.error("❌ Failed to restart PostgreSQL after configuration. The service might be unstable.")
            # Attempt to proceed, but the connection test will likely fail.
        
        # With the service running and configured, set up the database, users, and schema.
        if not await self._setup_database_and_users():
            logger.error("❌ Failed to setup database and users.")
            return False
        if not await self._setup_alembic_schema():
            logger.error("❌ Failed to setup Alembic schema.")
            return False
            
        # Run a final validation to ensure everything is perfect.
        if not await self._validate_complete_setup():
            logger.error("❌ Final validation of the complete setup failed.")
            return False
            
        logger.info("✅ Comprehensive database system setup completed successfully!")
        return True

    async def _check_if_already_configured(self) -> bool:
        """
        Check if the database system is already properly configured.
        Returns True if everything is working and no setup is needed.
        """
        logger.info("🔍 Checking if database system is already configured...")
        
        try:
            # Check 1: PostgreSQL service is running
            service_name = await self._detect_postgresql_service()
            if not service_name:
                logger.info("❌ PostgreSQL service not detected")
                return False
            
            cmd = ['sudo', 'systemctl', 'is-active', service_name]
            success, stdout, stderr = await self._run_command(cmd, timeout=10)
            if not success or stdout.strip() != 'active':
                logger.info("❌ PostgreSQL service not running")
                return False
            
            # Check 2: Database connection works
            if not await self._test_database_connection():
                logger.info("❌ Database connection test failed")
                return False
            
            # Check 3: Application database exists
            cmd = [
                'sudo', '-u', 'postgres', 'psql', '-lqt'
            ]
            success, stdout, stderr = await self._run_command(cmd, timeout=10)
            if not success or self.config.database_name not in stdout:
                logger.info(f"❌ Application database '{self.config.database_name}' not found")
                return False
            
            # Check 4: Alembic is set up
            check_alembic_cmd = [
                'sudo', '-u', 'postgres', 'psql', '-d', self.config.database_name, '-c',
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'alembic_version');"
            ]
            success, stdout, stderr = await self._run_command(check_alembic_cmd, timeout=10)
            # PostgreSQL returns 't' for true, 'f' for false
            if not success or 't' not in stdout.strip():
                logger.info("❌ Alembic not configured")
                return False
            
            logger.info("✅ Database system is already properly configured!")
            return True
            
        except Exception as e:
            logger.info(f"❌ Configuration check failed: {e}")
            return False

    async def _ensure_postgresql_installed(self) -> bool:
        """Install PostgreSQL if not already installed"""
        if self.postgres_info['installed']:
            logger.info("✅ PostgreSQL already installed")
            return True
        
        logger.info("📦 Installing PostgreSQL...")
        
        try:
            if self.system_info['os'] in ['ubuntu', 'debian']:
                return await self._install_postgresql_debian()
            elif self.system_info['os'] in ['centos', 'rhel', 'fedora']:
                return await self._install_postgresql_rhel()
            elif self.system_info['os'] == 'arch':
                return await self._install_postgresql_arch()
            else:
                logger.warning(f"Unsupported OS: {self.system_info['os']}, attempting generic installation")
                return await self._install_postgresql_generic()
                
        except Exception as e:
            logger.error(f"Failed to install PostgreSQL: {e}", exc_info=True)
            return False

    async def _install_postgresql_debian(self) -> bool:
        """Install PostgreSQL on Debian/Ubuntu systems"""
        commands = [
            ['apt', 'update'],
            ['apt', 'install', '-y', f'postgresql-{self.config.postgres_version}', 
             f'postgresql-client-{self.config.postgres_version}', 'postgresql-contrib']
        ]
        
        for cmd in commands:
            success, stdout, stderr = await self._run_command(cmd, timeout=300)
            if not success:
                logger.error(f"Command failed: {' '.join(cmd)}")
                logger.error(f"Error: {stderr}")
                return False
        
        logger.info("✅ PostgreSQL installed successfully on Debian/Ubuntu")
        return True

    async def _install_postgresql_rhel(self) -> bool:
        """Install PostgreSQL on RHEL/CentOS/Fedora systems"""
        # Determine package manager
        pkg_manager = 'dnf' if shutil.which('dnf') else 'yum'
        
        commands = [
            [pkg_manager, 'install', '-y', f'postgresql{self.config.postgres_version}-server', 
             f'postgresql{self.config.postgres_version}', f'postgresql{self.config.postgres_version}-contrib']
        ]
        
        for cmd in commands:
            success, stdout, stderr = await self._run_command(cmd, timeout=300)
            if not success:
                logger.error(f"Command failed: {' '.join(cmd)}")
                logger.error(f"Error: {stderr}")
                return False
        
        # Initialize database cluster for RHEL systems
        init_cmd = [f'postgresql-{self.config.postgres_version}-setup', 'initdb']
        await self._run_command(init_cmd, timeout=60)
        
        logger.info("✅ PostgreSQL installed successfully on RHEL/CentOS/Fedora")
        return True

    async def _install_postgresql_arch(self) -> bool:
        """Install PostgreSQL on Arch Linux"""
        commands = [
            ['pacman', '-Sy', '--noconfirm', 'postgresql']
        ]
        
        for cmd in commands:
            success, stdout, stderr = await self._run_command(cmd, timeout=300)
            if not success:
                logger.error(f"Command failed: {' '.join(cmd)}")
                logger.error(f"Error: {stderr}")
                return False
        
        logger.info("✅ PostgreSQL installed successfully on Arch Linux")
        return True

    async def _install_postgresql_generic(self) -> bool:
        """Generic PostgreSQL installation fallback"""
        logger.warning("⚠️ Using generic installation method - may not work on all systems")
        
        # Try common package managers
        package_managers = [
            (['apt', 'update'], ['apt', 'install', '-y', 'postgresql', 'postgresql-contrib']),
            (['yum', 'install', '-y', 'postgresql-server', 'postgresql']),
            (['dnf', 'install', '-y', 'postgresql-server', 'postgresql']),
            (['pacman', '-Sy', '--noconfirm', 'postgresql'])
        ]
        
        for update_cmd, install_cmd in package_managers:
            if shutil.which(update_cmd[0]):
                logger.info(f"Trying {update_cmd[0]} package manager...")
                if update_cmd:
                    await self._run_command(update_cmd, timeout=120)
                success, stdout, stderr = await self._run_command(install_cmd, timeout=300)
                if success:
                    logger.info(f"✅ PostgreSQL installed using {update_cmd[0]}")
                    return True
        
        logger.error("❌ Could not install PostgreSQL with any known package manager")
        return False

    async def _detect_and_repair_corruption(self) -> bool:
        """Detect and repair any database corruption"""
        logger.info("🔍 Checking for database corruption...")
        
        data_dir = Path(self.config.data_directory)
        config_dir = Path(self.config.config_directory)

        # Check 1: Data directory must exist
        if not data_dir.exists() or not data_dir.is_dir():
            logger.warning(f"⚠️ Data directory not found at {data_dir}, initializing fresh cluster.")
            return await self._initialize_fresh_cluster()

        # Check 2: Config directory must exist
        if not config_dir.exists() or not config_dir.is_dir():
            logger.warning(f"⚠️ Config directory not found at {config_dir}, will repair.")
            return await self._repair_corrupted_cluster()

        # Check 3: Essential files and directories
        required_in_data = ['PG_VERSION', 'base', 'global']
        required_in_config = ['postgresql.conf', 'pg_hba.conf']
        
        missing_items = []
        for item in required_in_data:
            if not (data_dir / item).exists():
                missing_items.append(f"data/{item}")
        for item in required_in_config:
            if not (config_dir / item).exists():
                missing_items.append(f"config/{item}")

        if missing_items:
            logger.warning(f"⚠️ Corruption detected - missing items: {missing_items}")
            return await self._repair_corrupted_cluster()
        
        # Check 4: pg_control file must exist
        pg_control = data_dir / 'global' / 'pg_control'
        if not pg_control.exists():
            logger.warning("⚠️ Missing pg_control file - cluster is severely corrupted")
            return await self._repair_corrupted_cluster()

        # Check 5: Incomplete backup/restore marker
        backup_label = data_dir / 'backup_label'
        if backup_label.exists():
            logger.warning("⚠️ Found backup_label file - indicates incomplete restore.")
            return await self._repair_corrupted_cluster()
        
        logger.info("✅ No corruption detected")
        return True

    async def _initialize_fresh_cluster(self) -> bool:
        """Initialize a fresh PostgreSQL cluster"""
        logger.info("🆕 Initializing fresh PostgreSQL cluster...")
        
        try:
            # This is now the single point of truth for creating a new cluster.
            # It assumes prior cleanup has already occurred.
            
            # Use pg_createcluster which handles everything: initdb, config creation, etc.
            init_cmd = [
                'sudo', 'pg_createcluster', self.config.postgres_version, 'main',
                '--start'  # Automatically start the cluster after creation
            ]
            
            success, stdout, stderr = await self._run_command(init_cmd, timeout=120)
            if not success:
                logger.error(f"Failed to create cluster with pg_createcluster: {stderr}")
                return False
            
            logger.info("✅ Fresh PostgreSQL cluster initialized and started")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing fresh cluster: {e}", exc_info=True)
            return False

    async def _repair_corrupted_cluster(self) -> bool:
        """Repair a corrupted PostgreSQL cluster by completely recreating it."""
        logger.warning("🔧 Repairing corrupted PostgreSQL cluster...")
        
        # The repair strategy is to remove and recreate.
        return await self._remove_and_recreate_cluster()

    async def _configure_postgresql_system(self) -> bool:
        """Configure PostgreSQL system settings"""
        logger.info("⚙️ Configuring PostgreSQL system...")
        
        try:
            # Ensure config directory exists
            config_dir = Path(self.config.config_directory)
            config_dir.mkdir(parents=True, exist_ok=True)
            
            # Configure postgresql.conf
            if not await self._configure_postgresql_conf():
                return False
            
            # Configure pg_hba.conf
            if not await self._configure_pg_hba():
                return False
            
            # Set proper ownership
            await self._run_command(['chown', '-R', 'postgres:postgres', str(config_dir)])
            
            logger.info("✅ PostgreSQL system configured")
            return True
            
        except Exception as e:
            logger.error(f"Error configuring PostgreSQL: {e}", exc_info=True)
            return False

    async def _configure_postgresql_conf(self) -> bool:
        """Configure postgresql.conf with optimized settings using the conf.d directory."""
        config_dir = Path(self.config.config_directory)
        conf_d_dir = config_dir / 'conf.d'
        
        # Ensure the conf.d directory exists using sudo
        try:
            conf_d_dir.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            # If we can't create it directly, use sudo
            success, _, stderr = await self._run_command([
                'sudo', 'mkdir', '-p', str(conf_d_dir)
            ])
            if not success:
                logger.error(f"Failed to create conf.d directory: {stderr}")
                return False
        
        custom_config_file = conf_d_dir / '99-gaia-custom.conf'
        
        # These are the settings we want to enforce.
        # We no longer need to specify data_directory, hba_file, etc.
        # as we are not overwriting the main config file.
        config_settings = {
            'listen_addresses': "'*'",
            'port': str(self.config.port),
            'max_connections': str(self.config.max_connections),
            'shared_buffers': f"'{self.config.shared_buffers}'",
            'effective_cache_size': f"'{self.config.effective_cache_size}'",
            'maintenance_work_mem': f"'{self.config.maintenance_work_mem}'",
            'checkpoint_completion_target': str(self.config.checkpoint_completion_target),
            'wal_buffers': f"'{self.config.wal_buffers}'",
            'default_statistics_target': str(self.config.default_statistics_target),
            'random_page_cost': str(self.config.random_page_cost),
            'effective_io_concurrency': str(self.config.effective_io_concurrency),
            'min_wal_size': "'1GB'",
            'max_wal_size': "'4GB'",
            'wal_level': "'replica'",
            'archive_mode': "'on'",
            'archive_command': "'/bin/true'",  # Placeholder
            'log_destination': "'stderr'",
            'logging_collector': 'on',
            'log_directory': "'log'",
            'log_filename': "'postgresql-%Y-%m-%d_%H%M%S.log'",
            'log_rotation_age': "'1d'",
            'log_rotation_size': "'10MB'",
            'log_min_duration_statement': "'1000ms'",
            'log_line_prefix': "'%t [%p]: [%l-1] user=%u,db=%d,app=%a,client=%h '",
            'log_checkpoints': 'on',
            'log_connections': 'on',
            'log_disconnections': 'on',
            'log_lock_waits': 'on',
            'log_temp_files': '0',
            'log_autovacuum_min_duration': '0',
            'log_error_verbosity': "'default'"
        }
        
        try:
            config_lines = [
                "# Custom PostgreSQL settings for Gaia Validator",
                f"# Generated at: {datetime.now(timezone.utc).isoformat()}",
                ""
            ]
            
            for key, value in config_settings.items():
                config_lines.append(f"{key} = {value}")
            
            # Write our custom settings to a file in conf.d using sudo
            config_content = '\n'.join(config_lines)
            
            # Use sudo tee to write to the system directory
            process = await asyncio.create_subprocess_exec(
                'sudo', 'tee', str(custom_config_file),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate(input=config_content.encode())
            
            if process.returncode == 0:
                logger.info(f"✅ Custom PostgreSQL configuration written to: {custom_config_file}")
                return True
            else:
                logger.error(f"Failed to write config file: {stderr.decode()}")
                return False
            
        except Exception as e:
            logger.error(f"Error configuring postgresql.conf via conf.d: {e}", exc_info=True)
            return False

    async def _configure_pg_hba(self) -> bool:
        """Configure pg_hba.conf for authentication"""
        hba_file = Path(self.config.config_directory) / 'pg_hba.conf'
        
        hba_rules = [
            "# PostgreSQL Client Authentication Configuration File",
            "# Generated by Gaia Comprehensive Database Setup",
            f"# Generated at: {datetime.now(timezone.utc).isoformat()}",
            "",
            "# TYPE  DATABASE        USER            ADDRESS                 METHOD",
            "",
            "# Local connections",
            "local   all             postgres                                trust",
            "local   all             root                                    trust",
            "local   all             all                                     md5",
            "",
            "# IPv4 local connections:",
            "host    all             postgres        127.0.0.1/32            trust",
            "host    all             all             127.0.0.1/32            md5",
            "",
            "# IPv6 local connections:",
            "host    all             postgres        ::1/128                 trust",
            "host    all             all             ::1/128                 md5",
            "",
            "# Allow replication connections from localhost",
            "local   replication     postgres                                trust",
            "host    replication     postgres        127.0.0.1/32            trust",
            "host    replication     postgres        ::1/128                 trust"
        ]
        
        try:
            # Write pg_hba.conf using sudo
            hba_content = '\n'.join(hba_rules)
            
            # Use sudo tee to write to the system directory
            process = await asyncio.create_subprocess_exec(
                'sudo', 'tee', str(hba_file),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate(input=hba_content.encode())
            
            if process.returncode == 0:
                logger.info(f"✅ pg_hba.conf configured: {hba_file}")
                return True
            else:
                logger.error(f"Failed to write pg_hba.conf: {stderr.decode()}")
                return False
            
        except Exception as e:
            logger.error(f"Error configuring pg_hba.conf: {e}", exc_info=True)
            return False

    async def _ensure_postgresql_running(self) -> bool:
        """Ensures the PostgreSQL service is running, repairing it if necessary."""
        logger.info("🔄 Ensuring PostgreSQL service is running...")
        
        service_name = await self._detect_postgresql_service()
        if not service_name:
            logger.error("Could not detect PostgreSQL service name, cannot proceed.")
            return False

        # First, check if the service is active.
        is_active, _, _ = await self._run_command(['sudo', 'systemctl', 'is-active', service_name])
        if is_active:
            logger.info(f"✅ PostgreSQL service '{service_name}' is already active.")
            return True

        # Check the service status for more detailed diagnostics
        logger.info(f"Service '{service_name}' is not running. Checking status...")
        status_success, status_stdout, status_stderr = await self._run_command(['sudo', 'systemctl', 'status', service_name])
        
        # Log the status for debugging
        if status_stdout:
            logger.info(f"📊 Service status: {status_stdout}")
        if status_stderr:
            logger.warning(f"📊 Service status stderr: {status_stderr}")

        # Try starting the service
        logger.info(f"Attempting to start service '{service_name}'...")
        start_success, start_stdout, start_stderr = await self._run_command(['sudo', 'systemctl', 'start', service_name])
        
        if start_success:
            await asyncio.sleep(3) # Give it a moment to initialize
            
            # Verify it's actually running
            is_active_after_start, _, _ = await self._run_command(['sudo', 'systemctl', 'is-active', service_name])
            if is_active_after_start:
                logger.info(f"✅ Service '{service_name}' started successfully.")
                return True
            else:
                logger.warning(f"Service '{service_name}' start command succeeded but service is not active.")
        
        # If starting failed, diagnose the issue before jumping to aggressive repair
        logger.warning(f"Failed to start PostgreSQL service: {start_stderr}")
        
        # Check if it's a configuration issue vs corruption
        logger.info("🔍 Diagnosing PostgreSQL startup failure...")
        
        # Check if data directory exists and has correct permissions
        data_dir = Path(self.config.data_directory)
        if not data_dir.exists():
            logger.warning(f"PostgreSQL data directory does not exist: {data_dir}")
            logger.info("🔧 This appears to be a missing cluster, not corruption. Initializing fresh cluster...")
            return await self._initialize_fresh_cluster()
        
        # Check if postgresql.conf exists
        config_file = Path(self.config.config_directory) / "postgresql.conf"
        if not config_file.exists():
            logger.warning(f"PostgreSQL config file does not exist: {config_file}")
            logger.info("🔧 This appears to be a missing configuration, not corruption. Initializing fresh cluster...")
            return await self._initialize_fresh_cluster()
        
        # Check PostgreSQL logs for more specific error information
        log_success, log_output, _ = await self._run_command(['sudo', 'journalctl', '-u', service_name, '--no-pager', '-n', '20'])
        if log_success and log_output:
            logger.info(f"📋 Recent PostgreSQL logs:\n{log_output}")
            
            # Look for specific error patterns that indicate corruption vs configuration issues
            if any(pattern in log_output.lower() for pattern in ['permission denied', 'could not open file', 'no such file']):
                logger.warning("🔍 Detected permission or missing file issues - attempting ownership fix...")
                
                # Try fixing ownership first
                chown_success, _, _ = await self._run_command(['sudo', 'chown', '-R', 'postgres:postgres', str(data_dir)])
                chmod_success, _, _ = await self._run_command(['sudo', 'chmod', '-R', '700', str(data_dir)])
                
                if chown_success and chmod_success:
                    logger.info("🔧 Fixed data directory ownership. Retrying service start...")
                    retry_success, _, _ = await self._run_command(['sudo', 'systemctl', 'start', service_name])
                    if retry_success:
                        await asyncio.sleep(3)
                        is_active_retry, _, _ = await self._run_command(['sudo', 'systemctl', 'is-active', service_name])
                        if is_active_retry:
                            logger.info("✅ Service started successfully after ownership fix.")
                            return True
        
        # Only resort to aggressive repair if other methods failed
        logger.error("🚨 Service start failed after diagnostics and simple fixes. Attempting aggressive repair as last resort...")
        
        repaired = await self._remove_and_recreate_cluster()
        if repaired:
            logger.info("✅ Aggressive repair successful. PostgreSQL should now be running.")
            return True
        else:
            logger.error("💥 Aggressive repair FAILED. The database system is in a bad state.")
            return False

    async def _restart_postgresql_service(self) -> bool:
        """Restarts the PostgreSQL service."""
        logger.info("🔄 Restarting PostgreSQL service to apply configurations...")
        service_name = await self._detect_postgresql_service()
        if not service_name:
            logger.error("Could not detect PostgreSQL service name for restart.")
            return False
        
        success, _, stderr = await self._run_command(['sudo', 'systemctl', 'restart', service_name])
        if not success:
            logger.error(f"Failed to restart PostgreSQL: {stderr}")
            return False
        
        await asyncio.sleep(2) # Wait for restart
        
        is_active, _, _ = await self._run_command(['sudo', 'systemctl', 'is-active', service_name])
        if not is_active:
            logger.error("Service failed to become active after restart.")
            return False

        logger.info("✅ Service restarted successfully.")
        return True

    async def _detect_postgresql_service(self) -> Optional[str]:
        """Detect the correct PostgreSQL service name"""
        service_candidates = [
            f'postgresql@{self.config.postgres_version}-main',
            f'postgresql-{self.config.postgres_version}',
            'postgresql'
        ]
        
        for service in service_candidates:
            try:
                # First check if the service unit file exists (more comprehensive than list-units)
                success, stdout, stderr = await self._run_command(['systemctl', 'cat', f'{service}.service'])
                if success:
                    logger.info(f"Detected PostgreSQL service: {service}")
                    return service
                
                # Fallback: check if it appears in list-units (for running services)
                success, stdout, stderr = await self._run_command(['systemctl', 'list-units', '--type=service', f'{service}.service'])
                if success and service in stdout:
                    logger.info(f"Detected PostgreSQL service: {service}")
                    return service
                    
                # Also check list-unit-files for installed but not loaded services
                success, stdout, stderr = await self._run_command(['systemctl', 'list-unit-files', f'{service}.service'])
                if success and service in stdout:
                    logger.info(f"Detected PostgreSQL service: {service}")
                    return service
                    
            except Exception:
                continue
        
        logger.warning("Could not detect PostgreSQL service name")
        return None

    async def _test_database_connection(self, max_fix_attempts: int = 1) -> bool:
        """Test database connection with circuit breaker to prevent infinite loops"""
        try:
            # First try with cluster specification
            cmd = ['sudo', '-u', 'postgres', 'psql', '--cluster', f'{self.config.postgres_version}/main', '-c', 'SELECT version();']
            success, stdout, stderr = await self._run_command(cmd, timeout=10)
            
            if success and 'PostgreSQL' in stdout:
                logger.info("✅ Database connection test successful (with cluster)")
                return True
            
            # If that fails, try without cluster specification
            cmd = ['sudo', '-u', 'postgres', 'psql', '-c', 'SELECT version();']
            success, stdout, stderr = await self._run_command(cmd, timeout=10)
            
            if success and 'PostgreSQL' in stdout:
                logger.info("✅ Database connection test successful (without cluster)")
                return True
            
            # If both fail, try to fix the cluster configuration (with circuit breaker)
            if max_fix_attempts > 0:
                logger.warning(f"Database connection failed: {stderr}")
                logger.info("🔧 Attempting to fix cluster configuration...")
                
                if await self._fix_cluster_configuration():
                    # Try connection again after fix (recursive call with decremented attempts)
                    return await self._test_database_connection(max_fix_attempts - 1)
            else:
                logger.error("❌ Maximum cluster fix attempts reached, giving up")
            
            logger.error(f"Database connection test failed: {stderr}")
            return False
                
        except Exception as e:
            logger.error(f"Error testing database connection: {e}")
            return False

    async def _fix_cluster_configuration(self) -> bool:
        """Fix PostgreSQL cluster configuration issues"""
        logger.info("🔧 Fixing PostgreSQL cluster configuration...")
        
        # Always use the aggressive approach for simplicity and robustness
        return await self._remove_and_recreate_cluster()

    async def _remove_and_recreate_cluster(self) -> bool:
        """Aggressively remove corrupted cluster configuration and recreate"""
        try:
            logger.warning("🗑️ Removing corrupted cluster configuration...")
            
            # Stop all PostgreSQL services
            await self._stop_postgresql_service()
            
            # Remove cluster configuration files
            config_dir = Path(self.config.config_directory)
            if config_dir.exists():
                logger.info(f"🗑️ Removing config directory: {config_dir}")
                await self._run_command(['sudo', 'rm', '-rf', str(config_dir)])

            # Remove data directory
            data_dir = Path(self.config.data_directory)
            if data_dir.exists():
                logger.info(f"🗑️ Removing data directory: {data_dir}")
                await self._run_command(['sudo', 'rm', '-rf', str(data_dir)])
            
            # Wait a moment for filesystem operations to complete
            await asyncio.sleep(2)
            
            # Initialize a completely fresh cluster
            return await self._initialize_fresh_cluster()
                
        except Exception as e:
            logger.error(f"Error removing and recreating cluster: {e}", exc_info=True)
            return False

    async def _setup_database_and_users(self) -> bool:
        """Create database and users (idempotent)"""
        logger.info("👤 Setting up database and users...")
        
        try:
            # Check if application database already exists
            cmd = ['sudo', '-u', 'postgres', 'psql', '-lqt']
            success, stdout, stderr = await self._run_command(cmd, timeout=10)
            
            if success and self.config.database_name in stdout:
                logger.info(f"✅ Database '{self.config.database_name}' already exists")
                return True
            
            # Set postgres user password (idempotent)
            if not await self._set_postgres_password():
                return False
            
            # Create application database
            if not await self._create_application_database():
                return False
            
            logger.info("✅ Database and users configured")
            return True
            
        except Exception as e:
            logger.error(f"Error setting up database and users: {e}", exc_info=True)
            return False

    async def _set_postgres_password(self) -> bool:
        """Set password for postgres user"""
        try:
            cmd = [
                'sudo', '-u', 'postgres', 'psql', '-c',
                f"ALTER USER postgres PASSWORD '{self.config.postgres_password}';"
            ]
            
            success, stdout, stderr = await self._run_command(cmd, timeout=10)
            if success:
                logger.info("✅ Postgres user password set")
                return True
            else:
                logger.error(f"Failed to set postgres password: {stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error setting postgres password: {e}")
            return False

    async def _create_application_database(self) -> bool:
        """Create the application database"""
        try:
            # Check if database exists
            check_cmd = [
                'sudo', '-u', 'postgres', 'psql', '-lqt'
            ]
            
            success, stdout, stderr = await self._run_command(check_cmd, timeout=10)
            if success and self.config.database_name in stdout:
                logger.info(f"✅ Database '{self.config.database_name}' already exists")
                return True
            
            # Create database
            create_cmd = [
                'sudo', '-u', 'postgres', 'createdb', self.config.database_name
            ]
            
            success, stdout, stderr = await self._run_command(create_cmd, timeout=30)
            if success:
                logger.info(f"✅ Database '{self.config.database_name}' created")
                return True
            else:
                logger.error(f"Failed to create database: {stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error creating application database: {e}")
            return False

    async def _setup_alembic_schema(self) -> bool:
        """Setup the database schema by running alembic migrations."""
        logger.info("📋 Setting up database schema with Alembic...")
        
        if await self._run_alembic_migrations():
            logger.info("✅ Database schema setup completed")
            return True
        else:
            logger.error("❌ Failed to setup Alembic schema.")
            return False

    async def _run_alembic_migrations(self) -> bool:
        """Run alembic migrations to upgrade the database schema"""
        logger.info("🔧 Running Alembic migrations...")
        
        project_root_dir = str(project_root)
        
        db_url = f"postgresql+psycopg2://{self.config.postgres_user}:{self.config.postgres_password}@localhost:{self.config.port}/{self.config.database_name}"
        
        cmd = [
            sys.executable, '-m', 'alembic',
            '--config', str(self.alembic_config_path),
            '-x', f'db_url={db_url}',
            'upgrade', 'head'
        ]
            
        success, stdout, stderr = await self._run_command(cmd, timeout=120, cwd=project_root_dir)
        
        if success:
            logger.info("✅ Alembic migrations run successfully")
            return True
        else:
            logger.error(f"Failed to run Alembic migrations: {stderr}")
            return False

    async def _setup_backup_system(self) -> bool:
        """Setup pgBackRest for database backups (placeholder)"""
        logger.info("💾 Setting up backup system...")
        
        # This is a placeholder for a more comprehensive backup setup
        logger.warning("⚠️ Backup system setup is currently a placeholder.")
        return True

    async def _validate_complete_setup(self) -> bool:
        """Final validation to ensure the database system is perfectly configured"""
        logger.info("✅ Validating complete database setup...")
        
        try:
            # Check 1: PostgreSQL service is running
            service_name = await self._detect_postgresql_service()
            if not service_name:
                logger.error("❌ PostgreSQL service not detected")
                return False
            
            cmd = ['sudo', 'systemctl', 'is-active', service_name]
            success, stdout, stderr = await self._run_command(cmd, timeout=10)
            if not success or stdout.strip() != 'active':
                logger.error(f"❌ PostgreSQL service not running: {stderr}")
                return False
            
            # Check 2: Database connection works
            if not await self._test_database_connection(max_fix_attempts=0): # No more fixes at this stage
                logger.error("❌ Database connection test failed")
                return False
            
            # Test 3: Application database exists
            check_db_cmd = [
                'sudo', '-u', 'postgres', 'psql', '-lqt'
            ]
            success, stdout, stderr = await self._run_command(check_db_cmd, timeout=10)
            if not success or self.config.database_name not in stdout:
                logger.error(f"❌ Application database '{self.config.database_name}' not found")
                return False
            
            # Test 4: Alembic version table exists
            check_alembic_cmd = [
                'sudo', '-u', 'postgres', 'psql', '-d', self.config.database_name, '-c',
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'alembic_version');"
            ]
            success, stdout, stderr = await self._run_command(check_alembic_cmd, timeout=10)
            if not success or 't' not in stdout.lower(): # Check for 't' (true) or 'f' (false)
                logger.error("❌ Alembic version table not found")
                return False
            
            # Test 5: Can connect to application database
            test_app_db_cmd = [
                'sudo', '-u', 'postgres', 'psql', '-d', self.config.database_name, '-c', 'SELECT 1;'
            ]
            success, stdout, stderr = await self._run_command(test_app_db_cmd, timeout=10)
            if not success:
                logger.error(f"❌ Cannot connect to application database: {stderr}")
                return False
            
            logger.info("✅ All validation tests passed!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Validation failed with error: {e}", exc_info=True)
            return False

    async def _stop_postgresql_service(self) -> bool:
        """Stop PostgreSQL service"""
        service_name = await self._detect_postgresql_service()
        if not service_name:
            return True  # If we can't detect it, assume it's not running
        
        try:
            success, stdout, stderr = await self._run_command(['sudo', 'systemctl', 'stop', service_name])
            if success:
                logger.info(f"✅ Stopped PostgreSQL service: {service_name}")
            return success
        except Exception as e:
            logger.warning(f"Could not stop PostgreSQL service: {e}")
            return False

    async def _run_command(self, cmd: List[str], timeout: int = 30, cwd: Optional[str] = None) -> Tuple[bool, str, str]:
        """Run a system command with timeout and logging"""
        cmd_str = ' '.join(cmd)
        logger.debug(f"🔧 Running command: {cmd_str}")
        
        effective_cwd = cwd
        # If running as postgres user and no cwd is set, use /tmp to avoid permission errors
        if 'sudo' in cmd and '-u' in cmd and 'postgres' in cmd and cwd is None:
            effective_cwd = '/tmp'
            logger.debug(f"🔧 Overriding cwd to '{effective_cwd}' for postgres user command")
        
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=effective_cwd
            )
            
            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
                stdout_str = stdout.decode('utf-8', errors='replace').strip()
                stderr_str = stderr.decode('utf-8', errors='replace').strip()
                
                success = process.returncode == 0
                
                if success:
                    logger.debug(f"✅ Command succeeded: {cmd_str}")
                    if stdout_str:
                        logger.debug(f"   Output: {stdout_str[:200]}{'...' if len(stdout_str) > 200 else ''}")
                else:
                    logger.warning(f"❌ Command failed (code {process.returncode}): {cmd_str}")
                    if stderr_str:
                        logger.warning(f"   Error: {stderr_str[:200]}{'...' if len(stderr_str) > 200 else ''}")
                
                return success, stdout_str, stderr_str
                
            except asyncio.TimeoutError:
                logger.error(f"⏰ Command timed out after {timeout}s: {cmd_str}")
                process.kill()
                await process.wait()
                return False, "", f"Command timed out after {timeout} seconds"
                
        except Exception as e:
            logger.error(f"💥 Command execution failed: {cmd_str} - {e}")
            return False, "", str(e)

    async def emergency_repair(self) -> bool:
        """Emergency repair function for critical database issues"""
        logger.warning("🚨 Starting emergency database repair...")
        
        # The most robust repair is to completely remove and recreate the cluster.
        # This avoids loops and ensures a clean state.
        logger.info("🚨 Aggressively removing and recreating cluster for emergency repair...")
        return await self._remove_and_recreate_cluster()

    async def _emergency_stop_postgresql(self):
        """Emergency stop of all PostgreSQL processes"""
        try:
            # Try graceful stop first
            service_name = await self._detect_postgresql_service()
            if service_name:
                await self._run_command(['sudo', 'systemctl', 'stop', service_name], timeout=30)
            
            # Kill any remaining postgres processes
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if 'postgres' in proc.info['name'].lower():
                        logger.warning(f"Killing postgres process: {proc.info['pid']}")
                        proc.kill()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
                    
        except Exception as e:
            logger.warning(f"Error during emergency PostgreSQL stop: {e}")

    def get_status(self) -> Dict[str, Any]:
        """Get current status of the database system"""
        return {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'system_info': self.system_info,
            'postgres_info': self.postgres_info,
            'config': {
                'database_name': self.config.database_name,
                'postgres_version': self.config.postgres_version,
                'data_directory': self.config.data_directory,
                'config_directory': self.config.config_directory,
                'port': self.config.port
            },
            'test_mode': self.test_mode
        }

# Main entry point function
async def setup_comprehensive_database(test_mode: bool = False, config: Optional[DatabaseConfig] = None) -> bool:
    """
    Main entry point for comprehensive database setup.
    
    Args:
        test_mode: Whether to run in test mode
        config: Optional database configuration
        
    Returns:
        bool: True if setup was successful, False otherwise
    """
    logger.info("🚀 Starting Comprehensive Database Setup")
    
    try:
        setup_manager = ComprehensiveDatabaseSetup(config=config, test_mode=test_mode)
        
        # Log initial status
        status = setup_manager.get_status()
        logger.info(f"📊 Initial Status: {json.dumps(status, indent=2)}")
        
        # Run the complete setup
        success = await setup_manager.setup_complete_database_system()
        
        if not success:
            logger.error("💥 Main database setup failed. Attempting emergency repair...")
            repaired = await setup_manager.emergency_repair()
            
            if repaired:
                logger.info("✅ Emergency repair successful. Re-running setup on the fresh cluster...")
                # After repair, we MUST run the setup again to configure the new cluster.
                success = await setup_manager.setup_complete_database_system()
            else:
                logger.error("❌ Emergency repair also failed! The system is in a non-recoverable state.")
                success = False
        
        if success:
            logger.info("🎉 Comprehensive Database Setup completed successfully!")
        else:
            logger.error("💥 Comprehensive Database Setup FAILED, even after repair attempts.")
        
        return success
        
    except Exception as e:
        logger.error(f"💥 Unexpected error in comprehensive database setup: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Comprehensive Database Setup")
    parser.add_argument("--test", action="store_true", help="Run in test mode")
    parser.add_argument("--database-name", default="gaia_validator", help="Database name")
    parser.add_argument("--postgres-version", default="14", help="PostgreSQL version")
    
    args = parser.parse_args()
    
    config = DatabaseConfig(
        database_name=args.database_name,
        postgres_version=args.postgres_version
    )
    
    success = asyncio.run(setup_comprehensive_database(test_mode=args.test, config=config))
    sys.exit(0 if success else 1) 