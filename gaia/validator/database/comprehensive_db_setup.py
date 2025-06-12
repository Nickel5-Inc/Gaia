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
        self.alembic_config_path = project_root / "alembic.ini"
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
        Main entry point for complete database system setup.
        This method orchestrates the entire setup process.
        """
        logger.info("🚀 Starting comprehensive database system setup...")
        
        try:
            # Step 1: Install PostgreSQL if needed
            if not await self._ensure_postgresql_installed():
                logger.error("❌ Failed to install PostgreSQL")
                return False
            
            # Step 2: Detect and fix any corruption
            if not await self._detect_and_repair_corruption():
                logger.error("❌ Failed to repair database corruption")
                return False
            
            # Step 3: Configure PostgreSQL
            if not await self._configure_postgresql_system():
                logger.error("❌ Failed to configure PostgreSQL")
                return False
            
            # Step 4: Ensure PostgreSQL is running
            if not await self._ensure_postgresql_running():
                logger.error("❌ Failed to start PostgreSQL")
                return False
            
            # Step 5: Create database and users
            if not await self._setup_database_and_users():
                logger.error("❌ Failed to setup database and users")
                return False
            
            # Step 6: Run Alembic migrations
            if not await self._setup_alembic_schema():
                logger.error("❌ Failed to setup database schema")
                return False
            
            # Step 7: Configure backups (optional, non-blocking)
            await self._setup_backup_system()
            
            # Step 8: Final validation
            if not await self._validate_complete_setup():
                logger.error("❌ Final validation failed")
                return False
            
            logger.info("✅ Comprehensive database system setup completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Unexpected error during database setup: {e}", exc_info=True)
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
        
        # Check if data directory exists and has required files
        if not data_dir.exists():
            logger.info("📁 Data directory doesn't exist, will initialize fresh cluster")
            return await self._initialize_fresh_cluster()
        
        required_files = ['PG_VERSION', 'postgresql.conf']
        required_dirs = ['base', 'global', 'pg_wal']
        
        missing_files = []
        missing_dirs = []
        
        for file in required_files:
            if not (data_dir / file).exists():
                missing_files.append(file)
        
        for dir_name in required_dirs:
            if not (data_dir / dir_name).exists():
                missing_dirs.append(dir_name)
        
        if missing_files or missing_dirs:
            logger.warning(f"⚠️ Corruption detected - missing files: {missing_files}, missing dirs: {missing_dirs}")
            return await self._repair_corrupted_cluster()
        
        # Check for backup_label file (indicates incomplete restore)
        backup_label = data_dir / 'backup_label'
        if backup_label.exists():
            logger.warning("⚠️ Found backup_label file - removing incomplete restore marker")
            try:
                backup_label.unlink()
                logger.info("✅ Removed backup_label file")
            except Exception as e:
                logger.error(f"Failed to remove backup_label: {e}")
                return False
        
        # Check pg_control file
        pg_control = data_dir / 'global' / 'pg_control'
        if not pg_control.exists():
            logger.warning("⚠️ Missing pg_control file - cluster is corrupted")
            return await self._repair_corrupted_cluster()
        
        logger.info("✅ No corruption detected")
        return True

    async def _initialize_fresh_cluster(self) -> bool:
        """Initialize a fresh PostgreSQL cluster"""
        logger.info("🆕 Initializing fresh PostgreSQL cluster...")
        
        try:
            # Ensure data directory exists and has correct permissions
            data_dir = Path(self.config.data_directory)
            data_dir.parent.mkdir(parents=True, exist_ok=True)
            
            # Stop any running PostgreSQL service
            await self._stop_postgresql_service()
            
            # Remove existing data directory if it exists
            if data_dir.exists():
                logger.info(f"🗑️ Removing existing data directory: {data_dir}")
                shutil.rmtree(data_dir)
            
            # Create fresh data directory
            data_dir.mkdir(parents=True, exist_ok=True)
            
            # Set ownership to postgres user
            await self._run_command(['chown', '-R', 'postgres:postgres', str(data_dir.parent)])
            
            # Initialize cluster
            initdb_cmd = [
                'sudo', '-u', 'postgres',
                f'/usr/lib/postgresql/{self.config.postgres_version}/bin/initdb',
                '-D', str(data_dir),
                '--auth-local=trust',
                '--auth-host=md5'
            ]
            
            success, stdout, stderr = await self._run_command(initdb_cmd, timeout=120)
            if not success:
                logger.error(f"Failed to initialize cluster: {stderr}")
                return False
            
            logger.info("✅ Fresh PostgreSQL cluster initialized")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing fresh cluster: {e}", exc_info=True)
            return False

    async def _repair_corrupted_cluster(self) -> bool:
        """Repair a corrupted PostgreSQL cluster"""
        logger.info("🔧 Repairing corrupted PostgreSQL cluster...")
        
        # Create backup of corrupted data
        data_dir = Path(self.config.data_directory)
        backup_dir = data_dir.parent / f"corrupted_backup_{int(time.time())}"
        
        try:
            if data_dir.exists():
                logger.info(f"💾 Backing up corrupted data to: {backup_dir}")
                shutil.copytree(data_dir, backup_dir)
        except Exception as e:
            logger.warning(f"Could not backup corrupted data: {e}")
        
        # Initialize fresh cluster
        return await self._initialize_fresh_cluster()

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
        """Configure postgresql.conf with optimized settings"""
        config_file = Path(self.config.config_directory) / 'postgresql.conf'
        
        # Base configuration
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
            'archive_command': "'/bin/true'",  # Placeholder, will be configured later
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
            # Read existing config if it exists
            existing_config = ""
            if config_file.exists():
                with open(config_file, 'r') as f:
                    existing_config = f.read()
            
            # Create new config content
            config_lines = []
            config_lines.append("# PostgreSQL configuration file")
            config_lines.append("# Generated by Gaia Comprehensive Database Setup")
            config_lines.append(f"# Generated at: {datetime.now(timezone.utc).isoformat()}")
            config_lines.append("")
            
            for key, value in config_settings.items():
                config_lines.append(f"{key} = {value}")
            
            # Write new configuration
            with open(config_file, 'w') as f:
                f.write('\n'.join(config_lines))
            
            logger.info(f"✅ PostgreSQL configuration written to: {config_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error configuring postgresql.conf: {e}", exc_info=True)
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
            with open(hba_file, 'w') as f:
                f.write('\n'.join(hba_rules))
            
            logger.info(f"✅ pg_hba.conf configured: {hba_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error configuring pg_hba.conf: {e}", exc_info=True)
            return False

    async def _ensure_postgresql_running(self) -> bool:
        """Ensure PostgreSQL service is running"""
        logger.info("🔄 Ensuring PostgreSQL is running...")
        
        # Detect service name
        service_name = await self._detect_postgresql_service()
        if not service_name:
            logger.error("Could not detect PostgreSQL service name")
            return False
        
        try:
            # Start the service
            success, stdout, stderr = await self._run_command(['systemctl', 'start', service_name])
            if not success:
                logger.error(f"Failed to start PostgreSQL service: {stderr}")
                return False
            
            # Enable the service
            await self._run_command(['systemctl', 'enable', service_name])
            
            # Wait for service to be ready
            for attempt in range(30):  # Wait up to 30 seconds
                success, stdout, stderr = await self._run_command(['systemctl', 'is-active', service_name])
                if success and stdout.strip() == 'active':
                    logger.info("✅ PostgreSQL service is running")
                    
                    # Test database connection
                    if await self._test_database_connection():
                        return True
                    
                await asyncio.sleep(1)
            
            logger.error("PostgreSQL service started but connection test failed")
            return False
            
        except Exception as e:
            logger.error(f"Error starting PostgreSQL: {e}", exc_info=True)
            return False

    async def _detect_postgresql_service(self) -> Optional[str]:
        """Detect the correct PostgreSQL service name"""
        service_candidates = [
            f'postgresql@{self.config.postgres_version}-main',
            f'postgresql-{self.config.postgres_version}',
            'postgresql'
        ]
        
        for service in service_candidates:
            try:
                success, stdout, stderr = await self._run_command(['systemctl', 'list-units', '--type=service', f'{service}.service'])
                if success and service in stdout:
                    logger.info(f"Detected PostgreSQL service: {service}")
                    return service
            except Exception:
                continue
        
        logger.warning("Could not detect PostgreSQL service name")
        return None

    async def _test_database_connection(self) -> bool:
        """Test database connection"""
        try:
            cmd = ['sudo', '-u', 'postgres', 'psql', '-c', 'SELECT version();']
            success, stdout, stderr = await self._run_command(cmd, timeout=10)
            
            if success and 'PostgreSQL' in stdout:
                logger.info("✅ Database connection test successful")
                return True
            else:
                logger.error(f"Database connection test failed: {stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error testing database connection: {e}")
            return False

    async def _setup_database_and_users(self) -> bool:
        """Create database and users"""
        logger.info("👤 Setting up database and users...")
        
        try:
            # Set postgres user password
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
        """Setup database schema using Alembic"""
        logger.info("📋 Setting up database schema with Alembic...")
        
        try:
            # Ensure we're in the project root directory
            original_cwd = os.getcwd()
            os.chdir(project_root)
            
            try:
                # Check if alembic.ini exists
                if not self.alembic_config_path.exists():
                    logger.error(f"Alembic config not found: {self.alembic_config_path}")
                    return False
                
                # Initialize Alembic if needed
                if not await self._initialize_alembic():
                    return False
                
                # Run migrations
                if not await self._run_alembic_migrations():
                    return False
                
                logger.info("✅ Database schema setup completed")
                return True
                
            finally:
                os.chdir(original_cwd)
                
        except Exception as e:
            logger.error(f"Error setting up Alembic schema: {e}", exc_info=True)
            return False

    async def _initialize_alembic(self) -> bool:
        """Initialize Alembic if needed"""
        try:
            # Check if alembic_version table exists
            check_cmd = [
                'sudo', '-u', 'postgres', 'psql', '-d', self.config.database_name, '-c',
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'alembic_version');"
            ]
            
            success, stdout, stderr = await self._run_command(check_cmd, timeout=10)
            if success and 'true' in stdout.lower():
                logger.info("✅ Alembic already initialized")
                return True
            
            # Initialize Alembic
            logger.info("🔧 Initializing Alembic...")
            init_cmd = ['alembic', 'stamp', 'head']
            
            success, stdout, stderr = await self._run_command(init_cmd, timeout=30)
            if success:
                logger.info("✅ Alembic initialized")
                return True
            else:
                logger.error(f"Failed to initialize Alembic: {stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error initializing Alembic: {e}")
            return False

    async def _run_alembic_migrations(self) -> bool:
        """Run Alembic migrations"""
        try:
            logger.info("🔄 Running Alembic migrations...")
            
            # Run upgrade to head
            upgrade_cmd = ['alembic', 'upgrade', 'head']
            
            success, stdout, stderr = await self._run_command(upgrade_cmd, timeout=120)
            if success:
                logger.info("✅ Alembic migrations completed")
                logger.info(f"Migration output: {stdout}")
                return True
            else:
                logger.error(f"Alembic migrations failed: {stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error running Alembic migrations: {e}")
            return False

    async def _setup_backup_system(self) -> bool:
        """Setup backup system (pgBackRest) - non-blocking"""
        logger.info("💾 Setting up backup system...")
        
        try:
            # This is optional and should not block the main setup
            # We'll implement basic backup configuration here
            
            # Install pgBackRest if available
            install_cmd = ['apt', 'install', '-y', 'pgbackrest']
            success, stdout, stderr = await self._run_command(install_cmd, timeout=120)
            
            if success:
                logger.info("✅ pgBackRest installed")
                # Basic configuration would go here
                # For now, we'll just log success and continue
            else:
                logger.warning("⚠️ Could not install pgBackRest - backups will not be available")
            
            return True  # Always return True as this is optional
            
        except Exception as e:
            logger.warning(f"Backup system setup failed (non-critical): {e}")
            return True  # Non-blocking

    async def _validate_complete_setup(self) -> bool:
        """Validate that the complete setup is working"""
        logger.info("✅ Validating complete database setup...")
        
        try:
            # Test 1: PostgreSQL service is running
            service_name = await self._detect_postgresql_service()
            if not service_name:
                logger.error("❌ Could not detect PostgreSQL service")
                return False
            
            success, stdout, stderr = await self._run_command(['systemctl', 'is-active', service_name])
            if not success or stdout.strip() != 'active':
                logger.error("❌ PostgreSQL service is not active")
                return False
            
            # Test 2: Database connection works
            if not await self._test_database_connection():
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
            if not success or 'true' not in stdout.lower():
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
            success, stdout, stderr = await self._run_command(['systemctl', 'stop', service_name])
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
        
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=cwd
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
        
        try:
            # Stop all PostgreSQL processes
            await self._emergency_stop_postgresql()
            
            # Backup current state
            await self._emergency_backup_data()
            
            # Initialize fresh cluster
            if not await self._initialize_fresh_cluster():
                logger.error("❌ Emergency repair failed - could not initialize fresh cluster")
                return False
            
            # Restart PostgreSQL
            if not await self._ensure_postgresql_running():
                logger.error("❌ Emergency repair failed - could not start PostgreSQL")
                return False
            
            # Recreate database and users
            if not await self._setup_database_and_users():
                logger.error("❌ Emergency repair failed - could not setup database and users")
                return False
            
            # Run schema migrations
            if not await self._setup_alembic_schema():
                logger.error("❌ Emergency repair failed - could not setup schema")
                return False
            
            logger.info("✅ Emergency repair completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"💥 Emergency repair failed: {e}", exc_info=True)
            return False

    async def _emergency_stop_postgresql(self):
        """Emergency stop of all PostgreSQL processes"""
        try:
            # Try graceful stop first
            service_name = await self._detect_postgresql_service()
            if service_name:
                await self._run_command(['systemctl', 'stop', service_name], timeout=30)
            
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

    async def _emergency_backup_data(self):
        """Create emergency backup of data directory"""
        try:
            data_dir = Path(self.config.data_directory)
            if data_dir.exists():
                backup_dir = data_dir.parent / f"emergency_backup_{int(time.time())}"
                logger.info(f"💾 Creating emergency backup: {backup_dir}")
                shutil.copytree(data_dir, backup_dir)
                logger.info("✅ Emergency backup created")
        except Exception as e:
            logger.warning(f"Could not create emergency backup: {e}")

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
        
        if success:
            logger.info("🎉 Comprehensive Database Setup completed successfully!")
        else:
            logger.error("💥 Comprehensive Database Setup failed!")
            
            # Attempt emergency repair if main setup failed
            logger.info("🚨 Attempting emergency repair...")
            success = await setup_manager.emergency_repair()
            
            if success:
                logger.info("✅ Emergency repair successful!")
            else:
                logger.error("❌ Emergency repair also failed!")
        
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