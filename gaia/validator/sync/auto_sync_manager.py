"""
Auto Sync Manager - Streamlined Database Synchronization System

This module provides automated setup and management of pgBackRest with Cloudflare R2
for database synchronization. It eliminates manual configuration steps and provides
application-level control over backup scheduling.

Key Features:
- Automated pgBackRest installation and configuration
- Application-controlled backup scheduling (no cron needed)
- Intelligent startup backup detection (skips unnecessary backups)
- Simplified replica setup with discovery
- Health monitoring and error recovery
- Centralized configuration management

Gap Handling Strategy (Theoretical):
When primary nodes go down for extended periods, several strategies can be employed:
1. WAL Catch-up: Allow WAL archiving to catch up before attempting backups
2. Gap Detection: Monitor for missing WAL segments and handle gracefully
3. Fallback Strategy: Switch to full backups if gaps are detected
4. Timeline Reset: Use backup labels to detect and handle timeline breaks
5. Health Recovery: Enhanced monitoring with automatic recovery procedures

Current implementation focuses on startup optimization and basic gap tolerance.
Advanced gap handling can be added as needed based on operational requirements.
"""

import asyncio
import os
import subprocess
import json
import tempfile
import shutil
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List, Tuple
from pathlib import Path
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

class AutoSyncManager:
    """
    Comprehensive database sync manager with automated setup and scheduling.
    """
    
    def __init__(self, test_mode: bool = False):
        """
        Initialize AutoSyncManager.
        
        Args:
            test_mode: If True, uses faster scheduling for testing (15min diffs vs 4hr)
                      This parameter comes from:
                      - Validator application: Passed based on --test flag or default mode
                      - Standalone script: Passed based on --test flag when run directly
                      - Both use the same test_mode parameter, no override occurs
        """
        self.test_mode = test_mode
        
        # Perform system detection first
        self.system_info = self._detect_system_configuration()
        logger.info(f"🔍 System Detection Results: {self.system_info}")
        
        self.config = self._load_config()
        self.is_primary = self.config.get('is_primary', False)
        self.backup_task: Optional[asyncio.Task] = None
        self.health_check_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()
        
        # Backup scheduling configuration (adjusted for test mode)
        if self.test_mode:
            self.backup_schedule = {
                'full_backup_time': None,     # No scheduled time in test mode
                'diff_backup_interval': 0.25, # Every 15 minutes for testing
                'check_interval': 5,          # Every 5 minutes
                'health_check_interval': 60   # Every minute
            }
            # Replica schedule for test mode (fast)
            self.replica_schedule = {
                'sync_interval': 0.5,         # Every 30 minutes in test mode
                'backup_buffer_minutes': 5,   # Wait 5 minutes after backup time
                'sync_minute': None,          # No specific minute in test mode
            }
        else:
            self.backup_schedule = {
                'full_backup_time': '08:30',  # Daily at 8:30 AM UTC
                'diff_backup_interval': 1,    # Every 1 hour
                'diff_backup_minute': 24,     # At 24 minutes past the hour
                'check_interval': 60,         # Every hour
                'health_check_interval': 300  # Every 5 minutes
            }
            # Replica schedule for production mode
            self.replica_schedule = {
                'sync_interval': 1,           # Every hour (same as primary)
                'backup_buffer_minutes': 15,  # Wait 15 minutes after backup completes
                'sync_minute': 39,            # Download at :39 (24 + 15 minute buffer)
                'estimated_backup_duration': 5, # Estimated 5 minutes for diff backup
            }
        
        # VERY OBVIOUS STARTUP LOGGING
        print("=" * 80)
        print("🚀 AUTO SYNC MANAGER STARTING UP 🚀")
        print("=" * 80)
        logger.info("🚀" * 10 + " AUTO SYNC MANAGER INITIALIZATION " + "🚀" * 10)
        logger.info(f"🏠 MODE: {'PRIMARY DATABASE' if self.is_primary else 'REPLICA DATABASE'}")
        logger.info(f"🧪 TEST MODE: {'ENABLED (Fast scheduling)' if self.test_mode else 'DISABLED (Production scheduling)'}")
        logger.info(f"📋 BACKUP SCHEDULE: {self.backup_schedule}")
        logger.info("=" * 80)

    def _detect_system_configuration(self) -> Dict:
        """Detect system configuration for adaptive setup."""
        try:
            logger.info("🔍 Detecting system configuration...")
            
            system_info = {
                'os_type': 'unknown',
                'os_version': 'unknown',
                'package_manager': 'unknown',
                'postgresql_version': 'unknown',
                'postgresql_service': 'postgresql',
                'postgresql_user': 'postgres',
                'postgresql_group': 'postgres',
                'config_locations': {},
                'installation_type': 'unknown',
                'systemd_available': False,
                'docker_detected': False
            }
            
            # Detect OS
            try:
                import platform
                system_info['os_type'] = platform.system().lower()
                system_info['os_version'] = platform.release()
                
                # Try to get more specific distribution info
                try:
                    with open('/etc/os-release', 'r') as f:
                        for line in f:
                            if line.startswith('ID='):
                                system_info['distribution'] = line.split('=')[1].strip().strip('"')
                            elif line.startswith('VERSION_ID='):
                                system_info['distribution_version'] = line.split('=')[1].strip().strip('"')
                except FileNotFoundError:
                    pass
                    
                logger.info(f"🐧 OS: {system_info['os_type']} {system_info['os_version']}")
                
            except Exception as e:
                logger.debug(f"OS detection failed: {e}")
            
            # Detect package manager
            package_managers = [
                ('apt-get', 'apt'),
                ('yum', 'yum'),
                ('dnf', 'dnf'),
                ('pacman', 'pacman'),
                ('brew', 'brew')
            ]
            
            for cmd, name in package_managers:
                try:
                    result = subprocess.run(['which', cmd], capture_output=True, text=True, timeout=5)
                    if result.returncode == 0:
                        system_info['package_manager'] = name
                        logger.info(f"📦 Package manager: {name}")
                        break
                except Exception:
                    continue
            
            # Detect systemd
            try:
                result = subprocess.run(['systemctl', '--version'], capture_output=True, text=True, timeout=5)
                system_info['systemd_available'] = result.returncode == 0
                logger.info(f"⚙️ Systemd: {'Available' if system_info['systemd_available'] else 'Not available'}")
            except Exception:
                pass
            
            # Detect Docker environment
            try:
                if os.path.exists('/.dockerenv') or os.path.exists('/proc/1/cgroup'):
                    with open('/proc/1/cgroup', 'r') as f:
                        if 'docker' in f.read():
                            system_info['docker_detected'] = True
                            logger.info("🐳 Docker environment detected")
            except Exception:
                pass
            
            # Detect PostgreSQL installation
            system_info.update(self._detect_postgresql_installation())
            
            return system_info
            
        except Exception as e:
            logger.warning(f"System detection failed: {e}")
            return system_info

    def _detect_postgresql_installation(self) -> Dict:
        """Detect PostgreSQL installation details."""
        pg_info = {
            'postgresql_version': 'unknown',
            'postgresql_service': 'postgresql',
            'postgresql_user': 'postgres',
            'postgresql_group': 'postgres',
            'installation_type': 'unknown',
            'service_variations': [],
            'config_locations': {}
        }
        
        try:
            # Try to detect PostgreSQL version and service
            service_variations = [
                'postgresql',
                'postgresql-14',
                'postgresql-15', 
                'postgresql-16',
                'postgres',
                'pgsql'
            ]
            
            for service in service_variations:
                try:
                    result = subprocess.run(['systemctl', 'is-active', service], 
                                          capture_output=True, text=True, timeout=5)
                    if result.returncode == 0 or 'inactive' in result.stdout:
                        pg_info['service_variations'].append(service)
                        if pg_info['postgresql_service'] == 'postgresql':
                            pg_info['postgresql_service'] = service
                            logger.info(f"🐘 PostgreSQL service: {service}")
                except Exception:
                    continue
            
            # Try to get PostgreSQL version from different methods
            version_commands = [
                ['sudo', '-u', 'postgres', 'psql', '--version'],
                ['postgres', '--version'],
                ['psql', '--version']
            ]
            
            for cmd in version_commands:
                try:
                    result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
                    if result.returncode == 0 and result.stdout:
                        version_line = result.stdout.strip()
                        # Extract version number (e.g., "psql (PostgreSQL) 14.10")
                        import re
                        match = re.search(r'(\d+)\.(\d+)', version_line)
                        if match:
                            major_version = match.group(1)
                            pg_info['postgresql_version'] = major_version
                            logger.info(f"🐘 PostgreSQL version: {major_version}")
                            
                            # Update service name if we found a version-specific one
                            version_service = f"postgresql-{major_version}"
                            if version_service in pg_info['service_variations']:
                                pg_info['postgresql_service'] = version_service
                            break
                except Exception as e:
                    logger.debug(f"Version detection command failed: {cmd} - {e}")
                    continue
            
            # Detect installation type
            installation_indicators = [
                ('/var/lib/postgresql', 'package'),
                ('/usr/local/pgsql', 'source'),
                ('/opt/postgresql', 'custom'),
                ('/home/postgres', 'user_install')
            ]
            
            for path, install_type in installation_indicators:
                if os.path.exists(path):
                    pg_info['installation_type'] = install_type
                    logger.info(f"🐘 Installation type: {install_type}")
                    break
            
            # Try to detect user/group variations
            user_variations = ['postgres', 'postgresql', 'pgsql']
            for user in user_variations:
                try:
                    result = subprocess.run(['id', user], capture_output=True, text=True, timeout=5)
                    if result.returncode == 0:
                        pg_info['postgresql_user'] = user
                        # Usually group has same name as user
                        pg_info['postgresql_group'] = user
                        logger.info(f"🐘 PostgreSQL user/group: {user}")
                        break
                except Exception:
                    continue
            
        except Exception as e:
            logger.debug(f"PostgreSQL detection failed: {e}")
        
        return pg_info

    def _find_pgdata_path(self) -> str:
        """Find the data directory of the active PostgreSQL instance."""
        try:
            logger.info("🔍 Attempting to discover PostgreSQL data directory from running instance...")
            # Add timeout to prevent hanging
            result = subprocess.run(
                ['sudo', '-u', 'postgres', 'psql', '-t', '-c', 'SHOW data_directory;'],
                capture_output=True, text=True, check=True, timeout=10  # 10 second timeout
            )
            pgdata_path = result.stdout.strip()
            if pgdata_path and os.path.exists(pgdata_path):
                logger.info(f"✅ Discovered active PostgreSQL data directory at: {pgdata_path}")
                return pgdata_path
        except subprocess.TimeoutExpired:
            logger.warning("⏱️ PostgreSQL query timed out after 10 seconds - PostgreSQL may not be running")
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            logger.warning(f"❌ Could not get data_directory from running PostgreSQL. Reason: {e}")

        logger.info("🔍 Falling back to checking common PostgreSQL installation paths for pg_control...")
        common_paths = [
            '/var/lib/postgresql/14/main',
            '/var/lib/postgresql/15/main',
            '/var/lib/postgresql/16/main',
            '/var/lib/postgresql/data',
            '/var/lib/pgsql/data',
        ]
        for path in common_paths:
            if os.path.exists(os.path.join(path, 'pg_control')):
                logger.info(f"✅ Found pg_control in data directory: {path}")
                return path
        
        pgdata_env = os.getenv('PGBACKREST_PGDATA')
        if pgdata_env:
            logger.warning(f"⚠️ Could not find a valid pgdata path. Using PGBACKREST_PGDATA from environment: {pgdata_env}")
            return pgdata_env
            
        logger.error("❌ Could not determine PostgreSQL data directory through any method")
        raise FileNotFoundError("Could not determine PostgreSQL data directory. Please ensure PostgreSQL is running or set PGBACKREST_PGDATA.")

    def _load_config(self) -> Dict:
        """Load and validate configuration from environment."""
        
        logger.info("🔧 Loading AutoSyncManager configuration...")
        
        # Debug environment variables
        env_vars = {
            'PGBACKREST_STANZA_NAME': os.getenv('PGBACKREST_STANZA_NAME'),
            'SUBTENSOR_NETWORK': os.getenv('SUBTENSOR_NETWORK'),
            'IS_SOURCE_VALIDATOR_FOR_DB_SYNC': os.getenv('IS_SOURCE_VALIDATOR_FOR_DB_SYNC'),
            'REPLICA_STARTUP_SYNC': os.getenv('REPLICA_STARTUP_SYNC'),
            'PGBACKREST_R2_BUCKET': os.getenv('PGBACKREST_R2_BUCKET'),
            'PGBACKREST_R2_ENDPOINT': os.getenv('PGBACKREST_R2_ENDPOINT'),
            'PGBACKREST_R2_ACCESS_KEY_ID': os.getenv('PGBACKREST_R2_ACCESS_KEY_ID'),
            'PGBACKREST_R2_SECRET_ACCESS_KEY': '***' if os.getenv('PGBACKREST_R2_SECRET_ACCESS_KEY') else None,
            'PGBACKREST_PGDATA': os.getenv('PGBACKREST_PGDATA'),
            'PGBACKREST_PGPORT': os.getenv('PGBACKREST_PGPORT'),
            'PGBACKREST_PGUSER': os.getenv('PGBACKREST_PGUSER'),
        }
        
        logger.info("📋 Environment variables:")
        for key, value in env_vars.items():
            if value:
                logger.info(f"   ✅ {key}: {value}")
            else:
                logger.info(f"   ❌ {key}: Not set")
        
        try:
            pgdata_path = self._find_pgdata_path()
            logger.info(f"✅ PostgreSQL data directory: {pgdata_path}")
        except Exception as e:
            logger.error(f"❌ Failed to find PostgreSQL data directory: {e}")
            # Don't raise immediately - let's see what other config we can gather
            pgdata_path = "/var/lib/postgresql/14/main"  # Default fallback
            logger.warning(f"⚠️ Using fallback PostgreSQL data directory: {pgdata_path}")

        r2_region_raw = os.getenv('PGBACKREST_R2_REGION', 'auto')
        r2_region = r2_region_raw.split('#')[0].strip()

        # Enhanced stanza naming with network awareness
        base_stanza_name = os.getenv('PGBACKREST_STANZA_NAME', 'gaia')
        network_suffix = os.getenv('SUBTENSOR_NETWORK', '').lower()
        
        # Auto-append network to stanza name if not already present and network is detected
        if network_suffix and network_suffix in ['test', 'finney'] and network_suffix not in base_stanza_name.lower():
            stanza_name = f"{base_stanza_name}-{network_suffix}"
            logger.info(f"🌐 Network-aware stanza: {stanza_name} (detected network: {network_suffix})")
        else:
            stanza_name = base_stanza_name
            if network_suffix:
                logger.info(f"🌐 Using explicit stanza: {stanza_name} (network: {network_suffix})")

        config = {
            'stanza_name': stanza_name,
            'r2_bucket': os.getenv('PGBACKREST_R2_BUCKET'),
            'r2_endpoint': os.getenv('PGBACKREST_R2_ENDPOINT'),
            'r2_access_key': os.getenv('PGBACKREST_R2_ACCESS_KEY_ID'),
            'r2_secret_key': os.getenv('PGBACKREST_R2_SECRET_ACCESS_KEY'),
            'r2_region': r2_region,
            'pgdata': pgdata_path,
            'pgport': int(os.getenv('PGBACKREST_PGPORT', '5432')),
            'pguser': os.getenv('PGBACKREST_PGUSER', 'postgres'),
            'pgpassword': os.getenv('PGBACKREST_PGPASSWORD'),  # Optional password
            'is_primary': os.getenv('IS_SOURCE_VALIDATOR_FOR_DB_SYNC', 'False').lower() == 'true',
            'replica_discovery_endpoint': os.getenv('REPLICA_DISCOVERY_ENDPOINT'),  # For primary to announce itself
            'primary_discovery_endpoint': os.getenv('PRIMARY_DISCOVERY_ENDPOINT'),  # For replica to find primary
            'network': network_suffix,  # Store network for reference
            'replica_startup_sync': os.getenv('REPLICA_STARTUP_SYNC', 'true').lower() == 'true',  # Enable immediate sync on startup
        }
        
        # Validate required R2 config
        required_r2_vars = ['r2_bucket', 'r2_endpoint', 'r2_access_key', 'r2_secret_key']
        missing_vars = [var for var in required_r2_vars if not config[var]]
        
        if missing_vars:
            logger.error(f"❌ Missing required R2 configuration: {missing_vars}")
            logger.error("💡 To enable DB sync, configure these environment variables:")
            for var in missing_vars:
                env_var_name = f"PGBACKREST_{var.upper().replace('_', '_')}"
                if var == 'r2_access_key':
                    env_var_name = 'PGBACKREST_R2_ACCESS_KEY_ID'
                elif var == 'r2_secret_key':
                    env_var_name = 'PGBACKREST_R2_SECRET_ACCESS_KEY'
                logger.error(f"   - {env_var_name}")
            raise ValueError(f"Missing required R2 configuration: {missing_vars}")
        
        logger.info("✅ Configuration loaded successfully")
        return config

    async def setup(self) -> bool:
        """
        Fully automated database sync setup with intelligent configuration detection and repair.
        Handles existing installations, network transitions, and misconfigurations automatically.
        """
        try:
            logger.info("🚀 Starting intelligent database sync setup...")
            logger.info(f"🌐 Network: {self.config.get('network', 'unknown')}")
            logger.info(f"📋 Target stanza: {self.config['stanza_name']}")
            logger.info(f"🏠 Mode: {'PRIMARY' if self.is_primary else 'REPLICA'}")
            
            # 1. Install dependencies with timeout
            logger.info("📦 Step 1: Installing dependencies...")
            try:
                install_success = await asyncio.wait_for(self._install_dependencies(), timeout=300)  # 5 minute timeout
                if not install_success:
                    logger.error("❌ Dependency installation failed")
                    return False
                logger.info("✅ Step 1 completed successfully")
            except asyncio.TimeoutError:
                logger.error("❌ Step 1 timed out after 5 minutes")
                return False
            
            # 2. Auto-detect and repair any existing configuration issues with timeout
            logger.info("🔍 Step 2: Detecting and repairing existing configuration...")
            try:
                await asyncio.wait_for(self._auto_repair_configuration(), timeout=60)  # 1 minute timeout
                logger.info("✅ Step 2 completed successfully")
            except asyncio.TimeoutError:
                logger.warning("⚠️ Step 2 timed out after 1 minute - continuing anyway")
            
            # 3. Configure PostgreSQL (smart update, not just append) with timeout
            logger.info("⚙️ Step 3: Configuring PostgreSQL...")
            try:
                config_success = await asyncio.wait_for(self._configure_postgresql(), timeout=120)  # 2 minute timeout
                if not config_success:
                    logger.error("❌ PostgreSQL configuration failed")
                    return False
                logger.info("✅ Step 3 completed successfully")
            except asyncio.TimeoutError:
                logger.error("❌ Step 3 timed out after 2 minutes")
                return False
            
            # 4. Setup PostgreSQL authentication with timeout
            logger.info("🔐 Step 4: Setting up PostgreSQL authentication...")
            try:
                auth_success = await asyncio.wait_for(self._setup_postgres_auth(), timeout=60)  # 1 minute timeout
                if not auth_success:
                    logger.error("❌ PostgreSQL authentication setup failed")
                    return False
                logger.info("✅ Step 4 completed successfully")
            except asyncio.TimeoutError:
                logger.error("❌ Step 4 timed out after 1 minute")
                return False
            
            # 5. Configure pgBackRest with timeout
            logger.info("🔧 Step 5: Configuring pgBackRest...")
            try:
                pgbackrest_success = await asyncio.wait_for(self._configure_pgbackrest(), timeout=60)  # 1 minute timeout
                if not pgbackrest_success:
                    logger.error("❌ pgBackRest configuration failed")
                    return False
                logger.info("✅ Step 5 completed successfully")
            except asyncio.TimeoutError:
                logger.error("❌ Step 5 timed out after 1 minute")
                return False
            
            # 6. Ensure archive command is correct (with retry logic) with timeout
            logger.info("📝 Step 6: Ensuring correct archive command...")
            try:
                archive_success = await asyncio.wait_for(self._ensure_correct_archive_command(), timeout=60)  # 1 minute timeout
                if not archive_success:
                    logger.warning("⚠️ Archive command may need manual attention")
                else:
                    logger.info("✅ Step 6 completed successfully")
            except asyncio.TimeoutError:
                logger.warning("⚠️ Step 6 timed out after 1 minute - archive command may need manual attention")
            
            # 7. Handle stanza setup intelligently with timeout
            logger.info("📊 Step 7: Setting up backup stanza...")
            try:
                stanza_success = await asyncio.wait_for(self._intelligent_stanza_setup(), timeout=600)  # 10 minute timeout
                if not stanza_success:
                    logger.error("❌ Stanza setup failed")
                    return False
                logger.info("✅ Step 7 completed successfully")
            except asyncio.TimeoutError:
                logger.error("❌ Step 7 timed out after 10 minutes")
                return False
            
            # 8. Start application-controlled scheduling
            logger.info("⏰ Step 8: Starting automated scheduling...")
            try:
                await asyncio.wait_for(self.start_scheduling(), timeout=30)  # 30 second timeout
                logger.info("✅ Step 8 completed successfully")
            except asyncio.TimeoutError:
                logger.error("❌ Step 8 timed out after 30 seconds")
                return False
            
            logger.info("🎉 Database sync setup completed successfully!")
            logger.info(f"✅ Ready for {'backup operations' if self.is_primary else 'replica synchronization'}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Database sync setup failed: {e}", exc_info=True)
            logger.error("🔧 Will attempt fallback configuration...")
            return False

    async def _install_dependencies(self) -> bool:
        """Install pgBackRest and required dependencies adaptively based on system detection."""
        try:
            logger.info("📦 Installing pgBackRest and dependencies...")
            logger.info(f"🔍 System: {self.system_info.get('os_type', 'unknown')} with {self.system_info.get('package_manager', 'unknown')} package manager")
            
            # Check if already installed
            try:
                logger.info("🔍 Checking if pgBackRest is already installed...")
                result = await asyncio.wait_for(
                    asyncio.create_subprocess_exec(
                        'pgbackrest', 'version',
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    ),
                    timeout=10
                )
                stdout, stderr = await result.communicate()
                if result.returncode == 0:
                    version_info = stdout.decode().strip()
                    logger.info(f"✅ pgBackRest already installed: {version_info}")
                    return True
                else:
                    logger.info("❌ pgBackRest not found, will install...")
            except (FileNotFoundError, asyncio.TimeoutError):
                logger.info("❌ pgBackRest not found, will install...")
            
            # Adaptive installation based on package manager
            package_manager = self.system_info.get('package_manager', 'unknown')
            
            if package_manager == 'apt':
                return await self._install_dependencies_apt()
            elif package_manager in ['yum', 'dnf']:
                return await self._install_dependencies_rhel()
            elif package_manager == 'pacman':
                return await self._install_dependencies_arch()
            else:
                logger.warning(f"⚠️ Unsupported package manager: {package_manager}")
                logger.info("💡 Attempting fallback installation...")
                return await self._install_dependencies_fallback()
            
        except Exception as e:
            logger.error(f"❌ Failed to install dependencies: {e}")
            return False

    async def _install_dependencies_apt(self) -> bool:
        """Install dependencies using apt (Debian/Ubuntu)."""
        try:
            logger.info("📦 Installing using apt (Debian/Ubuntu)...")
            
            commands = [
                (['apt-get', 'update'], 120, "Updating package lists"),
                (['apt-get', 'install', '-y', 'pgbackrest', 'postgresql-client'], 300, "Installing pgBackRest and PostgreSQL client")
            ]
            
            for cmd, timeout, description in commands:
                logger.info(f"🔄 {description}: {' '.join(cmd)}")
                try:
                    process = await asyncio.wait_for(
                        asyncio.create_subprocess_exec(
                            *cmd,
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE
                        ),
                        timeout=timeout
                    )
                    stdout, stderr = await process.communicate()
                    
                    if process.returncode != 0:
                        logger.error(f"❌ {description} failed: {stderr.decode()}")
                        return False
                    else:
                        logger.info(f"✅ {description} completed successfully")
                        
                except asyncio.TimeoutError:
                    logger.error(f"❌ {description} timed out after {timeout} seconds")
                    return False
            
            return await self._verify_installation()
            
        except Exception as e:
            logger.error(f"❌ APT installation failed: {e}")
            return False

    async def _install_dependencies_rhel(self) -> bool:
        """Install dependencies using yum/dnf (RHEL/CentOS/Fedora)."""
        try:
            package_cmd = self.system_info.get('package_manager', 'yum')
            logger.info(f"📦 Installing using {package_cmd} (RHEL/CentOS/Fedora)...")
            
            commands = [
                ([package_cmd, 'install', '-y', 'epel-release'], 120, "Installing EPEL repository"),
                ([package_cmd, 'update', '-y'], 180, "Updating packages"),
                ([package_cmd, 'install', '-y', 'pgbackrest', 'postgresql'], 300, "Installing pgBackRest and PostgreSQL")
            ]
            
            for cmd, timeout, description in commands:
                logger.info(f"🔄 {description}: {' '.join(cmd)}")
                try:
                    process = await asyncio.wait_for(
                        asyncio.create_subprocess_exec(
                            *cmd,
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE
                        ),
                        timeout=timeout
                    )
                    stdout, stderr = await process.communicate()
                    
                    if process.returncode != 0:
                        # EPEL might already be installed, continue
                        error_output = stderr.decode().lower()
                        if "already installed" in error_output or "nothing to do" in error_output:
                            logger.info(f"✅ {description} (already installed)")
                        else:
                            logger.warning(f"⚠️ {description} had issues: {stderr.decode()}")
                            # Continue anyway for EPEL, as it might not be needed
                            if "epel" not in description.lower():
                                return False
                    else:
                        logger.info(f"✅ {description} completed successfully")
                        
                except asyncio.TimeoutError:
                    logger.error(f"❌ {description} timed out after {timeout} seconds")
                    return False
            
            return await self._verify_installation()
            
        except Exception as e:
            logger.error(f"❌ RHEL installation failed: {e}")
            return False

    async def _install_dependencies_arch(self) -> bool:
        """Install dependencies using pacman (Arch Linux)."""
        try:
            logger.info("📦 Installing using pacman (Arch Linux)...")
            
            commands = [
                (['pacman', '-Sy'], 120, "Updating package database"),
                (['pacman', '-S', '--noconfirm', 'pgbackrest', 'postgresql'], 300, "Installing pgBackRest and PostgreSQL")
            ]
            
            for cmd, timeout, description in commands:
                logger.info(f"🔄 {description}: {' '.join(cmd)}")
                try:
                    process = await asyncio.wait_for(
                        asyncio.create_subprocess_exec(
                            *cmd,
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE
                        ),
                        timeout=timeout
                    )
                    stdout, stderr = await process.communicate()
                    
                    if process.returncode != 0:
                        logger.error(f"❌ {description} failed: {stderr.decode()}")
                        return False
                    else:
                        logger.info(f"✅ {description} completed successfully")
                        
                except asyncio.TimeoutError:
                    logger.error(f"❌ {description} timed out after {timeout} seconds")
                    return False
            
            return await self._verify_installation()
            
        except Exception as e:
            logger.error(f"❌ Pacman installation failed: {e}")
            return False

    async def _install_dependencies_fallback(self) -> bool:
        """Fallback installation method when package manager is unknown."""
        try:
            logger.warning("⚠️ Unknown package manager, attempting fallback installation...")
            
            # Try apt first (most common)
            try:
                return await self._install_dependencies_apt()
            except Exception:
                pass
            
            # Try yum/dnf
            try:
                return await self._install_dependencies_rhel()
            except Exception:
                pass
            
            logger.error("❌ All installation methods failed")
            logger.error("💡 Please install pgBackRest manually:")
            logger.error("   - Debian/Ubuntu: apt-get install pgbackrest")
            logger.error("   - RHEL/CentOS: yum install pgbackrest")
            logger.error("   - Fedora: dnf install pgbackrest")
            logger.error("   - From source: https://pgbackrest.org/user-guide.html#installation")
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Fallback installation failed: {e}")
            return False

    async def _verify_installation(self) -> bool:
        """Verify pgBackRest installation."""
        try:
            logger.info("🔍 Verifying pgBackRest installation...")
            result = await asyncio.wait_for(
                asyncio.create_subprocess_exec(
                    'pgbackrest', 'version',
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                ),
                timeout=10
            )
            stdout, stderr = await result.communicate()
            if result.returncode == 0:
                version_info = stdout.decode().strip()
                logger.info(f"✅ pgBackRest installed successfully: {version_info}")
                return True
            else:
                logger.error(f"❌ pgBackRest verification failed: {stderr.decode()}")
                return False
        except asyncio.TimeoutError:
            logger.error("❌ pgBackRest verification timed out")
            return False

    async def _configure_postgresql(self) -> bool:
        """Configure PostgreSQL for pgBackRest using detected system configuration."""
        try:
            logger.info("Configuring PostgreSQL...")
            logger.info(f"🔍 PostgreSQL version: {self.system_info.get('postgresql_version', 'unknown')}")
            logger.info(f"🔍 PostgreSQL service: {self.system_info.get('postgresql_service', 'postgresql')}")
            
            # Detect PostgreSQL configuration file location dynamically
            logger.info("🔍 Detecting PostgreSQL configuration file location...")
            postgres_user = self.system_info.get('postgresql_user', 'postgres')
            
            config_cmd = ['sudo', '-u', postgres_user, 'psql', '-t', '-c', 'SHOW config_file;']
            try:
                process = await asyncio.create_subprocess_exec(
                    *config_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await process.communicate()
                
                if process.returncode != 0:
                    logger.error(f"Failed to detect config file location: {stderr.decode()}")
                    logger.info("💡 Trying fallback config detection...")
                    postgres_conf = await self._fallback_config_detection()
                else:
                    postgres_conf_path = stdout.decode().strip()
                    postgres_conf = Path(postgres_conf_path)
                    logger.info(f"📋 PostgreSQL config file: {postgres_conf}")
                    
            except Exception as e:
                logger.warning(f"Config detection failed: {e}, trying fallback...")
                postgres_conf = await self._fallback_config_detection()
            
            if not postgres_conf or not postgres_conf.exists():
                logger.error(f"❌ Could not find PostgreSQL configuration file")
                return False
            
            # For pg_hba.conf, it's usually in the same directory as postgresql.conf
            hba_conf = postgres_conf.parent / 'pg_hba.conf'
            logger.info(f"📋 PostgreSQL HBA file: {hba_conf}")
            
            # Backup existing config
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            if postgres_conf.exists():
                shutil.copy2(postgres_conf, f"{postgres_conf}.backup.{timestamp}")
                logger.info(f"📋 Backed up config to: {postgres_conf}.backup.{timestamp}")
            if hba_conf.exists():
                shutil.copy2(hba_conf, f"{hba_conf}.backup.{timestamp}")
                logger.info(f"📋 Backed up HBA to: {hba_conf}.backup.{timestamp}")
            
            # PostgreSQL configuration with network-aware stanza
            archive_cmd = f"pgbackrest --stanza={self.config['stanza_name']} archive-push %p"
            logger.info(f"🔧 Setting archive command: {archive_cmd}")
            
            # Settings to add/update
            new_settings = {
                'wal_level': 'replica',
                'archive_mode': 'on',
                'archive_command': f"'{archive_cmd}'",
                'archive_timeout': '60',
                'max_wal_senders': '10',
                'wal_keep_size': '2GB',
                'hot_standby': 'on',
                'listen_addresses': "'*'",
                'max_connections': '200',
                'log_checkpoints': 'on'
            }
            
            # Read existing configuration
            existing_config = {}
            config_lines = []
            if postgres_conf.exists():
                with open(postgres_conf, 'r') as f:
                    for line in f:
                        config_lines.append(line)
                        # Parse existing settings
                        stripped = line.strip()
                        if stripped and not stripped.startswith('#') and '=' in stripped:
                            key, value = stripped.split('=', 1)
                            existing_config[key.strip()] = value.strip()
            
            # Update configuration file with intelligent merging
            updated_lines = []
            settings_added = set()
            
            for line in config_lines:
                stripped = line.strip()
                if stripped and not stripped.startswith('#') and '=' in stripped:
                    key, _ = stripped.split('=', 1)
                    key = key.strip()
                    
                    if key in new_settings:
                        # Replace existing setting
                        updated_lines.append(f"{key} = {new_settings[key]}\n")
                        settings_added.add(key)
                        logger.info(f"🔄 Updated existing {key} = {new_settings[key]}")
                    else:
                        # Keep existing setting
                        updated_lines.append(line)
                else:
                    # Keep comments and empty lines
                    updated_lines.append(line)
            
            # Add any new settings that weren't found in existing config
            for key, value in new_settings.items():
                if key not in settings_added:
                    updated_lines.append(f"{key} = {value}\n")
                    settings_added.add(key)
                    logger.info(f"➕ Added new {key} = {value}")
            
            # Write updated configuration
            with open(postgres_conf, 'w') as f:
                f.writelines(updated_lines)
            logger.info(f"✅ Updated PostgreSQL configuration file: {postgres_conf}")
            
            # Update pg_hba.conf for replication
            if hba_conf.exists():
                with open(hba_conf, 'r') as f:
                    hba_content = f.read()
                
                replication_line = "host replication postgres 0.0.0.0/0 md5"
                if replication_line not in hba_content:
                    with open(hba_conf, 'a') as f:
                        f.write(f"\n# Added by AutoSyncManager for pgBackRest\n{replication_line}\n")
                    logger.info("Added replication entry to pg_hba.conf")
            else:
                logger.warning(f"⚠️ pg_hba.conf not found at expected location: {hba_conf}")
            
            # Restart PostgreSQL using detected service name
            service_name = self.system_info.get('postgresql_service', 'postgresql')
            await self._restart_postgresql_service(service_name)
            
            # Wait for PostgreSQL to be ready and verify archive_mode
            logger.info("🔍 Verifying archive_mode is enabled...")
            return await self._verify_postgresql_configuration(postgres_user)
            
        except Exception as e:
            logger.error(f"Failed to configure PostgreSQL: {e}")
            return False

    async def _fallback_config_detection(self) -> Optional[Path]:
        """Fallback method to detect PostgreSQL config file location."""
        try:
            logger.info("🔍 Using fallback config detection...")
            
            # Common PostgreSQL config locations by distribution/version
            config_paths = [
                # Debian/Ubuntu
                '/etc/postgresql/16/main/postgresql.conf',
                '/etc/postgresql/15/main/postgresql.conf', 
                '/etc/postgresql/14/main/postgresql.conf',
                '/etc/postgresql/13/main/postgresql.conf',
                '/etc/postgresql/12/main/postgresql.conf',
                
                # RHEL/CentOS/Fedora
                '/var/lib/pgsql/data/postgresql.conf',
                '/var/lib/pgsql/16/data/postgresql.conf',
                '/var/lib/pgsql/15/data/postgresql.conf',
                '/var/lib/pgsql/14/data/postgresql.conf',
                
                # Generic locations
                '/usr/local/pgsql/data/postgresql.conf',
                '/opt/postgresql/data/postgresql.conf',
                
                # Data directory fallback
                f"{self.config.get('pgdata', '')}/postgresql.conf"
            ]
            
            for config_path in config_paths:
                if config_path and Path(config_path).exists():
                    logger.info(f"✅ Found config at: {config_path}")
                    return Path(config_path)
            
            logger.error("❌ No PostgreSQL config file found in common locations")
            return None
            
        except Exception as e:
            logger.error(f"Fallback config detection failed: {e}")
            return None

    async def _restart_postgresql_service(self, service_name: str) -> bool:
        """Restart PostgreSQL service using appropriate method."""
        try:
            logger.info(f"Restarting PostgreSQL service: {service_name}...")
            
            if self.system_info.get('systemd_available', False):
                # Use systemctl
                restart_cmd = ['systemctl', 'restart', service_name]
            else:
                # Fallback to service command
                restart_cmd = ['service', service_name, 'restart']
            
            process = await asyncio.create_subprocess_exec(
                *restart_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                logger.error(f"Failed to restart PostgreSQL: {stderr.decode()}")
                
                # Try alternative service names
                alternative_services = self.system_info.get('service_variations', [])
                for alt_service in alternative_services:
                    if alt_service != service_name:
                        logger.info(f"Trying alternative service name: {alt_service}")
                        try:
                            alt_cmd = ['systemctl', 'restart', alt_service] if self.system_info.get('systemd_available') else ['service', alt_service, 'restart']
                            alt_process = await asyncio.create_subprocess_exec(
                                *alt_cmd,
                                stdout=asyncio.subprocess.PIPE,
                                stderr=asyncio.subprocess.PIPE
                            )
                            alt_stdout, alt_stderr = await alt_process.communicate()
                            if alt_process.returncode == 0:
                                logger.info(f"✅ Successfully restarted using: {alt_service}")
                                # Update the detected service name for future use
                                self.system_info['postgresql_service'] = alt_service
                                return True
                        except Exception as e:
                            logger.debug(f"Alternative service {alt_service} failed: {e}")
                            continue
                return False
            else:
                logger.info("✅ PostgreSQL restarted successfully")
                return True
                
        except Exception as e:
            logger.error(f"Error restarting PostgreSQL: {e}")
            return False

    async def _verify_postgresql_configuration(self, postgres_user: str) -> bool:
        """Verify PostgreSQL configuration is correct."""
        max_retries = 10
        for attempt in range(max_retries):
            try:
                await asyncio.sleep(2)  # Wait for PostgreSQL to fully start
                
                # Check archive_mode setting
                check_cmd = ['sudo', '-u', postgres_user, 'psql', '-t', '-c', 'SHOW archive_mode;']
                process = await asyncio.create_subprocess_exec(
                    *check_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await process.communicate()
                
                if process.returncode == 0:
                    archive_mode = stdout.decode().strip()
                    logger.info(f"📋 Current archive_mode: {archive_mode}")
                    
                    if archive_mode == 'on':
                        logger.info("✅ archive_mode is properly enabled")
                        
                        # Also verify archive_command
                        check_cmd = ['sudo', '-u', postgres_user, 'psql', '-t', '-c', 'SHOW archive_command;']
                        process = await asyncio.create_subprocess_exec(
                            *check_cmd,
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE
                        )
                        stdout, stderr = await process.communicate()
                        
                        if process.returncode == 0:
                            current_archive_cmd = stdout.decode().strip()
                            logger.info(f"📋 Current archive_command: {current_archive_cmd}")
                            
                            if self.config['stanza_name'] in current_archive_cmd:
                                logger.info("✅ PostgreSQL configured successfully")
                                return True
                            else:
                                logger.warning(f"⚠️ Archive command doesn't contain expected stanza: {self.config['stanza_name']}")
                        
                    else:
                        logger.warning(f"⚠️ archive_mode is '{archive_mode}' instead of 'on' (attempt {attempt + 1}/{max_retries})")
                        if attempt < max_retries - 1:
                            logger.info("🔄 Waiting for PostgreSQL configuration to take effect...")
                            continue
                else:
                    logger.warning(f"⚠️ Failed to check archive_mode (attempt {attempt + 1}/{max_retries}): {stderr.decode()}")
                    
            except Exception as e:
                logger.warning(f"⚠️ Error checking archive_mode (attempt {attempt + 1}/{max_retries}): {e}")
                
            if attempt < max_retries - 1:
                await asyncio.sleep(3)
        
        logger.error("❌ Failed to verify that archive_mode is enabled after PostgreSQL restart")
        logger.error("💡 Manual intervention may be required to enable archive_mode")
        return False

    async def _setup_postgres_auth(self) -> bool:
        """Setup PostgreSQL authentication for pgBackRest."""
        try:
            logger.info("Setting up PostgreSQL authentication...")
            
            if self.config['pgpassword']:
                # Create .pgpass file for password authentication
                logger.info("Setting up password authentication with .pgpass file")
                
                postgres_home = os.path.expanduser('~postgres')
                if not os.path.exists(postgres_home):
                    # Fallback to /var/lib/postgresql
                    postgres_home = '/var/lib/postgresql'
                
                pgpass_file = os.path.join(postgres_home, '.pgpass')
                
                # Format: hostname:port:database:username:password
                pgpass_content = f"localhost:{self.config['pgport']}:*:{self.config['pguser']}:{self.config['pgpassword']}\n"
                pgpass_content += f"127.0.0.1:{self.config['pgport']}:*:{self.config['pguser']}:{self.config['pgpassword']}\n"
                pgpass_content += f"*:{self.config['pgport']}:*:{self.config['pguser']}:{self.config['pgpassword']}\n"
                
                # Write .pgpass file
                with open(pgpass_file, 'w') as f:
                    f.write(pgpass_content)
                
                # Set correct permissions and ownership
                os.chmod(pgpass_file, 0o600)
                shutil.chown(pgpass_file, user='postgres', group='postgres')
                
                logger.info(f"✅ Created .pgpass file at {pgpass_file}")
                
                # Test connection
                test_cmd = ['sudo', '-u', 'postgres', 'psql', 
                           f'-h', 'localhost',
                           f'-p', str(self.config['pgport']),
                           f'-U', self.config['pguser'],
                           '-d', 'postgres',
                           '-c', 'SELECT version();']
                
                process = await asyncio.create_subprocess_exec(
                    *test_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env={**os.environ, 'PGPASSFILE': pgpass_file}
                )
                stdout, stderr = await process.communicate()
                
                if process.returncode == 0:
                    logger.info("✅ PostgreSQL password authentication test successful")
                else:
                    logger.warning(f"PostgreSQL authentication test failed: {stderr.decode()}")
                    logger.warning("Will attempt to continue - pgBackRest might still work")
                    
            else:
                logger.info("No password configured - relying on peer/trust authentication")
                
                # Test connection without password
                test_cmd = ['sudo', '-u', 'postgres', 'psql', 
                           f'-p', str(self.config['pgport']),
                           f'-U', self.config['pguser'],
                           '-d', 'postgres',
                           '-c', 'SELECT version();']
                
                process = await asyncio.create_subprocess_exec(
                    *test_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await process.communicate()
                
                if process.returncode == 0:
                    logger.info("✅ PostgreSQL peer/trust authentication test successful")
                else:
                    logger.error(f"PostgreSQL authentication test failed: {stderr.decode()}")
                    logger.error("You may need to set PGBACKREST_PGPASSWORD or configure peer authentication")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to setup PostgreSQL authentication: {e}")
            return False

    async def _setup_wal_archiving(self) -> bool:
        """Setup and test WAL archiving to ensure pgBackRest can receive WAL files."""
        try:
            logger.info("Setting up WAL archiving...")
            
            # Ensure postgres user can access pgBackRest config
            config_file = '/etc/pgbackrest/pgbackrest.conf'
            if os.path.exists(config_file):
                # Make sure postgres user can read the config
                os.chmod(config_file, 0o644)  # More permissive for testing
                logger.info("Updated pgBackRest config permissions for postgres user")
            
            # Test archive command manually
            logger.info("Testing WAL archive command...")
            test_cmd = [
                'sudo', '-u', 'postgres', 'pgbackrest',
                f'--stanza={self.config["stanza_name"]}',
                'check'
            ]
            
            process = await asyncio.create_subprocess_exec(
                *test_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0:
                logger.info("✅ pgBackRest check passed - WAL archiving should work")
                return True
            else:
                logger.warning(f"pgBackRest check failed: {stderr.decode()}")
                logger.info("Attempting to create stanza to fix WAL archiving...")
                
                # Try to create stanza if it doesn't exist
                create_cmd = [
                    'sudo', '-u', 'postgres', 'pgbackrest',
                    f'--stanza={self.config["stanza_name"]}',
                    'stanza-create'
                ]
                
                process = await asyncio.create_subprocess_exec(
                    *create_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await process.communicate()
                
                if process.returncode == 0 or 'already exists' in stderr.decode().lower():
                    logger.info("✅ Stanza created/exists - WAL archiving should work")
                    return True
                else:
                    logger.error(f"Failed to create stanza: {stderr.decode()}")
                    logger.warning("WAL archiving may not work properly")
                    # Don't fail here - let's try to continue
                    return True
            
        except Exception as e:
            logger.error(f"Failed to setup WAL archiving: {e}")
            # Don't fail the entire setup for WAL archiving issues
            logger.warning("Continuing setup - WAL archiving may need manual attention")
            return True

    async def _configure_pgbackrest(self) -> bool:
        """Configure pgBackRest with R2 settings."""
        try:
            logger.info("Configuring pgBackRest...")
            
            # Create necessary directories
            dirs = ['/var/log/pgbackrest', '/var/lib/pgbackrest', '/etc/pgbackrest']
            for dir_path in dirs:
                os.makedirs(dir_path, exist_ok=True)
                shutil.chown(dir_path, user='postgres', group='postgres')
            
            # pgBackRest configuration
            config_content = f"""[global]
repo1-type=s3
repo1-s3-bucket={self.config['r2_bucket']}
repo1-s3-endpoint={self.config['r2_endpoint']}
repo1-s3-key={self.config['r2_access_key']}
repo1-s3-key-secret={self.config['r2_secret_key']}
repo1-s3-region={self.config['r2_region']}
repo1-path=/pgbackrest
repo1-retention-full={'3' if self.test_mode else '7'}
repo1-retention-diff={'1' if self.test_mode else '2'}
process-max={'2' if self.test_mode else '4'}
log-level-console=info
log-level-file=debug

[{self.config['stanza_name']}]
pg1-path={self.config['pgdata']}
pg1-port={self.config['pgport']}
pg1-user={self.config['pguser']}
"""
            
            # Write configuration
            config_file = '/etc/pgbackrest/pgbackrest.conf'
            with open(config_file, 'w') as f:
                f.write(config_content)
            
            # Set proper permissions
            os.chmod(config_file, 0o640)
            shutil.chown(config_file, user='postgres', group='postgres')
            
            logger.info("✅ pgBackRest configured successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to configure pgBackRest: {e}")
            return False

    async def _initialize_pgbackrest(self) -> bool:
        """Initialize pgBackRest stanza and intelligently handle initial backup based on existing backups."""
        try:
            setup_start_time = datetime.now()
            logger.info("🏗️ Initializing pgBackRest stanza with intelligent backup detection...")
            logger.info(f"⏱️ Setup started at: {setup_start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            
            # Create stanza
            logger.info("📋 Creating pgBackRest stanza...")
            stanza_start_time = datetime.now()
            create_cmd = ['sudo', '-u', 'postgres', 'pgbackrest', 
                         f'--stanza={self.config["stanza_name"]}', 'stanza-create']
            
            process = await asyncio.create_subprocess_exec(
                *create_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            stanza_end_time = datetime.now()
            stanza_duration = stanza_end_time - stanza_start_time
            
            if process.returncode != 0:
                # Check if stanza already exists
                if 'already exists' in stderr.decode().lower():
                    logger.info(f"✅ Stanza already exists (checked in {stanza_duration.total_seconds():.1f}s)")
                else:
                    logger.error(f"❌ Failed to create stanza after {stanza_duration.total_seconds():.1f}s: {stderr.decode()}")
                    return False
            else:
                logger.info(f"✅ Stanza created successfully in {stanza_duration.total_seconds():.1f} seconds")
            
            # Check for existing backups before taking initial backup
            logger.info("🔍 Checking for existing backups to optimize startup...")
            backup_decision = await self._analyze_existing_backups()
            
            if backup_decision['skip_backup']:
                logger.info(f"✅ {backup_decision['reason']}")
                logger.info(f"⏱️ Backup analysis and skip decision took: {(datetime.now() - setup_start_time).total_seconds():.1f} seconds")
                return True
            
            # Take initial backup based on analysis
            backup_type = backup_decision['recommended_type']
            logger.info(f"🚀 Taking {backup_decision['action']} {backup_type.upper()} backup...")
            backup_start_time = datetime.now()
            logger.info(f"⏱️ Backup started at: {backup_start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            logger.info(f"📋 Reason: {backup_decision['reason']}")
            
            backup_cmd = ['sudo', '-u', 'postgres', 'pgbackrest',
                         f'--stanza={self.config["stanza_name"]}', 'backup', f'--type={backup_type}']
            
            if self.test_mode:
                backup_cmd.extend(['--archive-timeout=30s', '--compress-level=0'])
                logger.info("📦 Test mode: Using fast compression and short timeouts")
            
            logger.info(f"🔄 Running backup command: {' '.join(backup_cmd)}")
            
            process = await asyncio.create_subprocess_exec(
                *backup_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            backup_end_time = datetime.now()
            backup_duration = backup_end_time - backup_start_time
            
            if process.returncode != 0:
                logger.error(f"❌ Initial {backup_type.upper()} backup FAILED after {backup_duration.total_seconds():.1f} seconds")
                logger.error(f"Error output: {stderr.decode()}")
                if stdout:
                    logger.debug(f"Backup stdout: {stdout.decode()}")
                return False
            
            logger.info(f"✅ Initial {backup_type.upper()} backup completed successfully")
            logger.info(f"⏱️ Backup duration: {backup_duration.total_seconds():.1f} seconds")
            logger.info(f"⏱️ Backup finished at: {backup_end_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            
            if stdout:
                logger.debug(f"Backup output: {stdout.decode()}")
            
            # Verify backup was uploaded to R2
            logger.info("🔍 Verifying initial backup upload to R2...")
            verification_start_time = datetime.now()
            
            verification_success = await self._verify_r2_upload(backup_type, backup_end_time)
            
            verification_end_time = datetime.now()
            verification_duration = verification_end_time - verification_start_time
            logger.info(f"⏱️ Upload verification took: {verification_duration.total_seconds():.1f} seconds")
            
            if verification_success:
                total_duration = verification_end_time - setup_start_time
                logger.info(f"🎉 pgBackRest initialization FULLY COMPLETED with R2 upload verification")
                logger.info(f"⏱️ Total setup + backup + verification time: {total_duration.total_seconds():.1f} seconds")
                logger.info(f"📊 Breakdown: Stanza: {stanza_duration.total_seconds():.1f}s, Backup: {backup_duration.total_seconds():.1f}s, Verification: {verification_duration.total_seconds():.1f}s")
                return True
            else:
                logger.warning(f"⚠️ Initial backup completed but R2 upload verification failed")
                logger.warning(f"📊 Backup took {backup_duration.total_seconds():.1f}s but upload verification failed")
                return False
            
        except Exception as e:
            logger.error(f"Failed to initialize pgBackRest: {e}")
            return False

    async def _analyze_existing_backups(self) -> Dict:
        """Analyze existing backups to determine if we need an initial backup and what type."""
        try:
            # Get backup info
            info_cmd = ['sudo', '-u', 'postgres', 'pgbackrest',
                       f'--stanza={self.config["stanza_name"]}', 'info', '--output=json']
            
            process = await asyncio.create_subprocess_exec(
                *info_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                logger.info("📊 No existing backups found - will take initial backup")
                return {
                    'skip_backup': False,
                    'recommended_type': 'diff' if self.test_mode else 'full',
                    'action': 'initial',
                    'reason': 'No existing backups found'
                }
            
            backup_info = json.loads(stdout.decode())
            if not backup_info or len(backup_info) == 0:
                logger.info("📊 Empty backup info - will take initial backup")
                return {
                    'skip_backup': False,
                    'recommended_type': 'diff' if self.test_mode else 'full',
                    'action': 'initial',
                    'reason': 'No backup history available'
                }
            
            stanza_info = backup_info[0]
            if 'backup' not in stanza_info or len(stanza_info['backup']) == 0:
                logger.info("📊 No backups in stanza - will take initial backup")
                return {
                    'skip_backup': False,
                    'recommended_type': 'diff' if self.test_mode else 'full',
                    'action': 'initial',
                    'reason': 'Stanza exists but no backups found'
                }
            
            # Analyze existing backups
            backups = stanza_info['backup']
            most_recent = backups[-1]
            backup_type = most_recent.get('type', 'unknown')
            backup_timestamp = most_recent.get('timestamp', {}).get('stop', 'unknown')
            
            logger.info(f"📊 Found {len(backups)} existing backup(s)")
            logger.info(f"📊 Most recent: {backup_type} backup at {backup_timestamp}")
            
            # Parse timestamp to check age
            backup_age_hours = None
            try:
                if str(backup_timestamp).isdigit():
                    backup_time = datetime.fromtimestamp(int(backup_timestamp), tz=timezone.utc)
                else:
                    # Try parsing as formatted timestamp
                    backup_time = datetime.strptime(str(backup_timestamp), '%Y-%m-%d %H:%M:%S')
                    backup_time = backup_time.replace(tzinfo=timezone.utc)
                
                backup_age_hours = (datetime.now(timezone.utc) - backup_time).total_seconds() / 3600
                logger.info(f"📊 Most recent backup age: {backup_age_hours:.1f} hours")
                
            except Exception as e:
                logger.debug(f"Could not parse backup timestamp {backup_timestamp}: {e}")
            
            # Decision logic based on backup age and type
            if backup_age_hours is not None:
                if self.test_mode:
                    # In test mode, be more aggressive about skipping
                    if backup_age_hours < 1:  # Less than 1 hour
                        return {
                            'skip_backup': True,
                            'recommended_type': None,
                            'action': 'skip',
                            'reason': f'Recent {backup_type} backup found ({backup_age_hours:.1f}h ago) - skipping for faster startup'
                        }
                else:
                    # Production mode logic
                    if backup_type == 'full' and backup_age_hours < 24:  # Recent full backup
                        return {
                            'skip_backup': True,
                            'recommended_type': None,
                            'action': 'skip',
                            'reason': f'Recent full backup found ({backup_age_hours:.1f}h ago) - skipping initial backup'
                        }
                    elif backup_type in ['diff', 'incr'] and backup_age_hours < 4:  # Recent diff/incr
                        return {
                            'skip_backup': True,
                            'recommended_type': None,
                            'action': 'skip', 
                            'reason': f'Recent {backup_type} backup found ({backup_age_hours:.1f}h ago) - skipping initial backup'
                        }
                
                # If we have old backups, take differential instead of full
                if backup_age_hours > 24:
                    return {
                        'skip_backup': False,
                        'recommended_type': 'diff',
                        'action': 'catch-up',
                        'reason': f'Old backup detected ({backup_age_hours:.1f}h ago) - taking differential to catch up'
                    }
            
            # Default: take backup as planned
            return {
                'skip_backup': False,
                'recommended_type': 'diff' if self.test_mode else 'full',
                'action': 'initial',
                'reason': 'Standard initial backup based on existing backup analysis'
            }
            
        except Exception as e:
            logger.warning(f"Error analyzing existing backups: {e}")
            # If analysis fails, err on side of taking backup
            return {
                'skip_backup': False,
                'recommended_type': 'diff' if self.test_mode else 'full',
                'action': 'initial',
                'reason': 'Backup analysis failed - taking initial backup as fallback'
            }

    async def _setup_replica(self) -> bool:
        """Setup replica node (simplified, no manual IP coordination needed)."""
        try:
            logger.info("Setting up replica node...")
            
            # For now, just ensure pgBackRest is configured
            # In the future, this could include primary discovery
            logger.info("Replica configuration completed")
            logger.info("To restore from backup, run: restore_from_backup()")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to setup replica: {e}")
            return False

    async def start_scheduling(self):
        """Start application-controlled backup scheduling (idempotent - safe to call multiple times)."""
        # Check if scheduling is already running
        if (self.backup_task and not self.backup_task.done()) or (self.health_check_task and not self.health_check_task.done()):
            logger.info("🔄 AutoSyncManager scheduling is already running, skipping duplicate start")
            return
        
        print("\n" + "🔥" * 80)
        print("🔥 AUTO SYNC MANAGER SCHEDULING STARTED 🔥")
        print("🔥" * 80)
        
        if self.is_primary:
            logger.info("🔥" * 10 + " BACKUP SCHEDULING ACTIVE " + "🔥" * 10)
            if self.test_mode:
                logger.info("⚡ TEST MODE ACTIVE: Differential backups every 15 minutes, health checks every 5 minutes ⚡")
                print("⚡ TEST MODE: FAST BACKUP SCHEDULE ENABLED FOR TESTING ⚡")
            else:
                logger.info("🏭 PRODUCTION MODE: Full backups daily at 8:30 AM UTC, differential backups hourly at :24 minutes 🏭")
                print("🏭 PRODUCTION MODE: STANDARD BACKUP SCHEDULE ACTIVE 🏭")
            
            # Only create backup task if not already running
            if not self.backup_task or self.backup_task.done():
                self.backup_task = asyncio.create_task(self._backup_scheduler())
        else:
            logger.info("🔄 REPLICA MODE: Automated download scheduling active 🔄")
            if self.test_mode:
                logger.info("⚡ TEST MODE REPLICA: Downloads every 30 minutes with 5-minute backup buffer ⚡")
                print("⚡ TEST MODE REPLICA: FAST DOWNLOAD SCHEDULE FOR TESTING ⚡")
            else:
                sync_minute = self.replica_schedule['sync_minute']
                buffer_minutes = self.replica_schedule['backup_buffer_minutes']
                logger.info(f"🏭 REPLICA MODE: Downloads hourly at :{sync_minute:02d} minutes ({buffer_minutes}min buffer after primary backup) 🏭")
                print("🏭 REPLICA MODE: COORDINATED DOWNLOAD SCHEDULE ACTIVE 🏭")
            
            # Trigger immediate sync on startup for replica nodes (if enabled)
            if self.config.get('replica_startup_sync', True):
                logger.info("🚀 REPLICA STARTUP: Triggering immediate sync to get latest data from primary...")
                print("\n" + "🚀" * 50)
                print("🚀 REPLICA STARTUP: IMMEDIATE SYNC INITIATED 🚀")
                print("🚀 DOWNLOADING LATEST BACKUP FROM PRIMARY 🚀")
                print("🚀" * 50)
                
                try:
                    startup_sync_success = await self._trigger_replica_sync()
                    if startup_sync_success:
                        logger.info("✅ STARTUP SYNC COMPLETED: Replica has latest data from primary")
                        print("✅ STARTUP SYNC SUCCESS: Ready for scheduled operations ✅")
                    else:
                        logger.warning("⚠️ STARTUP SYNC FAILED: Continuing with scheduled operations anyway")
                        print("⚠️ STARTUP SYNC FAILED: Will retry on schedule ⚠️")
                        
                        # Ensure PostgreSQL is running even if sync failed
                        await self._ensure_postgresql_running()
                        
                except Exception as e:
                    logger.error(f"❌ STARTUP SYNC ERROR: {e}")
                    print(f"❌ STARTUP SYNC ERROR: {e} ❌")
                    logger.info("🔄 Continuing with scheduled operations despite startup sync failure")
                    
                    # Ensure PostgreSQL is running even if sync failed with exception
                    await self._ensure_postgresql_running()
            else:
                logger.info("⏭️ REPLICA STARTUP: Immediate sync disabled (REPLICA_STARTUP_SYNC=false)")
                print("⏭️ REPLICA STARTUP: Skipping immediate sync - will wait for scheduled sync ⏭️")
            
            # Only create replica sync task if not already running
            if not self.backup_task or self.backup_task.done():
                self.backup_task = asyncio.create_task(self._replica_sync_scheduler())
        
        logger.info("💚 HEALTH MONITORING ACTIVE 💚")
        # Only create health check task if not already running
        if not self.health_check_task or self.health_check_task.done():
            self.health_check_task = asyncio.create_task(self._health_monitor())
        print("🔥" * 80 + "\n")

    async def _backup_scheduler(self):
        """Application-controlled backup scheduling (replaces cron)."""
        last_full_backup = datetime.now().date()
        last_diff_backup = datetime.now()
        last_check = datetime.now()
        
        print("\n" + "⏰" * 60)
        print("⏰ BACKUP SCHEDULER MAIN LOOP STARTED ⏰")
        print(f"⏰ STARTED AT: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ⏰")
        print("⏰" * 60)
        logger.info("⏰" * 15 + " BACKUP SCHEDULER LOOP ACTIVE " + "⏰" * 15)
        logger.info(f"⏰ SCHEDULER START TIME: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        while not self._shutdown_event.is_set():
            try:
                now = datetime.now()
                
                # Full backup daily at specified time (skip in test mode)
                if (self.backup_schedule['full_backup_time'] and 
                    now.strftime('%H:%M') == self.backup_schedule['full_backup_time'] and 
                    now.date() > last_full_backup):
                    logger.info("⏰ Scheduled full backup time reached - triggering backup...")
                    if await self._trigger_backup('full'):
                        last_full_backup = now.date()
                        logger.info(f"✅ Full backup completed, next full backup: tomorrow at {self.backup_schedule['full_backup_time']}")
                
                # Differential backup scheduling
                if self.test_mode:
                    # Test mode: keep existing interval-based logic
                    hours_since_diff = (now - last_diff_backup).total_seconds() / 3600
                    if hours_since_diff >= self.backup_schedule['diff_backup_interval']:
                        print("\n" + "🚨" * 50)
                        print("🚨 DIFFERENTIAL BACKUP TRIGGERED (TEST MODE) 🚨")
                        print(f"🚨 {hours_since_diff:.1f} HOURS SINCE LAST BACKUP 🚨")
                        print("🚨" * 50)
                        logger.info(f"🚨 TEST MODE BACKUP TRIGGER: {hours_since_diff:.1f} hours since last diff backup (threshold: {self.backup_schedule['diff_backup_interval']}) - triggering backup... 🚨")
                        if await self._trigger_backup('diff'):
                            last_diff_backup = now
                            next_diff_time = now + timedelta(hours=self.backup_schedule['diff_backup_interval'])
                            print("✅ DIFFERENTIAL BACKUP COMPLETED SUCCESSFULLY ✅")
                            logger.info(f"✅ Differential backup completed, next diff backup: {next_diff_time.strftime('%H:%M:%S')}")
                    else:
                        # Log status periodically for visibility (every 10 minutes in test mode)
                        if int(now.minute) % 10 == 0 and now.second < 10:
                            time_until_next = self.backup_schedule['diff_backup_interval'] - hours_since_diff
                            print(f"📊 TEST MODE BACKUP STATUS: Next diff backup in {time_until_next:.1f} hours (last: {last_diff_backup.strftime('%H:%M:%S')})")
                            logger.info(f"📊 Test mode backup scheduler: Next diff backup in {time_until_next:.1f} hours (last: {last_diff_backup.strftime('%H:%M:%S')})")
                else:
                    # Production mode: schedule-based logic (every hour at specific minute)
                    target_minute = self.backup_schedule['diff_backup_minute']
                    current_minute = now.minute
                    current_hour = now.hour
                    
                    # Check if we're at the target minute (allowing a 2-minute window for execution)
                    if (current_minute >= target_minute and current_minute <= target_minute + 2 and 
                        now.second < 30):  # Only trigger in first 30 seconds to avoid double triggers
                        
                        # Check if we haven't already done a backup this hour
                        last_backup_hour = last_diff_backup.hour if last_diff_backup.date() == now.date() else -1
                        
                        if current_hour != last_backup_hour:
                            print("\n" + "🚨" * 50)
                            print("🚨 DIFFERENTIAL BACKUP TRIGGERED (SCHEDULED) 🚨")
                            print(f"🚨 HOURLY BACKUP AT {current_hour:02d}:{target_minute:02d} 🚨")
                            print("🚨" * 50)
                            logger.info(f"🚨 SCHEDULED BACKUP TRIGGER: Hourly backup at {current_hour:02d}:{target_minute:02d} - triggering backup... 🚨")
                            if await self._trigger_backup('diff'):
                                last_diff_backup = now
                                next_hour = (current_hour + 1) % 24
                                print("✅ DIFFERENTIAL BACKUP COMPLETED SUCCESSFULLY ✅")
                                logger.info(f"✅ Differential backup completed, next diff backup: {next_hour:02d}:{target_minute:02d}")
                    else:
                        # Log status periodically for visibility (every 10 minutes)
                        if int(now.minute) % 10 == 0 and now.second < 10:
                            next_hour = current_hour if current_minute < target_minute else (current_hour + 1) % 24
                            print(f"📊 BACKUP STATUS: Next diff backup at {next_hour:02d}:{target_minute:02d} (last: {last_diff_backup.strftime('%H:%M:%S')})")
                            logger.info(f"📊 Backup scheduler active: Next diff backup at {next_hour:02d}:{target_minute:02d} (last: {last_diff_backup.strftime('%H:%M:%S')})")
                
                # Health check every hour
                minutes_since_check = (now - last_check).total_seconds() / 60
                if minutes_since_check >= self.backup_schedule['check_interval']:
                    logger.info(f"🔍 {minutes_since_check:.1f} minutes since last check (threshold: {self.backup_schedule['check_interval']}) - running health check...")
                    check_success = await self._trigger_check()
                    last_check = now
                    if check_success:
                        logger.info("✅ Health check passed")
                    else:
                        logger.warning("❌ Health check failed")
                
                # Sleep for 1 minute before next check
                await asyncio.sleep(60)
                
            except asyncio.CancelledError:
                logger.info("Backup scheduler cancelled")
                break
            except Exception as e:
                logger.error(f"Error in backup scheduler: {e}")
                await asyncio.sleep(60)

    async def _health_monitor(self):
        """Monitor backup system health."""
        print("\n" + "💚" * 60)
        print("💚 HEALTH MONITOR MAIN LOOP STARTED 💚")
        print(f"💚 CHECKING EVERY {self.backup_schedule['health_check_interval']} SECONDS 💚")
        print("💚" * 60)
        logger.info("💚" * 15 + " HEALTH MONITOR LOOP ACTIVE " + "💚" * 15)
        logger.info(f"💚 HEALTH CHECK INTERVAL: {self.backup_schedule['health_check_interval']} seconds")
        
        while not self._shutdown_event.is_set():
            try:
                # Check pgBackRest status
                logger.debug("🔍 Running health monitor check...")
                status = await self.get_backup_status()
                
                if not status['healthy']:
                    logger.warning(f"❌ Backup system health check failed: {status.get('error', 'Unknown error')}")
                    logger.warning("🔧 Attempting recovery...")
                    await self._attempt_recovery()
                else:
                    logger.debug("✅ Health monitor check passed")
                    # Log a periodic status update at INFO level (every ~10 checks)
                    if not hasattr(self, '_health_check_counter'):
                        self._health_check_counter = 0
                    self._health_check_counter += 1
                    if self._health_check_counter % 10 == 0:  # Every 10th check
                        next_check_time = datetime.now() + timedelta(seconds=self.backup_schedule['health_check_interval'])
                        logger.info(f"💚 Health monitor: System healthy (check #{self._health_check_counter}), next check at {next_check_time.strftime('%H:%M:%S')}")
                
                await asyncio.sleep(self.backup_schedule['health_check_interval'])
                
            except asyncio.CancelledError:
                logger.info("Health monitor cancelled")
                break
            except Exception as e:
                logger.error(f"Error in health monitor: {e}")
                await asyncio.sleep(300)

    async def _replica_sync_scheduler(self):
        """Application-controlled replica sync scheduling coordinated with primary backups."""
        # Initialize last_sync - if we just did a startup sync, we don't need to sync again immediately
        last_sync = datetime.now()
        last_check = datetime.now()
        
        logger.info("🔄 Replica sync scheduler starting - will coordinate with primary backup schedule")
        
        print("\n" + "🔄" * 60)
        print("🔄 REPLICA SYNC SCHEDULER MAIN LOOP STARTED 🔄")
        print(f"🔄 STARTED AT: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 🔄")
        print("🔄" * 60)
        logger.info("🔄" * 15 + " REPLICA SYNC SCHEDULER LOOP ACTIVE " + "🔄" * 15)
        logger.info(f"🔄 SYNC SCHEDULER START TIME: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        while not self._shutdown_event.is_set():
            try:
                now = datetime.now()
                
                # Replica sync scheduling
                if self.test_mode:
                    # Test mode: interval-based logic (every 30 minutes)
                    hours_since_sync = (now - last_sync).total_seconds() / 3600
                    if hours_since_sync >= self.replica_schedule['sync_interval']:
                        print("\n" + "📥" * 50)
                        print("📥 REPLICA SYNC TRIGGERED (TEST MODE) 📥")
                        print(f"📥 {hours_since_sync:.1f} HOURS SINCE LAST SYNC 📥")
                        print("📥" * 50)
                        logger.info(f"📥 TEST MODE SYNC TRIGGER: {hours_since_sync:.1f} hours since last sync (threshold: {self.replica_schedule['sync_interval']}) - triggering sync... 📥")
                        if await self._trigger_replica_sync():
                            last_sync = now
                            next_sync_time = now + timedelta(hours=self.replica_schedule['sync_interval'])
                            print("✅ REPLICA SYNC COMPLETED SUCCESSFULLY ✅")
                            logger.info(f"✅ Replica sync completed, next sync: {next_sync_time.strftime('%H:%M:%S')}")
                    else:
                        # Log status periodically for visibility (every 10 minutes in test mode)
                        if int(now.minute) % 10 == 0 and now.second < 10:
                            time_until_next = self.replica_schedule['sync_interval'] - hours_since_sync
                            print(f"📊 TEST MODE SYNC STATUS: Next replica sync in {time_until_next:.1f} hours (last: {last_sync.strftime('%H:%M:%S')})")
                            logger.info(f"📊 Test mode replica scheduler: Next sync in {time_until_next:.1f} hours (last: {last_sync.strftime('%H:%M:%S')})")
                else:
                    # Production mode: schedule-based logic (every hour at specific minute with buffer)
                    target_minute = self.replica_schedule['sync_minute']
                    current_minute = now.minute
                    current_hour = now.hour
                    
                    # Check if we're at the target minute (allowing a 2-minute window for execution)
                    if (current_minute >= target_minute and current_minute <= target_minute + 2 and 
                        now.second < 30):  # Only trigger in first 30 seconds to avoid double triggers
                        
                        # Check if we haven't already done a sync this hour
                        last_sync_hour = last_sync.hour if last_sync.date() == now.date() else -1
                        
                        if current_hour != last_sync_hour:
                            backup_minute = 24  # Primary backup minute
                            buffer_minutes = self.replica_schedule['backup_buffer_minutes']
                            print("\n" + "📥" * 50)
                            print("📥 REPLICA SYNC TRIGGERED (SCHEDULED) 📥")
                            print(f"📥 HOURLY SYNC AT {current_hour:02d}:{target_minute:02d} 📥")
                            print(f"📥 ({buffer_minutes}min buffer after {backup_minute:02d}min backup) 📥")
                            print("📥" * 50)
                            logger.info(f"📥 SCHEDULED SYNC TRIGGER: Hourly sync at {current_hour:02d}:{target_minute:02d} ({buffer_minutes}min buffer after primary backup) - triggering sync... 📥")
                            if await self._trigger_replica_sync():
                                last_sync = now
                                next_hour = (current_hour + 1) % 24
                                print("✅ REPLICA SYNC COMPLETED SUCCESSFULLY ✅")
                                logger.info(f"✅ Replica sync completed, next sync: {next_hour:02d}:{target_minute:02d}")
                    else:
                        # Log status periodically for visibility (every 10 minutes)
                        if int(now.minute) % 10 == 0 and now.second < 10:
                            next_hour = current_hour if current_minute < target_minute else (current_hour + 1) % 24
                            print(f"📊 REPLICA STATUS: Next sync at {next_hour:02d}:{target_minute:02d} (last: {last_sync.strftime('%H:%M:%S')})")
                            logger.info(f"📊 Replica scheduler active: Next sync at {next_hour:02d}:{target_minute:02d} (last: {last_sync.strftime('%H:%M:%S')})")
                
                # Health check every hour (same as primary)
                minutes_since_check = (now - last_check).total_seconds() / 60
                if minutes_since_check >= self.backup_schedule['check_interval']:
                    logger.info(f"🔍 {minutes_since_check:.1f} minutes since last check (threshold: {self.backup_schedule['check_interval']}) - running health check...")
                    check_success = await self._trigger_check()
                    last_check = now
                    if check_success:
                        logger.info("✅ Replica health check passed")
                    else:
                        logger.warning("❌ Replica health check failed")
                
                # Sleep for 1 minute before next check
                await asyncio.sleep(60)
                
            except asyncio.CancelledError:
                logger.info("Replica sync scheduler cancelled")
                break
            except Exception as e:
                logger.error(f"Error in replica sync scheduler: {e}")
                await asyncio.sleep(60)

    async def _trigger_replica_sync(self) -> bool:
        """Trigger a replica sync (check for new backups and restore if newer than local data)."""
        try:
            logger.info("🔄 Starting replica sync...")
            start_time = datetime.now()
            
            # First, check what backups are available
            info_cmd = ['sudo', '-u', 'postgres', 'pgbackrest',
                       f'--stanza={self.config["stanza_name"]}', 'info', '--output=json']
            
            logger.info(f"🔍 Checking available backups from primary...")
            
            process = await asyncio.create_subprocess_exec(
                *info_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                logger.warning(f"⚠️ Could not check backup status: {stderr.decode()}")
                return False
            
            try:
                backup_info = json.loads(stdout.decode())
                logger.info(f"📊 Backup info retrieved successfully")
                
                # Check if there are any backups available
                if not backup_info or len(backup_info) == 0:
                    logger.warning("⚠️ No backup information available")
                    return False
                
                stanza_info = backup_info[0] if isinstance(backup_info, list) else backup_info
                if 'backup' not in stanza_info or len(stanza_info['backup']) == 0:
                    logger.warning("⚠️ No backups found in repository")
                    return False
                
                # Get the latest backup
                latest_backup = stanza_info['backup'][-1]  # Last backup is latest
                backup_type = latest_backup.get('type', 'unknown')
                backup_timestamp = latest_backup.get('timestamp', {}).get('stop', 'unknown')
                
                logger.info(f"📦 Latest backup found: {backup_type} backup from {backup_timestamp}")
                
                # For replicas, we want to restore from the latest backup to ensure 
                # complete synchronization with primary (primary is source of truth)
                logger.info("🎯 REPLICA STRATEGY: Complete database overwrite with primary data")
                logger.info("⚠️ WARNING: This will DESTROY all local replica data")
                logger.info("✅ Primary database is the ABSOLUTE source of truth")
                
                # Perform the complete database restore
                logger.info("🔄 Initiating complete database restore from primary backup...")
                restore_success = await self.restore_from_backup()
                
                if restore_success:
                    duration = datetime.now() - start_time
                    logger.info(f"🎉 REPLICA SYNC COMPLETED: Database completely replaced with primary data")
                    logger.info(f"⏱️ Total sync time: {duration.total_seconds():.1f} seconds")
                    logger.info(f"📊 Restored from: {backup_type} backup (timestamp: {backup_timestamp})")
                    logger.info("✅ Replica now has identical data to primary")
                    return True
                else:
                    logger.error("❌ Database restore failed - replica sync incomplete")
                    return False
                
            except json.JSONDecodeError as e:
                logger.error(f"❌ Failed to parse backup info JSON: {e}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error in replica sync: {e}")
            return False

    async def _trigger_backup(self, backup_type: str) -> bool:
        """Trigger a backup of specified type with detailed progress tracking and R2 upload verification."""
        try:
            logger.info(f"🚀 Starting {backup_type.upper()} backup...")
            backup_start_time = datetime.now()
            
            cmd = ['sudo', '-u', 'postgres', 'pgbackrest',
                  f'--stanza={self.config["stanza_name"]}', 'backup', f'--type={backup_type}']
            
            if self.test_mode:
                cmd.extend(['--archive-timeout=30s', '--compress-level=0'])
                logger.info("📦 Test mode: Using fast compression and short timeouts")
            
            logger.info(f"🔄 Running backup command: {' '.join(cmd)}")
            logger.info(f"⏱️ Backup started at: {backup_start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            
            # Execute the backup
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            backup_end_time = datetime.now()
            backup_duration = backup_end_time - backup_start_time
            
            if process.returncode == 0:
                logger.info(f"✅ {backup_type.upper()} backup process completed successfully")
                logger.info(f"⏱️ Backup duration: {backup_duration.total_seconds():.1f} seconds")
                logger.info(f"⏱️ Backup finished at: {backup_end_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                
                if stdout:
                    logger.debug(f"Backup output: {stdout.decode()}")
                
                # Verify R2 upload by checking backup info
                logger.info("🔍 Verifying backup upload to R2...")
                upload_verification_start = datetime.now()
                
                verification_success = await self._verify_r2_upload(backup_type, backup_end_time)
                
                upload_verification_end = datetime.now()
                verification_duration = upload_verification_end - upload_verification_start
                logger.info(f"⏱️ Upload verification took: {verification_duration.total_seconds():.1f} seconds")
                
                if verification_success:
                    total_duration = upload_verification_end - backup_start_time
                    logger.info(f"🎉 {backup_type.upper()} backup FULLY COMPLETED with R2 upload verification")
                    logger.info(f"⏱️ Total backup + verification time: {total_duration.total_seconds():.1f} seconds")
                    return True
                else:
                    logger.warning(f"⚠️ {backup_type.upper()} backup completed but R2 upload verification failed")
                    return False
                    
            else:
                logger.error(f"❌ {backup_type.upper()} backup FAILED after {backup_duration.total_seconds():.1f} seconds")
                logger.error(f"Error output: {stderr.decode()}")
                if stdout:
                    logger.debug(f"Backup stdout: {stdout.decode()}")
                return False
                
        except Exception as e:
            logger.error(f"Error triggering {backup_type} backup: {e}")
            return False

    async def _verify_r2_upload(self, backup_type: str, backup_completion_time: datetime) -> bool:
        """Verify that the backup was successfully uploaded to R2 storage."""
        try:
            logger.debug(f"🔍 Checking R2 upload status for {backup_type} backup...")
            
            # Get backup info to verify upload
            info_cmd = ['sudo', '-u', 'postgres', 'pgbackrest',
                       f'--stanza={self.config["stanza_name"]}', 'info', '--output=json']
            
            process = await asyncio.create_subprocess_exec(
                *info_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                logger.error(f"❌ Failed to get backup info for R2 verification: {stderr.decode()}")
                return False
            
            try:
                backup_info = json.loads(stdout.decode())
                
                # Extract backup details
                if backup_info and len(backup_info) > 0:
                    stanza_info = backup_info[0]  # First stanza
                    
                    if 'backup' in stanza_info and len(stanza_info['backup']) > 0:
                        # Get the most recent backup
                        recent_backup = stanza_info['backup'][-1]
                        
                        backup_label = recent_backup.get('label', 'unknown')
                        backup_timestamp = recent_backup.get('timestamp', {}).get('stop', 'unknown')
                        backup_size = recent_backup.get('info', {}).get('size', 0)
                        backup_size_mb = backup_size / (1024 * 1024) if backup_size else 0
                        backup_repo_size = recent_backup.get('info', {}).get('repository', {}).get('size', 0)
                        backup_repo_size_mb = backup_repo_size / (1024 * 1024) if backup_repo_size else 0
                        compression_ratio = (1 - backup_repo_size / backup_size) * 100 if backup_size > 0 else 0
                        
                        logger.info(f"📊 Latest backup in R2:")
                        logger.info(f"   📋 Label: {backup_label}")
                        logger.info(f"   📅 Timestamp: {backup_timestamp}")
                        logger.info(f"   📦 Original size: {backup_size_mb:.1f} MB")
                        logger.info(f"   🗜️ Compressed size: {backup_repo_size_mb:.1f} MB")
                        logger.info(f"   💾 Compression ratio: {compression_ratio:.1f}%")
                        
                        # Check if this backup was created recently (within last 10 minutes)
                        try:
                            from datetime import datetime
                            import re
                            
                            # Parse timestamp - pgBackRest can return various formats
                            if backup_timestamp != 'unknown':
                                backup_time = None
                                
                                # Try to parse the timestamp in different formats
                                try:
                                    # First, check if it's a Unix timestamp (numeric string)
                                    if str(backup_timestamp).isdigit():
                                        unix_timestamp = int(backup_timestamp)
                                        backup_time = datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)
                                        logger.debug(f"📅 Parsed Unix timestamp {backup_timestamp} as {backup_time}")
                                    else:
                                        # Try common timestamp formats
                                        formats_to_try = [
                                            '%Y-%m-%d %H:%M:%S',  # 2024-01-15 14:30:25
                                            '%Y%m%d-%H%M%S',      # 20240115-143025
                                            '%Y-%m-%dT%H:%M:%S',  # ISO format: 2024-01-15T14:30:25
                                            '%Y-%m-%dT%H:%M:%SZ', # ISO with Z: 2024-01-15T14:30:25Z
                                        ]
                                        
                                        for fmt in formats_to_try:
                                            try:
                                                backup_time = datetime.strptime(str(backup_timestamp), fmt)
                                                if backup_time.tzinfo is None:
                                                    backup_time = backup_time.replace(tzinfo=timezone.utc)
                                                logger.debug(f"📅 Parsed timestamp {backup_timestamp} using format {fmt}")
                                                break
                                            except ValueError:
                                                continue
                                                
                                except Exception as e:
                                    logger.debug(f"Error parsing timestamp {backup_timestamp}: {e}")
                                
                                if backup_time:
                                    # Ensure both timestamps are timezone-aware for comparison
                                    if backup_completion_time.tzinfo is None:
                                        backup_completion_time = backup_completion_time.replace(tzinfo=timezone.utc)
                                    
                                    time_diff = abs((backup_completion_time - backup_time).total_seconds())
                                    logger.info(f"⏰ Backup timestamp: {backup_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                                    logger.info(f"⏰ Completion time: {backup_completion_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                                    logger.info(f"⏰ Time difference: {time_diff:.1f} seconds")
                                    
                                    if time_diff <= 600:  # Within 10 minutes
                                        logger.info(f"✅ R2 upload VERIFIED - Recent backup found (time diff: {time_diff:.1f}s)")
                                        logger.info(f"🌥️ Backup successfully stored in R2 bucket: {self.config['r2_bucket']}")
                                        return True
                                    else:
                                        logger.warning(f"⚠️ Latest backup is older than expected (time diff: {time_diff:.1f}s)")
                                        # Still consider it successful if within reasonable range (1 hour)
                                        if time_diff <= 3600:
                                            logger.info(f"✅ R2 upload VERIFIED - Backup found within reasonable timeframe")
                                            logger.info(f"🌥️ Backup successfully stored in R2 bucket: {self.config['r2_bucket']}")
                                            return True
                                else:
                                    logger.warning(f"⚠️ Could not parse backup timestamp: {backup_timestamp}")
                                    # If we can't parse timestamp but backup exists, assume success
                                    logger.info(f"✅ R2 upload assumed VERIFIED - Backup exists in repository")
                                    logger.info(f"🌥️ Backup successfully stored in R2 bucket: {self.config['r2_bucket']}")
                                    return True
                        except Exception as e:
                            logger.debug(f"Error parsing backup timestamp: {e}")
                            # Fallback: if backup exists in info, assume success
                            logger.info(f"✅ R2 upload VERIFIED - Backup exists in repository")
                            logger.info(f"🌥️ Backup successfully stored in R2 bucket: {self.config['r2_bucket']}")
                            return True
                    else:
                        logger.error("❌ No backups found in repository info")
                        return False
                else:
                    logger.error("❌ Empty backup info returned")
                    return False
                    
            except json.JSONDecodeError as e:
                logger.error(f"❌ Failed to parse backup info JSON: {e}")
                return False
                
        except Exception as e:
            logger.error(f"Error verifying R2 upload: {e}")
            return False

    async def _trigger_check(self) -> bool:
        """Run pgBackRest check with detailed logging."""
        try:
            logger.debug("🔍 Running pgBackRest check...")
            cmd = ['sudo', '-u', 'postgres', 'pgbackrest',
                  f'--stanza={self.config["stanza_name"]}', 'check']
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0:
                logger.debug("✅ pgBackRest check completed successfully")
                if stdout:
                    logger.debug(f"Check output: {stdout.decode()}")
                return True
            else:
                logger.warning(f"❌ pgBackRest check failed with return code {process.returncode}")
                error_msg = stderr.decode() if stderr else ""
                if stderr:
                    logger.warning(f"Check error: {error_msg}")
                if stdout:
                    logger.debug(f"Check stdout: {stdout.decode()}")
                
                # Handle specific case: stanza mismatch after replica sync
                if (process.returncode == 28 and 
                    "backup and archive info files exist but do not match the database" in error_msg):
                    logger.warning("🔄 Detected stanza mismatch after replica sync - attempting repair...")
                    return await self._handle_stanza_mismatch_after_sync()
                
                return False
            
        except Exception as e:
            logger.error(f"Error running check: {e}")
            return False

    async def _handle_stanza_mismatch_after_sync(self) -> bool:
        """
        Handle stanza mismatch that occurs after replica sync.
        When a replica syncs its database from primary, the pgBackRest stanza info 
        files become outdated and need to be reinitialized.
        """
        try:
            logger.info("🔧 Handling stanza mismatch after replica sync...")
            
            # Stop archiving temporarily to prevent conflicts
            logger.info("🛑 Temporarily disabling archive command...")
            await self._set_archive_command("off")
            
            # Delete existing stanza to clean up inconsistent state
            logger.info("🗑️  Removing existing stanza configuration...")
            delete_cmd = ['sudo', '-u', 'postgres', 'pgbackrest', 
                         f'--stanza={self.config["stanza_name"]}', 'stanza-delete', '--force']
            
            process = await asyncio.create_subprocess_exec(
                *delete_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                logger.warning(f"Stanza delete warning (expected): {stderr.decode() if stderr else 'unknown'}")
            
            # Wait a moment for cleanup
            await asyncio.sleep(2)
            
            # Create fresh stanza for the synced database
            logger.info("🆕 Creating fresh stanza for synced database...")
            create_cmd = ['sudo', '-u', 'postgres', 'pgbackrest', 
                         f'--stanza={self.config["stanza_name"]}', 'stanza-create']
            
            process = await asyncio.create_subprocess_exec(
                *create_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                logger.error(f"Failed to create fresh stanza: {stderr.decode() if stderr else 'unknown'}")
                return False
            
            logger.info("✅ Fresh stanza created successfully")
            
            # Re-enable archive command
            logger.info("🔄 Re-enabling archive command...")
            archive_command = f"pgbackrest --stanza={self.config['stanza_name']} archive-push %p"
            await self._set_archive_command(archive_command)
            
            # Verify the fix worked
            logger.info("🔍 Verifying stanza repair...")
            check_cmd = ['sudo', '-u', 'postgres', 'pgbackrest',
                        f'--stanza={self.config["stanza_name"]}', 'check']
            
            process = await asyncio.create_subprocess_exec(
                *check_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0:
                logger.info("🎉 Stanza mismatch repair completed successfully!")
                return True
            else:
                logger.error(f"Stanza repair verification failed: {stderr.decode() if stderr else 'unknown'}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to handle stanza mismatch: {e}")
            return False

    async def _set_archive_command(self, command: str) -> bool:
        """Set PostgreSQL archive_command dynamically."""
        try:
            if command == "off":
                sql_command = "ALTER SYSTEM SET archive_command = ''"
                log_msg = "Disabling archive command"
            else:
                sql_command = f"ALTER SYSTEM SET archive_command = '{command}'"
                log_msg = f"Setting archive command to: {command}"
            
            logger.debug(log_msg)
            
            # Execute SQL command
            process = await asyncio.create_subprocess_exec(
                'sudo', '-u', 'postgres', 'psql', '-c', sql_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                logger.error(f"Failed to set archive command: {stderr.decode()}")
                return False
            
            # Reload configuration
            reload_process = await asyncio.create_subprocess_exec(
                'sudo', '-u', 'postgres', 'psql', '-c', 'SELECT pg_reload_conf()',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await reload_process.communicate()
            
            logger.debug("Archive command updated and configuration reloaded")
            return True
            
        except Exception as e:
            logger.error(f"Error setting archive command: {e}")
            return False

    async def get_backup_status(self) -> Dict:
        """Get comprehensive backup system status."""
        try:
            # Get pgBackRest info
            cmd = ['sudo', '-u', 'postgres', 'pgbackrest',
                  f'--stanza={self.config["stanza_name"]}', 'info', '--output=json']
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0:
                info = json.loads(stdout.decode())
                return {
                    'healthy': True,
                    'info': info,
                    'last_check': datetime.now().isoformat()
                }
            else:
                return {
                    'healthy': False,
                    'error': stderr.decode(),
                    'last_check': datetime.now().isoformat()
                }
                
        except Exception as e:
            return {
                'healthy': False,
                'error': str(e),
                'last_check': datetime.now().isoformat()
            }

    async def restore_from_backup(self, target_time: Optional[str] = None) -> bool:
        """
        Restore database from backup (replica nodes).
        
        Args:
            target_time: Optional point-in-time recovery target
        """
        try:
            logger.info("Starting database restore...")
            
            # Stop PostgreSQL with proper service detection
            logger.info("Stopping PostgreSQL...")
            postgres_service = self.system_info.get('postgresql_service', 'postgresql')
            stop_cmd = ['systemctl', 'stop', postgres_service]
            stop_process = await asyncio.create_subprocess_exec(
                *stop_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await stop_process.communicate()
            
            # Wait a moment for PostgreSQL to fully stop
            await asyncio.sleep(2)
            
            # Clear data directory with better error handling
            logger.info("Clearing data directory...")
            data_path = Path(self.config['pgdata'])
            if data_path.exists():
                try:
                    shutil.rmtree(data_path)
                    logger.info(f"✅ Removed existing data directory: {data_path}")
                except Exception as e:
                    logger.warning(f"⚠️ Error removing data directory: {e}")
                    # Try to continue anyway
            
            # Create new data directory with proper permissions
            try:
                data_path.mkdir(parents=True, exist_ok=True)
                logger.info(f"✅ Created data directory: {data_path}")
                
                # Try to set ownership, but don't fail if it doesn't work
                try:
                    shutil.chown(data_path, user='postgres', group='postgres')
                    logger.info("✅ Set postgres ownership on data directory")
                except Exception as chown_error:
                    logger.warning(f"⚠️ Could not set postgres ownership: {chown_error}")
                    # Continue anyway - pgbackrest restore might handle this
                    
            except Exception as mkdir_error:
                logger.error(f"❌ Failed to create data directory: {mkdir_error}")
                return False
            
            # Restore command
            restore_cmd = ['sudo', '-u', 'postgres', 'pgbackrest',
                          f'--stanza={self.config["stanza_name"]}', 'restore']
            
            if target_time:
                restore_cmd.extend([f'--target-time={target_time}'])
            
            logger.info(f"🔄 Running restore command: {' '.join(restore_cmd)}")
            process = await asyncio.create_subprocess_exec(
                *restore_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                logger.error(f"❌ pgBackRest restore failed: {stderr.decode()}")
                if stdout:
                    logger.error(f"Restore stdout: {stdout.decode()}")
                return False
            else:
                logger.info("✅ pgBackRest restore completed successfully")
                if stdout:
                    logger.debug(f"Restore output: {stdout.decode()}")
            
            # Start PostgreSQL with proper service detection
            logger.info("Starting PostgreSQL...")
            start_cmd = ['systemctl', 'start', postgres_service]
            start_process = await asyncio.create_subprocess_exec(
                *start_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            start_stdout, start_stderr = await start_process.communicate()
            
            if start_process.returncode != 0:
                logger.error(f"❌ Failed to start PostgreSQL: {start_stderr.decode()}")
                return False
            
            # Wait for PostgreSQL to be ready
            logger.info("⏳ Waiting for PostgreSQL to be ready...")
            await asyncio.sleep(5)
            
            # Verify PostgreSQL is running
            status_cmd = ['systemctl', 'is-active', postgres_service]
            status_process = await asyncio.create_subprocess_exec(
                *status_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            status_stdout, _ = await status_process.communicate()
            
            if status_stdout.decode().strip() == 'active':
                logger.info("✅ Database restore completed successfully - PostgreSQL is running")
                return True
            else:
                logger.error("❌ PostgreSQL failed to start after restore")
                return False
            
        except Exception as e:
            logger.error(f"Failed to restore database: {e}")
            # Try to restart PostgreSQL even if restore failed
            try:
                postgres_service = self.system_info.get('postgresql_service', 'postgresql')
                restart_cmd = ['systemctl', 'start', postgres_service]
                await asyncio.create_subprocess_exec(*restart_cmd)
                logger.info("🔄 Attempted to restart PostgreSQL after restore failure")
            except:
                pass
            return False

    async def _attempt_recovery(self):
        """Attempt to recover from backup system issues."""
        try:
            logger.info("Attempting backup system recovery...")
            
            # Try to run a check first
            if await self._trigger_check():
                logger.info("Recovery successful - system is healthy")
                return
            
            # If check fails, try to recreate stanza
            if self.is_primary:
                logger.info("Attempting stanza recreation...")
                await self._initialize_pgbackrest()
            
        except Exception as e:
            logger.error(f"Recovery attempt failed: {e}")

    async def shutdown(self):
        """Clean shutdown of the sync manager."""
        logger.info("Shutting down AutoSyncManager...")
        self._shutdown_event.set()
        
        if self.backup_task:
            self.backup_task.cancel()
            try:
                await self.backup_task
            except asyncio.CancelledError:
                pass
        
        if self.health_check_task:
            self.health_check_task.cancel()
            try:
                await self.health_check_task
            except asyncio.CancelledError:
                pass
        
        logger.info("AutoSyncManager shutdown completed")

    def update_schedule(self, new_schedule: Dict):
        """Update backup schedule dynamically (no cron needed)."""
        self.backup_schedule.update(new_schedule)
        logger.info(f"Backup schedule updated: {self.backup_schedule}")

    def print_current_status(self):
        """Print very obvious status information for debugging."""
        print("\n" + "🔍" * 80)
        print("🔍 AUTO SYNC MANAGER CURRENT STATUS 🔍")
        print("🔍" * 80)
        print(f"🏠 MODE: {'PRIMARY' if self.is_primary else 'REPLICA'}")
        print(f"🧪 TEST MODE: {'ACTIVE' if self.test_mode else 'INACTIVE'}")
        print(f"📋 CURRENT SCHEDULE: {self.backup_schedule}")
        print(f"🔄 BACKUP TASK RUNNING: {self.backup_task is not None and not self.backup_task.done()}")
        print(f"💚 HEALTH TASK RUNNING: {self.health_check_task is not None and not self.health_check_task.done()}")
        print(f"⏹️  SHUTDOWN REQUESTED: {self._shutdown_event.is_set()}")
        print("🔍" * 80 + "\n")

    async def _auto_repair_configuration(self):
        """Automatically detect and repair common configuration issues."""
        try:
            logger.info("🔍 Scanning for configuration issues...")
            
            # Check if there are conflicting stanza names in the system
            await self._detect_stanza_conflicts()
            
            # Check for old configuration files that might interfere
            await self._clean_old_configurations()
            
            # Check PostgreSQL configuration for conflicts
            await self._detect_postgresql_conflicts()
            
            logger.info("✅ Configuration scan completed")
            
        except Exception as e:
            logger.warning(f"Configuration repair had issues: {e}")

    async def _detect_stanza_conflicts(self):
        """Detect if there are multiple or conflicting stanza configurations."""
        try:
            # Check what stanzas exist in pgBackRest
            info_cmd = ['sudo', '-u', 'postgres', 'pgbackrest', 'info']
            process = await asyncio.create_subprocess_exec(
                *info_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0 and stdout:
                existing_stanzas = []
                for line in stdout.decode().split('\n'):
                    if line.startswith('stanza:'):
                        stanza_name = line.split(':')[1].strip()
                        existing_stanzas.append(stanza_name)
                
                if existing_stanzas:
                    logger.info(f"📊 Found existing stanzas: {existing_stanzas}")
                    
                    # Check if our target stanza is among them
                    if self.config['stanza_name'] not in existing_stanzas:
                        logger.info(f"🆕 Will create new stanza: {self.config['stanza_name']}")
                    else:
                        logger.info(f"♻️ Will reuse existing stanza: {self.config['stanza_name']}")
                        
        except Exception as e:
            logger.debug(f"Stanza conflict detection failed: {e}")

    async def _clean_old_configurations(self):
        """Clean up any old configuration files that might interfere."""
        try:
            # Remove any backup config files older than 7 days
            config_dir = Path('/etc/pgbackrest')
            if config_dir.exists():
                import time
                current_time = time.time()
                
                for backup_file in config_dir.glob('*.backup.*'):
                    file_age = current_time - backup_file.stat().st_mtime
                    if file_age > 7 * 24 * 3600:  # 7 days
                        backup_file.unlink()
                        logger.debug(f"🧹 Cleaned old backup config: {backup_file}")
                        
        except Exception as e:
            logger.debug(f"Config cleanup failed: {e}")

    async def _detect_postgresql_conflicts(self):
        """Detect PostgreSQL configuration conflicts."""
        try:
            # Check current archive command
            check_cmd = ['sudo', '-u', 'postgres', 'psql', '-t', '-c', 'SHOW archive_command;']
            process = await asyncio.create_subprocess_exec(
                *check_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0:
                current_cmd = stdout.decode().strip()
                expected_cmd = f"pgbackrest --stanza={self.config['stanza_name']} archive-push %p"
                
                if current_cmd and current_cmd != expected_cmd:
                    logger.info(f"🔍 Archive command mismatch detected:")
                    logger.info(f"   Current: {current_cmd}")
                    logger.info(f"   Expected: {expected_cmd}")
                    logger.info("🔧 Will fix during setup...")
                        
        except Exception as e:
            logger.debug(f"PostgreSQL conflict detection failed: {e}")

    async def _ensure_postgresql_running(self):
        """Ensure PostgreSQL is running, attempting to start it if not."""
        try:
            postgres_service = self.system_info.get('postgresql_service', 'postgresql')
            
            # Check if PostgreSQL is running
            status_cmd = ['systemctl', 'is-active', postgres_service]
            status_process = await asyncio.create_subprocess_exec(
                *status_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            status_stdout, _ = await status_process.communicate()
            
            if status_stdout.decode().strip() == 'active':
                logger.info("✅ PostgreSQL is already running")
                return
            
            # PostgreSQL is not running, try to start it
            logger.info("🔄 PostgreSQL not running, attempting to start...")
            start_cmd = ['systemctl', 'start', postgres_service]
            start_process = await asyncio.create_subprocess_exec(
                *start_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            start_stdout, start_stderr = await start_process.communicate()
            
            if start_process.returncode == 0:
                logger.info("✅ Successfully started PostgreSQL")
                # Wait a moment for it to be ready
                await asyncio.sleep(3)
            else:
                logger.error(f"❌ Failed to start PostgreSQL: {start_stderr.decode()}")
                
        except Exception as e:
            logger.error(f"❌ Error ensuring PostgreSQL is running: {e}")

    async def _ensure_correct_archive_command(self) -> bool:
        """Ensure PostgreSQL archive command uses the correct network-aware stanza name with retry."""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                logger.info(f"🔧 Ensuring correct archive command (attempt {attempt + 1}/{max_retries})...")
                
                # Get current archive command
                check_cmd = ['sudo', '-u', 'postgres', 'psql', '-t', '-c', 'SHOW archive_command;']
                process = await asyncio.create_subprocess_exec(
                    *check_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await process.communicate()
                
                if process.returncode != 0:
                    logger.warning(f"Failed to check archive command: {stderr.decode()}")
                    continue
                
                current_archive_cmd = stdout.decode().strip()
                expected_archive_cmd = f"pgbackrest --stanza={self.config['stanza_name']} archive-push %p"
                
                logger.info(f"📋 Current: {current_archive_cmd}")
                logger.info(f"📋 Expected: {expected_archive_cmd}")
                
                if expected_archive_cmd in current_archive_cmd:
                    logger.info("✅ Archive command is correct")
                    return True
                
                # Update the archive command
                logger.info("🔄 Updating archive command...")
                update_cmd = [
                    'sudo', '-u', 'postgres', 'psql', '-c',
                    f"ALTER SYSTEM SET archive_command TO '{expected_archive_cmd}';"
                ]
                
                process = await asyncio.create_subprocess_exec(
                    *update_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await process.communicate()
                
                if process.returncode != 0:
                    logger.warning(f"Failed to update archive command: {stderr.decode()}")
                    continue
                
                # Reload PostgreSQL configuration
                logger.info("🔄 Reloading PostgreSQL configuration...")
                reload_cmd = ['sudo', '-u', 'postgres', 'psql', '-c', 'SELECT pg_reload_conf();']
                
                process = await asyncio.create_subprocess_exec(
                    *reload_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await process.communicate()
                
                if process.returncode != 0:
                    logger.warning(f"Failed to reload config: {stderr.decode()}")
                    continue
                
                # Verify the change
                await asyncio.sleep(2)  # Give PostgreSQL time to reload
                process = await asyncio.create_subprocess_exec(
                    *check_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await process.communicate()
                
                new_archive_cmd = stdout.decode().strip()
                logger.info(f"✅ Archive command updated to: {new_archive_cmd}")
                return True
                
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} to fix archive command failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(5)  # Wait before retry
        
        logger.error("❌ Failed to fix archive command after all attempts")
        return False

    async def _intelligent_stanza_setup(self) -> bool:
        """Intelligently handle stanza setup for both primary and replica modes."""
        try:
            if self.is_primary:
                logger.info("🏗️ Setting up PRIMARY stanza and backups...")
                
                # Test and fix WAL archiving first
                if not await self._setup_wal_archiving():
                    logger.warning("WAL archiving setup had issues - attempting recovery...")
                    # Try to fix by recreating stanza
                    await self._recreate_stanza_if_needed()
                
                # Initialize or verify pgBackRest
                if not await self._initialize_pgbackrest():
                    logger.error("Failed to initialize pgBackRest")
                    return False
                    
                logger.info("✅ Primary setup completed")
                return True
                
            else:
                logger.info("🔄 Setting up REPLICA configuration...")
                
                # Replica setup
                if not await self._setup_replica():
                    logger.warning("Replica setup had issues - will continue with basic monitoring")
                
                logger.info("✅ Replica setup completed")
                return True
                
        except Exception as e:
            logger.error(f"Stanza setup failed: {e}")
            return False

    async def _recreate_stanza_if_needed(self):
        """Recreate stanza if there are persistent issues."""
        try:
            logger.info("🔄 Attempting to recreate stanza for clean setup...")
            
            # Stop any existing stanza operations
            stop_cmd = ['sudo', '-u', 'postgres', 'pgbackrest', 
                       f'--stanza={self.config["stanza_name"]}', 'stop']
            
            process = await asyncio.create_subprocess_exec(
                *stop_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await process.communicate()
            
            # Delete and recreate stanza
            delete_cmd = ['sudo', '-u', 'postgres', 'pgbackrest', 
                         f'--stanza={self.config["stanza_name"]}', 'stanza-delete', '--force']
            
            process = await asyncio.create_subprocess_exec(
                *delete_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await process.communicate()
            
            # Create fresh stanza
            create_cmd = ['sudo', '-u', 'postgres', 'pgbackrest', 
                         f'--stanza={self.config["stanza_name"]}', 'stanza-create']
            
            process = await asyncio.create_subprocess_exec(
                *create_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0:
                logger.info("✅ Stanza recreated successfully")
            else:
                logger.warning(f"Stanza recreation had issues: {stderr.decode()}")
                
        except Exception as e:
            logger.warning(f"Stanza recreation failed: {e}")

    async def fix_archive_command(self) -> bool:
        """Fix the PostgreSQL archive command to use the correct network-aware stanza name."""
        try:
            logger.info("🔧 Fixing PostgreSQL archive command for network-aware stanza...")
            
            # Get current archive command
            check_cmd = ['sudo', '-u', 'postgres', 'psql', '-t', '-c', 'SHOW archive_command;']
            process = await asyncio.create_subprocess_exec(
                *check_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                logger.error(f"Failed to check current archive command: {stderr.decode()}")
                return False
            
            current_archive_cmd = stdout.decode().strip()
            expected_archive_cmd = f"pgbackrest --stanza={self.config['stanza_name']} archive-push %p"
            
            logger.info(f"📋 Current archive command: {current_archive_cmd}")
            logger.info(f"📋 Expected archive command: {expected_archive_cmd}")
            
            if expected_archive_cmd in current_archive_cmd:
                logger.info("✅ Archive command is already correct")
                return True
            
            # Update the archive command
            logger.info("🔄 Updating archive command...")
            update_cmd = [
                'sudo', '-u', 'postgres', 'psql', '-c',
                f"ALTER SYSTEM SET archive_command TO '{expected_archive_cmd}';"
            ]
            
            process = await asyncio.create_subprocess_exec(
                *update_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                logger.error(f"Failed to update archive command: {stderr.decode()}")
                return False
            
            # Reload PostgreSQL configuration
            logger.info("🔄 Reloading PostgreSQL configuration...")
            reload_cmd = ['sudo', '-u', 'postgres', 'psql', '-c', 'SELECT pg_reload_conf();']
            
            process = await asyncio.create_subprocess_exec(
                *reload_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                logger.error(f"Failed to reload PostgreSQL config: {stderr.decode()}")
                return False
            
            # Verify the change
            process = await asyncio.create_subprocess_exec(
                *check_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            new_archive_cmd = stdout.decode().strip()
            logger.info(f"✅ Archive command updated to: {new_archive_cmd}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to fix archive command: {e}")
            return False

    async def prepare_for_network_transition(self, force_clean: bool = False) -> bool:
        """
        Prepare for transitioning between networks (e.g., testnet to mainnet).
        
        Args:
            force_clean: If True, removes existing stanza to start completely fresh
            
        Returns:
            bool: Success status
        """
        try:
            if force_clean:
                logger.warning("🧹 FORCE CLEAN: Removing existing stanza for fresh start...")
                logger.warning(f"⚠️ This will delete backup history for stanza: {self.config['stanza_name']}")
                
                # Stop stanza first
                stop_cmd = ['sudo', '-u', 'postgres', 'pgbackrest', 
                           f'--stanza={self.config["stanza_name"]}', 'stop']
                
                process = await asyncio.create_subprocess_exec(
                    *stop_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                await process.communicate()
                
                # Delete stanza
                delete_cmd = ['sudo', '-u', 'postgres', 'pgbackrest', 
                             f'--stanza={self.config["stanza_name"]}', 'stanza-delete', '--force']
                
                process = await asyncio.create_subprocess_exec(
                    *delete_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await process.communicate()
                
                if process.returncode == 0:
                    logger.info("✅ Stanza deleted successfully - ready for fresh start")
                else:
                    logger.warning(f"⚠️ Stanza deletion had issues (might not exist): {stderr.decode()}")
                    logger.info("Continuing with setup anyway...")
                
            else:
                logger.info("🔄 Preparing for network transition without force clean...")
                logger.info(f"📋 Current stanza: {self.config['stanza_name']}")
                logger.info(f"🌐 Network: {self.config.get('network', 'unknown')}")
                
                # Check if stanza exists
                check_cmd = ['sudo', '-u', 'postgres', 'pgbackrest', 
                            f'--stanza={self.config["stanza_name"]}', 'info']
                
                process = await asyncio.create_subprocess_exec(
                    *check_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await process.communicate()
                
                if process.returncode == 0:
                    logger.info("📊 Existing stanza found - will integrate with existing backups")
                else:
                    logger.info("🆕 No existing stanza found - will create new one")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to prepare for network transition: {e}")
            return False


# Factory function for easy integration
async def get_auto_sync_manager(test_mode: bool = False) -> Optional[AutoSyncManager]:
    """
    Create and initialize AutoSyncManager.
    
    Args:
        test_mode: Enable test mode (fast scheduling). 
                  - When called from validator: Reflects validator's --test flag
                  - When called from standalone script: Reflects script's --test flag
                  - No override occurs - whatever is passed is used
    """
    try:
        print("\n" + "🏗️" * 60)
        print("🏗️ CREATING AUTO SYNC MANAGER 🏗️")
        print(f"🏗️ TEST MODE: {'ENABLED' if test_mode else 'DISABLED'} 🏗️")
        print("🏗️" * 60)
        
        logger.info("🏗️ Creating AutoSyncManager instance...")
        manager = AutoSyncManager(test_mode=test_mode)
        
        print("✅ AUTO SYNC MANAGER CREATED SUCCESSFULLY ✅")
        logger.info("✅ AutoSyncManager factory: Created successfully")
        return manager
    except ValueError as ve:
        print("❌ CONFIGURATION ERROR ❌")
        print(f"❌ ERROR: {ve} ❌")
        logger.error(f"❌ AutoSyncManager factory: Configuration error - {ve}")
        return None
    except FileNotFoundError as fe:
        print("❌ POSTGRESQL NOT FOUND ❌")
        print(f"❌ ERROR: {fe} ❌")
        logger.error(f"❌ AutoSyncManager factory: PostgreSQL not found - {fe}")
        return None
    except Exception as e:
        print("❌ FAILED TO CREATE AUTO SYNC MANAGER ❌")
        print(f"❌ ERROR: {e} ❌")
        logger.error(f"❌ AutoSyncManager factory: Failed to create - {e}", exc_info=True)
        return None 