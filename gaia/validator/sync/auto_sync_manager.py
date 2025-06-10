"""
Auto Sync Manager - Streamlined Database Synchronization System

This module provides automated setup and management of pgBackRest with Cloudflare R2
for database synchronization. It eliminates manual configuration steps and provides
application-level control over backup scheduling.

Key Features:
- Automated pgBackRest installation and configuration
- Application-controlled backup scheduling (no cron needed)
- Simplified replica setup with discovery
- Health monitoring and error recovery
- Centralized configuration management
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
        else:
            self.backup_schedule = {
                'full_backup_time': '02:00',  # Daily at 2 AM
                'diff_backup_interval': 4,    # Every 4 hours
                'check_interval': 60,         # Every hour
                'health_check_interval': 300  # Every 5 minutes
            }
        
        # VERY OBVIOUS STARTUP LOGGING
        print("=" * 80)
        print("🚀 AUTO SYNC MANAGER STARTING UP 🚀")
        print("=" * 80)
        logger.info("🚀" * 10 + " AUTO SYNC MANAGER INITIALIZATION " + "🚀" * 10)
        logger.info(f"🏠 MODE: {'PRIMARY DATABASE' if self.is_primary else 'REPLICA DATABASE'}")
        logger.info(f"🧪 TEST MODE: {'ENABLED (Fast scheduling for testing)' if test_mode else 'DISABLED (Production scheduling)'}")
        logger.info(f"📋 BACKUP SCHEDULE: {self.backup_schedule}")
        logger.info("=" * 80)

    def _find_pgdata_path(self) -> str:
        """Find the data directory of the active PostgreSQL instance."""
        try:
            result = subprocess.run(
                ['sudo', '-u', 'postgres', 'psql', '-t', '-c', 'SHOW data_directory;'],
                capture_output=True, text=True, check=True
            )
            pgdata_path = result.stdout.strip()
            if pgdata_path and os.path.exists(pgdata_path):
                logger.info(f"Discovered active PostgreSQL data directory at: {pgdata_path}")
                return pgdata_path
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            logger.warning(f"Could not get data_directory from running PostgreSQL. Reason: {e}")

        logger.info("Falling back to checking common PostgreSQL installation paths for pg_control.")
        common_paths = [
            '/var/lib/postgresql/14/main',
            '/var/lib/postgresql/data',
            '/var/lib/pgsql/data',
        ]
        for path in common_paths:
            if os.path.exists(os.path.join(path, 'pg_control')):
                logger.info(f"Found pg_control in data directory: {path}")
                return path
        
        pgdata_env = os.getenv('PGBACKREST_PGDATA')
        if pgdata_env:
            logger.warning(f"Could not find a valid pgdata path. Using PGBACKREST_PGDATA from environment: {pgdata_env}")
            return pgdata_env
            
        raise FileNotFoundError("Could not determine PostgreSQL data directory. Please ensure PostgreSQL is running or set PGBACKREST_PGDATA.")

    def _load_config(self) -> Dict:
        """Load and validate configuration from environment."""
        
        pgdata_path = self._find_pgdata_path()

        r2_region_raw = os.getenv('PGBACKREST_R2_REGION', 'auto')
        r2_region = r2_region_raw.split('#')[0].strip()

        config = {
            'stanza_name': os.getenv('PGBACKREST_STANZA_NAME', 'gaia'),
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
        }
        
        # Validate required R2 config
        required_r2_vars = ['r2_bucket', 'r2_endpoint', 'r2_access_key', 'r2_secret_key']
        missing_vars = [var for var in required_r2_vars if not config[var]]
        
        if missing_vars:
            raise ValueError(f"Missing required R2 configuration: {missing_vars}")
        
        return config

    async def setup(self) -> bool:
        """
        Automatically set up the database sync system.
        This replaces the manual setup scripts.
        """
        try:
            logger.info("Starting automated database sync setup...")
            
            # 1. Install dependencies
            if not await self._install_dependencies():
                return False
            
            # 2. Configure PostgreSQL
            if not await self._configure_postgresql():
                return False
            
            # 3. Setup PostgreSQL authentication
            if not await self._setup_postgres_auth():
                return False
            
            # 4. Configure pgBackRest
            if not await self._configure_pgbackrest():
                return False
            
            # 5. Test and fix WAL archiving (Primary only)
            if self.is_primary:
                if not await self._setup_wal_archiving():
                    return False
            
            # 6. Initialize pgBackRest (primary only)
            if self.is_primary:
                if not await self._initialize_pgbackrest():
                    return False
            else:
                # Replica setup
                if not await self._setup_replica():
                    return False
            
            # 7. Start application-controlled scheduling
            await self.start_scheduling()
            
            logger.info("✅ Database sync setup completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Database sync setup failed: {e}", exc_info=True)
            return False

    async def _install_dependencies(self) -> bool:
        """Install pgBackRest and required dependencies."""
        try:
            logger.info("Installing pgBackRest and dependencies...")
            
            # Check if already installed
            try:
                result = await asyncio.create_subprocess_exec(
                    'pgbackrest', 'version',
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                await result.communicate()
                if result.returncode == 0:
                    logger.info("pgBackRest already installed")
                    return True
            except FileNotFoundError:
                pass
            
            # Install via apt
            commands = [
                ['apt-get', 'update'],
                ['apt-get', 'install', '-y', 'pgbackrest', 'postgresql-client']
            ]
            
            for cmd in commands:
                logger.info(f"Running: {' '.join(cmd)}")
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await process.communicate()
                
                if process.returncode != 0:
                    logger.error(f"Command failed: {stderr.decode()}")
                    return False
            
            logger.info("✅ Dependencies installed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to install dependencies: {e}")
            return False

    async def _configure_postgresql(self) -> bool:
        """Configure PostgreSQL for pgBackRest."""
        try:
            logger.info("Configuring PostgreSQL...")
            
            postgres_conf = Path(self.config['pgdata']) / 'postgresql.conf'
            hba_conf = Path(self.config['pgdata']) / 'pg_hba.conf'
            
            # Backup existing config
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            if postgres_conf.exists():
                shutil.copy2(postgres_conf, f"{postgres_conf}.backup.{timestamp}")
            if hba_conf.exists():
                shutil.copy2(hba_conf, f"{hba_conf}.backup.{timestamp}")
            
            # PostgreSQL configuration
            archive_cmd = f"pgbackrest --stanza={self.config['stanza_name']} archive-push %p"
            
            pg_config = f"""
# pgBackRest Configuration (Auto-generated by AutoSyncManager)
wal_level = replica
archive_mode = on
archive_command = '{archive_cmd}'
archive_timeout = 60
max_wal_senders = 10
wal_keep_size = 2GB
hot_standby = on
listen_addresses = '*'
max_connections = 200
log_checkpoints = on
"""
            
            # Append to postgresql.conf
            with open(postgres_conf, 'a') as f:
                f.write(pg_config)
            
            # pg_hba.conf configuration
            hba_config = f"""
# pgBackRest replication (Auto-generated by AutoSyncManager)
local   replication     postgres                                peer
host    replication     postgres        127.0.0.1/32            trust
host    all             postgres        10.0.0.0/8              md5
host    replication     postgres        10.0.0.0/8              md5
"""
            
            # Append to pg_hba.conf
            with open(hba_conf, 'a') as f:
                f.write(hba_config)
            
            # Restart PostgreSQL
            logger.info("Restarting PostgreSQL...")
            restart_cmd = ['systemctl', 'restart', 'postgresql']
            process = await asyncio.create_subprocess_exec(
                *restart_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await process.communicate()
            
            if process.returncode != 0:
                logger.error("Failed to restart PostgreSQL")
                return False
            
            # Enable PostgreSQL
            enable_cmd = ['systemctl', 'enable', 'postgresql']
            await asyncio.create_subprocess_exec(*enable_cmd)
            
            # Wait a moment for PostgreSQL to fully restart
            await asyncio.sleep(5)
            
            logger.info("✅ PostgreSQL configured successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to configure PostgreSQL: {e}")
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
        """Initialize pgBackRest stanza and take first backup (primary only)."""
        try:
            logger.info("Initializing pgBackRest stanza...")
            
            # Create stanza
            create_cmd = ['sudo', '-u', 'postgres', 'pgbackrest', 
                         f'--stanza={self.config["stanza_name"]}', 'stanza-create']
            
            process = await asyncio.create_subprocess_exec(
                *create_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                # Check if stanza already exists
                if 'already exists' in stderr.decode().lower():
                    logger.info("Stanza already exists, continuing...")
                else:
                    logger.error(f"Failed to create stanza: {stderr.decode()}")
                    return False
            
            # Take initial backup
            logger.info("Taking initial full backup...")
            backup_type = 'diff' if self.test_mode else 'full'  # Faster for testing
            
            backup_cmd = ['sudo', '-u', 'postgres', 'pgbackrest',
                         f'--stanza={self.config["stanza_name"]}', 'backup', f'--type={backup_type}']
            
            process = await asyncio.create_subprocess_exec(
                *backup_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                logger.error(f"Initial backup failed: {stderr.decode()}")
                return False
            
            logger.info("✅ pgBackRest initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize pgBackRest: {e}")
            return False

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
        """Start application-controlled backup scheduling."""
        print("\n" + "🔥" * 80)
        print("🔥 AUTO SYNC MANAGER SCHEDULING STARTED 🔥")
        print("🔥" * 80)
        
        if self.is_primary:
            logger.info("🔥" * 10 + " BACKUP SCHEDULING ACTIVE " + "🔥" * 10)
            if self.test_mode:
                logger.info("⚡ TEST MODE ACTIVE: Differential backups every 15 minutes, health checks every 5 minutes ⚡")
                print("⚡ TEST MODE: FAST BACKUP SCHEDULE ENABLED FOR TESTING ⚡")
            else:
                logger.info("🏭 PRODUCTION MODE: Full backups daily at 2:00 AM, differential backups every 4 hours 🏭")
                print("🏭 PRODUCTION MODE: STANDARD BACKUP SCHEDULE ACTIVE 🏭")
            self.backup_task = asyncio.create_task(self._backup_scheduler())
        else:
            logger.info("🔄 REPLICA MODE: No backup scheduling, only health monitoring 🔄")
            print("🔄 REPLICA MODE: MONITORING ONLY (NO BACKUPS) 🔄")
        
        logger.info("💚 HEALTH MONITORING ACTIVE 💚")
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
                
                # Differential backup every N hours
                hours_since_diff = (now - last_diff_backup).total_seconds() / 3600
                if hours_since_diff >= self.backup_schedule['diff_backup_interval']:
                    print("\n" + "🚨" * 50)
                    print("🚨 DIFFERENTIAL BACKUP TRIGGERED 🚨")
                    print(f"🚨 {hours_since_diff:.1f} HOURS SINCE LAST BACKUP 🚨")
                    print("🚨" * 50)
                    logger.info(f"🚨 BACKUP TRIGGER: {hours_since_diff:.1f} hours since last diff backup (threshold: {self.backup_schedule['diff_backup_interval']}) - triggering backup... 🚨")
                    if await self._trigger_backup('diff'):
                        last_diff_backup = now
                        next_diff_time = now + timedelta(hours=self.backup_schedule['diff_backup_interval'])
                        print("✅ DIFFERENTIAL BACKUP COMPLETED SUCCESSFULLY ✅")
                        logger.info(f"✅ Differential backup completed, next diff backup: {next_diff_time.strftime('%H:%M:%S')}")
                else:
                    if self.test_mode and (int(now.minute) % 5 == 0 and now.second < 10):  # Log every 5 minutes in test mode
                        time_until_next = self.backup_schedule['diff_backup_interval'] - hours_since_diff
                        print(f"📊 BACKUP STATUS: Next diff backup in {time_until_next:.1f} hours (last: {last_diff_backup.strftime('%H:%M:%S')})")
                        logger.info(f"📊 Next diff backup in {time_until_next:.1f} hours (last: {last_diff_backup.strftime('%H:%M:%S')})")
                
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
                
                await asyncio.sleep(self.backup_schedule['health_check_interval'])
                
            except asyncio.CancelledError:
                logger.info("Health monitor cancelled")
                break
            except Exception as e:
                logger.error(f"Error in health monitor: {e}")
                await asyncio.sleep(300)

    async def _trigger_backup(self, backup_type: str) -> bool:
        """Trigger a backup of specified type with progress tracking."""
        try:
            logger.info(f"🚀 Starting {backup_type.upper()} backup...")
            start_time = datetime.now()
            
            cmd = ['sudo', '-u', 'postgres', 'pgbackrest',
                  f'--stanza={self.config["stanza_name"]}', 'backup', f'--type={backup_type}']
            
            if self.test_mode:
                cmd.extend(['--archive-timeout=30s', '--compress-level=0'])
                logger.info("📦 Test mode: Using fast compression and short timeouts")
            
            logger.info(f"🔄 Running command: {' '.join(cmd)}")
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            duration = datetime.now() - start_time
            
            if process.returncode == 0:
                logger.info(f"✅ {backup_type.upper()} backup completed successfully in {duration.total_seconds():.1f} seconds")
                if stdout:
                    logger.debug(f"Backup output: {stdout.decode()}")
                return True
            else:
                logger.error(f"❌ {backup_type.upper()} backup failed after {duration.total_seconds():.1f} seconds")
                logger.error(f"Error output: {stderr.decode()}")
                if stdout:
                    logger.debug(f"Backup stdout: {stdout.decode()}")
                return False
                
        except Exception as e:
            logger.error(f"Error triggering {backup_type} backup: {e}")
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
                if stderr:
                    logger.warning(f"Check error: {stderr.decode()}")
                if stdout:
                    logger.debug(f"Check stdout: {stdout.decode()}")
                return False
            
        except Exception as e:
            logger.error(f"Error running check: {e}")
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
            
            # Stop PostgreSQL
            logger.info("Stopping PostgreSQL...")
            stop_cmd = ['systemctl', 'stop', 'postgresql']
            await asyncio.create_subprocess_exec(*stop_cmd)
            
            # Clear data directory
            logger.info("Clearing data directory...")
            data_path = Path(self.config['pgdata'])
            if data_path.exists():
                shutil.rmtree(data_path)
            data_path.mkdir(parents=True, exist_ok=True)
            shutil.chown(data_path, user='postgres', group='postgres')
            
            # Restore command
            restore_cmd = ['sudo', '-u', 'postgres', 'pgbackrest',
                          f'--stanza={self.config["stanza_name"]}', 'restore']
            
            if target_time:
                restore_cmd.extend([f'--target-time={target_time}'])
            
            process = await asyncio.create_subprocess_exec(
                *restore_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                logger.error(f"Restore failed: {stderr.decode()}")
                return False
            
            # Start PostgreSQL
            logger.info("Starting PostgreSQL...")
            start_cmd = ['systemctl', 'start', 'postgresql']
            await asyncio.create_subprocess_exec(*start_cmd)
            
            logger.info("✅ Database restore completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to restore database: {e}")
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
        
        manager = AutoSyncManager(test_mode=test_mode)
        
        print("✅ AUTO SYNC MANAGER CREATED SUCCESSFULLY ✅")
        logger.info("✅ AutoSyncManager factory: Created successfully")
        return manager
    except Exception as e:
        print("❌ FAILED TO CREATE AUTO SYNC MANAGER ❌")
        print(f"❌ ERROR: {e} ❌")
        logger.error(f"❌ AutoSyncManager factory: Failed to create - {e}")
        return None 