import os
import sys
import subprocess
from pathlib import Path
import getpass

def check_python_version():
    """Ensure Python version is 3.10 or higher"""
    if sys.version_info < (3, 10):
        sys.exit("Python 3.10 or higher is required")

def setup_python_environment():
    """Set up Python virtual environment"""
    try:
        print("Setting up Python virtual environment...")
        # Get absolute path to project root and one level up
        project_root = Path(__file__).resolve().parent.parent
        parent_dir = project_root.parent
        venv_path = parent_dir / ".gaia"
        
        if not venv_path.exists():
            subprocess.run([sys.executable, "-m", "venv", str(venv_path)], check=True)
        
        # Get paths
        if sys.platform == "win32":
            python_path = str(venv_path / "Scripts" / "python.exe")
            pip_path = str(venv_path / "Scripts" / "pip.exe")
            activate_script = str(venv_path / "Scripts" / "activate.bat")
        else:
            python_path = str(venv_path / "bin" / "python")
            pip_path = str(venv_path / "bin" / "pip")
            activate_script = str(venv_path / "bin" / "activate")

        # Activate virtual environment by modifying PATH and VIRTUAL_ENV
        venv_env = os.environ.copy()
        venv_env["VIRTUAL_ENV"] = str(venv_path)
        venv_env["PATH"] = str(venv_path / "bin") + os.pathsep + venv_env["PATH"]
        
        # Upgrade pip using the virtual environment
        subprocess.run(
            [python_path, "-m", "pip", "install", "--upgrade", "pip"],
            env=venv_env,
            check=True
        )
        
        print("Python virtual environment setup completed successfully")
        return python_path, pip_path, project_root, venv_path, venv_env

    except Exception as e:
        print(f"Error setting up Python environment: {e}")
        sys.exit(1)

def install_system_dependencies():
    """Install system-level dependencies"""
    try:
        print("Installing system dependencies...")
        commands = [
            "apt-get update",
            "apt-get install -y curl",
            "apt-get install -y postgresql postgresql-contrib",
            "apt-get install -y python3-dev libpq-dev",
            "apt-get install -y gdal-bin",
            "apt-get install -y libgdal-dev",
            "apt-get install -y python3-gdal",
            "systemctl start postgresql",
            "systemctl enable postgresql"
        ]

        # Run the basic installation commands
        for cmd in commands:
            print(f"Running: {cmd}")
            subprocess.run(cmd.split(), check=True)

        # Set postgres password
        subprocess.run(
            [
                "sudo",
                "-u",
                "postgres",
                "psql",
                "-c",
                "ALTER USER postgres PASSWORD 'postgres';",
            ],
            check=True,
        )
        
        # Set environment variables for GDAL
        os.environ["CPLUS_INCLUDE_PATH"] = "/usr/include/gdal"
        os.environ["C_INCLUDE_PATH"] = "/usr/include/gdal"

        print("System dependencies installed successfully")
    except Exception as e:
        print(f"Error installing system dependencies: {e}")
        sys.exit(1)

def install_python_dependencies(python_path, pip_path, project_root, venv_env):
    """Install required Python packages in virtual environment"""
    try:
        print("Installing required Python packages...")
        
        # Get GDAL version from system
        gdal_version = (
            subprocess.check_output(["gdal-config", "--version"]).decode().strip()
        )
        
        # Install GDAL first since it needs to match system version
        print(f"Installing GDAL=={gdal_version}...")
        subprocess.run([pip_path, "install", f"GDAL=={gdal_version}"], env=venv_env, check=True)

        # Install project in editable mode - this will handle all other dependencies
        print(f"Installing project from {project_root}")
        subprocess.run([pip_path, "install", "-e", str(project_root)], env=venv_env, check=True)
            
        print("Python dependencies installed successfully")
    except Exception as e:
        print(f"Error installing Python dependencies: {e}")
        sys.exit(1)

def setup_postgresql(python_path, default_user="postgres", default_password="postgres", venv_env=None):
    """Configure PostgreSQL for the project by running setup through the virtual environment"""
    try:
        print("Setting up PostgreSQL...")
        
        # Create a temporary script with the PostgreSQL setup code
        setup_script = """
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def setup_db():
    try:
        # Get credentials from env or use defaults
        postgres_user = os.getenv("POSTGRES_USER", "{default_user}")
        postgres_password = os.getenv("POSTGRES_PASSWORD", "{default_password}")

        conn = psycopg2.connect(
            dbname="postgres",
            user=postgres_user,
            password=postgres_password,
            host="localhost"
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Check if role exists before creating
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname='gaia'")
        role_exists = cur.fetchone() is not None

        if not role_exists:
            cur.execute("CREATE USER gaia WITH PASSWORD 'postgres';")

        # Drop existing databases if they exist
        databases = ["validator_db", "miner_db"]
        for db in databases:
            cur.execute(f"DROP DATABASE IF EXISTS {{db}};")
            cur.execute(f"CREATE DATABASE {{db}};")
            cur.execute(f"GRANT ALL PRIVILEGES ON DATABASE {{db}} TO gaia;")

        with open(".env", "w") as f:
            f.write(f"DB_USER={{postgres_user}}\\n")
            f.write(f"DB_PASSWORD={{postgres_password}}\\n")
            f.write("DB_HOST=localhost\\n")
            f.write("DB_PORT=5432\\n")

        print("PostgreSQL configuration completed successfully")
        
    except Exception as e:
        print(f"Error in database setup: {{e}}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    setup_db()
"""
        
        # Write the setup script
        script_path = "scripts/db_setup_temp.py"
        os.makedirs("scripts", exist_ok=True)
        with open(script_path, "w") as f:
            f.write(setup_script.format(
                default_user=default_user,
                default_password=default_password
            ))
        
        try:
            # Run the script using the virtual environment's Python
            subprocess.run([python_path, script_path], env=venv_env, check=True)
        finally:
            # Clean up the temporary script
            os.remove(script_path)

    except Exception as e:
        print(f"Error setting up PostgreSQL: {e}")
        sys.exit(1)

def main():
    """Main setup function"""
    if os.geteuid() != 0:
        print("This script must be run as root (sudo)")
        sys.exit(1)
        
    print("Starting project setup...")

    check_python_version()

    print("\nSetting up Python virtual environment...")
    python_path, pip_path, project_root, venv_path, venv_env = setup_python_environment()

    print("\nInstalling system dependencies...")
    install_system_dependencies()

    print("\nInstalling Python dependencies in virtual environment...")
    install_python_dependencies(python_path, pip_path, project_root, venv_env)

    print("\nSetting up PostgreSQL...")
    setup_postgresql(python_path, venv_env=venv_env)

    print("\nSetup completed successfully!")
    print("\nNext steps:")
    print("1. Activate virtual environment:")
    print(f"   source {venv_path}/bin/activate")
    print("2. Configure your .env file with any additional environment variables")
    print("3. Run database migrations")

if __name__ == "__main__":
    main()
