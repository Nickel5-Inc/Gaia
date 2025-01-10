import os
import sys
import subprocess
from pathlib import Path
import getpass

def check_python_version():
    """Ensure Python version is 3.10 or higher"""
    if sys.version_info < (3, 10):
        sys.exit("Python 3.10 or higher is required")

def install_python_dependencies():
    """Install required Python packages"""
    try:
        print("Installing required Python packages...")
        packages = [
            "psycopg2-binary",
            "python-dotenv",
            "bittensor",
            "fastapi",
            "uvicorn",
            "pandas",
            "numpy",
            "scipy",
            "rasterio",
            "geopandas",
            "earthengine-api",
            "httpx",
            "pytest",
            "pytest-asyncio"
        ]
        
        for package in packages:
            print(f"Installing {package}...")
            subprocess.run([sys.executable, "-m", "pip", "install", package], check=True)
            
        print("Python dependencies installed successfully")
    except Exception as e:
        print(f"Error installing Python dependencies: {e}")
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

def setup_postgresql(default_user="postgres", default_password="postgres"):
    """Configure PostgreSQL for the project"""
    try:
        print("Setting up PostgreSQL...")
        # First import psycopg2 here after it's installed
        import psycopg2
        from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
        
        # Allow overriding the default user and password with environment variables
        postgres_user = os.getenv("POSTGRES_USER", default_user)
        postgres_password = os.getenv("POSTGRES_PASSWORD", default_password)

        conn = psycopg2.connect(
            dbname="postgres", user=postgres_user, password=postgres_password, host="localhost"
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
            cur.execute(f"DROP DATABASE IF EXISTS {db};")
            cur.execute(f"CREATE DATABASE {db};")
            cur.execute(f"GRANT ALL PRIVILEGES ON DATABASE {db} TO gaia;")

        with open(".env", "w") as f:
            f.write(f"DB_USER={postgres_user}\n")
            f.write(f"DB_PASSWORD={postgres_password}\n")
            f.write(f"DB_HOST=localhost\n")
            f.write(f"DB_PORT=5432\n")

        print("PostgreSQL configuration completed successfully")

    except Exception as e:
        print(f"Error setting up PostgreSQL: {e}")
        sys.exit(1)
    finally:
        if "conn" in locals():
            conn.close()

def setup_python_environment():
    """Set up Python virtual environment and install dependencies"""
    try:
        print("Setting up Python virtual environment...")
        venv_path = Path("../.gaia")
        
        if not venv_path.exists():
            subprocess.run([sys.executable, "-m", "venv", str(venv_path)], check=True)
        
        python_path = str(venv_path / "bin" / "python")
        pip_path = str(venv_path / "bin" / "pip")

        subprocess.run(
            [python_path, "-m", "pip", "install", "--upgrade", "pip"], check=True
        )

        # Get GDAL version from system
        gdal_version = (
            subprocess.check_output(["gdal-config", "--version"]).decode().strip()
        )
        subprocess.run([pip_path, "install", f"GDAL=={gdal_version}"], check=True)

        # Install project in editable mode
        subprocess.run([pip_path, "install", "-e", ".."], check=True)

        print("Python environment setup completed successfully")

    except Exception as e:
        print(f"Error setting up Python environment: {e}")
        sys.exit(1)

def main():
    """Main setup function"""
    if os.geteuid() != 0:
        print("This script must be run as root (sudo)")
        sys.exit(1)
        
    print("Starting project setup...")

    check_python_version()

    print("\nInstalling Python dependencies...")
    install_python_dependencies()

    print("\nInstalling system dependencies...")
    install_system_dependencies()

    print("\nSetting up PostgreSQL...")
    setup_postgresql()

    print("\nSetting up Python environment...")
    setup_python_environment()

    print("\nSetup completed successfully!")
    print("\nNext steps:")
    print("1. Activate virtual environment:")
    print("   source ../.gaia/bin/activate")
    print("2. Configure your .env file with any additional environment variables")
    print("3. Run database migrations")

if __name__ == "__main__":
    main()
