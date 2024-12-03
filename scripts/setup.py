import os
import sys
import subprocess
from pathlib import Path
import getpass
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def check_python_version():
    """Ensure Python version is 3.10 or higher"""
    if sys.version_info < (3, 10):
        sys.exit("Python 3.10 or higher is required")


def install_system_dependencies():
    """Install system-level dependencies"""
    commands = [
        "sudo apt-get update",
        "sudo apt-get install -y curl",
        "sudo apt-get install -y postgresql postgresql-contrib",
        "sudo apt-get install -y python3-psycopg2",
        "sudo apt-get install -y python3-dev libpq-dev",
        "sudo apt-get install -y gdal-bin",
        "sudo apt-get install -y libgdal-dev",
        "sudo apt-get install -y python3-gdal",
        "export CPLUS_INCLUDE_PATH=/usr/include/gdal",
        "export C_INCLUDE_PATH=/usr/include/gdal",
        "sudo systemctl start postgresql",
        "sudo systemctl enable postgresql",
    ]

    # Run the basic installation commands
    for cmd in commands:
        if cmd.startswith("export"):
            var, value = cmd.split("=")
            var = var.replace("export ", "")
            os.environ[var] = value
        else:
            subprocess.run(cmd.split(), check=True)

    # Set postgres password separately (don't split this command)
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


def setup_postgresql(default_user="postgres", default_password="postgres"):
    """Configure PostgreSQL for the project"""
    try:
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
    finally:
        if "conn" in locals():
            conn.close()


def setup_python_environment():
    """Set up Python virtual environment and install dependencies"""
    try:
        subprocess.run([sys.executable, "-m", "venv", "../.gaia"], check=True)
        python_path = "../.gaia/bin/python"
        pip_path = "../.gaia/bin/pip"

        subprocess.run(
            [python_path, "-m", "pip", "install", "--upgrade", "pip"], check=True
        )

        # Get GDAL version from system
        gdal_version = (
            subprocess.check_output(["gdal-config", "--version"]).decode().strip()
        )
        subprocess.run([pip_path, "install", f"GDAL=={gdal_version}"], check=True)

        subprocess.run([pip_path, "install", "-r", "requirements.txt"], check=True)

        print("Python environment setup completed successfully")

    except Exception as e:
        print(f"Error setting up Python environment: {e}")


def main():
    """Main setup function"""
    print("Starting project setup...")

    check_python_version()

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
