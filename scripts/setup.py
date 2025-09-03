import os
import sys
import subprocess
from pathlib import Path
import getpass
import shutil
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


def ensure_uv_installed() -> str:
    """Ensure uv (Astral) is installed and return its executable path."""
    uv_path = shutil.which("uv")
    if uv_path:
        return uv_path
    try:
        # Install uv (non-interactive)
        subprocess.run(
            ["bash", "-lc", "curl -LsSf https://astral.sh/uv/install.sh | sh"],
            check=True,
        )
    except Exception as e:
        print(f"Error installing uv: {e}")
        raise
    # Re-check PATH and default install location
    return shutil.which("uv") or str(Path.home() / ".local/bin/uv")


def setup_postgresql(default_user="postgres", default_password="postgres"):
    """Configure PostgreSQL for the project"""
    try:
        # Allow overriding the default user and password with environment variables
        postgres_user = os.getenv("DB_USER", default_user)
        postgres_password = os.getenv("DB_PASSWORD", default_password)

        conn = psycopg2.connect(
            dbname="postgres",
            user=postgres_user,
            password=postgres_password,
            host="localhost",
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
    """Set up Python virtual environment and install dependencies using uv."""
    try:
        uv = ensure_uv_installed()

        venv_path = Path("../.gaia").resolve()
        # Create venv using current interpreter
        subprocess.run([uv, "venv", "--python", sys.executable, str(venv_path)], check=True)
        python_path = str(venv_path / "bin" / "python")

        # Install GDAL matching system version
        gdal_version = (
            subprocess.check_output(["gdal-config", "--version"]).decode().strip()
        )
        subprocess.run([uv, "pip", "install", "--python", python_path, f"GDAL=={gdal_version}"], check=True)

        # Prefer syncing from lockfile if present
        repo_root = Path(__file__).resolve().parents[1]
        req_lock = repo_root / "requirements.lock"
        req_txt = repo_root / "requirements.txt"
        if req_lock.exists():
            subprocess.run([uv, "pip", "sync", "--python", python_path, str(req_lock)], check=True)
        elif req_txt.exists():
            subprocess.run([uv, "pip", "install", "--python", python_path, "-r", str(req_txt)], check=True)

        # Editable install of the project itself (ensures entry points/modules are importable)
        subprocess.run([uv, "pip", "install", "--python", python_path, "-e", str(repo_root)], check=True)

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
    print(
        "2. Configure your .env file with any additional environment variables (Miner OR Validator)"
    )


if __name__ == "__main__":
    main()
