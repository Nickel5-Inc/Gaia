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
        "sudo apt-get install -y postgresql postgresql-contrib",
        "sudo apt-get install -y python3-dev libpq-dev",
        "sudo systemctl start postgresql",
        "sudo systemctl enable postgresql",
    ]
    
    # Run the basic installation commands
    for cmd in commands:
        subprocess.run(cmd.split(), check=True)
    
    # Set postgres password separately (don't split this command)
    subprocess.run(
        ["sudo", "-u", "postgres", "psql", "-c", "ALTER USER postgres PASSWORD 'postgres';"],
        check=True
    )

def setup_postgresql():
    """Configure PostgreSQL for the project"""
    try:
        conn = psycopg2.connect(
            dbname='postgres',
            user='postgres',
            password='postgres',
            host='localhost'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        new_postgres_password = getpass.getpass("Enter NEW password for PostgreSQL superuser: ")
        cur.execute("ALTER USER postgres PASSWORD %s;", (new_postgres_password,))
        
        project_password = getpass.getpass("Enter password for project database user: ")
        cur.execute("CREATE USER project_user WITH PASSWORD %s;", (project_password,))
        
        databases = ['validator_db', 'miner_db']
        for db in databases:
            cur.execute(f"CREATE DATABASE {db};")
            cur.execute(f"GRANT ALL PRIVILEGES ON DATABASE {db} TO project_user;")
        
        with open('.env', 'w') as f:
            f.write(f"DB_USER=project_user\n")
            f.write(f"DB_PASSWORD={project_password}\n")
            f.write(f"DB_HOST=localhost\n")
            f.write(f"DB_PORT=5432\n")
        
        print("PostgreSQL configuration completed successfully")
        
    except Exception as e:
        print(f"Error setting up PostgreSQL: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

def setup_python_environment():
    """Set up Python virtual environment and install dependencies"""
    try:
        subprocess.run([sys.executable, "-m", "venv", "../.gaia"], check=True)
        python_path = "../.gaia/bin/python"
        pip_path = "../.gaia/bin/pip"
        
        subprocess.run([python_path, "-m", "pip", "install", "--upgrade", "pip"], check=True)
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
    print("   source .gaia/bin/activate")
    print("2. Configure your .env file with any additional environment variables")
    print("3. Run database migrations")

if __name__ == "__main__":
    main()
