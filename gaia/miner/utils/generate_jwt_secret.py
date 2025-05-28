import secrets
import base64
import os
from pathlib import Path

def generate_secure_secret():
    """Generate a secure random secret key for JWT signing."""
    random_bytes = secrets.token_bytes(32)
    secret_key = base64.b64encode(random_bytes).decode('utf-8')
    return secret_key

def main():
    project_root = Path(os.getcwd())
    env_path = project_root / '.env'
    
    if not env_path.exists():
        print(f"Error: No .env file found in the current directory: {project_root}")
        print("Please ensure you run this script from the project root directory ('/root/Gaia/' in this case)")
        print("and that the .env file exists there.")
        return
        
    new_secret = generate_secure_secret()
    
    with open(env_path, 'r') as f:
        content = f.read()
    
    if 'MINER_JWT_SECRET_KEY=' in content:
        print("Warning: MINER_JWT_SECRET_KEY already exists in .env file.")
        print("Please manually update it with the new value if needed.")
        print(f"New secret key: {new_secret}")
    else:
        with open(env_path, 'a') as f:
            f.write(f"\nMINER_JWT_SECRET_KEY={new_secret}\n")
        print(f"Added MINER_JWT_SECRET_KEY to {env_path}")

if __name__ == "__main__":
    main() 