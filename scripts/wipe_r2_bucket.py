#!/usr/bin/env python3
"""
R2 Bucket Wipe Script

This script will DELETE ALL OBJECTS in the specified R2 bucket.
Use with extreme caution - this operation cannot be undone!

Usage:
    python scripts/wipe_r2_bucket.py [--confirm] [--dry-run]
    
Options:
    --confirm: Skip the interactive confirmation prompt
    --dry-run: Show what would be deleted without actually deleting
"""

import boto3
import os
import sys
from pathlib import Path
from typing import List, Dict
import argparse
from dotenv import load_dotenv

def load_r2_config() -> Dict[str, str]:
    """Load R2 configuration from environment variables."""
    
    # Try to load from .env file in the project root
    project_root = Path(__file__).parent.parent
    env_file = project_root / '.env'
    
    if env_file.exists():
        load_dotenv(env_file)
        print(f"✅ Loaded configuration from {env_file}")
    else:
        print(f"⚠️ No .env file found at {env_file}, using environment variables")
    
    config = {
        'bucket': os.getenv('PGBACKREST_R2_BUCKET'),
        'endpoint': os.getenv('PGBACKREST_R2_ENDPOINT'), 
        'access_key': os.getenv('PGBACKREST_R2_ACCESS_KEY_ID'),
        'secret_key': os.getenv('PGBACKREST_R2_SECRET_ACCESS_KEY'),
        'region': os.getenv('PGBACKREST_R2_REGION', 'auto'),
    }
    
    # Validate required config
    missing = [key for key, value in config.items() if not value and key != 'region']
    if missing:
        raise ValueError(f"Missing required R2 configuration: {missing}")
    
    return config

def create_r2_client(config: Dict[str, str]):
    """Create boto3 S3 client configured for Cloudflare R2."""
    
    return boto3.client(
        's3',
        endpoint_url=config['endpoint'],
        aws_access_key_id=config['access_key'],
        aws_secret_access_key=config['secret_key'],
        region_name=config['region']
    )

def list_bucket_objects(s3_client, bucket_name: str) -> List[Dict]:
    """List all objects in the bucket."""
    
    print(f"📋 Listing objects in bucket: {bucket_name}")
    
    objects = []
    paginator = s3_client.get_paginator('list_objects_v2')
    
    try:
        for page in paginator.paginate(Bucket=bucket_name):
            if 'Contents' in page:
                objects.extend(page['Contents'])
    except Exception as e:
        print(f"❌ Error listing objects: {e}")
        raise
    
    return objects

def delete_objects(s3_client, bucket_name: str, objects: List[Dict], dry_run: bool = False) -> bool:
    """Delete objects from the bucket."""
    
    if not objects:
        print("✅ No objects to delete")
        return True
    
    if dry_run:
        print(f"🔍 DRY RUN: Would delete {len(objects)} objects:")
        for obj in objects[:10]:  # Show first 10
            print(f"   - {obj['Key']} ({obj['Size']} bytes)")
        if len(objects) > 10:
            print(f"   ... and {len(objects) - 10} more objects")
        return True
    
    print(f"🗑️ Deleting {len(objects)} objects...")
    
    # Delete in batches of 1000 (S3 limit)
    batch_size = 1000
    deleted_count = 0
    
    for i in range(0, len(objects), batch_size):
        batch = objects[i:i + batch_size]
        
        # Prepare delete request
        delete_request = {
            'Objects': [{'Key': obj['Key']} for obj in batch],
            'Quiet': False
        }
        
        try:
            response = s3_client.delete_objects(
                Bucket=bucket_name,
                Delete=delete_request
            )
            
            if 'Deleted' in response:
                deleted_count += len(response['Deleted'])
                print(f"✅ Deleted batch {i//batch_size + 1}: {len(response['Deleted'])} objects")
            
            if 'Errors' in response:
                print(f"❌ Errors in batch {i//batch_size + 1}:")
                for error in response['Errors']:
                    print(f"   - {error['Key']}: {error['Message']}")
                    
        except Exception as e:
            print(f"❌ Error deleting batch {i//batch_size + 1}: {e}")
            return False
    
    print(f"✅ Successfully deleted {deleted_count} objects")
    return True

def confirm_deletion(bucket_name: str, object_count: int) -> bool:
    """Interactive confirmation for deletion."""
    
    print("\n" + "⚠️" * 60)
    print("⚠️  DANGER: IRREVERSIBLE OPERATION")
    print("⚠️" * 60)
    print(f"⚠️  You are about to DELETE ALL {object_count} objects from:")
    print(f"⚠️  Bucket: {bucket_name}")
    print(f"⚠️  This includes all pgBackRest backups and WAL files!")
    print(f"⚠️  THIS OPERATION CANNOT BE UNDONE!")
    print("⚠️" * 60)
    
    response = input("\n🤔 Are you absolutely sure? Type 'DELETE ALL' to confirm: ")
    
    return response.strip() == 'DELETE ALL'

def main():
    parser = argparse.ArgumentParser(description='Wipe Cloudflare R2 bucket for fresh start')
    parser.add_argument('--confirm', action='store_true', 
                       help='Skip interactive confirmation prompt')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be deleted without actually deleting')
    
    args = parser.parse_args()
    
    print("🧹 R2 Bucket Wipe Script")
    print("=" * 50)
    
    try:
        # Load configuration
        config = load_r2_config()
        bucket_name = config['bucket']
        
        print(f"🎯 Target bucket: {bucket_name}")
        print(f"🌐 Endpoint: {config['endpoint']}")
        print(f"🔑 Access key: {config['access_key'][:8]}...")
        
        if args.dry_run:
            print("🔍 DRY RUN MODE: No actual deletions will occur")
        
        # Create R2 client
        s3_client = create_r2_client(config)
        
        # Test connection
        print("\n🔌 Testing R2 connection...")
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print("✅ Successfully connected to R2")
        except Exception as e:
            print(f"❌ Failed to connect to R2: {e}")
            return 1
        
        # List objects
        print("\n📋 Scanning bucket contents...")
        objects = list_bucket_objects(s3_client, bucket_name)
        
        if not objects:
            print("✅ Bucket is already empty")
            return 0
        
        # Calculate total size
        total_size = sum(obj['Size'] for obj in objects)
        total_size_mb = total_size / (1024 * 1024)
        
        print(f"📊 Found {len(objects)} objects ({total_size_mb:.1f} MB total)")
        
        # Show some example objects
        print(f"\n📁 Sample objects:")
        for obj in objects[:5]:
            size_mb = obj['Size'] / (1024 * 1024)
            print(f"   - {obj['Key']} ({size_mb:.1f} MB)")
        if len(objects) > 5:
            print(f"   ... and {len(objects) - 5} more objects")
        
        # Confirmation
        if not args.dry_run and not args.confirm:
            if not confirm_deletion(bucket_name, len(objects)):
                print("❌ Operation cancelled by user")
                return 1
        
        # Delete objects
        print(f"\n🗑️ {'DRY RUN: Simulating deletion of' if args.dry_run else 'Deleting'} {len(objects)} objects...")
        
        if delete_objects(s3_client, bucket_name, objects, args.dry_run):
            if args.dry_run:
                print("✅ DRY RUN completed successfully")
                print("🔄 Run without --dry-run to perform actual deletion")
            else:
                print("✅ Bucket wipe completed successfully")
                print("🆕 Bucket is now empty and ready for fresh start")
            return 0
        else:
            print("❌ Bucket wipe failed")
            return 1
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

if __name__ == '__main__':
    sys.exit(main()) 