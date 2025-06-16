#!/usr/bin/env python3
"""
Test script for the comprehensive database setup system.

This script tests the comprehensive database setup to ensure it works correctly
before integrating it into the main validator startup process.
"""

import asyncio
import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from gaia.validator.database.comprehensive_db_setup import setup_comprehensive_database, DatabaseConfig

async def test_comprehensive_setup():
    """Test the comprehensive database setup"""
    print("🧪 Testing Comprehensive Database Setup")
    print("=" * 60)
    
    # Create test configuration
    test_config = DatabaseConfig(
        database_name="test_gaia_validator",
        postgres_version="14",
        postgres_password="test_postgres",
        postgres_user="postgres",
        port=5432
    )
    
    print(f"📋 Test Configuration:")
    print(f"   Database: {test_config.database_name}")
    print(f"   PostgreSQL Version: {test_config.postgres_version}")
    print(f"   Port: {test_config.port}")
    print(f"   Data Directory: {test_config.data_directory}")
    print(f"   Config Directory: {test_config.config_directory}")
    print()
    
    # Run the comprehensive setup
    print("🚀 Starting comprehensive database setup test...")
    success = await setup_comprehensive_database(
        test_mode=True,
        config=test_config
    )
    
    if success:
        print("\n✅ Comprehensive Database Setup Test PASSED!")
        print("🎉 The system is ready for production use!")
        return True
    else:
        print("\n❌ Comprehensive Database Setup Test FAILED!")
        print("💥 Please check the logs for errors!")
        return False

def main():
    """Main test function"""
    print("🔧 Comprehensive Database Setup Test")
    print("=" * 60)
    print("This test will:")
    print("  1. Install PostgreSQL if needed")
    print("  2. Configure PostgreSQL optimally")
    print("  3. Create test database and users")
    print("  4. Run Alembic migrations")
    print("  5. Validate the complete setup")
    print("=" * 60)
    
    # Check if running as root (required for PostgreSQL installation)
    if os.geteuid() != 0:
        print("⚠️  WARNING: This test should be run as root for PostgreSQL installation")
        print("   If PostgreSQL is already installed, you can continue")
        response = input("   Continue anyway? (y/N): ")
        if response.lower() != 'y':
            print("❌ Test cancelled")
            return False
    
    try:
        success = asyncio.run(test_comprehensive_setup())
        return success
    except KeyboardInterrupt:
        print("\n⚠️  Test interrupted by user")
        return False
    except Exception as e:
        print(f"\n💥 Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 