#!/usr/bin/env python3

import os
import time
import sys
import asyncio

# Set environment variables before importing
os.environ['VALIDATOR_MEMORY_MONITORING_ENABLED'] = 'true'
os.environ['VALIDATOR_PM2_RESTART_ENABLED'] = 'true'
os.environ['VALIDATOR_MEMORY_WARNING_THRESHOLD_MB'] = '1000'   # 1GB for testing
os.environ['VALIDATOR_MEMORY_EMERGENCY_THRESHOLD_MB'] = '2000' # 2GB for testing  
os.environ['VALIDATOR_MEMORY_CRITICAL_THRESHOLD_MB'] = '3000'  # 3GB for testing

class MockArgs:
    wallet = 'test'
    hotkey = 'test'
    netuid = 237
    test = True

async def test_validator_memory_monitor():
    print("🧪 Testing validator memory monitoring...")
    
    try:
        # Import after setting environment variables
        from gaia.validator.validator import GaiaValidator
        
        args = MockArgs()
        validator = GaiaValidator(args)
        
        print(f"Memory monitoring enabled: {validator.memory_monitor_enabled}")
        print(f"PM2 restart enabled: {validator.pm2_restart_enabled}")
        print(f"Warning threshold: {validator.memory_warning_threshold_mb} MB")
        print(f"Emergency threshold: {validator.memory_emergency_threshold_mb} MB")
        print(f"Critical threshold: {validator.memory_critical_threshold_mb} MB")
        
        # Test the memory checking method
        print("\nTesting memory check method...")
        current_time = time.time()
        await validator._check_memory_usage(current_time)
        
        # Test critical operations detection
        print("\nTesting critical operations detection...")
        
        # Simulate a critical operation
        validator.task_health['scoring']['status'] = 'processing'
        validator.task_health['scoring']['current_operation'] = 'weight_setting'
        
        critical_ops = validator._check_critical_operations_active()
        print(f"Critical operations detected: {critical_ops}")
        
        # Reset status
        validator.task_health['scoring']['status'] = 'idle'
        validator.task_health['scoring']['current_operation'] = None
        
        critical_ops_after = validator._check_critical_operations_active()
        print(f"Critical operations after reset: {critical_ops_after}")
        
        print("✅ Test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(test_validator_memory_monitor())
    sys.exit(0 if success else 1) 