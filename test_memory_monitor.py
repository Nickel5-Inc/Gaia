#!/usr/bin/env python3

import os
import time
import sys

# Set environment variables before importing
os.environ['WEATHER_MINER_ENABLED'] = 'false'
os.environ['MINER_MEMORY_MONITORING_ENABLED'] = 'true'
os.environ['MINER_PM2_RESTART_ENABLED'] = 'true'

from gaia.miner.miner import Miner

class MockArgs:
    wallet = 'test'
    hotkey = 'test'
    netuid = 237
    port = 33334
    public_port = 33333
    subtensor = type('obj', (object,), {'chain_endpoint': None, 'network': None})

def test_memory_monitor():
    print("🧪 Testing thread-based memory monitoring...")
    
    try:
        args = MockArgs()
        miner = Miner(args)
        
        print(f"Memory monitoring enabled: {miner.memory_monitor_enabled}")
        print(f"PM2 restart enabled: {miner.pm2_restart_enabled}")
        
        # Test the thread-based monitoring
        print("Starting memory monitoring thread...")
        miner._start_memory_monitoring_thread()
        
        print("Monitoring thread started, waiting 5 seconds to see output...")
        time.sleep(5)
        
        print("✅ Test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_memory_monitor()
    sys.exit(0 if success else 1) 