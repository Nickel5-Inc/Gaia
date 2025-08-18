#!/usr/bin/env python3
"""
Debug validator polling issues - validator side only.
"""
import asyncio
import sys
import os

# Add project root to path
sys.path.insert(0, "/root/Gaia")

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager

async def main():
    """Check validator side polling state."""
    print("🔍 Validator Polling Debug")
    print("=" * 50)
    
    db = ValidatorDatabaseManager()
    await db.initialize_database()
    
    try:
        # 1. Check the specific job being polled
        job_id = "aaa708bc-03a1-464e-9b7b-8179477729ee"
        miner_hotkey = "5H1Lx7YN"
        
        print(f"Checking job_id: {job_id}")
        print(f"Miner hotkey: {miner_hotkey}")
        
        # 2. Find the weather_miner_responses entry
        response = await db.fetch_one("""
            SELECT wmr.*, wfr.gfs_init_time_utc, nt.ip, nt.port
            FROM weather_miner_responses wmr
            JOIN weather_forecast_runs wfr ON wmr.run_id = wfr.id
            LEFT JOIN node_table nt ON wmr.miner_uid = nt.uid
            WHERE wmr.job_id = :job_id AND wmr.miner_hotkey LIKE :hotkey
        """, {"job_id": job_id, "hotkey": f"{miner_hotkey}%"})
        
        if response:
            print(f"\n✅ Found response record:")
            print(f"  Response ID: {response['id']}")
            print(f"  Miner UID: {response['miner_uid']}")
            print(f"  Miner Hotkey: {response['miner_hotkey']}")
            print(f"  Status: {response['status']}")
            print(f"  Job ID: {response['job_id']}")
            print(f"  Run ID: {response['run_id']}")
            print(f"  Miner IP: {response.get('ip', 'N/A')}")
            print(f"  Miner Port: {response.get('port', 'N/A')}")
            print(f"  GFS Init: {response.get('gfs_init_time_utc', 'N/A')}")
        else:
            print(f"\n❌ No response record found for job_id: {job_id}")
            
        # 3. Check node_table for miner details
        node = await db.fetch_one("""
            SELECT uid, hotkey, ip, port, symmetric_key_encrypted, symmetric_key_updated_at
            FROM node_table 
            WHERE hotkey LIKE :hotkey
        """, {"hotkey": f"{miner_hotkey}%"})
        
        if node:
            print(f"\n✅ Found miner in node_table:")
            print(f"  UID: {node['uid']}")
            print(f"  Hotkey: {node['hotkey']}")
            print(f"  IP: {node['ip']}")
            print(f"  Port: {node['port']}")
            print(f"  Has Cached Key: {'Yes' if node['symmetric_key_encrypted'] else 'No'}")
            print(f"  Key Updated: {node['symmetric_key_updated_at']}")
            
            # Build the URL the validator is trying to reach
            miner_url = f"https://{node['ip']}:{node['port']}"
            print(f"  Target URL: {miner_url}/weather-get-input-status")
        else:
            print(f"\n❌ Miner not found in node_table")
            
        # 4. Check recent poll jobs for this miner
        poll_jobs = await db.fetch_all("""
            SELECT id, payload, status, created_at, scheduled_at, attempts, last_error
            FROM validator_jobs_queue
            WHERE job_type = 'weather.poll_miner'
            AND payload::text LIKE :job_pattern
            ORDER BY created_at DESC
            LIMIT 5
        """, {"job_pattern": f"%{job_id}%"})
        
        print(f"\n📋 Recent poll jobs for this job_id ({len(poll_jobs)} found):")
        for job in poll_jobs:
            print(f"  Job ID: {job['id']}")
            print(f"  Status: {job['status']}")
            print(f"  Attempts: {job['attempts']}")
            print(f"  Created: {job['created_at']}")
            print(f"  Scheduled: {job['scheduled_at']}")
            print(f"  Last Error: {job['last_error'] or 'None'}")
            print(f"  Payload: {job['payload']}")
            print("  ---")
            
    finally:
        await db.close_all_connections()
    
    print("\n💡 WHAT TO CHECK ON MINER:")
    print("1. Is the miner listening on the expected IP:PORT?")
    print("2. Does the job_id exist in miner's weather_miner_jobs table?")
    print("3. Are there any miner logs showing connection attempts?")
    print("4. Is the miner's /weather-get-input-status endpoint working?")
    print("5. Are there any firewall/network issues?")

if __name__ == "__main__":
    asyncio.run(main())
