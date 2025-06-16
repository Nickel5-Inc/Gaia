# Miner Memory Monitoring & Protection Features

## Overview

The miner now includes comprehensive memory monitoring and protection features to prevent OOM kills and request spam attacks.

## Memory Monitoring Features

### 1. Process-Level Memory Monitoring
- **Location**: Built into the main Miner class (`gaia/miner/miner.py`)
- **Frequency**: Checks every 10 seconds
- **Logging**: Regular status every 5 minutes, warnings/errors as needed

### 2. Proactive PM2 Restart
- **Purpose**: Triggers controlled restart before Linux OOM killer intervenes
- **Advantage**: PM2 auto-restart works vs. OOM kill which may fail to restart
- **Triggers**: Critical memory levels that don't respond to garbage collection

### 3. Emergency Garbage Collection
- **Automatic**: Triggered at emergency and critical memory levels
- **Multi-pass**: Up to 5 GC passes for maximum memory recovery
- **Verification**: Re-checks memory after GC to confirm effectiveness

## Environment Variables

### Memory Monitoring Configuration
```bash
# Enable/disable memory monitoring (default: true)
MINER_MEMORY_MONITORING_ENABLED=true

# Memory thresholds in MB
MINER_MEMORY_WARNING_THRESHOLD_MB=8000    # 8GB - Warning logs
MINER_MEMORY_EMERGENCY_THRESHOLD_MB=12000 # 12GB - Emergency GC + warnings  
MINER_MEMORY_CRITICAL_THRESHOLD_MB=14000  # 14GB - Critical GC + restart trigger

# Enable/disable PM2 restart capability (default: true)
MINER_PM2_RESTART_ENABLED=true
```

### Rate Limiting Configuration
```bash
# Maximum requests per minute per IP (default: 100)
MINER_RATE_LIMIT_PER_MINUTE=100
```

## Memory Threshold Behavior

### Warning Level (8GB default)
- 🟡 Logs high memory usage warnings
- No automatic action taken
- Good for monitoring trends

### Emergency Level (12GB default)  
- 🚨 Logs emergency memory pressure
- Triggers light garbage collection
- Warns about potential OOM risk

### Critical Level (14GB default)
- 💀 Logs critical memory state
- Triggers aggressive garbage collection (multiple passes)
- **If memory remains >90% of critical after GC**: Triggers PM2 restart
- **If PM2 restart disabled**: Logs warning about potential OOM kill

## PM2 Integration

The system automatically detects if running under PM2:
- **PM2 detected**: Uses `pm2 restart <instance_id>` for controlled restart
- **PM2 not detected**: Uses `sys.exit(1)` for graceful shutdown

PM2 instance ID is detected from the `pm2_id` environment variable.

## Rate Limiting

### Request Spam Protection
- **Per-IP tracking**: Maintains request counts per client IP
- **Sliding window**: 60-second rolling window for rate limit calculation
- **Automatic cleanup**: Removes old request records to prevent memory leaks
- **Response**: Returns HTTP 429 "Rate limit exceeded" when threshold exceeded

### Default Limits
- **100 requests per minute per IP** (configurable)
- Suitable for normal validator operations while blocking spam

## Startup Logging

Look for these log messages to confirm features are active:

```
🔍 Initializing miner memory monitoring...
🔍 Starting memory monitoring for miner process...
System memory: 16.0 GB total, 12.3 GB available
Memory monitoring thresholds: Warning=8000MB, Emergency=12000MB, Critical=14000MB
PM2 restart enabled: True
PM2 instance ID: 0
✅ Miner memory monitoring started
✅ Miner memory monitoring initialization completed
```

## Troubleshooting

### Memory Monitoring Not Starting
1. Check `MINER_MEMORY_MONITORING_ENABLED=true`
2. Ensure `psutil` is installed: `pip install psutil`
3. Look for startup error logs

### PM2 Restart Not Working
1. Verify running under PM2: `pm2 list`
2. Check PM2 instance ID in logs
3. Ensure `MINER_PM2_RESTART_ENABLED=true`

### Rate Limiting Too Aggressive
1. Increase `MINER_RATE_LIMIT_PER_MINUTE`
2. Check client IP in logs to verify correct detection
3. Monitor legitimate validator request patterns

## Memory Optimization Tips

1. **Monitor trends**: Watch for gradual memory increases
2. **Batch size tuning**: Reduce processing batch sizes if seeing frequent warnings
3. **Regular restarts**: Consider scheduled restarts during low activity periods
4. **System memory**: Ensure adequate swap space as backup

## Log Examples

### Normal Operation
```
Miner memory status: 3242.1 MB RSS (34.2% system memory)
```

### Warning Level
```
🟡 HIGH MEMORY: Miner process using 8456.3 MB (67.8% of system) (threshold: 8000 MB)
```

### Emergency Level
```
🚨 EMERGENCY MEMORY PRESSURE: 12834.7 MB - OOM risk HIGH! (threshold: 12000 MB)
Emergency light GC collected 142 objects
```

### Critical Level with Restart
```
💀 CRITICAL MEMORY: 14234.8 MB - OOM imminent! (threshold: 14000 MB)
Emergency GC freed 1823 objects
🔄 TRIGGERING PM2 RESTART: Memory still critical after GC (14089.2 MB)
```

### Rate Limiting
```
Rate limit exceeded for IP 135.181.221.87: 127 requests in last minute
``` 