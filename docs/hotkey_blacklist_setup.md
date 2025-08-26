# Hotkey Blacklist Middleware Setup

This document explains how to use the hotkey blacklist feature in the Gaia miner subnet to block requests from specific validator hotkeys.

## Overview

The blacklist middleware provides multiple approaches to block requests from specific hotkeys at the miner level. This is useful for:
- Blocking malicious validators
- Preventing abuse from specific hotkeys
- Managing access control for your miner

## Configuration Methods

### Method 1: Environment Variable

Set blacklisted hotkeys as a comma-separated list in your environment:

```bash
export BLACKLISTED_HOTKEYS="5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY,5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
```

### Method 2: JSON File

Create a `blacklisted_hotkeys.json` file in your Gaia root directory:

```json
[
  "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
  "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty",
  "5HpG9w8EBLe5XCrbczpwq5TSXvedjrBGCwqxK1iQ7qUsSWFc"
]
```

Or specify a custom file path:

```bash
export BLACKLIST_FILE_PATH="/path/to/your/blacklist.json"
```

### Method 3: Both Methods Combined

The system will load hotkeys from both the environment variable AND the JSON file, combining them into a single blacklist.

## How It Works

The implementation provides four different approaches:

### Approach 1: Middleware Class (HotkeyBlacklistMiddleware)
- Runs automatically for ALL endpoints
- Currently limited due to encrypted payloads
- Best for future use when hotkey extraction is standardized

### Approach 2: Custom Dependency (check_hotkey_blacklist)
- Can be added to individual routes
- Works after payload decryption
- Flexible but requires manual integration

### Approach 3: Enhanced Verification (enhanced_verify_request_with_blacklist)
- Wraps Fiber's verify_request
- Combines verification and blacklist checking
- Drop-in replacement for verify_request

### Approach 4: Dependency Injection (BlacklistChecker) - **RECOMMENDED**
- Currently implemented in all weather routes
- Runs after Fiber's standard verification
- Easy to add/remove from routes
- Most compatible with existing Fiber dependencies

## Current Implementation

The miner routes currently use **Approach 4** (BlacklistChecker) which:
1. Runs after Fiber's `verify_request` and `blacklist_low_stake` checks
2. Attempts to extract the sender's hotkey from various sources
3. Blocks the request with HTTP 403 if the hotkey is blacklisted
4. Logs all blocked attempts

## Dynamic Blacklist Management

You can update the blacklist at runtime using the `update_blacklist` function:

```python
# Add hotkeys to blacklist
await update_blacklist(
    hotkeys=["5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"],
    action="add"
)

# Remove hotkeys from blacklist
await update_blacklist(
    hotkeys=["5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"],
    action="remove"
)
```

## Logging

All blacklist activities are logged:
- Loading blacklist from file/env
- Blocked requests with hotkey and endpoint info
- Dynamic blacklist updates

Look for log entries like:
```
INFO: Total blacklisted hotkeys: 3
WARNING: Blocked request from blacklisted hotkey: 5Grwva... Endpoint: /weather-forecast-request
```

## Testing the Blacklist

1. Add a test hotkey to your blacklist file
2. Restart your miner
3. Attempt to send a request from that hotkey
4. Verify you receive a 403 Forbidden response
5. Check logs for the blocked request

## Security Considerations

1. **File Permissions**: Ensure your blacklist file has appropriate permissions (e.g., `chmod 600 blacklisted_hotkeys.json`)
2. **Regular Updates**: Keep your blacklist updated based on network activity
3. **Backup**: Keep a backup of your blacklist configuration
4. **Monitor Logs**: Regularly check logs for blocked attempts

## Troubleshooting

### Blacklist Not Working
1. Check if the blacklist file exists and is readable
2. Verify the JSON format is valid
3. Check logs for loading errors
4. Ensure the hotkeys are in the correct SS58 format

### Performance Impact
- The blacklist check is O(1) using a Python set
- Minimal performance impact even with thousands of blacklisted hotkeys
- File loading happens only at startup

### Integration with Other Services
The blacklist is loaded at startup and shared across all route handlers. If you need to share the blacklist with other services, consider:
- Using a shared Redis/database
- Implementing a webhook for updates
- Using a configuration management system

## Example Integration in Custom Routes

To add blacklist checking to your own custom routes:

```python
@router.post("/custom-endpoint")
async def custom_handler(
    request: Request,
    # Standard Fiber dependencies
    _verify = Depends(verify_request),
    _stake_check = Depends(blacklist_low_stake),
    # Add blacklist checking
    _blacklist = Depends(hotkey_blacklist_checker),
    # Your payload
    payload: CustomPayload = Depends(
        partial(decrypt_general_payload, CustomPayload)
    )
):
    # Your handler logic here
    pass
```

## Support

For issues or questions about the blacklist middleware:
1. Check the logs for detailed error messages
2. Verify your configuration files
3. Ensure Fiber dependencies are up to date
4. Contact the Gaia development team if issues persist
