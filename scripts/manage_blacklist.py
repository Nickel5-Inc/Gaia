#!/usr/bin/env python3
"""
Blacklist Management Script for Gaia Miner

This script provides a command-line interface to manage the hotkey blacklist.

Usage:
    python manage_blacklist.py --action add --hotkeys 5Grw...,5FHn...
    python manage_blacklist.py --action remove --hotkeys 5Grw...
    python manage_blacklist.py --action list
    python manage_blacklist.py --action clear
    python manage_blacklist.py --action validate
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import List, Set

# Default blacklist file path
DEFAULT_BLACKLIST_FILE = Path(__file__).parent.parent / "blacklisted_hotkeys.json"

def load_blacklist(filepath: Path) -> Set[str]:
    """Load blacklist from JSON file."""
    if not filepath.exists():
        return set()
    
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
            if isinstance(data, list):
                return set(data)
            else:
                print(f"Warning: Blacklist file contains non-list data: {type(data)}")
                return set()
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in blacklist file: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error loading blacklist: {e}")
        sys.exit(1)

def save_blacklist(filepath: Path, blacklist: Set[str]) -> None:
    """Save blacklist to JSON file."""
    try:
        # Create parent directory if it doesn't exist
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        with open(filepath, 'w') as f:
            json.dump(sorted(list(blacklist)), f, indent=2)
        
        # Set appropriate permissions (readable/writable by owner only)
        os.chmod(filepath, 0o600)
        
    except Exception as e:
        print(f"Error saving blacklist: {e}")
        sys.exit(1)

def validate_hotkey(hotkey: str) -> bool:
    """Basic validation of SS58 hotkey format."""
    # SS58 addresses typically start with specific characters and have a specific length
    # This is a basic check - you might want to add more sophisticated validation
    if not hotkey:
        return False
    
    # Basic length check (SS58 addresses are typically 47-48 characters)
    if len(hotkey) < 47 or len(hotkey) > 48:
        return False
    
    # Basic character check (should start with a number or capital letter)
    if not hotkey[0].isalnum():
        return False
    
    return True

def add_hotkeys(filepath: Path, hotkeys: List[str]) -> None:
    """Add hotkeys to the blacklist."""
    blacklist = load_blacklist(filepath)
    initial_size = len(blacklist)
    
    valid_hotkeys = []
    invalid_hotkeys = []
    
    for hotkey in hotkeys:
        hotkey = hotkey.strip()
        if validate_hotkey(hotkey):
            valid_hotkeys.append(hotkey)
            blacklist.add(hotkey)
        else:
            invalid_hotkeys.append(hotkey)
    
    if invalid_hotkeys:
        print(f"Warning: Skipping invalid hotkeys: {invalid_hotkeys}")
    
    if valid_hotkeys:
        save_blacklist(filepath, blacklist)
        added_count = len(blacklist) - initial_size
        print(f"✅ Added {added_count} new hotkey(s) to blacklist")
        print(f"   Total blacklisted: {len(blacklist)}")
    else:
        print("❌ No valid hotkeys to add")

def remove_hotkeys(filepath: Path, hotkeys: List[str]) -> None:
    """Remove hotkeys from the blacklist."""
    blacklist = load_blacklist(filepath)
    initial_size = len(blacklist)
    
    removed = []
    not_found = []
    
    for hotkey in hotkeys:
        hotkey = hotkey.strip()
        if hotkey in blacklist:
            blacklist.remove(hotkey)
            removed.append(hotkey)
        else:
            not_found.append(hotkey)
    
    if not_found:
        print(f"Warning: Hotkeys not in blacklist: {not_found}")
    
    if removed:
        save_blacklist(filepath, blacklist)
        print(f"✅ Removed {len(removed)} hotkey(s) from blacklist")
        print(f"   Total blacklisted: {len(blacklist)}")
    else:
        print("❌ No hotkeys removed")

def list_hotkeys(filepath: Path) -> None:
    """List all blacklisted hotkeys."""
    blacklist = load_blacklist(filepath)
    
    if not blacklist:
        print("📋 Blacklist is empty")
        return
    
    print(f"📋 Blacklisted hotkeys ({len(blacklist)} total):")
    for i, hotkey in enumerate(sorted(blacklist), 1):
        # Show abbreviated hotkey for privacy
        abbreviated = f"{hotkey[:8]}...{hotkey[-6:]}" if len(hotkey) > 20 else hotkey
        print(f"   {i:3d}. {abbreviated}")

def clear_blacklist(filepath: Path) -> None:
    """Clear all hotkeys from the blacklist."""
    blacklist = load_blacklist(filepath)
    initial_size = len(blacklist)
    
    if initial_size == 0:
        print("📋 Blacklist is already empty")
        return
    
    # Confirm before clearing
    response = input(f"⚠️  Are you sure you want to clear {initial_size} hotkey(s)? [y/N]: ")
    if response.lower() != 'y':
        print("❌ Clear operation cancelled")
        return
    
    save_blacklist(filepath, set())
    print(f"✅ Cleared {initial_size} hotkey(s) from blacklist")

def validate_blacklist(filepath: Path) -> None:
    """Validate all entries in the blacklist."""
    blacklist = load_blacklist(filepath)
    
    if not blacklist:
        print("📋 Blacklist is empty")
        return
    
    valid = []
    invalid = []
    
    for hotkey in blacklist:
        if validate_hotkey(hotkey):
            valid.append(hotkey)
        else:
            invalid.append(hotkey)
    
    print(f"🔍 Validation Results:")
    print(f"   ✅ Valid hotkeys: {len(valid)}")
    print(f"   ❌ Invalid hotkeys: {len(invalid)}")
    
    if invalid:
        print(f"\n   Invalid entries:")
        for hotkey in invalid:
            print(f"      - {hotkey}")
        
        response = input("\n⚠️  Remove invalid entries? [y/N]: ")
        if response.lower() == 'y':
            save_blacklist(filepath, set(valid))
            print(f"✅ Removed {len(invalid)} invalid entries")

def main():
    parser = argparse.ArgumentParser(
        description="Manage Gaia Miner hotkey blacklist",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Add hotkeys:     %(prog)s --action add --hotkeys "5Grw...,5FHn..."
  Remove hotkeys:  %(prog)s --action remove --hotkeys "5Grw..."
  List all:        %(prog)s --action list
  Clear all:       %(prog)s --action clear
  Validate:        %(prog)s --action validate
        """
    )
    
    parser.add_argument(
        '--action',
        choices=['add', 'remove', 'list', 'clear', 'validate'],
        required=True,
        help='Action to perform on the blacklist'
    )
    
    parser.add_argument(
        '--hotkeys',
        type=str,
        help='Comma-separated list of hotkeys (for add/remove actions)'
    )
    
    parser.add_argument(
        '--file',
        type=Path,
        default=DEFAULT_BLACKLIST_FILE,
        help=f'Path to blacklist file (default: {DEFAULT_BLACKLIST_FILE})'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.action in ['add', 'remove'] and not args.hotkeys:
        parser.error(f"--hotkeys is required for '{args.action}' action")
    
    # Parse hotkeys if provided
    hotkeys = []
    if args.hotkeys:
        hotkeys = [h.strip() for h in args.hotkeys.split(',') if h.strip()]
    
    # Execute action
    if args.action == 'add':
        add_hotkeys(args.file, hotkeys)
    elif args.action == 'remove':
        remove_hotkeys(args.file, hotkeys)
    elif args.action == 'list':
        list_hotkeys(args.file)
    elif args.action == 'clear':
        clear_blacklist(args.file)
    elif args.action == 'validate':
        validate_blacklist(args.file)

if __name__ == '__main__':
    main()
