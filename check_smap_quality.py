#!/usr/bin/env python3
"""
Script to check the quality of SMAP files in the cache.
This helps diagnose data availability and quality issues.
"""

import os
import sys
import numpy as np
import xarray as xr
from pathlib import Path
from datetime import datetime
import traceback

# Add the project root to Python path
sys.path.append('/root/Gaia')

from fiber.logging_utils import get_logger

logger = get_logger(__name__)

def check_smap_file_quality(filepath):
    """Check the quality and content of a SMAP file."""
    try:
        print(f"\n=== Analyzing SMAP file: {Path(filepath).name} ===")
        
        # Check file size
        file_size = os.path.getsize(filepath)
        print(f"File size: {file_size / (1024*1024):.1f} MB")
        
        # Open and analyze the dataset
        with xr.open_dataset(filepath, group="Geophysical_Data") as ds:
            print(f"Dataset variables: {list(ds.variables.keys())}")
            
            # Check surface soil moisture
            if "sm_surface" in ds:
                surface_data = ds["sm_surface"].values
                print(f"Surface SM shape: {surface_data.shape}")
                
                total_pixels = surface_data.size
                nan_count = np.sum(np.isnan(surface_data))
                valid_count = total_pixels - nan_count
                nan_percentage = (nan_count / total_pixels) * 100
                
                print(f"Surface SM - Total pixels: {total_pixels}")
                print(f"Surface SM - Valid pixels: {valid_count} ({100-nan_percentage:.1f}%)")
                print(f"Surface SM - NaN pixels: {nan_count} ({nan_percentage:.1f}%)")
                
                if valid_count > 0:
                    valid_data = surface_data[~np.isnan(surface_data)]
                    print(f"Surface SM - Value range: {valid_data.min():.4f} to {valid_data.max():.4f}")
                    print(f"Surface SM - Mean: {valid_data.mean():.4f}")
                
            # Check rootzone soil moisture
            if "sm_rootzone" in ds:
                rootzone_data = ds["sm_rootzone"].values
                print(f"Rootzone SM shape: {rootzone_data.shape}")
                
                total_pixels = rootzone_data.size
                nan_count = np.sum(np.isnan(rootzone_data))
                valid_count = total_pixels - nan_count
                nan_percentage = (nan_count / total_pixels) * 100
                
                print(f"Rootzone SM - Total pixels: {total_pixels}")
                print(f"Rootzone SM - Valid pixels: {valid_count} ({100-nan_percentage:.1f}%)")
                print(f"Rootzone SM - NaN pixels: {nan_count} ({nan_percentage:.1f}%)")
                
                if valid_count > 0:
                    valid_data = rootzone_data[~np.isnan(rootzone_data)]
                    print(f"Rootzone SM - Value range: {valid_data.min():.4f} to {valid_data.max():.4f}")
                    print(f"Rootzone SM - Mean: {valid_data.mean():.4f}")
            
            # Check attributes for fill values
            print(f"\nAttributes:")
            for var_name in ["sm_surface", "sm_rootzone"]:
                if var_name in ds:
                    var = ds[var_name]
                    print(f"{var_name} attributes: {dict(var.attrs)}")
        
        return True
        
    except Exception as e:
        print(f"Error analyzing {filepath}: {e}")
        traceback.print_exc()
        return False

def check_smap_region_extraction(filepath, bounds, crs):
    """Check how a specific region extraction would look."""
    try:
        print(f"\n=== Testing region extraction ===")
        print(f"Bounds: {bounds}")
        print(f"CRS: {crs}")
        
        # This would need the full extraction logic from smap_api.py
        # For now, just check the overall data quality
        return check_smap_file_quality(filepath)
        
    except Exception as e:
        print(f"Error testing region extraction: {e}")
        return False

def main():
    """Main function to check SMAP cache files."""
    cache_dir = Path("smap_cache")
    
    if not cache_dir.exists():
        print("SMAP cache directory does not exist")
        return
    
    # List all SMAP files
    smap_files = list(cache_dir.glob("*.h5"))
    
    if not smap_files:
        print("No SMAP files found in cache")
        return
    
    print(f"Found {len(smap_files)} SMAP files in cache:")
    
    # Sort by modification time (newest first)
    smap_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    
    for smap_file in smap_files:
        stat = smap_file.stat()
        mod_time = datetime.fromtimestamp(stat.st_mtime)
        size_mb = stat.st_size / (1024*1024)
        
        print(f"  {smap_file.name}: {size_mb:.1f} MB, modified: {mod_time}")
    
    # Analyze the most recent file in detail
    print(f"\n{'='*60}")
    print("DETAILED ANALYSIS OF MOST RECENT FILE")
    print(f"{'='*60}")
    
    most_recent = smap_files[0]
    success = check_smap_file_quality(str(most_recent))
    
    if success:
        print(f"\n✅ Successfully analyzed {most_recent.name}")
    else:
        print(f"\n❌ Failed to analyze {most_recent.name}")
    
    # Test region extraction with the problematic bounds from logs
    test_bounds = [600000.0, -1409760.0, 709800.0, -1299960.0]
    test_crs = "32636"
    
    print(f"\n{'='*60}")
    print("TESTING PROBLEMATIC REGION EXTRACTION")
    print(f"{'='*60}")
    
    check_smap_region_extraction(str(most_recent), test_bounds, test_crs)

if __name__ == "__main__":
    main() 