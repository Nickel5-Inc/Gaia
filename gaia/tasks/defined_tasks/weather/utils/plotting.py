import matplotlib.pyplot as plt
import numpy as np
import xarray as xr

def plot_all_variables(ds, pressure_level=500):
    """
    Plotting tool to visualize all dynamic variables
    
    Args:
        ds: xarray Dataset containing the GFS variables
        pressure_level: Pressure level in hPa to plot for atmospheric variables (default: 500)
    """
    plot_settings = {
        # Surface variables
        '2t': {'title': '2m Temperature (K)', 'cmap': 'RdBu_r', 'vmin': 220, 'vmax': 320},
        '10u': {'title': '10m U-Wind (m/s)', 'cmap': 'RdBu_r', 'vmin': -20, 'vmax': 20},
        '10v': {'title': '10m V-Wind (m/s)', 'cmap': 'RdBu_r', 'vmin': -20, 'vmax': 20},
        'msl': {'title': 'Mean Sea Level Pressure (hPa)', 'cmap': 'viridis', 'vmin': 980, 'vmax': 1030},
        # Atmospheric variables
        't': {'title': f'Temperature at {pressure_level}hPa (K)', 'cmap': 'RdBu_r', 'vmin': 200, 'vmax': 300},
        'u': {'title': f'U-Wind at {pressure_level}hPa (m/s)', 'cmap': 'RdBu_r', 'vmin': -50, 'vmax': 50},
        'v': {'title': f'V-Wind at {pressure_level}hPa (m/s)', 'cmap': 'RdBu_r', 'vmin': -50, 'vmax': 50},
        'q': {'title': f'Specific Humidity at {pressure_level}hPa (kg/kg)', 'cmap': 'Blues', 'vmin': 0, 'vmax': 0.01},
        'z': {'title': f'Geopotential at {pressure_level}hPa (m²/s²)', 'cmap': 'terrain', 'vmin': None, 'vmax': None},
    }
    
    fig, axes = plt.subplots(3, 3, figsize=(18, 15))
    axes = axes.flatten()
    
    surface_vars = ['2t', '10u', '10v', 'msl']
    atmospheric_vars = ['t', 'u', 'v', 'q', 'z']
    
    for i, var_name in enumerate(surface_vars + atmospheric_vars):
        ax = axes[i]
        settings = plot_settings[var_name]
        
        if var_name == 'msl':
            data = ds[var_name] / 100.0  # Convert Pa to hPa
        elif var_name in atmospheric_vars:
            if 'lev' in ds[var_name].dims:
                level_idx = np.abs(ds.lev.values - pressure_level).argmin()
                data = ds[var_name].isel(lev=level_idx).squeeze()
            else:
                data = ds[var_name].squeeze()
        else:
            data = ds[var_name].squeeze()
        
        if 'time' in data.dims:
            data = data.isel(time=0)
        
        vmin = settings['vmin']
        vmax = settings['vmax']
        
        if var_name == 'z' and (vmin is None or vmax is None):
            vmin = data.min().item()
            vmax = data.max().item()
        
        im = ax.imshow(
            data, 
            origin='upper',
            cmap=settings['cmap'],
            vmin=vmin, 
            vmax=vmax,
            aspect='auto'
        )
        
        # Add title
        ax.set_title(settings['title'])
        
        if i >= 6:
            ax.set_xlabel('Longitude')
        if i % 3 == 0:
            ax.set_ylabel('Latitude')
            
        ax.set_xticks([])
        ax.set_yticks([])
        cbar = plt.colorbar(im, ax=ax, orientation='horizontal', pad=0.05, shrink=0.8)
        
    plt.tight_layout()
    plt.subplots_adjust(top=0.95)
    fig.suptitle('GFS Variables for Aurora Model', fontsize=16)
    
    return fig