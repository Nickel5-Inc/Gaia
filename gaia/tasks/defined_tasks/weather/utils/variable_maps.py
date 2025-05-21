AURORA_TO_GFS_VAR_MAP = {
    # Surface vars
    '2t': 'tmp2m',        # 2-meter Temperature
    '10u': 'ugrd10m',     # 10-meter U-component of wind
    '10v': 'vgrd10m',     # 10-meter V-component of wind
    'msl': 'prmslmsl',    # Mean Sea Level Pressure

    # Atmos vars
    't': 'tmpprs',        # Temperature at pressure levels
    'u': 'ugrdprs',       # U-component of wind at pressure levels
    'v': 'vgrdprs',       # V-component of wind at pressure levels
    'q': 'spfhprs',       # Specific Humidity at pressure levels
    'z': 'hgtprs',        # Geopotential Height at pressure levels (converted to geopotential)
    
    'temperature_2m': 'tmp2m',
    'u_wind_10m': 'ugrd10m',
    'v_wind_10m': 'vgrd10m',
    'mean_sea_level_pressure': 'prmslmsl',
    'temperature_plev': 'tmpprs',
    'u_wind_plev': 'ugrdprs',
    'v_wind_plev': 'vgrdprs',
    'specific_humidity_plev': 'spfhprs',
    'geopotential_height_plev': 'hgtprs',
}

ALL_MAPPED_GFS_VARS_FOR_FETCH = list(set(AURORA_TO_GFS_VAR_MAP.values()))