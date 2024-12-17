# Miner Custom Models

***

Miners are highly encouraged to make custom models which will improve scores and enhance Gaia.

> [NOTE]
> The system dynamically loads custom models by locating the file and class based on these naming conventions.
> Deviating from these standards will result in the base model being used instead, or errors.
>

***

## Soil Moisture Task

### Integration

1. **File Requirements**

> - The custom model file must be named `custom_soil_model.py`
> - **It must be placed in the directory:** `Gaia/gaia/models/custom_models/`

2. **Implementing the Custom Model**

> **IMPORTANT: This Class must**:
> - Be named exactly `CustomSoilModel`
> - Implement a method named exactly `run_inference()` as specified below

```python
from typing import Dict, List
import numpy as np
#add any imports here

"""
TEMPLATE FOR CUSTOM SOIL MODEL

Implement your custom soil model in this file.
Add any methods as needed.


IMPORTANT: This class must:
1. Be named exactly 'CustomSoilModel'
2. Reside in a file named exactly 'custom_soil_model.py'
3. Placed in the 'Gaia/gaia/models/custom_models/' directory
3. Implement a method named exactly 'run_inference()' as specified below
"""

class CustomSoilModel:
    def __init__(self):
        """Initialize your custom model"""
        self._load_model()

    def _load_model(self):
        """Load your model implementation"""
        pass

    def run_inference(self, inputs: Dict[str, np.ndarray]) -> Dict[str, List[List[float]]]:
        """
        Required method that must be implemented to run model predictions.
        run_inference should call all the methods needed to load, process data, and run the model end to end
        Add any additional methods for data collection, preprocessing, etc. as needed.
        
        Args:
            inputs: Dictionary containing raw numpy arrays:
                sentinel_ndvi: np.ndarray[float32] [3, H, W] array with B8, B4, NDVI bands
                elevation: np.ndarray[float32] [1, H, W] array with elevation data
                era5: np.ndarray[float32] [17, H, W] array with 17 IFS weather variables

                reference the README or HuggingFace page for more information on the input data
                and weather variables

                Raw arrays at 500m resolution, no normalization applied (~222x222)
        
        Returns:
            Dictionary containing exactly:
                surface: list[list[float]] - 11x11 list of lists with values 0-1
                rootzone: list[list[float]] - 11x11 list of lists with values 0-1

                Must match 11x11 pixel resolution (9km resolution of SMAP L4)
                
        Example:
            model = CustomSoilModel()
            predictions = model.run_inference({
                'sentinel_ndvi': sentinel_array,  # numpy array
                'elevation': elevation_array,     # numpy array
                'era5': era5_array               # numpy array
            })
        Output example: 
            predictions = {
                'surface': [[0.1, 0.2, ...], [0.3, 0.4, ...], ...],  # 11x11
                'rootzone': [[0.2, 0.3, ...], [0.4, 0.5, ...], ...]  # 11x11
            }
        """
        pass



"""
INPUT DATA:
222x222 pixels, 500m resolution, no normalization
Some regions may have gaps in the data, check for NaNs, INFs and invalid values (negatives in SRTM)

dict:
{
    "sentinel_ndvi": np.ndarray,  # shape [3, w, H]
    "elevation": np.ndarray,      # shape [1, W, H] 
    "era5": np.ndarray,           # shape [17, W, H]
}
```

#### **Input Format**

The input to `run_inference` is a dictionary containing:

- `sentinel_ndvi`: A numpy array of shape [3, H, W] representing Sentinel-2 bands B8, B4, and NDVI.
- `elevation`: A numpy array of shape [1, H, W] representing elevation data.
- `era5`: A numpy array of shape [17, H, W] containing weather variables.

#### **Output Format**

The output must be a dictionary with:

- `surface`: A nested list (11x11) of floats between 0-1 representing surface soil moisture predictions.
- `rootzone`: A nested list (11x11) of floats between 0-1 representing root zone soil moisture predictions.

#### **Weather Data Details**

IFS weather variables (in order):
- t2m: Surface air temperature (2m height) (Kelvin)
- tp: Total precipitation (m/day)
- ssrd: Surface solar radiation downwards (Joules/m²)
- st: Soil temperature at surface (Kelvin)
- stl2: Soil temperature at 2m depth (Kelvin)
- stl3: Soil temperature at 3m depth (Kelvin)
- sp: Surface pressure (Pascals)
- d2m: Dewpoint temperature (Kelvin)
- u10: Wind components at 10m (m/s)
- v10: Wind components at 10m (m/s)
- ro: Total runoff (m/day)
- msl: Mean sea level pressure (Pascals)
- et0: Reference evapotranspiration (mm/day)
- bare_soil_evap: Bare soil evaporation (mm/day)
- svp: Saturated vapor pressure (kPa)
- avp: Actual vapor pressure (kPa)
- r_n: Net radiation (MJ/m²/day) 

**Note**:
Evapotranspiration are variables computed using the Penman-Monteith equation (FAO-56 compliant). 
see soil_apis.py for more information on the data processing, transformations, and scaling.

***

## Geomagnetic Task

### Integration

1. **File Requirements**

> - The custom model file must be named `custom_geomagnetic_model.py`
> - **It must be placed in the directory:** `Gaia/gaia/models/custom_models/`

2. **Implementing the Custom Model**

> **IMPORTANT: This Class must**:
> - Be named exactly `CustomGeomagneticModel`
> - Implement a method named exactly `run_inference()` as specified below

```python
from typing import Dict, Any
import pandas as pd
import numpy as np
#add any imports here

class CustomGeomagneticModel:
    def __init__(self):
        """Initialize your custom geomagnetic model."""
        self._load_model()

    def _load_model(self):
        """
        Load your model implementation.

        Use this method to load any resources needed for the model, such as
        machine learning weights, configuration files, or constants.
        """

    def run_inference(self, inputs: pd.DataFrame) -> Dict[str, Any]:
        """
        Required method that must be implemented to run model predictions.
        This method should call all necessary methods to preprocess data,
        and run the model end-to-end.

        Args:
            inputs (pd.DataFrame): Preprocessed input data for the model, containing:
                - `timestamp` (datetime): Datetime information (column can also be named `ds`).
                - `value` (float): Geomagnetic Dst index (column can also be named `y`).

        Returns:
            Dict[str, Any]: Predicted Dst value for the next hour.

        Example:
            model = CustomGeomagneticModel()
            predictions = model.run_inference(
                pd.DataFrame({
                    "timestamp": [datetime(2024, 12, 10, 0, 0), datetime(2024, 12, 10, 1, 0)],
                    "value": [-20, -25]
                })
            )
            print(predictions)  # Output: -24.5
        """
```
**Input Format**

The input to `run_inference` is a pandas DataFrame with:

- `timestamp`: Datetime values representing observation times.
- `value`: Float values representing the Dst index.

**Output Format**

The output must be a dictionary with:

- `predicted_value`: A single float representing the predicted Dst index.
- `prediction_time`: A string representing the associated timestamp.

