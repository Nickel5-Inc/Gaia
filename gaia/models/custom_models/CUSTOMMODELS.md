# Miner Custom Models

***

Miners are highly encouraged to make custom models which will improve scores and enhance Gaia.

> [!NOTE]
> The system dynamically loads custom models by locating the file and class based on these naming conventions.
> Deviating from these standards will result in the base model being used instead.
>

***

## Soil Moisture Task

### Integration

1. **File Requirements**

> - The custom model file must be named `custom_geomagnetic_model.py`
> - **It must be placed in the directory:** `Gaia/gaia/models/custom_models/`

2. **Implementing the Custom Model**

> **IMPORTANT**:
> - Be named exactly `CustomGeomagneticModel`
> - Implement a method named exactly `run_inference()` as specified below

```python
class CustomSoilModel:
    def __init__(self):
        """Initialize your custom soil model."""
        self._load_model()

    def _load_model(self):
        """
        Load your model implementation.

        Use this method to load any resources needed for the model, such as
        machine learning weights, configuration files, or constants.
        """

    def run_inference(self, inputs: Dict[str, np.ndarray]) -> Dict[str, List[List[float]]]:
        """
        Required method that must be implemented to run model predictions.
        This method should call all necessary methods to preprocess data,
        and run the model end-to-end.

        Args:
            inputs: Dictionary containing raw numpy arrays:
                - `sentinel_ndvi`: np.ndarray [3, H, W] array with B8, B4, NDVI bands.
                - `elevation`: np.ndarray [1, H, W] array with elevation data.
                - `era5`: np.ndarray [17, H, W] array with weather variables.

        Returns:
            Dictionary containing exactly:
                - `surface`: list[list[float]] - 11x11 list of lists with values 0-1.
                - `rootzone`: list[list[float]] - 11x11 list of lists with values 0-1.

        Example:
            model = CustomSoilModel()
            predictions = model.run_inference({
                'sentinel_ndvi': sentinel_array,
                'elevation': elevation_array,
                'era5': era5_array
            })
            print(predictions)  # Output example
            {
                'surface': [[0.1, 0.2, ...], [0.3, 0.4, ...]],
                'rootzone': [[0.2, 0.3, ...], [0.4, 0.5, ...]]
            }
        """
        pass
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

***

## Geomagnetic Task

### Integration

1. **File Requirements**

> - The custom model file must be named `custom_geomagnetic_model.py`
> - **It must be placed in the directory:** `Gaia/gaia/models/custom_models/`

2. **Implementing the Custom Model**

> **IMPORTANT**:
> - Be named exactly `CustomGeomagneticModel`
> - Implement a method named exactly `run_inference()` as specified below

```python
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

#### **Input Format**

The input to `run_inference` is a pandas DataFrame with:

- `timestamp`: Datetime values representing observation times.
- `value`: Float values representing the Dst index.

#### **Output Format**

The output must be a dictionary with:

- `predicted_value`: A single float representing the predicted Dst index.
- `prediction_time`: A string representing the associated timestamp.

