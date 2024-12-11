# Miner Custom Models 

***
 
Miners are highly encouraged to make custom models which will improve scores and enhance Gaia.

>[!NOTE]
>The system dynamically loads custom models by locating the file and class based on these naming conventions. 
Deviating from these standards will result in the base model being used instead.
> 

***

## Soil Moisture Task
### Intergration
>
> 
> 

***

## Geomagnetic Task
### Intergration
1. **File Requirements**
> - The custom model file must be named `custom_geomagnetic_model.py`
> - **It must be placed in the directory:** `Gaia/gaia/models/custom_models/`

2. **Implementing the Custom Model**
> **IMPORTANT**:
> - Be named exactly `CustomGeomagneticModel`
> - Implement a method named exactly `run_inference()` as specified below

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

        def run_inference(self, inputs: pd.DataFrame) -> float:
            """
            Required method that must be implemented to run model predictions.
            This method should call all necessary methods to preprocess data,
            and run the model end-to-end.

            Args:
                inputs (pd.DataFrame): Preprocessed input data for the model, containing:
                    - `timestamp` (datetime): Datetime information (column can also be named `ds`).
                    - `value` (float): Geomagnetic Dst index (column can also be named `y`).
    
            Returns:
                float: Predicted Dst value for the next hour.

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

