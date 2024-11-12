## GEOMAGNETIC Task By *vValentine*  
The Geomagnetic Task is designed to predict geomagnetic disturbances, specifically through the DST (Disturbance Storm 
Time) index, an essential measure in space weather monitoring. Geomagnetic disturbances, driven by solar activity, can
have significant impacts on Earth's technology systems, including GPS, communications, power grids, and satellites. 
Accurate forecasting of the DST index allows for proactive mitigation strategies, helping protect critical 
infrastructure and reduce risks associated with geomagnetic storms.

### Data
The Geomagnetic Task utilizes DST index data, a time-series measure that reflects the intensity of geomagnetic 
disturbances. 
- A cleaned DataFrame containing recent DST values, measured hourly will be sent to the Miner from the Validator.
- The cleaned DataFrame will ONLY have data of the current month. 
- Miners can ALWAYS retrieve more historical data to make prediction more accurate.

## Steps for Running the Geomagnetic Task as a Miner

### Step 1: Obtain and Prepare Data
- **Retrieve Recent DST Data**:
  - Obtain a cleaned DataFrame (`data`) containing recent DST values, with required columns like `timestamp` and `value`.
  - This is from the Validator

### Step 2: Choose the Model
- **Decide on the Model Source**:
  - You can choose between:
    - **Base Model from HuggingFace**: To use the model hosted on HuggingFace, set the `--use_base_model` flag when running the task.
    - **Local Model**: To use the local model within your project, omit the flag.

- **Command to Start the Miner**:
  - If using the HuggingFace base model, use this command:
    ```bash
    python start_miner.py --use_base_model
    ```
  - To use the local model, run:
    ```bash
    python start_miner.py
    ```

### Step 3: Run `start_miner.py` to Initialize the Model
- **Run `start_miner.py`**:
  - Based on the flag, `start_miner.py` will initialize either the HuggingFace model or the local model.
  - The following code in `start_miner.py` will load the model accordingly:
    ```python
    if args.use_base_model:
        from tasks.base.models.geomag_basemodel import GeoMagBaseModel
        geomag_model = GeoMagBaseModel()
        print("Using GeoMagBaseModel from HuggingFace.")
    else:
        from models.geomag_basemodel import GeoMagBaseModel
        geomag_model = GeoMagBaseModel()
        print("Using local GeoMagBaseModel.")
    ```
  - This loads the model into the `geomag_model` variable, which will be used for predictions.

### Step 4: Preprocess the Data (Optional)
- **Prepare Data for Prediction**:
  - If further data transformation is needed, call the `preprocess` method from `GeomagneticPreprocessing`.

    ```python
    from tasks.defined_tasks.geomagnetic.geomagnetic_preprocessing import GeomagneticPreprocessing
    preprocessing = GeomagneticPreprocessing()
    processed_data = preprocessing.preprocess(data)
    ```

### Step 5: Make the Prediction
- **Predict the Next Hour’s DST Value**:
  - Use the initialized model to make a prediction by calling the `predict_next_hour` method in `GeomagneticPreprocessing`. This method will return both the predicted DST value and the timestamp in UTC.

    ```python
    prediction_result = preprocessing.predict_next_hour(processed_data, model=geomag_model)
    predicted_value = prediction_result["predicted_value"]
    timestamp_utc = prediction_result["timestamp"]
    ```

  - Now, `predicted_value` contains the DST prediction, and `timestamp_utc` contains the timestamp in UTC format.

### Step 6: Return the Prediction to the Validator
- **Package the Prediction Result**:
  - The miner should package the prediction result with the following data:
    - **Predicted DST Value**: The DST index predicted for the next hour.
    - **Timestamp (UTC)**: The timestamp of the last observation used in making the prediction.

    ```python
    results = {
        "predicted_value": predicted_value,
        "timestamp": timestamp_utc
    }
    ```

### Step 7: Send the Prediction Results to the Validator
- **Submit the Results for Validation**:
  - The validator will use the real-time DST value to calculate the score based on the miner’s prediction.
  - If integrated within the `GeomagneticTask`, call the `validator_execute` method with the `data` and `actual_value` arguments.

    ```python
    from tasks.defined_tasks.geomagnetic.geomagnetic_task import GeomagneticTask
    task = GeomagneticTask()
    formatted_results = task.validator_execute(data, actual_value)
    print("Prediction Results:", formatted_results)
    ```
---

### Summary of What the Miner Should Return to the Validator

The miner must return the following to the Validator for evaluation:

- **Predicted DST Value**: The miner’s predicted DST index for the next hour.
- **Timestamp (UTC)**: The UTC timestamp of the last observation used in the prediction, ensuring it’s standardized.



## DESCRIPTION OF SOIL MOISTURE BY STEVEN - why it matters, what data it uses, how to run the model

#### Create dev.env file for miner with the following components:
```bash
WALLET_NAME=<YOUR_WALLET.NAME>
HOTKEY_NAME=<YOUR_WALLET_HOTKEY>
NETUID=<NETUID>
SUBTENSOR_NETWORK=<NETWORK>
MIN_STAKE_THRESHOLD=<INT>
```

#### Run the miner
```bash
cd miner
python miner.py
```