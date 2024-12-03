## GeoMagnetic Task

### Why It Matters
Geomagnetic disturbances, driven by solar activity, can have significant impacts on Earth's technology systems, including:
- GPS and communication systems.
- Power grids and satellite operations.
- Navigation and aviation industries.

Accurate forecasting of the DST (Disturbance Storm Time) index allows for proactive mitigation strategies, protecting critical infrastructure and reducing risks associated with geomagnetic storms.

---

### What Data It Uses
The Geomagnetic Task uses DST index data, a time-series measure reflecting the intensity of geomagnetic disturbances:
1. **Hourly DST Index**:
   - A cleaned DataFrame containing recent hourly DST values, sent by the Validator.
   - Includes `timestamp` and `value` columns for the current month.
2. **Historical DST Data** (optional):
   - Miners can gather additional historical data to improve model performance.

Validators preprocess the DST data to ensure consistency and provide it to Miners for prediction.

---

### How to Run the Model

#### Step 1: Prepare Data
- The Validator sends a cleaned DataFrame (`data`) with columns:
  - `timestamp`: The time of the observation (in UTC).
  - `value`: The geomagnetic disturbance intensity.

#### Step 2: Generate Model Input
- Use **`process_geomag_data.py`** to retrieve and preprocess historical DST data (if needed).
- Combine this data with the Validator-provided DataFrame to create a prediction-ready input.

#### Step 3: Run the Geomagnetic Model
- Use **`GeomagneticPreprocessing`** to generate predictions:
  ```python
  from tasks.defined_tasks.geomagnetic.geomagnetic_preprocessing import GeomagneticPreprocessing

  preprocessing = GeomagneticPreprocessing()
  prediction_result = preprocessing.predict_next_hour(data, model=geomag_model)


### Summary of What the Miner Should Return to the Validator

The miner must return the following to the Validator for evaluation:

- **Predicted DST Value**: The miner’s predicted DST index for the next hour.
- **Timestamp (UTC)**: The UTC timestamp of the last observation used in the prediction, ensuring it’s standardized.

---

## Soil Moisture Task

### Why It Matters
Soil moisture is a critical factor in understanding environmental processes, agriculture, and weather forecasting. 
Accurate soil moisture data helps: 
- Optimize agricultural water use.
- Predict droughts and floods.
- Enhance weather and climate models.
- Support ecological research.

### What Data It Uses
The soil moisture model integrates various datasets to provide comprehensive insights:
1. **Sentinel-2 Imagery**:
   - High-resolution satellite data for monitoring vegetation and land cover.
2. **IFS Forecasting Data**:
   - Supplies weather forecasts, including precipitation and temperature, relevant for soil moisture modeling.
3. **SMAP Data**:
   - Global soil moisture data for scoring and analysis.
4. **SRTM Data**:
   - Elevation data from the Shuttle Radar Topography Mission to incorporate terrain information.
5. **NDVI (Normalized Difference Vegetation Index)**:
   - Tracks vegetation health and coverage, crucial for understanding land surface conditions.

**These datasets are aligned based on the Sentinel-2 region boundaries to ensure spatial consistency and precision.**

### How to Run the Model

#### Step 1: Prepare Data
- Use **`region_selection.py`** to select random regions for analysis, avoiding urban areas and large water bodies.
- Retrieve necessary datasets using **`soil_apis.py`**:
  - Sentinel-2 imagery.
  - IFS weather forecasts.
  - SRTM elevation tiles.
  - NDVI vegetation data.

#### Step 2: Generate Model Input
- Compile the data into a `.tiff` file with the following band order:
  - `[Sentinel-2, IFS, SRTM, NDVI]`.
- Store the corresponding bounds and CRS (Coordinate Reference System) for later validation.

#### Step 3: Run the Soil Moisture Model
- Use **`soil_model.py`** to process the `.tiff` file and generate soil moisture predictions.

#### Step 4: Post-Processing
- Run **`Inference_classes.py`** to format predictions and prepare them for validation.

#### Step 5: Validation
- Submit the predictions, along with region bounds and CRS, to the validator for comparison with ground truth data.


---

#### Create dev.env file for miner with the following components:
```bash
WALLET_NAME=<YOUR_WALLET.NAME>
HOTKEY_NAME=<YOUR_WALLET_HOTKEY>
NETUID=<NETUID>
SUBTENSOR_NETWORK=<NETWORK> # finney or test
MIN_STAKE_THRESHOLD=<INT> # 10000 for mainnet, 5 for testnet
SUBTENSOR_ADDRESS=<SUBTENSOR_ADDRESS> # wss://test.finney.opentensor.ai:443/ for testnet, wss://finney.opentensor.ai:443/ for mainnet (chain endpoint)
```

#### Run the miner
```bash
cd gaia
cd miner
python miner.py
```