# API's required

## NASA EarthData
1. Create an account at https://urs.earthdata.nasa.gov/
2. Accept the necessary EULAs for the following collections:
    - GESDISC Test Data Archive 
    - OB.DAAC Data Access 
    - Sentinel EULA

3. Generate an API token and save it in the .env file (details below)



#### Create .env file for validator with the following components:
```bash
WALLET_NAME=<YOUR_WALLET_NAME>
HOTKEY_NAME=<YOUR_WALLET_HOTKEY>
NETUID=<NETUID> # 57 for mainnet, 237 for testnet
SUBTENSOR_NETWORK=<NETWORK> # finney or test
MIN_STAKE_THRESHOLD=<INT> # 100000 for mainnet, 5 for testnet

EARTHDATA_USERNAME=<YOUR_EARTHDATA_USERNAME>
EARTHDATA_PASSWORD=<YOUR_EARTHDATA_PASSWORD>
CDS_API_KEY=<YOUR_CDS_API_KEY>
```

#### Run the validator
```bash
cd gaia
cd validator
python validator.py
```










