# *Validator Information*

---


## **API's required**


### NASA EarthData
1. Create an account at https://urs.earthdata.nasa.gov/
2. Accept the necessary EULAs for the following collections:
    - GESDISC Test Data Archive 
    - OB.DAAC Data Access 
    - Sentinel EULA

3. Generate an API token and save it in the .env file (details below)

---

#### Create .env file for validator with the following components:
```bash
DB_USER=<YOUR_DB_USER> # postgres is default from setup script
DB_PASSWORD=<YOUR_DB_PASSWORD> # postgres is default from setup script
DB_HOST=<YOUR_DB_HOST> # localhost is default from setup script
DB_PORT=<YOUR_DB_PORT> # 5432

ENV=prod #dev for debug logs


WALLET_NAME=<YOUR_WALLET_NAME>
HOTKEY_NAME=<YOUR_WALLET_HOTKEY>
NETUID=<NETUID> # 57 for mainnet, 237 for testnet
SUBTENSOR_NETWORK=<NETWORK> # finney or test
SUBTENSOR_ADDRESS=<SUBTENSOR_ADDRESS> # wss://test.finney.opentensor.ai:443/ for testnet, wss://finney.opentensor.ai:443/ for mainnet
MIN_STAKE_THRESHOLD=<INT> # 100000 for mainnet, 5 for testnet

EARTHDATA_USERNAME=<YOUR_EARTHDATA_USERNAME> 
EARTHDATA_PASSWORD=<YOUR_EARTHDATA_PASSWORD>
CDS_API_KEY=<YOUR_CDS_API_KEY> # earthdata api key for downloading data from NASA
```

#### Run the validator
```bash
pm2 start --name validator --instances 1 python -- gaia/validator/validator.py 
```

---


