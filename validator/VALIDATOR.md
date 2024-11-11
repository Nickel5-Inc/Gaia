# API's required


#### Create .env file for validator with the following components:
```bash
WALLET_NAME=<YOUR_WALLET.NAME>
HOTKEY_NAME=<YOUR_WALLET_HOTKEY>
NETUID=<NETUID>
SUBTENSOR_NETWORK=<NETWORK>
MIN_STAKE_THRESHOLD=<INT>
```

#### Run the validator
```bash
cd validator
python validator.py
```