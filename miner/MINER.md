# DESCRIPTION OF GEOMAGNETIC TASK BY GABBY - why it matters, what data it uses, how to run the model

# DESCRIPTION OF SOIL MOISTURE BY STEVEN - why it matters, what data it uses, how to run the model

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