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
cd gaia
cd validator
python validator.py
```




## General Notes and Structure


The validator neuron is responsible for gathering & processing data for tasks, constructing queries, sending requests to miners, and scoring results from those requests.

It runs similar to an Operating System.


## Main Loop









