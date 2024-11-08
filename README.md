# GAIA

### Welcome to the subnet 
GAIA is a one-of-a-kind subnet where the world is our oyster. The purpose of this 
subnet is to inspire the world with the help of the Bittensor community. 
We base all of our models off of 'ground-truth' and believe that tasks will
generate SoTA scientific results. 

![Project Logo](logo.png) THIS COULD BE A NICE ADDITION


### *Quicklinks*

[Miner](docs/README.md) NEED TO UPDATE PATH!

[Validator](docs/README.md) NEED TO UPDATE PATH!



## Fiber

Fiber is a lightweight, developer-friendly package for running Bittensor subnets.

Fiber is designed to be a highly secure networking framework that utilizes Multi-Layer Transport Security (MLTS) for enhanced data protection, offers DDoS resistance, and is designed to be easily extendable across multiple programming languages.


### Installation

#### Install Full fiber - with all networking + chain stuff

----
```bash
pip install "git+https://github.com/rayonlabs/fiber.git@x.y.z#egg=fiber[full]"
```

Replace x.y.z with the desired version (or remove it to install the latest version)

### Install Fiber with only Chain interactions

----


```bash
pip install "git+https://github.com/rayonlabs/fiber.git@x.y.z"
```

Or:


```bash
pip install "git+https://github.com/rayonlabs/fiber.git@x.y.z#egg=fiber[chain]"
```


---

#### For dev
```bash
python -m venv venv || python3 -m venv venv
source venv/bin/activate
pip install -e .
pre-commit install
```

#### Register miner and/or validator on subnet
```bash
btcli subnets register --subtensor.network <NETWORK> --netuid <NETUID> --wallet.name <COLDKEY> --wallet.hotkey <HOTKEY>
```

#### Post IP to chain
```bash
fiber-post-ip --netuid <NETUID> --external_ip <YOUR_IP> --external_port <YOUR_PORT> --subtensor.network <NETWORK> --wallet.name <COLDKEY> --wallet.hotkey <HOTKEY> 
```

#### Create dev.env file for miner with the following components:
```bash
WALLET_NAME=<YOUR_WALLET.NAME>
HOTKEY_NAME=<YOUR_WALLET_HOTKEY>
NETUID=<NETUID>
SUBTENSOR_NETWORK=<NETWORK>
MIN_STAKE_THRESHOLD=<INT>
```

#### Run dev miner
```bash
cd dev_utils
python start_miner.py
```

#### Create .env file for validator with the following components:
```bash
WALLET_NAME=<YOUR_WALLET.NAME>
HOTKEY_NAME=<YOUR_WALLET_HOTKEY>
NETUID=<NETUID>
SUBTENSOR_NETWORK=<NETWORK>
MIN_STAKE_THRESHOLD=<INT>
```

#### Run dev validator
```bash
cd dev_utils
python run_validator.py
```
