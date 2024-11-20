![Project Logo](logo-full.png)

# <center>Welcome to Gaia</center>

<div style="text-align: center;"><a href="https://www.gaiaresearch.ai/">Website</a></div>

----

Gaia is the intersection of geospatial data analysis and machine learning. Most current models, whether in academia/research or industry, are purpose built for a single task. They’re often quite small and tuned on incredibly specific and well-prepared datasets. There is hardly any advancement towards a “foundational” type mega-model. There have been attempts, sure - like LLMs trained on openstreetmap data, attempts to train in spatial understanding , etc. 

Gaia will begin at the cutting-edge of several different applications (beginning with two) and will expand toward a Grand Foundational Model. Geospatial data is diverse and expansive but all intertwined. Read more in our whitepaper [here] *DONT FORGET TO ATTACH LINK*

## Miners
[Quicklink](miner/MINER.md)

Miners develop models to understand future events. These events currently include soil moisture and geomagnetic readings at the equator. Miners will receive data from validators for the models that we have in place. They are also free to gather their own data from other resources. The tasks are consistent in design and in timing; this predictability allows miners the flexibility to retrieve any data that their model requires. 

Miners can choose between these two tasks or perform both. Incentive is split 50:50 between the tasks.


## Validators
[Quicklink](validator/VALIDATOR.md)

Validators will connect to a few API's to provide miners with the data they need to run models.

## Installation

Gaia is built on Fiber, the package developed by Namoray to simplify Subnets. 

### Install fiber

----
```bash
pip install "git+https://github.com/rayonlabs/fiber.git@1.0.0#egg=fiber[full]"
```

#### Create Python venv
```bash
python -m venv venv || python3 -m venv venv
source venv/bin/activate
pip install -e .
```

#### Register miner and/or validator on subnet
```bash
btcli subnets register --subtensor.network <NETWORK> --netuid <NETUID> --wallet.name <COLDKEY> --wallet.hotkey <HOTKEY>
```

#### Post IP to chain
```bash
fiber-post-ip --netuid <NETUID> --external_ip <YOUR_IP> --external_port <YOUR_PORT> --subtensor.network <NETWORK> --wallet.name <COLDKEY> --wallet.hotkey <HOTKEY> 
```

#### Setup Proxy server
Gaia uses a Proxy server to handle connections to Validators. You can setup your own proxy server, or use our script for an nginx server as follows:

```bash
./setup_proxy_server.sh --ip <YOUR IP> --port <PORT> --forwarding_port <PORT_FOR_MINER_OR_VALIDATOR> --server_name <NAME>
```

The port chosen here should match the port that you posted to the chain above. This will forward connections to the specified `--forwarding_port` which will be used when starting up the miner or validator (defaults to +1 of `--port`).

## Gaia's Bounty Program

Gaia currently offers two tasks: Soil Moisture and Geomagnetism (What do we want to call these? Should we have dope names for them?)
We will be ever-expanding these tasks to cover various areas of Geospatial research. Some tasks will build on each other and others will be entirely independent.

Nickel5 will not be the only people creating Geospatial tasks. We are offering a Bounty of (HOW MUCH TAO?) to anybody that creates a Geospatial task for our Subnet. We love open-source research and want to incentivize anybody with a Geospatial passion to work with us.

Details about task creation:
1. Must be on the cutting-edge of geospatial research and the task should be adequately applicable to Bittensor.
2. Must target large addressable markets.
3. Bounty reward is left to the discretion of Nickel5. If we use your task (through accepting your PR), we will reward the Bounty. Contact us through DM with any Task inquiries. 