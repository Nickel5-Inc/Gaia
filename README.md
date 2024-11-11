# Gaia

Lets get a good introduction for this. Pushing research forward, targets these markets, built on fiber, blah blah blah

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
./setup_proxy_server.sh --ip <YOUR IP> --port <PORT USED ABOVE> --server_name <NAME>
```

## Gaia's Bounty Program

Gaia currently offers two tasks: Soil Moisture and Geomagnetism (What do we want to call these? Should we have dope names for them?)
We will be ever-expanding these tasks to cover various areas of Geospatial research. Some tasks will build on each other and others will be entirely independent.

Nickel5 will not be the only people creating Geospatial tasks. We are offering a Bounty of (HOW MUCH TAO?) to anybody that creates a Geospatial task for our Subnet. We love open-source research and want to incentivize anybody with a Geospatial passion to work with us.

Details about task creation:
1. Must be on the cutting-edge of geospatial research and the task should be adequately applicable to Bittensor.
2. Must target large addressable markets.
3. Bounty reward is left to the discretion of Nickel5. If we use your task (through accepting your PR), we will reward the Bounty. Contact us through DM with any Task inquiries. 