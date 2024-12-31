![Project Logo](docs/logo-full.png)

# <center>Welcome to Gaia</center>

<div style="text-align: center;"><a href="https://www.gaiaresearch.ai/">Website</a></div>

----

Gaia is a platform for research and development of geospatial machine learning models. 
Read more about the long-term vision in our [whitepaper](https://www.gaiaresearch.ai/whitepaper).

>[!NOTE]
> BETA VERSION = 1.0.2
>
> The beta version of Gaia launches with limited functionality. Many of the planned features are not yet available, however we are still accepting miners and validators for the initial tasks. 

### **Clone the Repository**
```bash
git clone https://github.com/Nickel5-Inc/Gaia.git
cd Gaia
```

## Miners
[Quicklink](docs/MINER.md)

Miners develop models to understand future events. These events currently include soil moisture and geomagnetic readings at the equator. Miners will receive data from validators for the models that we have in place. They are also free to gather their own data from other resources. The tasks are consistent in design and in timing; this predictability allows miners the flexibility to retrieve any data that their model requires. 

Miners can choose between these two tasks or perform both. Incentive is split 50:50 between the tasks.


## Validators
[Quicklink](docs/VALIDATOR.md)

Validators will connect to a few API's to provide miners with the data they need to run models.

## Installation (For ALL Neurons)

Gaia is built on [Fiber](https://github.com/rayonlabs/fiber) - special thanks to namoray and the Rayon labs team.

### Run the setup script

```bash
python./scripts/setup.py
```
- This will create a virtual environment and install all dependencies
- The virtual environment (.gaia) will be located above the gaia directory.
- Activate it after running the setup script using 
```bash
source ../.gaia/bin/activate
```


## Install fiber

----
```bash
pip install "git+https://github.com/rayonlabs/fiber.git@production#egg=fiber[full]"
```


#### Register miner and/or validator on subnet
```bash
btcli subnets register --subtensor.network <NETWORK> --netuid <NETUID> --wallet.name <COLDKEY> --wallet.hotkey <HOTKEY>
```



#### Setup Proxy server
Gaia uses a Proxy server to handle connections to Validators. You can setup your own proxy server, or use our script for an nginx server as follows:

```bash
./setup_proxy_server.sh --ip <YOUR IP> --port <PORT> --forwarding_port <PORT_FOR_MINER_OR_VALIDATOR> --server_name <NAME>
```

- This will run as a background process, but it must be running for proper communication between Miners and Validators
- IMPORTANT: the port argument is your external facing port, and the forwarding port is the INTERNAL port that the miner or validator will be using to communicate with the proxy server
- The server name argument is optional and could be set to whatever you'd like

#### Post IP to chain -- IMPORTANT
```bash
fiber-post-ip --netuid <NETUID> --external_ip <YOUR_IP> --external_port <YOUR_PORT> --subtensor.network <NETWORK> --wallet.name <COLDKEY> --wallet.hotkey <HOTKEY> 
```
- You only need to do this once per key, but if you get deregistered or your IP changes, you'll need to re-post your IP. We recommend using a static IP for all nodes.
- Make sure that you use the EXTERNAL port that you configured in the proxy server script above
- This will be automated in a future version, but for now you'll need to post your IP manually


### Starting Miner
- Further instructions are linked below, but ensure that you start the miner with the `--port` argument pointing to the FORWARDING/INTERNAL port configured in the proxy server script above.


### Follow the Setup Guides for [Miner](docs/MINER.md) or [Validator](docs/VALIDATOR.md)
#### Custom Miner Models: [HERE](gaia/models/custom_models/CUSTOMMODELS.md)

---

## Compute Requirements (min)

### Miners
- CPU: 6-core processor
- RAM: 8 GB
- Network: Reliable connection with at least 80 Mbps upload and download speeds. 1Tb monthly bandwidth.
### Validators
- CPU: 8-core processor
- RAM: 16 GB
- Network: Reliable connection with at least 80 Mbps upload and download speeds. 1Tb monthly bandwidth.

--- 

## Data acknowledgements

### ECMWF Open Data
#### Copyright statement
Copyright © [2024] European Centre for Medium-Range Weather Forecasts (ECMWF).

#### Licence Statement
Copyright 2024 Nickel5 Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#### Disclaimer
ECMWF does not accept any liability whatsoever for any error or omission in the data, their availability, or for any loss or damage arising from their use.

Material has been modified by the following: resolution transformations, spatial transformations, and evapotranspiration calculations on existing bands of ECMWF data.

[ECMWF Open Data](https://www.ecmwf.int/en/forecasts/datasets/open-data)

### HLS Sentinel-2

Masek, J., Ju, J., Roger, J., Skakun, S., Vermote, E., Claverie, M., Dungan, J., Yin, Z., Freitag, B., Justice, C. (2021). HLS Sentinel-2 Multi-spectral Instrument Surface Reflectance Daily Global 30m v2.0 [Data set]. NASA EOSDIS Land Processes Distributed Active Archive Center. Accessed 2024-11-27 from https://doi.org/10.5067/HLS/HLSS30.002

[HLSS30](https://lpdaac.usgs.gov/products/hlss30v002/)

### Soil Moisture Active Passive

Reichle, R., De Lannoy, G., Koster, R. D., Crow, W. T., Kimball, J. S., Liu, Q. & Bechtold, M. (2022). SMAP L4 Global 3-hourly 9 km EASE-Grid Surface and Root Zone Soil Moisture Geophysical Data. (SPL4SMGP, Version 7). [Data Set]. Boulder, Colorado USA. NASA National Snow and Ice Data Center Distributed Active Archive Center. https://doi.org/10.5067/EVKPQZ4AFC4D. Date Accessed 11-27-2024.

[SPL4SMGP](https://nsidc.org/data/spl4smgp/versions/7)

### Nasa Shuttle Radar Topography Mission

NASA JPL (2013). NASA Shuttle Radar Topography Mission Global 1 arc second [Data set]. NASA EOSDIS Land Processes Distributed Active Archive Center. Accessed 2024-11-27 from https://doi.org/10.5067/MEaSUREs/SRTM/SRTMGL1.003

[SRTMl1v003](https://lpdaac.usgs.gov/products/srtmgl1v003/)

### World Data Center for Geomagnetism, Kyoto

World Data Center for Geomagnetism, Kyoto. World Data Center for Geomagnetism, Kyoto. Kyoto University. Accessed December 2, 2024.

[Dst Open Data](https://wdc.kugi.kyoto-u.ac.jp/dst_realtime/index.html)
