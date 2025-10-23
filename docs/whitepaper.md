# Gaia Whitepaper V2
## A Mixture-of-Experts Platform for Geospatial Data Analysis

---

## Current Status (V2 Update)

**Current Focus: Weather Forecasting Network**

As of Version 2, Gaia has evolved into a specialized weather forecasting network built on the Microsoft Aurora foundation model. The platform currently operates as a decentralized weather prediction system where:

- **Miners** generate detailed weather forecasts using the Aurora model, processing GFS (Global Forecast System) data to produce 40-step forecasts in Zarr format
- **Validators** coordinate the network, provide GFS data to miners, and score forecasts against ERA5 ground truth data using sophisticated meteorological evaluation metrics
- **Scoring System** implements a two-tier approach: Day1 quality control using 5 essential variables, followed by comprehensive ERA5 evaluation across 9 variables and 13 pressure levels

The current implementation represents a focused, production-ready weather forecasting system that demonstrates Gaia's core architectural principles while delivering immediate value to the meteorological community.

**Historical Context**: The original vision for Gaia as a comprehensive geospatial platform remains intact, with the weather forecasting system serving as the foundational proof-of-concept for the broader mixture-of-experts architecture described below.

---

## Introduction

The term "geospatial" encompasses a vast and diverse domain, characterized by geolocated and temporal data. This data is ubiquitous - freely accessible in many cases - and plays a vital role in almost every industry. Businesses, governments, and individuals alike rely on geospatial data to gain deeper insights into the world around them. From supply chain optimization to urban planning, geospatial analysis drives innovation and efficiency across a spectrum of applications.

However, the complexity of geospatial data presents significant challenges. Its diverse formats, sources, and inherent noisiness make it difficult to standardize and analyze. Additionally, modeling geospatial phenomena often requires computationally intensive solutions to account for the intricate interplay of physical systems. For instance, turbulence — a still-unsolved problem in physics — illustrates the mathematical and computational difficulties of modeling such systems.

At the intersection of geospatial data analysis and machine learning, most existing models are purpose-built for narrow tasks, trained on specific datasets, and optimized for limited objectives. The development of a comprehensive "foundational model" for geospatial analysis remains elusive, though there have been notable efforts, such as large-scale language models trained on OpenStreetMap data or spatial understanding systems. [Citations, There are a few]

This white paper introduces Gaia, an innovative platform designed to bridge the gap between small expert models and an all-encompassing foundational geospatial model. Gaia leverages the principles of open-source collaboration and distributed computing to create a scalable, modular system. By fostering community-driven contributions and utilizing a decentralized architecture, Gaia aims to evolve continuously, growing its capabilities and utility over time.

The vision for Gaia is ambitious yet grounded in practicality. It recognizes the need for distributed contribution at scale and aims to establish a foundational framework to catalyze progress. In doing so, Gaia sets the stage for a new era of geospatial intelligence, empowering industries, governments, and individuals with the tools to unlock the full potential of geospatial data.

## Why Build It on Bittensor?

In the rapidly evolving domain of machine learning, reinventing the wheel is both impractical and inefficient. As members of the Bittensor community, we collectively recognize Bittensor as a groundbreaking platform that successfully incentivizes open-source, distributed computation and research. Its proven track record and unique infrastructure make it an ideal foundation for Gaia.

Bittensor's decentralized network provides the essential incentive structure and computational resources to power Gaia's ambitious objectives. By leveraging this existing framework, Gaia can avoid duplicating efforts and focus on its core mission: advancing geospatial data analysis. Among the many available options, Bittensor stands out as the clear choice—no other platform comes close to matching its capabilities in fostering open collaboration and scalability.

## The Architecture

Gaia is conceptualized as a mixture-of-experts (MoE) platform, a cutting-edge approach in machine learning that emphasizes modularity and specialization. Traditionally, MoE models feature tightly coupled components, where a gating layer dynamically selects expert layers to solve specific tasks. This integrated design allows for the scaling of model parameters—and consequently, accuracy—while maintaining relatively constant inference costs. Many leading commercial large language models (LLMs) now incorporate MoE principles to enhance performance.

However, decentralizing large-scale applications across numerous worker nodes introduces significant inefficiencies, particularly in the domains of training and inference. These challenges stem from the inherent complexity of distributed systems, which lack the streamlined efficiency of centralized architectures. While breakthroughs in this area remain a work in progress, Gaia takes a pragmatic approach by reimagining the MoE architecture.

Instead of a tightly integrated system, Gaia employs a decoupled architecture that separates the gating network layer from the expert models. This trade-off sacrifices some flexibility and efficiency but significantly simplifies the challenges of training massive models across distributed nodes. By decoupling components, Gaia achieves the following advantages:

1. **Selective Specialization**: Gaia can target specific tasks and employ tailored expert models, avoiding the need for exhaustive end-to-end retraining.
2. **Iterative Improvement**: Expert models can be independently improved, updated, or replaced without requiring adjustments to the overarching architecture.
3. **Enhanced Modularity**: New expert models or combinations can be added over time, extending the platform's capabilities without major disruptions.

While this approach may incur increased latency and reduced efficiency compared to traditional MoE systems, it aligns with Gaia's vision of a scalable, distributed platform. The emphasis is on producing actionable, high-quality outputs rather than achieving centralized system optimization. Gaia's decoupled architecture allows for continuous evolution, ensuring the platform remains adaptable and robust as it expands to tackle more complex geospatial tasks.

## Core Components

The Gaia platform relies on a modular and scalable framework, centered around two key components: the Orchestrator and the Task Class. Together, these components enable efficient coordination, execution, and modularity, ensuring the platform can address a wide range of geospatial challenges.

### Orchestrator

The Orchestrator serves as the central hub for managing data and coordinating computational nodes. Its primary role is to streamline operations by reducing inter-validator communication and optimizing task allocation. Validators can then focus their resources on critical functions such as scoring miner outputs and preprocessing data.

Initially, the Orchestrator is designed to:

1. **Coordinate Tasks**: Assign tasks efficiently to the appropriate nodes.
2. **Streamline Communication**: Minimize overhead by reducing unnecessary node-to-node interactions.
3. **Interface for Organic Tasks**: Provide a mechanism for submitting and retrieving outputs for tasks requested by real users or external systems.

While the Orchestrator plays a pivotal role in Gaia's early development, the long-term vision is to phase out its centralized functionality. Over time, its responsibilities will be distributed among validators, ensuring a fully decentralized system.

### Task Class

The Task Class defines the fundamental unit of work within Gaia. Each task is characterized by specific inputs, outputs, and execution processes, making it a highly modular component. Tasks can range from simple data gathering and preprocessing to advanced inference using specialized expert models.

**Key Features of Tasks:**

- **Inference-Driven**: Tasks often involve inference steps using predefined expert models.
- **Modular Design**: Tasks can be composed of one or more subtasks, arranged in a graph-like structure to handle dependencies efficiently.
- **Scoring Mechanisms**: Each task includes well-defined scoring parameters to ensure output quality and reliability.

**Synthetic and Organic Tasks:**

- **Synthetic Tasks**: Designed for miner evaluation, training, or improvement, these tasks are integral to the system's self-optimization.
- **Organic Tasks**: Real-world tasks requested by users, providing practical applications of Gaia's capabilities.

### Illustrative Example: Agricultural Analysis Task

To demonstrate Gaia's task structure, consider an Agricultural Analysis Task aimed at providing actionable insights for farmers planning their crop seasons. This task combines various data sources, including ground sensors, satellite imagery, and climate models, to deliver a detailed analysis and forecast for a specific region.

**Goal**: Provide an actionable, user-friendly report detailing agricultural conditions and forecasts for a defined area.

**Subtasks:**

1. **Market Analysis:**
   - Scraping and summarizing market data.

2. **Crop Yield Forecasting:**
   - Soil Carbon Content Modeling: Supported by a cloud cover removal tool.
   - Predictive Soil Moisture Modeling: Utilizing satellite and ground sensor data.

3. **Climate Modeling:**
   - Short-term runs from Graphcast models.
   - Integrations with NWS climate data.

**Dependency Structure:**

The subtasks are organized in a tree-like hierarchy:

- **Leaf Nodes**: Solve fundamental subtasks such as data preprocessing or individual model inferences (e.g., satellite soil moisture modeling).
- **Intermediate Nodes**: Combine outputs from leaf nodes into higher-order insights.
- **Trunk Node**: Aggregates all results into the final output delivered to the user.

This hierarchical design ensures that dependencies are addressed systematically, maintaining data integrity and compatibility across all layers.

### Ensuring Data Integrity and Scalability

As Gaia tackles increasingly complex tasks, the importance of strict input/output definitions cannot be overstated. Each expert model must adhere to rigorous standards to ensure seamless integration. While adaptive and dynamic models represent a future goal, the current roadmap emphasizes robust, carefully defined task hierarchies to build a solid foundation.

## Starting Tasks

*Note: The following tasks represent Gaia's original vision and historical development. The current V2 implementation focuses exclusively on Weather Forecasting as described in the Current Status section above.*

### Dst Index Prediction Task

Space weather significantly impacts various systems on Earth, influencing satellite operations, GPS accuracy, and power grid stability. Geomagnetic storms, triggered by solar activity, induce currents in Earth's magnetic field, disrupting technological systems and posing risks to critical infrastructure. The Disturbance Storm Time (Dst) index is a key measure of geomagnetic activity, quantifying fluctuations in Earth's magnetic field caused by these storms. Accurate, real-time Dst index predictions are essential for anticipating and mitigating the impacts of space weather events.

In this task, miners are tasked with predicting the Dst index one hour ahead. To achieve this:

1. **Miners:**
   - Receive a DataFrame containing recent Dst data.
   - Build or apply base models to generate predictions with a minimum confidence interval of 0.70.
   - Use historical data from the current month to ensure model relevance.
   - Optionally, enhance their models by gathering additional data or adjusting parameters.

2. **Validators:**
   - Access real-time Dst values, which update hourly.
   - Use these values as ground truth to assess the accuracy of miner predictions.

The ground truth data for this task is sourced from the World Data Center for Geomagnetism in Kyoto, ensuring reliability and consistency for validation purposes. This dataset supports Gaia's geomagnetic tasks, allowing for robust evaluation of miner outputs.

This task is informed by key research in space weather prediction, which highlights the importance of machine learning and distributed computing in advancing forecasting capabilities. Foundational studies include:

- https://agupubs.onlinelibrary.wiley.com/doi/10.1029/2022SW003049
- https://arxiv.org/abs/2209.12571

### Soil Moisture Prediction Task

Soil moisture has become an increasingly important measurement in understanding human driven climate change, disaster risk assessment, and agricultural modeling. The SMAP (Soil Moisture Active Passive) mission gives global remote measurements of surface and subsurface soil moisture, by predicting these values, more informed decisions can be made to warn of potential flood risk, monitor future drought conditions, and build predictive models that understand how regional ecosystems will change.

In this task, Miners will predict surface and rootzone soil moisture six hours in advance:

1. **Miners:**
   - Receive a combined file with all bands for each region
     - B4, B8, NDVI from sentinel2
     - 15 weather forecast bands from the ECMWF IFS model
     - SRTM elevation data
   - Build or modify a model to process the input bands, and return 2d arrays for surface and rootzone moisture predictions.
   - Optionally, supplement their models by incorporating data such as SAR, historical weather data, soil type, and climate classification

2. **Validators:**
   - Select 1 degree sized square regions using a global H3 grid system
     - Filter out Urban areas and bodies of water
   - Query ECMWF, EarthData to collect and combine daily data for needed inference
     - Data is pulled based on randomly selected region
     - Data is processed, cropped, and aligned
     - Evaporation bands are computed and appended to the data
   - 3 Days later, Ground truth SMAP data is retrieved and Miner predictions will be scored.

**Limitations:**

Geospatial data often has a latency, and temporal resolution limit. This can prove difficult in ensuring miners get relevant feedback and scores in a short time span. Furthermore, certain measurables do not have large changes on a daily timescale. SMAPL4 has a typical data latency of 3 days, with a temporal resolution of 3hrs. With this context, miners will receive weather forecasts from ECMWF, and predict soil moisture six hours into the future, Due to latency they will not be scored for that region until three days later.

**Goal**

We aim to achieve accurate, global, near-real-time soil moisture forecasts that meet or exceed research benchmarks (0.02 - 0.05 RMSE). With accurate predictions, Latency gaps in the SMAP products will be filled enabling continuous analysis of short term climate processes dependent on soil moisture.

The ground truth data for this task is the SMAP L4 Global 3-hourly 9 km EASE-Grid Surface and Root Zone Soil Moisture Geophysical Data V007 data product, sourced from Nasa EarthData.

**References Informing the task:**

- https://arxiv.org/abs/1707.06611
- https://arxiv.org/pdf/2206.09649
- https://www.mdpi.com/2072-4292/15/2/366
- https://journals.ametsoc.org/view/journals/hydr/21/3/jhm-d-19-0169.1.xml
- https://nsidc.org/data/spl4smgp/versions/3
- https://hls.gsfc.nasa.gov/
- https://www.ecmwf.int/en/forecasts/datasets/open-data
- https://www.earthdata.nasa.gov/data/instruments/srtm
- https://www.fao.org/4/x0490e/x0490e00.htm

## Bounty/Incentive System

Given the ambitious scope of Gaia, a centralized approach to task creation and definition is neither feasible nor desirable. Instead, we envision a decentralized, community-driven system where contributors are incentivized to expand Gaia's functionality. This will be achieved through a bounty system funded by subnet owner emissions, creative adjustments to miner/validator rewards, or a combination of the two.

1. **Bounty Model:**
   - Tasks are defined as bounties, encouraging community contributions to create and refine base models, define inputs/outputs, and develop processing steps.
   - Contributors are rewarded proportionally based on the utility of their submissions.

2. **Royalty Mechanism:**
   - Contributors may receive ongoing rewards based on how frequently their task is called or its utility as a subtask in broader workflows.
   - This incentivizes high-quality, reusable contributions.

3. **Governance Integration:**
   - Leveraging dTAO and smart contracts, contributors can submit task definitions as pull requests to the main repository.
   - A voting mechanism, similar to Bittensor's, allows subnet token holders to approve or modify tasks.

## Roles

**Miners'** responsibilities align with traditional roles in decentralized systems, focusing on:

- Improving model outputs and task efficiency.
- Registering for tasks that align with their expertise or interests.
- Contributing to specific research areas.

**Validators** ensure the integrity and reliability of Gaia by:

- Providing miners with necessary datasets through API integrations.
- Storing and scoring miner outputs using real-world ground truth data as it becomes available.

## Demand-Based Incentive Pools

As the number of potential tasks grows, maintaining equitable support for all tasks becomes a challenge. Popular or competitive tasks may attract a disproportionate share of resources, leaving less glamorous but critical tasks under-supported. To address this, Gaia proposes a market-based system of incentive pools:

1. **Dynamic Incentives:**
   - Tasks experiencing low miner participation will see their share of miner rewards increase until equilibrium is restored.
   - Conversely, tasks with oversupply will have their reward share adjusted downward.

2. **Task Complexity Adjustment:**
   - Tasks will vary in difficulty and computational demand, requiring a nuanced approach to reward distribution.
   - Incentive adjustments will account for these variations to ensure fairness and sustainability.

This approach introduces complexity but ensures all tasks are adequately supported. As a conceptual framework, it will evolve with community input and real-world feedback.

## References

1. "Towards Crowdsourced Training of Large Neural Networks using Decentralized Mixture-of-Experts" https://arxiv.org/pdf/2002.04013

