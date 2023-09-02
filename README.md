# FRESCO Anvil Database Access and Analytics Notebook

## Notebook Access
https://www.jupyter.fresco-hpc.org/

## Overview

The purpose of this notebook is to allow the user to:
- Extract filtered data from the Anvil database.
- Conduct statistical analyses on the filtered data.
- Visualize the results of the analyses.

To achieve these goals, the notebook is structured into three main sections:

### 1. Data Filtering
Define your analysis scope by selecting a specific datetime window. Customize your dataset further with various filters.

### 2. Data Analysis Options
Explore a range of analysis options and select the ones that align with your requirements.

### 3. Data Analysis and Visualizations
Execute the chosen analysis on the filtered dataset and visualize the outcomes.

## Background Information
1. The FRESCO repository has been updated and now contains data from Anvil for the period July 2022 - May 2023.
2. The Anvil job accounting data now also includes anonymized job names to enable performance analysis of possibly repetitive jobs using the same underlying codes.

### 12/2022 Update:
The FRESCO repository has been updated with data from the Anvil capacity HPC system at Purdue University that entered operations in 2022. The Anvil data includes job accounting data from the Slurm scheduler and job-level resource usage timeseries data obtained from XDMoD for 480,728 jobs submitted between July 2022 and November 2022.

#### Anvil Architecture Summary
The Anvil cluster comprises 1000 CPU nodes with the 3rd generation AMD EPYC "Milan" processor and 100 Gbps HDR Infiniband interconnect. A separate sub-cluster comprises 16 GPU nodes with four NVIDIA A100 (40GB) GPUs each. Each CPU node has 256 GB DDR4-3200 memory, while the GPU nodes have 512 GB of memory. Further details of Anvil's architecture and job queues can be found in the [Anvil Documentation](#).

### How to cite this dataset:
Saurabh Bagchi, Todd Evans, Rakesh Kumar, Rajesh Kalyanam, Stephen Harrell, Carolyn Ellis, Carol Song "FRESCO: Job failure and performance data repository from Purdue University", March, 2018. At: [https://www.datadepot.rcac.purdue.edu/sbagchi/fresco](https://www.datadepot.rcac.purdue.edu/sbagchi/fresco)

### Contacts
- Dr. Rajesh Kalyanam: [rkalyana@purdue.edu](mailto:rkalyana@purdue.edu)
- Stephen Harrell: [sharrell@tacc.utexas.edu](mailto:sharrell@tacc.utexas.edu)
- Dr. Amiya Maji: [amaji@purdue.edu](mailto:amaji@purdue.edu)
- Dr. Carol Song: [cxsong@purdue.edu](mailto:cxsong@purdue.edu)
- Dr. Saurabh Bagchi: [sbagchi@purdue.edu](mailto:sbagchi@purdue.edu)
- Joshua McKerracher: [jmckerra@purdue.edu](mailto:jmckerra@purdue.edu)