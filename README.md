# FRESCO Job Failure and Performance Data Repository

## Notebook Access
https://www.jupyter.fresco-hpc.org/

## Version 3.0

### 06/2023 Update:
1. The FRESCO repository has been updated and now contains data from Anvil for the period July 2022 - May 2023.
2. The Anvil job accounting data now also includes anonymized job names to enable performance analysis of possibly repetitive jobs using the same underlying codes.

### 12/2022 Update:
The FRESCO repository has been updated with data from the Anvil capacity HPC system at Purdue University that entered operations in 2022. The Anvil data includes job accounting data from the Slurm scheduler and job-level resource usage timeseries data obtained from XDMoD for 480,728 jobs submitted between July 2022 and November 2022.

#### Anvil Architecture Summary
The Anvil cluster comprises 1000 CPU nodes with the 3rd generation AMD EPYC "Milan" processor and 100 Gbps HDR Infiniband interconnect. A separate sub-cluster comprises 16 GPU nodes with four NVIDIA A100 (40GB) GPUs each. Each CPU node has 256 GB DDR4-3200 memory, while the GPU nodes have 512 GB of memory. Further details of Anvil's architecture and job queues can be found in the [Anvil Documentation](#).

## Version 2.0
This version of the FRESCO repository comprises event and performance data for scientific code execution jobs submitted to Purdue University's Conte cluster between March 2015 and June 2017, and, to the University of Texas at Austin's Stampede 1 cluster between 2013 and 2016.

The Conte cluster comprises 580 nodes totaling 9280 cores with 40 Gbps Infiniband interconnects. Each node in the cluster has 64 GB of RAM and includes two additional 60-core Xeon Phi accelerators. The repository contains data for 10.8M jobs run on Conte over the 28-month period between March 2015 and June 2017.

The Stampede 1 cluster at the time of decommissioning consisted of 6400 nodes with a total of 522,080 processing cores. The repository contains data for 8.7M jobs during the 2013 - 2016 period.

### Accessing the repository
You can browse and download individual datasets from this repository by visiting the links under the Data Sets menus, or use Globus to download the entire data repository.
- Instructions on using Globus can be found in the [Documentation](#).

### How to cite this dataset:
Saurabh Bagchi, Todd Evans, Rakesh Kumar, Rajesh Kalyanam, Stephen Harrell, Carolyn Ellis, Carol Song "FRESCO: Job failure and performance data repository from Purdue University", March, 2018. At: [https://www.datadepot.rcac.purdue.edu/sbagchi/fresco](https://www.datadepot.rcac.purdue.edu/sbagchi/fresco)

### Contacts
- Dr. Rajesh Kalyanam: [rkalyana@purdue.edu](mailto:rkalyana@purdue.edu)
- Stephen Harrell: [sharrell@tacc.utexas.edu](mailto:sharrell@tacc.utexas.edu)
- Dr. Amiya Maji: [amaji@purdue.edu](mailto:amaji@purdue.edu)
- Dr. Carol Song: [cxsong@purdue.edu](mailto:cxsong@purdue.edu)
- Dr. Saurabh Bagchi: [sbagchi@purdue.edu](mailto:sbagchi@purdue.edu)
- Joshua McKerracher [jmckerra@purdue.edu](mailto:jmckerra@purdue.edu)