# Remote Client-Based Recruitment ETL Pipeline with Hadoop, Hive, PySpark, and Selenium

This project provides a ETL solution to efficiently extract, transform, and load recruitment data from Glints into a medallion architecture, utilizing Apache Hadoop and Apache Hive. It integrates a powerful toolset, including Hadoop, Hive, Spark, Selenium, and Power BI, to streamline data processing and visualization.
## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [System Setup](#system-setup)
- [Results](#results)

## Overview

This ETL pipeline carries out the following key steps:

1. **Data Extraction**  
   Scrapes recruitment data from Glints using Selenium for efficient and automated collection.

2. **Raw Data Storage**  
   Saves the extracted raw data directly into Hadoop HDFS via the [HDFS Driver](./src/hdfsDriver/Driver.py), preserving the original dataset.

3. **Data Transformation**  
   Cleans and transforms the data using Pandas, storing the processed data in the "silver" layer in HDFS, optimizing it for analysis.

4. **Data Aggregation**  
   Aggregates and refines the transformed data based on specific user requirements, then loads it into Apache Hive using PySpark in remote mode for seamless integration.

5. **Data Visualization**  
   Connects to Apache Hive from Power BI to visualize the aggregated data, enabling interactive and insightful data analysis.


## Architecture
![Pipeline Architecture](assets/architecture.png)

## Prerequisites
1. VMware and Linux ISO image
2. Hadoop, Hive, Spark installed on a remote machine
3. Spark setup on the client
4. Python version 3.9 or higher

Alternatively, set up the environment using Docker [here](https://github.com/myamafuj/hadoop-hive-spark-docker.git).

## System Setup
> **Note**: This is a minimum setup; for stronger machines, higher specs are recommended.

### Hadoop Main Node
- OS: `ubuntu-24.04-desktop-amd64.iso`
- Memory: 4 GB
- Processor: 4 cores
- Storage: 100 GB

### Worker Nodes (1 and 2)
- OS: `ubuntu-24.04-live-server-amd64.iso`
- Memory: 4 GB
- Processor: 2 cores
- Storage: 40 GB

## Results
### Job and Salary Analysis by Category
![Job and Salary by Category](assets/BI-1.png)

### Job and Salary Analysis by Requirement
![Job and Salary by Requirement](assets/BI-2.png)

### Insights on Work Type Preferences
![Work Type Insights](assets/BI-3.png)
