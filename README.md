# Zigflow: A pipeline for datasets processing and for analytics.

### This project is a demonstration of building an end-to-end data processing pipeline using modern tools.

### Dataset: [The Netflix prize](https://www.kaggle.com/datasets/netflix-inc/netflix-prize-data)

#### Project has five components:

1. Data Ingestion
2. Efficiently processing of data sets using Apache Spark
3. Analysis
4. Storing datasets
5. Visualization

#### 1. Datasets are ingested from the file system using Spark.

#### 2. Datasets are then processed which includes cleaning, sanitizing and transforming it into structured format.

#### 3. When transformation is finised it then performs some basic analysis such as calculating averages, distributions and various descriptive stats.

#### 4. Datasets are then stored on file system in the (output) folder.

#### 5. Matplotlib is used for visualizing processed data and streamlit for building a web-dashboard.

**_Note_**: I've tested sparkpipeline on an Intel 4th Gen core i5 (4) cores processor having 8gb ram, to run the pipeline smoothly system must have atleat 8gb of ram you can run it on much slower system however it will take much more time for pipeline to finish processing.

## Running the application:

### Download the dataset and extract it to project root/datasets

#### 1. To run the standalone Spark Java pipeline:

#### Requirements:

1. Java 17 or above.
2. Apache spark 3.5.1 or above.
3. Python 3.x.x

When in project root run the following python script.

```shell
python scripts/run.py
```
