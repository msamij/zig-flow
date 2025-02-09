# Zigflow: A pipeline for datasets processing and for analytics

## This project is a demonstration of building an end-to-end data processing pipeline using modern tools

### Dataset: [The Netflix prize](https://www.kaggle.com/datasets/netflix-inc/netflix-prize-data)

#### Project has five components

1. Data Ingestion
2. Processing of data sets using Apache Spark
3. Analysis
4. Storing datasets
5. Visualization

#### 1. Datasets are ingested from the file system using Spark

#### 2. Datasets are then processed which includes cleaning, sanitizing and transforming it into structured format

#### 3. When transformation is finised it then performs some basic analysis such as calculating averages, distributions and various descriptive stats

#### 4. Datasets are then stored on file system in the (output) folder

#### 5. Matplotlib is used for visualizing processed data and streamlit for building a web-dashboard

**_Note_**: I've tested sparkpipeline on an Intel 4th Gen core i5 (4) cores processor having 8gb ram, to run the pipeline smoothly system must have atleat 8gb of ram you can run it on much slower system however it will take much more time for pipeline to finish processing.

## Running the application

### First install python packages from requirements.txt file by creating a virtual environment in project root

```shell
# 1. Creates an environment.
python3 -m venv .venv

# 2. Activate the environment.
source .venv/bin/activate

# 3. Install packages.
pip install -r requirements.txt
```

### Download the dataset and extract it to project root/datasets

### 1. To run the standalone Spark Java pipeline

#### Requirements

1. Java 17 or above.
2. Apache spark 3.5.1 or above.
3. Python 3.x.x

#### Then run the following when in project root

```shell
python scripts/run.py
```

### 2. To run the pipeline with the scheduler

When in project root run the following python file.
**I have scheduled the pipeline to run every 20 minutes.**

```shell
python scheduler/scheduler.py
```

### 3. To manage all the dependecies of the sparkpipeline I've created a Dockerfile in project root to run the pipeline via docker

#### Run the following when project root to build the image and run the container

```shell
sudo docker build -t sparkpipeline:latest .

sudo docker run -it --name sparkpipeline-container sparkpipeline

# When inside the docker container run the following to run spark pipeline.
cd sparkpipeline

mvn clean & mvn install

spark-submit --class com.msamiaj.zigflow.Main /app/sparkpipeline/target/sparkpipeline-1.0-SNAPSHOT.jar
```

### 4. To use plotter

#### Run the following when project root to plot the datasets

```shell
python plot/plot.py
```

### 5. To use visualize datasets on web using streamlit

#### Run the following when project root to plot the datasets on the web

```shell
streamlit run web/visualization.py
```
