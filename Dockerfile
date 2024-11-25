FROM ubuntu:20.04

# Prevent interactive prompts during package installations
ENV DEBIAN_FRONTEND=noninteractive

# Update and install required dependencies
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    wget \
    curl \
    git \
    bash \
    build-essential \
    python3 \
    python3-pip \
    && apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Spark and Maven
ARG SPARK_VERSION=3.5.3
ARG MAVEN_VERSION=3.9.5

RUN wget https://dlcdn.apache.org/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz -O /tmp/spark.tgz \
    && tar -xzvf /tmp/spark.tgz -C /opt \
    && ln -s /opt/spark-$SPARK_VERSION-bin-hadoop3 /opt/spark \
    && rm /tmp/spark.tgz \
    && wget https://downloads.apache.org/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz -O /tmp/maven.tar.gz \
    && tar -xzf /tmp/maven.tar.gz -C /opt \
    && ln -s /opt/apache-maven-$MAVEN_VERSION /opt/maven \
    && ln -s /opt/maven/bin/mvn /usr/bin/mvn \
    && rm /tmp/maven.tar.gz

# Set environment variables for Spark and Maven
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

# Set Maven home
ENV MAVEN_HOME=/opt/maven

# Set working directory
WORKDIR /app

# Copy project files
COPY /sparkpipeline /app/sparkpipeline
COPY /datasets /app/datasets

RUN mkdir output
RUN mkdir checkpoint

# Expose Spark's default web UI port
EXPOSE 4040

# Set default command to run Bash
CMD ["bash"]
