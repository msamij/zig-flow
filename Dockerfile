FROM openjdk:17-jdk-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    wget \
    curl \
    bash \
    build-essential \
    && apt-get clean

ARG SPARK_VERSION=3.5.4
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

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

ENV MAVEN_HOME=/opt/maven

WORKDIR /app

COPY /sparkpipeline /app/sparkpipeline
COPY /datasets /app/datasets

RUN mkdir output
RUN mkdir checkpoint

EXPOSE 4040

CMD ["bash"]
