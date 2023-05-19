FROM python:3.8-slim-buster

WORKDIR /app
COPY . /app

# update and install packages
RUN apt-get update && \
    apt-get install -y default-jre default-jdk wget && \
    rm -rf /var/lib/apt/lists/*

# install spark
RUN wget https://dlcdn.apache.org/spark/spark-3.2.4/spark-3.2.4-bin-hadoop2.7.tgz && \
    tar -xvzf spark-3.2.4-bin-hadoop2.7.tgz && \
    mv spark-3.2.4-bin-hadoop2.7 /usr/local/spark && \
    rm -rf spark-3.2.4-bin-hadoop2.7.tgz

# set environment variables
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYSPARK_PYTHON=/usr/local/bin/python3.8
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.8

RUN apt-get update && apt-get install -y procps

RUN pip install --upgrade pip
# install pyspark and watchdog
RUN pip3 install pyspark watchdog

RUN pip install -r requirements.txt

