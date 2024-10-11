
FROM apache/airflow:2.7.1-python3.11

USER root

RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow==2.7.1 \
    apache-airflow-providers-apache-spark \
    pyspark \
    scikit-learn \
    psycopg2-binary \
    pandas \
    xgboost \
    pyvis 
