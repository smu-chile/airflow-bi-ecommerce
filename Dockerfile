FROM apache/airflow:3.1.7

USER root

# Install dependencies required for the project (e.g. Java for jaydebeapi)
RUN apt-get update && \
    apt-get install -y default-jdk \
    default-jre \
    python3-dev \
    build-essential \
    libpq-dev \
    libgdal-dev \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME="/usr/lib/jvm/default-java/"
# Note: LD_LIBRARY_PATH might need adjustment based on the Debian version in the base image.
ENV LD_LIBRARY_PATH="/usr/lib/jvm/default-java/jre/lib/amd64/server/"

USER airflow

ADD requirements.txt /opt/airflow/

RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r /opt/airflow/requirements.txt \
    && pip install --no-cache-dir \
        "apache-airflow-providers-google" \
        "google-cloud-bigquery" \
        "google-cloud-storage"