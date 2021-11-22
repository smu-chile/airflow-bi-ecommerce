FROM apache/airflow:2.2.1-python3.8

COPY requirements.txt .
COPY ./dags /opt/airflow/dags
COPY ./plugins /opt/airflow/plugins

RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt
