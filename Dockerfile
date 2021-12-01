FROM reigncl/airflow:2.1.3-python3.8-onbuild

USER airflow

RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt
