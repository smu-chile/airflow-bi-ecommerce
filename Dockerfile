FROM reigncl/airflow:2.2.4-python3.9-onbuild

USER root
RUN sudo rm -f /etc/apt/sources.list.d/pgdg.list
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys B7B3B788A8D3785C
RUN echo "deb https://apt-archive.postgresql.org/pub/repos/apt buster-pgdg main" > /etc/apt/sources.list.d/postgresql.list
RUN echo "deb http://archive.debian.org/debian buster main contrib non-free" > /etc/apt/sources.list && \
    echo "deb http://archive.debian.org/debian-security buster/updates main" >> /etc/apt/sources.list && \
    apt-get -o Acquire::Check-Valid-Until=false update

RUN apt-get install default-jre -y
RUN apt-get install python3-dev -y
ENV JAVA_HOME="/usr/lib/jvm/java-1.8-openjdk/"
ENV LD_LIBRARY_PATH="/usr/lib/jvm/java-8-openjdk/jre/lib/amd64/server/"
ADD requirements.txt /opt/airflow/

USER airflow
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir \
        "apache-airflow-providers-google>=4.0,<4.2" \
        "google-cloud-bigquery>=2.0.0,<3.0.0" \
        "google-cloud-storage>=1.30,<2.0.0" 