FROM reigncl/airflow:2.1.3-python3.8-onbuild

USER root
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 467B942D3A79BD29 \
    && apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 8C718D3B5072E1F5
RUN apt-get clean && apt-get update
RUN apt-get install default-jre -y
RUN apt-get install python3-dev -y
ENV JAVA_HOME="/usr/lib/jvm/java-1.8-openjdk/"
ENV LD_LIBRARY_PATH="/usr/lib/jvm/java-8-openjdk/jre/lib/amd64/server/"

USER airflow
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt
