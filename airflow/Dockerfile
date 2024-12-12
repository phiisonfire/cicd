FROM apache/airflow:2.10.3

USER airflow

WORKDIR /opt/airflow

COPY requirements.txt .
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    rm -rf /var/lib/apt/lists/*

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

    
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/cicd/dags
ENV AIRFLOW__CORE__PLUGINS_FOLDER=/opt/airflow/cicd/plugins
ENV AIRFLOW__LOGGING__BASE_LOG_FOLDER=/opt/airflow/cicd/logs

# COPY connections.yaml .
# RUN airflow connections import ./connections.yaml