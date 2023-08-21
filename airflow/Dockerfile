# Use the official Airflow image as the base image
FROM apache/airflow:2.6.3

# Install Docker Compose
USER root
RUN apt-get update && apt-get install -y curl
RUN curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
RUN chmod +x /usr/local/bin/docker-compose

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-docker \
    apache-airflow-providers-smtp \
    apache-airflow-providers-airbyte

ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://<username>:<password>@<hostname>:<port>/<database>
ENV AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=120

