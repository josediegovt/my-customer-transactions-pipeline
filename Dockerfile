FROM apache/airflow:2.9.1-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    dbt-postgres==1.7.0 \
    apache-airflow-providers-postgres==5.10.0

USER root
COPY dbt /opt/airflow/dbt
RUN chown -R airflow:root /opt/airflow/dbt

USER airflow
RUN /home/airflow/.local/bin/dbt deps --project-dir /opt/airflow/dbt