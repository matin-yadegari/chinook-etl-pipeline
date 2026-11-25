FROM apache/airflow:2.10.2-python3.11

USER airflow

RUN pip install --no-cache-dir \
        "apache-airflow==2.10.2" \
        "pymssql==2.3.1" \
        "clickhouse-connect==0.8.3" \
        "prometheus-client>=0.20,<1.0"


ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"
