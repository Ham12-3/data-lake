FROM apache/airflow:2.8.1-python3.11

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy project modules so Airflow can import them
COPY --chown=airflow:root ingestion/   /opt/airflow/ingestion/
COPY --chown=airflow:root transformation/ /opt/airflow/transformation/
COPY --chown=airflow:root quality/     /opt/airflow/quality/
COPY --chown=airflow:root lineage/     /opt/airflow/lineage/
COPY --chown=airflow:root versioning/  /opt/airflow/versioning/
COPY --chown=airflow:root dags/        /opt/airflow/dags/
