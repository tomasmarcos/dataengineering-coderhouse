FROM apache/airflow:2.7.3
# Copy my dag use this custom parameter 
COPY --chown=airflow:root project_dag.py /opt/airflow/dags
COPY --chown=airflow:root scripts /opt/airflow/dags
# Copy and install python libraries / requirements
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
# Copy .env file and set environment variables. 
COPY .env /
RUN set -a 
RUN source /.env
RUN set +a