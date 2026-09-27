# Versión estable fijada. Subirla es una decisión explícita: airflow-init migra la base de datos al arrancar.
ARG AIRFLOW_VERSION=3.3.1
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12
ARG AIRFLOW_VERSION

COPY requirements.txt /requirements.txt

# Fijar apache-airflow en la misma instalación impide que pip lo actualice o degrade al resolver dependencias.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
