FROM apache/airflow:2.8.1-python3.11

# Instala dependencias das DAGs (requests, pandas, sqlalchemy, psycopg2)
# e o dbt-postgres para que o Airflow possa invocar o dbt internamente.
# Ambos instalados como usuario airflow (padrao da imagem oficial).
COPY dags/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir \
        -r /tmp/requirements.txt \
        dbt-postgres==1.7.9
