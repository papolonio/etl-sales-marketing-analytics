FROM apache/airflow:2.8.1-python3.11

# Copia o arquivo de dependencias das DAGs e instala como usuario airflow
COPY dags/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
