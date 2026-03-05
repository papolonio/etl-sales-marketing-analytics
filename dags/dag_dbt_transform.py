"""
DAG: dbt_transform
==================
DAG consumidora disparada por Data-Aware Scheduling (Airflow Datasets).

Roda automaticamente assim que AMBAS as DAGs produtoras concluirem
a carga na camada Raw:
    - meta_ads_ingestion  emite: dataset_meta  (urn:raw:meta_ads)
    - kommo_ingestion     emite: dataset_kommo (urn:raw:kommo_leads)

O Airflow so dispara esta DAG quando os dois datasets forem atualizados
desde o ultimo run — garantindo que o dbt sempre processa dados completos.

Task:
    dbt_run_marts — executa `dbt run` na pasta /opt/airflow/dbt_analytics,
                    que e o volume montado com os modelos Silver e Gold.
"""

from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

# URIs identicos aos das DAGs produtoras — Airflow correlaciona por URI
dataset_meta  = Dataset("urn:raw:meta_ads")
dataset_kommo = Dataset("urn:raw:kommo_leads")

_DBT_DIR = "/opt/airflow/dbt_analytics"


@dag(
    dag_id="dbt_transform",
    description="Roda dbt run (Silver + Gold) apos ingestao Meta e Kommo (Data-Aware)",
    schedule=[dataset_meta, dataset_kommo],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "gold", "transform", "sprint-5"],
    default_args={
        "owner":   "data-engineering",
        "retries": 1,
    },
)
def dbt_transform():

    BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {_DBT_DIR} && dbt run --profiles-dir .",
    )


dbt_transform()
