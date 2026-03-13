"""
Extrai dados do Mock KommoCRM API e persiste na camada Raw do Data Warehouse.
"""

import os
import logging
from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

# Dataset produzido por esta DAG — consumido por dag_dbt_transform
dataset_kommo = Dataset("urn:raw:kommo_leads")


def _build_clients():
    """
    Instancia KommoAPIClient e PostgresClient com vars de ambiente.
    Chamado dentro de cada @task para garantir execucao no worker (nao no parser).
    """
    from utils.kommo_client import KommoAPIClient
    from utils.db_client import PostgresClient

    client = KommoAPIClient(
        base_url= os.environ["KOMMO_API_URL"],
        token=    os.environ["KOMMO_TOKEN"],
    )
    db = PostgresClient(
        host=     os.getenv("POSTGRES_HOST", "postgres"),
        port=     os.getenv("POSTGRES_PORT", "5432"),
        db=       os.getenv("POSTGRES_DB", "data_warehouse"),
        user=     os.getenv("POSTGRES_USER", "admin"),
        password= os.getenv("POSTGRES_PASSWORD", "admin"),
    )
    return client, db


@dag(
    dag_id="kommo_ingestion",
    description="Extrai leads, contacts, events, pipelines, users e custom_fields do Kommo (mock) -> raw layer",
    schedule_interval="0 7 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["kommo", "raw", "ingestion", "sprint-2"],
    default_args={
        "owner":   "data-engineering",
        "retries": 2,
    },
)
def kommo_ingestion():

    # Grupo 1: dados transacionais

    @task()
    def extract_leads() -> str:
        """
        Extrai todos os leads com motivo de perda.
        raw.kommo_leads — chave de JOIN com Meta via custom_fields_values (UTM_AD_ID).
        """
        client, db = _build_clients()
        n = db.load(client.get_leads(), table="kommo_leads")
        db.dispose()
        log.info("[DAG kommo] extract_leads: %d registros", n)
        return f"{n} registros em raw.kommo_leads"

    @task()
    def extract_contacts() -> str:
        """Extrai contatos. raw.kommo_contacts"""
        client, db = _build_clients()
        n = db.load(client.get_contacts(), table="kommo_contacts")
        db.dispose()
        log.info("[DAG kommo] extract_contacts: %d registros", n)
        return f"{n} registros em raw.kommo_contacts"

    @task()
    def extract_events() -> str:
        """
        Extrai eventos de mudanca de status (lead_status_changed).
        raw.kommo_events — base para calculo de funil e tempo de conversao.
        """
        client, db = _build_clients()
        n = db.load(client.get_events(), table="kommo_events")
        db.dispose()
        log.info("[DAG kommo] extract_events: %d registros", n)
        return f"{n} registros em raw.kommo_events"

    # Grupo 2: dados de referencia (apos grupo 1)

    @task(outlets=[dataset_kommo])
    def extract_pipelines() -> str:
        """
        Extrai pipelines e explode os status em linhas planas.
        raw.kommo_pipelines_status — tabela de dimensao para o modelo Gold.

        Logica de flatten (replicada do script original):
            para cada pipeline -> para cada status -> uma linha
        """
        client, db = _build_clients()

        raw_pipelines = client.get_pipelines()
        flat_rows = []
        for pipeline in raw_pipelines:
            statuses = pipeline.get("_embedded", {}).get("statuses", [])
            for status in statuses:
                flat_rows.append({
                    "pipeline_id":             pipeline.get("id"),
                    "pipeline_name":           pipeline.get("name"),
                    "pipeline_sort":           pipeline.get("sort"),
                    "pipeline_is_main":        pipeline.get("is_main"),
                    "pipeline_is_unsorted_on": pipeline.get("is_unsorted_on"),
                    "pipeline_is_archive":     pipeline.get("is_archive"),
                    "pipeline_account_id":     pipeline.get("account_id"),
                    "status_id":               status.get("id"),
                    "status_name":             status.get("name"),
                    "status_sort":             status.get("sort"),
                    "status_is_editable":      status.get("editable"),
                    "status_color":            status.get("color"),
                    "status_type":             status.get("type"),
                    "status_account_id":       status.get("account_id"),
                })

        n = db.load(flat_rows, table="kommo_pipelines_status")
        db.dispose()
        log.info("[DAG kommo] extract_pipelines: %d registros", n)
        return f"{n} registros em raw.kommo_pipelines_status"

    @task(outlets=[dataset_kommo])
    def extract_users() -> str:
        """Extrai usuarios responsaveis. raw.kommo_users"""
        client, db = _build_clients()
        n = db.load(client.get_users(), table="kommo_users")
        db.dispose()
        log.info("[DAG kommo] extract_users: %d registros", n)
        return f"{n} registros em raw.kommo_users"

    @task(outlets=[dataset_kommo])
    def extract_custom_fields() -> str:
        """
        Extrai definicao dos campos customizados (inclui os campos UTM).
        raw.kommo_custom_fields — dicionario de metadados dos campos.
        """
        client, db = _build_clients()
        n = db.load(client.get_custom_fields(), table="kommo_custom_fields")
        db.dispose()
        log.info("[DAG kommo] extract_custom_fields: %d registros", n)
        return f"{n} registros em raw.kommo_custom_fields"

    # ── Orquestracao ──────────────────────────────────────────────────────────
    leads_t    = extract_leads()
    contacts_t = extract_contacts()
    events_t   = extract_events()

    pipelines_t = extract_pipelines()
    users_t     = extract_users()
    cf_t        = extract_custom_fields()

    # Grupo 1 (paralelo) deve finalizar antes de Grupo 2 (paralelo) comecar
    [leads_t, contacts_t, events_t] >> pipelines_t
    [leads_t, contacts_t, events_t] >> users_t
    [leads_t, contacts_t, events_t] >> cf_t


kommo_ingestion()
