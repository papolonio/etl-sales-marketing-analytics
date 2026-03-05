"""
DAG: meta_ads_ingestion
=======================
Extrai dados do Mock Meta Ads API e persiste na camada Raw do Data Warehouse.

Fluxo de tasks:
    extract_insights >> extract_campaigns

Tabelas geradas:
    raw.meta_insights   — metricas diarias por Ad (ultimos 15 dias)
    raw.meta_campaigns  — lista de campanhas da conta

Schedule: diario as 06:00 UTC
"""

import os
import logging
from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

# Dataset produzido por esta DAG — consumido por dag_dbt_transform
dataset_meta = Dataset("urn:raw:meta_ads")


def _build_meta_client():
    """Instancia MetaAdsClient com vars de ambiente (executa no worker)."""
    from utils.meta_client import MetaAdsClient
    return MetaAdsClient(
        base_url=     os.environ["META_API_URL"],
        access_token= os.environ["META_ACCESS_TOKEN"],
        account_id=   os.environ["META_ACCOUNT_ID"],
    )


def _build_db_client():
    """Instancia PostgresClient com vars de ambiente (executa no worker)."""
    from utils.db_client import PostgresClient
    return PostgresClient(
        host=     os.getenv("POSTGRES_HOST", "postgres"),
        port=     os.getenv("POSTGRES_PORT", "5432"),
        db=       os.getenv("POSTGRES_DB", "data_warehouse"),
        user=     os.getenv("POSTGRES_USER", "admin"),
        password= os.getenv("POSTGRES_PASSWORD", "admin"),
    )


@dag(
    dag_id="meta_ads_ingestion",
    description="Extrai insights e campanhas do Meta Ads (mock) -> raw layer",
    schedule_interval="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["meta", "raw", "ingestion", "sprint-2"],
    default_args={
        "owner":   "data-engineering",
        "retries": 2,
    },
)
def meta_ads_ingestion():

    @task()
    def extract_insights() -> str:
        """
        Extrai metricas no nivel de Ad (ultimos 15 dias) e carrega em raw.meta_insights.

        Campos: ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name,
                account_id, account_name, spend, clicks, inline_link_clicks,
                impressions, objective, actions (JSON), date_start
        """
        client = _build_meta_client()
        db     = _build_db_client()

        records = client.get_insights(days=15)
        n = db.load(records, table="meta_insights")
        db.dispose()

        log.info("[DAG meta_ads] extract_insights concluido: %d registros", n)
        return f"{n} registros em raw.meta_insights"

    @task(outlets=[dataset_meta])
    def extract_campaigns() -> str:
        """
        Extrai campanhas da conta e carrega em raw.meta_campaigns.

        Campos: id, name, status, start_time
        """
        client = _build_meta_client()
        db     = _build_db_client()

        records = client.get_campaigns()
        n = db.load(records, table="meta_campaigns")
        db.dispose()

        log.info("[DAG meta_ads] extract_campaigns concluido: %d registros", n)
        return f"{n} registros em raw.meta_campaigns"

    # ── Orquestracao ──────────────────────────────────────────────────────────
    # insights primeiro (tabela principal de metricas), depois campaigns (referencia)
    extract_insights() >> extract_campaigns()


meta_ads_ingestion()
