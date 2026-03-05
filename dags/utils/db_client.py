"""
PostgresClient
==============
Cliente POO para persistencia na camada Raw do Data Warehouse.

Comportamento Raw Layer (Truncate + Insert):
    Cada execucao da DAG substitui completamente os dados da tabela.
    Isso garante que a Raw reflita sempre o estado mais recente da API-fonte,
    sem necessidade de logica de merge complexa nesta camada.

Uso:
    db = PostgresClient(host, port, db, user, password)
    rows_inserted = db.load(records, table="meta_insights")
    db.dispose()
"""

import json
import logging
from typing import Any, Dict, List, Union

import pandas as pd
from psycopg2.errors import UniqueViolation
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

log = logging.getLogger(__name__)


class PostgresClient:
    """
    Wrapper sobre SQLAlchemy para operacoes de carga na camada Raw.

    - Cria o schema automaticamente se nao existir.
    - Serializa colunas com listas/dicts para JSON string (compativel com TEXT).
    - Truncate + Insert por padrao (ideal para Raw layer).
    """

    def __init__(
        self,
        host: str,
        port: Union[str, int],
        db: str,
        user: str,
        password: str,
    ) -> None:
        conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
        self._engine: Engine = create_engine(conn_str, pool_pre_ping=True)
        log.info(
            "[PostgresClient] Engine criado | host=%s port=%s db=%s",
            host,
            port,
            db,
        )

    # ───────────────────────────────────────────────────── metodos privados ──

    def _ensure_schema(self, schema: str) -> None:
        """
        Cria o schema se nao existir.
        O try/except absorve a UniqueViolation que ocorre quando duas tasks
        paralelas executam o CREATE SCHEMA simultaneamente (race condition).
        """
        try:
            with self._engine.begin() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        except IntegrityError as exc:
            if isinstance(exc.orig, UniqueViolation):
                log.debug(
                    "[PostgresClient] Schema '%s' ja criado por task concorrente, ignorando.",
                    schema,
                )
            else:
                raise

    @staticmethod
    def _serialize_nested(df: pd.DataFrame) -> pd.DataFrame:
        """
        Converte colunas que contenham dicts ou listas para JSON string.
        Necessario porque o PostgreSQL nao aceita objetos Python nativos;
        na camada Silver o dbt vai parsear esses campos com json_extract_path.
        """
        for col in df.columns:
            has_nested = df[col].apply(lambda x: isinstance(x, (dict, list))).any()
            if has_nested:
                df[col] = df[col].apply(
                    lambda x: json.dumps(x, ensure_ascii=False)
                    if isinstance(x, (dict, list))
                    else x
                )
        return df

    # ────────────────────────────────────────────────────── metodos publicos ──

    def load(
        self,
        records: List[Dict[str, Any]],
        table: str,
        schema: str = "raw",
    ) -> int:
        """
        Carrega uma lista de dicionarios em {schema}.{table}.

        Usa if_exists='replace': o pandas faz DROP + CREATE + INSERT de forma
        atomica, substituindo o TRUNCATE manual que nao e suportado com IF EXISTS
        no PostgreSQL.

        Args:
            records: dados extraidos da API (list[dict])
            table:   nome da tabela destino
            schema:  schema do DW (default: 'raw')

        Returns:
            Numero de linhas inseridas (0 se records estiver vazio).
        """
        if not records:
            log.warning(
                "[PostgresClient] Nenhum registro para inserir em %s.%s",
                schema,
                table,
            )
            return 0

        self._ensure_schema(schema)

        df = pd.DataFrame(records)
        df = self._serialize_nested(df)

        df.to_sql(
            name=table,
            con=self._engine,
            schema=schema,
            if_exists="replace",
            index=False,
            chunksize=500,
            method="multi",
        )

        log.info(
            "[PostgresClient] %d linhas inseridas em %s.%s",
            len(df),
            schema,
            table,
        )
        return len(df)

    def dispose(self) -> None:
        """Libera todas as conexoes do pool (chamar ao fim de cada task)."""
        self._engine.dispose()
        log.info("[PostgresClient] Pool de conexoes encerrado")
