"""
Gerenciamento de conexao com o PostgreSQL via SQLAlchemy.

Padrao FastAPI: get_db() e uma dependencia injetada nos endpoints via Depends().
"""

import logging
import os

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

log = logging.getLogger(__name__)

# Leitura das variaveis de ambiente — mesmas usadas pelas DAGs do Airflow
_HOST = os.environ.get("POSTGRES_HOST",     "localhost")
_PORT = os.environ.get("POSTGRES_PORT",     "5432")
_DB   = os.environ.get("POSTGRES_DB",       "data_warehouse")
_USER = os.environ.get("POSTGRES_USER",     "admin")
_PASS = os.environ.get("POSTGRES_PASSWORD", "admin")

DATABASE_URL = f"postgresql+psycopg2://{_USER}:{_PASS}@{_HOST}:{_PORT}/{_DB}"

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,   # detecçao conexoes mortas antes de usar
    pool_size=5,
    max_overflow=10,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

log.info("DatabaseEngine configurado | host=%s port=%s db=%s", _HOST, _PORT, _DB)


def get_db():
    """
    Dependencia FastAPI: fornece uma sessao SQLAlchemy por request
    e o fechamento.
    """
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def ping() -> bool:
    """Verifica se o banco responde. Usado no evento de startup da API."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as exc:
        log.error("Falha no ping ao banco de dados: %s", exc)
        return False
