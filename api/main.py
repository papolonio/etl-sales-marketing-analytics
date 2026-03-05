"""
main.py — Lead-to-Cash Data API
================================
API RestFul que serve os dados da camada Gold (dbt) para times de BI ou Front-end.

Endpoints:
    GET /                              health check
    GET /api/v1/roi                    todos os registros (paginado)
    GET /api/v1/roi/{campaign_id}      filtrado por campanha
    GET /api/v1/roi/ad/{ad_id}         filtrado por anuncio (granularidade maxima)

Acesso: http://localhost:8001
Docs  : http://localhost:8001/docs   (Swagger UI automatico)
"""

import logging
from contextlib import asynccontextmanager
from typing import List

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from psycopg2.errors import UndefinedTable
from sqlalchemy import text
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.orm import Session

from database import get_db, ping
from schemas import RoiRecord, RoiResponse

log = logging.getLogger("uvicorn.error")

# Startup / Shutdown

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Verifica a conexao com o banco no startup. Falha rapido se o DB nao esta pronto."""
    if not ping():
        log.warning(
            "AVISO: API iniciada mas o banco de dados nao respondeu ao ping. "
            "Verifique se o container postgres esta saudavel."
        )
    else:
        log.info("Conexao com o banco de dados verificada com sucesso.")
    yield

# Aplicacao

app = FastAPI(
    title="Lead-to-Cash Data API",
    description=(
        "API RestFul que expoe os dados da camada Gold do pipeline ETL. "
        "Cruza investimento em Meta Ads com resultados de CRM (Kommo) "
        "para calcular CAC, ROAS e CPL por anuncio."
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# Constante com o nome completo da tabela Gold
_TABELA_ROI = "public.fato_roi_marketing"

# Handlers de erro reutilizaveis

def _handle_db_error(exc: Exception, context: str) -> None:
    """
    Converte excecoes do SQLAlchemy/psycopg2 em HTTPException semanticas.
      - Tabela nao existe (dbt nao rodou) -> 503 com instrucao clara
      - Outros erros de banco               -> 503 generico
    """
    # UndefinedTable e encapsulada dentro de ProgrammingError pelo SQLAlchemy
    orig = getattr(exc, "orig", None)
    if isinstance(orig, UndefinedTable):
        log.error("[%s] Tabela %s nao encontrada: %s", context, _TABELA_ROI, exc)
        raise HTTPException(
            status_code=503,
            detail=(
                f"A tabela '{_TABELA_ROI}' nao existe. "
                "Execute 'dbt run --profiles-dir .' na pasta dbt_analytics antes de usar esta rota."
            ),
        )
    log.error("[%s] Erro de banco de dados: %s", context, exc)
    raise HTTPException(
        status_code=503,
        detail="Erro ao acessar o banco de dados. Tente novamente em instantes.",
    )


# =============================================================================
# Endpoints
# =============================================================================

@app.get("/", tags=["health"])
def health_check():
    """Verificacao de saude da API."""
    return {
        "status":  "API de Dados Operacional",
        "version": "1.0.0",
        "tabela":  _TABELA_ROI,
        "docs":    "/docs",
    }


@app.get(
    "/api/v1/roi",
    response_model=RoiResponse,
    summary="Lista todos os registros de ROI",
    tags=["ROI Marketing"],
)
def get_roi(
    limit:  int = Query(100, ge=1, le=1000, description="Maximo de registros por pagina"),
    offset: int = Query(0,   ge=0,          description="Posicao inicial da paginacao"),
    db: Session = Depends(get_db),
):
    """
    Retorna todos os registros de `public.fato_roi_marketing` com paginacao.

    - **limit**: maximo de 1000 registros por chamada
    - **offset**: deslocamento para paginacao sequencial
    """
    sql = text(f"""
        SELECT *
        FROM {_TABELA_ROI}
        ORDER BY total_spend DESC
        LIMIT  :limit
        OFFSET :offset
    """)
    try:
        rows = db.execute(sql, {"limit": limit, "offset": offset}).mappings().all()
    except (OperationalError, ProgrammingError) as exc:
        _handle_db_error(exc, "GET /api/v1/roi")

    return RoiResponse(
        data=[RoiRecord(**dict(row)) for row in rows],
        total=len(rows),
        limit=limit,
        offset=offset,
    )


@app.get(
    "/api/v1/roi/{campaign_id}",
    response_model=List[RoiRecord],
    summary="ROI filtrado por campanha",
    tags=["ROI Marketing"],
)
def get_roi_by_campaign(campaign_id: str, db: Session = Depends(get_db)):
    """
    Retorna todos os anuncios de uma campanha especifica com suas metricas de ROI.
    - **campaign_id**: ID da campanha no Meta Ads (ex: `120210000000001`)
    """
    sql = text(f"""
        SELECT *
        FROM {_TABELA_ROI}
        WHERE campaign_id = :campaign_id
        ORDER BY total_spend DESC
    """)
    try:
        rows = db.execute(sql, {"campaign_id": campaign_id}).mappings().all()
    except (OperationalError, ProgrammingError) as exc:
        _handle_db_error(exc, f"GET /api/v1/roi/{campaign_id}")

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Campanha '{campaign_id}' nao encontrada em {_TABELA_ROI}.",
        )

    return [RoiRecord(**dict(row)) for row in rows]


@app.get(
    "/api/v1/roi/ad/{ad_id}",
    response_model=RoiRecord,
    summary="ROI de um anuncio especifico (granularidade maxima)",
    tags=["ROI Marketing"],
)
def get_roi_by_ad(ad_id: str, db: Session = Depends(get_db)):
    """
    Retorna as metricas de ROI de um unico anuncio.
    Este e o nivel mais granular do pipeline: Campaign > AdSet > **Ad**.

    - **ad_id**: ID do anuncio no Meta Ads (ex: `120230000000001`)
    """
    sql = text(f"""
        SELECT *
        FROM {_TABELA_ROI}
        WHERE ad_id = :ad_id
        LIMIT 1
    """)
    try:
        row = db.execute(sql, {"ad_id": ad_id}).mappings().first()
    except (OperationalError, ProgrammingError) as exc:
        _handle_db_error(exc, f"GET /api/v1/roi/ad/{ad_id}")

    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"Anuncio '{ad_id}' nao encontrado em {_TABELA_ROI}.",
        )

    return RoiRecord(**dict(row))
