"""
routers/dashboard.py
====================
Endpoints do Dashboard Executivo Lead-to-Cash.

Todos os endpoints consultam a camada Gold (public.*) gerada pelo dbt.
Prefixo registrado em main.py: /api/v1/dashboard

Endpoints:
    GET  /kpis                  KPIs globais (faturamento, vendas, CAC, CPL)
    GET  /ranking-vendedores    Top vendedores por receita
    GET  /run-rate              Faturamento acumulado vs. meta mensal
    GET  /evolucao-marketing    Spend diario vs. receita diaria (serie temporal)
    GET  /funil                 Tempo medio de fechamento + distribuicao por bucket
    GET  /campanhas             Performance por campanha (ROAS, CAC, CPL)
    GET  /metas                 Metas mensais por vendedor (fact_targets)
    POST /metas                 Atualiza metas diretamente na tabela Gold

Nota sobre o write-back de metas:
    O POST atualiza public.fact_targets via DELETE + INSERT transacional.
    Para persistir no CSV (dbt seed), atualize manualmente o arquivo
    dbt_analytics/seeds/metas_vendedores.csv e rode:
        dbt seed --select metas_vendedores --profiles-dir .
"""

import logging
from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.orm import Session

from database import get_db
from schemas_dashboard import (
    BucketFunil,
    CampanhaPerformance,
    EvolucaoItem,
    FunilResponse,
    KpisResponse,
    MetaItem,
    MetaUpdateRequest,
    RunRateItem,
    VendedorRanking,
)

log = logging.getLogger("uvicorn.error")
router = APIRouter()


# ── Utilitario de erro ────────────────────────────────────────────────────────

def _db_error(exc: Exception, ctx: str) -> None:
    log.error("[dashboard/%s] %s", ctx, exc)
    raise HTTPException(
        status_code=503,
        detail=(
            "Erro ao consultar o banco de dados. Verifique se o dbt build "
            "foi executado e as tabelas Gold existem."
        ),
    )


# =============================================================================
# Visao Executiva
# =============================================================================

@router.get(
    "/kpis",
    response_model=KpisResponse,
    summary="KPIs globais do periodo",
)
def get_kpis(
    data_inicio: Optional[date] = Query(None, description="Filtro: data inicial (YYYY-MM-DD)"),
    data_fim:    Optional[date] = Query(None, description="Filtro: data final  (YYYY-MM-DD)"),
    db: Session = Depends(get_db),
):
    """
    Retorna os KPIs consolidados: Faturamento, Numero de Vendas, Ticket Medio,
    Total de Leads, Total Investido, CAC e CPL.

    Sem filtros retorna o historico completo. Com `data_inicio`/`data_fim`
    restringe a janela temporal para todos os calculos.
    """
    sql = text("""
        WITH vendas AS (
            SELECT
                COUNT(*)                    AS total_vendas,
                COALESCE(SUM(receita), 0)   AS faturamento_total
            FROM public.fact_sales
            WHERE (:data_inicio IS NULL OR data_venda >= :data_inicio)
              AND (:data_fim    IS NULL OR data_venda <= :data_fim)
        ),
        leads AS (
            SELECT COUNT(*) AS total_leads
            FROM public.fact_leads
            WHERE (:data_inicio IS NULL OR data_lead >= :data_inicio)
              AND (:data_fim    IS NULL OR data_lead <= :data_fim)
        ),
        spend AS (
            SELECT COALESCE(SUM(total_spend), 0) AS total_spend
            FROM public.fact_marketing_spend
            WHERE (:data_inicio IS NULL OR data >= :data_inicio)
              AND (:data_fim    IS NULL OR data <= :data_fim)
        )
        SELECT
            v.total_vendas,
            v.faturamento_total,
            ROUND(v.faturamento_total / NULLIF(v.total_vendas,  0), 2) AS ticket_medio,
            l.total_leads,
            s.total_spend,
            ROUND(s.total_spend       / NULLIF(v.total_vendas,  0), 2) AS cac,
            ROUND(s.total_spend       / NULLIF(l.total_leads,   0), 2) AS cpl
        FROM vendas v, leads l, spend s
    """)
    try:
        row = db.execute(sql, {
            "data_inicio": data_inicio,
            "data_fim":    data_fim,
        }).mappings().first()
    except (OperationalError, ProgrammingError) as exc:
        _db_error(exc, "kpis")

    return KpisResponse(**dict(row))


@router.get(
    "/ranking-vendedores",
    response_model=List[VendedorRanking],
    summary="Ranking de vendedores por receita",
)
def get_ranking_vendedores(
    data_inicio: Optional[date] = Query(None, description="Filtro: data inicial (YYYY-MM-DD)"),
    data_fim:    Optional[date] = Query(None, description="Filtro: data final  (YYYY-MM-DD)"),
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
):
    """
    Retorna os vendedores ordenados por receita total.
    Inclui numero de vendas, ticket medio e tempo medio de fechamento.
    Aceita filtros opcionais de data_inicio e data_fim (campo data_venda de fact_sales).
    """
    sql = text("""
        SELECT
            fs.responsible_user_id,
            ds.salesperson_name,
            COUNT(*)                        AS total_vendas,
            ROUND(SUM(fs.receita),       2) AS receita_total,
            ROUND(AVG(fs.receita),       2) AS ticket_medio,
            ROUND(AVG(fs.days_to_close), 1) AS avg_days_to_close
        FROM public.fact_sales fs
        LEFT JOIN public.dim_salesperson ds
            ON fs.responsible_user_id = ds.salesperson_id
        WHERE (:data_inicio IS NULL OR fs.data_venda >= :data_inicio)
          AND (:data_fim    IS NULL OR fs.data_venda <= :data_fim)
        GROUP BY fs.responsible_user_id, ds.salesperson_name
        ORDER BY receita_total DESC
        LIMIT :limit
    """)
    try:
        rows = db.execute(sql, {
            "data_inicio": data_inicio,
            "data_fim":    data_fim,
            "limit":       limit,
        }).mappings().all()
    except (OperationalError, ProgrammingError) as exc:
        _db_error(exc, "ranking-vendedores")

    return [VendedorRanking(**dict(r)) for r in rows]


# =============================================================================
# Run Rate & Evolucao Temporal
# =============================================================================

@router.get(
    "/run-rate",
    response_model=List[RunRateItem],
    summary="Faturamento acumulado vs. meta mensal por vendedor",
)
def get_run_rate(
    mes: Optional[date] = Query(None, description="Filtrar por mes especifico (YYYY-MM-01)"),
    db: Session = Depends(get_db),
):
    """
    Compara a receita realizada com a meta mensal para cada vendedor.
    `pct_atingimento` = (receita_realizada / meta_receita) * 100.

    Sem filtro retorna todos os meses do seed. Com `mes` filtra um mes especifico.
    """
    sql = text("""
        SELECT
            ft.responsible_user_id,
            ft.user_name,
            ft.mes,
            ft.meta_receita,
            COALESCE(SUM(fs.receita), 0)    AS receita_realizada,
            ROUND(
                COALESCE(SUM(fs.receita), 0)
                / NULLIF(ft.meta_receita, 0) * 100,
                1
            )                               AS pct_atingimento
        FROM public.fact_targets ft
        LEFT JOIN public.fact_sales fs
            ON  ft.responsible_user_id = fs.responsible_user_id
            AND ft.mes                 = fs.mes_venda
        WHERE (:mes IS NULL OR ft.mes = :mes)
        GROUP BY
            ft.responsible_user_id, ft.user_name,
            ft.mes, ft.meta_receita
        ORDER BY ft.mes DESC, receita_realizada DESC
    """)
    try:
        rows = db.execute(sql, {"mes": mes}).mappings().all()
    except (OperationalError, ProgrammingError) as exc:
        _db_error(exc, "run-rate")

    return [RunRateItem(**dict(r)) for r in rows]


@router.get(
    "/evolucao-marketing",
    response_model=List[EvolucaoItem],
    summary="Serie temporal: gasto de marketing vs. receita de vendas",
)
def get_evolucao_marketing(
    data_inicio: Optional[date] = Query(None),
    data_fim:    Optional[date] = Query(None),
    db: Session = Depends(get_db),
):
    """
    Retorna uma linha por dia com o gasto total de marketing (Meta Ads)
    e a receita das vendas fechadas naquele dia.
    Ideal para o grafico de area/linha da Visao Executiva.
    """
    sql = text("""
        WITH spend_diario AS (
            SELECT
                data,
                SUM(total_spend) AS total_spend
            FROM public.fact_marketing_spend
            WHERE (:data_inicio IS NULL OR data >= :data_inicio)
              AND (:data_fim    IS NULL OR data <= :data_fim)
            GROUP BY data
        ),
        receita_diaria AS (
            SELECT
                data_venda          AS data,
                SUM(receita)        AS receita
            FROM public.fact_sales
            WHERE (:data_inicio IS NULL OR data_venda >= :data_inicio)
              AND (:data_fim    IS NULL OR data_venda <= :data_fim)
            GROUP BY data_venda
        )
        SELECT
            s.data,
            ROUND(s.total_spend,          2) AS total_spend,
            ROUND(COALESCE(r.receita, 0), 2) AS receita
        FROM spend_diario s
        LEFT JOIN receita_diaria r ON s.data = r.data
        ORDER BY s.data
    """)
    try:
        rows = db.execute(sql, {
            "data_inicio": data_inicio,
            "data_fim":    data_fim,
        }).mappings().all()
    except (OperationalError, ProgrammingError) as exc:
        _db_error(exc, "evolucao-marketing")

    return [EvolucaoItem(**dict(r)) for r in rows]


# =============================================================================
# Funil & Marketing
# =============================================================================

@router.get(
    "/funil",
    response_model=FunilResponse,
    summary="Tempo medio de fechamento e distribuicao do funil por bucket",
)
def get_funil(db: Session = Depends(get_db)):
    """
    Retorna o tempo medio de fechamento (somente vendas ganhas) e a
    distribuicao de todos os leads pelos buckets de ciclo de vida.
    """
    sql_summary = text("""
        SELECT
            ROUND(AVG(days_to_close), 1) AS avg_days_to_close,
            COUNT(*)                     AS total_vendas
        FROM public.fact_sales
        WHERE days_to_close IS NOT NULL
    """)
    sql_buckets = text("""
        SELECT
            ciclo_bucket,
            COUNT(*)                                          AS total_leads,
            COUNT(*) FILTER (WHERE is_ganho)                 AS ganhos,
            COUNT(*) FILTER (WHERE is_perdido)               AS perdidos,
            ROUND(
                COUNT(*) FILTER (WHERE is_ganho)::NUMERIC
                / NULLIF(COUNT(*), 0) * 100,
                1
            )                                                AS taxa_conversao_pct
        FROM public.fact_leads
        GROUP BY ciclo_bucket
        ORDER BY
            CASE ciclo_bucket
                WHEN 'Rapido (0-3d)'     THEN 1
                WHEN 'Normal (4-14d)'    THEN 2
                WHEN 'Longo (15-30d)'    THEN 3
                WHEN 'Muito Longo (>30d)'THEN 4
                ELSE 5
            END
    """)
    try:
        summary = db.execute(sql_summary).mappings().first()
        buckets = db.execute(sql_buckets).mappings().all()
    except (OperationalError, ProgrammingError) as exc:
        _db_error(exc, "funil")

    return FunilResponse(
        avg_days_to_close=summary["avg_days_to_close"],
        total_vendas=summary["total_vendas"],
        distribuicao=[BucketFunil(**dict(b)) for b in buckets],
    )


@router.get(
    "/campanhas",
    response_model=List[CampanhaPerformance],
    summary="Performance por campanha: ROAS, CAC, CPL",
)
def get_campanhas(
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    Agrega spend (Meta Ads) e resultados de CRM por campanha.
    Retorna ROAS, CAC e CPL calculados com NULLIF para proteger divisoes por zero.
    """
    sql = text("""
        WITH leads_por_campanha AS (
            SELECT
                campaign_id,
                COUNT(*)                                          AS total_leads,
                COUNT(*) FILTER (WHERE is_ganho)                 AS total_ganhos,
                COALESCE(SUM(preco) FILTER (WHERE is_ganho), 0)  AS receita
            FROM public.fact_leads
            WHERE campaign_id IS NOT NULL
            GROUP BY campaign_id
        ),
        spend_por_campanha AS (
            SELECT
                campaign_id,
                campaign_name,
                SUM(total_spend) AS total_spend
            FROM public.fact_marketing_spend
            GROUP BY campaign_id, campaign_name
        )
        SELECT
            s.campaign_id,
            s.campaign_name,
            ROUND(s.total_spend,                                    2) AS total_spend,
            COALESCE(l.total_leads,  0)                                AS total_leads,
            COALESCE(l.total_ganhos, 0)                                AS total_ganhos,
            ROUND(COALESCE(l.receita, 0),                           2) AS receita,
            ROUND(COALESCE(l.receita, 0) / NULLIF(s.total_spend, 0), 4) AS roas,
            ROUND(s.total_spend / NULLIF(l.total_ganhos,  0),       2) AS cac,
            ROUND(s.total_spend / NULLIF(l.total_leads,   0),       2) AS cpl
        FROM spend_por_campanha s
        LEFT JOIN leads_por_campanha l ON s.campaign_id = l.campaign_id
        ORDER BY s.total_spend DESC
        LIMIT :limit
    """)
    try:
        rows = db.execute(sql, {"limit": limit}).mappings().all()
    except (OperationalError, ProgrammingError) as exc:
        _db_error(exc, "campanhas")

    return [CampanhaPerformance(**dict(r)) for r in rows]


# =============================================================================
# Metas — GET + POST (write-back)
# =============================================================================

@router.get(
    "/metas",
    response_model=List[MetaItem],
    summary="Metas mensais por vendedor",
)
def get_metas(
    mes: Optional[date] = Query(None, description="Filtrar por mes (YYYY-MM-01)"),
    db: Session = Depends(get_db),
):
    """Retorna as metas de public.fact_targets. Sem filtro retorna todos os meses."""
    sql = text("""
        SELECT responsible_user_id, user_name, mes, meta_receita
        FROM public.fact_targets
        WHERE (:mes IS NULL OR mes = :mes)
        ORDER BY mes DESC, user_name
    """)
    try:
        rows = db.execute(sql, {"mes": mes}).mappings().all()
    except (OperationalError, ProgrammingError) as exc:
        _db_error(exc, "metas-get")

    return [MetaItem(**dict(r)) for r in rows]


@router.post(
    "/metas",
    response_model=List[MetaItem],
    summary="Atualiza metas mensais (write-back)",
)
def post_metas(payload: MetaUpdateRequest, db: Session = Depends(get_db)):
    """
    Recebe a lista completa de metas e aplica uma substituicao transacional
    na tabela `public.fact_targets`:
      1. DELETE das linhas cujo (responsible_user_id, mes) esta no payload.
      2. INSERT das novas linhas.

    IMPORTANTE: `public.fact_targets` e gerenciada pelo dbt. Um `dbt run`
    posterior vai resetar a tabela com base no seed CSV. Para persistir as
    alteracoes, atualize tambem o arquivo:
        dbt_analytics/seeds/metas_vendedores.csv
    e execute `dbt seed --select metas_vendedores --profiles-dir .`
    """
    if not payload.metas:
        raise HTTPException(status_code=422, detail="Lista de metas nao pode estar vazia.")

    try:
        with db.begin():
            # Passo 1: remove apenas os pares (user, mes) presentes no payload
            for m in payload.metas:
                db.execute(
                    text("""
                        DELETE FROM public.fact_targets
                        WHERE responsible_user_id = :uid AND mes = :mes
                    """),
                    {"uid": m.responsible_user_id, "mes": m.mes},
                )

            # Passo 2: insere os novos valores
            db.execute(
                text("""
                    INSERT INTO public.fact_targets
                        (responsible_user_id, user_name, mes, meta_receita)
                    VALUES
                        (:uid, :user_name, :mes, :meta_receita)
                """),
                [
                    {
                        "uid":          m.responsible_user_id,
                        "user_name":    m.user_name,
                        "mes":          m.mes,
                        "meta_receita": m.meta_receita,
                    }
                    for m in payload.metas
                ],
            )
    except (OperationalError, ProgrammingError) as exc:
        _db_error(exc, "metas-post")

    log.info("[dashboard/metas] %d meta(s) atualizadas.", len(payload.metas))

    # Retorna o estado atual da tabela apos a escrita
    return get_metas(mes=None, db=db)
