"""
Endpoints internos do Dashboard Executivo.
Rotas ocultas do Swagger (include_in_schema=False no router).

Todos os endpoints aceitam os query params:
    data_inicio: str  (YYYY-MM-DD)
    data_fim:    str  (YYYY-MM-DD)
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from database import get_db
from schemas_dashboard import (
    EvolucaoItem,
    KpisResponse,
    RankingVendedorItem,
)

router = APIRouter(include_in_schema=False)


@router.get("/kpis", response_model=KpisResponse)
def get_kpis(
    data_inicio: str = Query(..., description="Data inicial (YYYY-MM-DD)"),
    data_fim:    str = Query(..., description="Data final (YYYY-MM-DD)"),
    db: Session = Depends(get_db),
):
    sql_vendas = text("""
        SELECT
            COALESCE(SUM(receita), 0)         AS faturamento_total,
            COUNT(*)                           AS total_vendas,
            AVG(receita)                       AS ticket_medio
        FROM public.fact_sales
        WHERE data_venda BETWEEN :data_inicio AND :data_fim
    """)

    sql_leads = text("""
        SELECT COUNT(*) AS total_leads
        FROM public.fact_leads
        WHERE data_lead BETWEEN :data_inicio AND :data_fim
    """)

    sql_spend = text("""
        SELECT COALESCE(SUM(total_spend), 0) AS total_spend
        FROM public.fact_marketing_spend
        WHERE data BETWEEN :data_inicio AND :data_fim
    """)

    params = {"data_inicio": data_inicio, "data_fim": data_fim}

    row_v  = db.execute(sql_vendas, params).mappings().one()
    row_l  = db.execute(sql_leads,  params).mappings().one()
    row_sp = db.execute(sql_spend,  params).mappings().one()

    faturamento = float(row_v["faturamento_total"] or 0)
    total_v     = int(row_v["total_vendas"] or 0)
    ticket      = float(row_v["ticket_medio"]) if row_v["ticket_medio"] else None
    total_leads = int(row_l["total_leads"] or 0)
    total_spend = float(row_sp["total_spend"] or 0)

    cac = round(total_spend / total_v,     2) if total_v     > 0 else None
    cpl = round(total_spend / total_leads, 2) if total_leads > 0 else None

    return KpisResponse(
        faturamento_total=faturamento,
        total_vendas=total_v,
        ticket_medio=ticket,
        total_leads=total_leads,
        total_spend=total_spend,
        cac=cac,
        cpl=cpl,
    )


@router.get("/evolucao-marketing", response_model=list[EvolucaoItem])
def get_evolucao_marketing(
    data_inicio: str = Query(...),
    data_fim:    str = Query(...),
    db: Session = Depends(get_db),
):
    # Parametros com nomes unicos por ocorrencia: SQLAlchemy 1.4 + psycopg2 lanca
    # ProgrammingError quando o mesmo bind parameter aparece em multiplos contextos
    # distintos dentro de um unico text() (generate_series + dois WHERE).
    # generate_series movido para FROM (padrao PostgreSQL; evita set-returning no SELECT).
    sql = text("""
        SELECT
            d.data,
            COALESCE(s.total_spend, 0) AS total_spend,
            COALESCE(v.receita,     0) AS receita
        FROM generate_series(
            CAST(:gs_inicio AS DATE),
            CAST(:gs_fim AS DATE),
            '1 day'::interval
        ) AS d(data)
        LEFT JOIN (
            SELECT data, SUM(total_spend) AS total_spend
            FROM public.fact_marketing_spend
            WHERE data BETWEEN :sp_inicio AND :sp_fim
            GROUP BY data
        ) s ON d.data = s.data
        LEFT JOIN (
            SELECT data_venda AS data, SUM(receita) AS receita
            FROM public.fact_sales
            WHERE data_venda BETWEEN :sv_inicio AND :sv_fim
            GROUP BY data_venda
        ) v ON d.data = v.data
        ORDER BY d.data
    """)

    rows = db.execute(sql, {
        "gs_inicio": data_inicio, "gs_fim": data_fim,
        "sp_inicio": data_inicio, "sp_fim": data_fim,
        "sv_inicio": data_inicio, "sv_fim": data_fim,
    }).mappings().all()
    return [EvolucaoItem(**dict(r)) for r in rows]


@router.get("/ranking-vendedores", response_model=list[RankingVendedorItem])
def get_ranking_vendedores(
    data_inicio: str = Query(...),
    data_fim:    str = Query(...),
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db),
):
    sql = text("""
        SELECT
            COALESCE(d.salesperson_name, 'Desconhecido') AS salesperson_name,
            SUM(s.receita)  AS receita_total,
            COUNT(*)        AS total_vendas
        FROM public.fact_sales s
        LEFT JOIN public.dim_salesperson d
            ON s.responsible_user_id = d.salesperson_id
        WHERE s.data_venda BETWEEN :data_inicio AND :data_fim
        GROUP BY d.salesperson_name
        ORDER BY receita_total DESC
        LIMIT :limit
    """)

    rows = db.execute(sql, {"data_inicio": data_inicio, "data_fim": data_fim, "limit": limit}).mappings().all()
    return [RankingVendedorItem(**dict(r)) for r in rows]
