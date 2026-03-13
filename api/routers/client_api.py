"""
Endpoints publicos da DaaS API (Data as a Service).
Protegidos por API Key via header X-API-Key.
Visiveis no Swagger em /docs.
"""

from typing import List

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from database import get_db
from dependencies import verify_api_key
from schemas_dashboard import LeadRecord, SaleRecord

router = APIRouter(dependencies=[Depends(verify_api_key)])


@router.get(
    "/sales",
    response_model=List[SaleRecord],
    summary="Lista vendas ganhas",
    tags=["DaaS - API do Cliente"],
)
def get_sales(
    limit:  int = Query(100, ge=1, le=1000),
    offset: int = Query(0,   ge=0),
    db: Session = Depends(get_db),
):
    """Retorna registros de `public.fact_sales` com paginacao. Requer `X-API-Key`."""
    sql = text("""
        SELECT *
        FROM public.fact_sales
        ORDER BY data_venda DESC
        LIMIT  :limit
        OFFSET :offset
    """)
    rows = db.execute(sql, {"limit": limit, "offset": offset}).mappings().all()
    return [SaleRecord(**dict(r)) for r in rows]


@router.get(
    "/leads",
    response_model=List[LeadRecord],
    summary="Lista todos os leads",
    tags=["DaaS - API do Cliente"],
)
def get_leads(
    limit:  int = Query(100, ge=1, le=1000),
    offset: int = Query(0,   ge=0),
    db: Session = Depends(get_db),
):
    """Retorna registros de `public.fact_leads` com paginacao. Requer `X-API-Key`."""
    sql = text("""
        SELECT *
        FROM public.fact_leads
        ORDER BY data_lead DESC
        LIMIT  :limit
        OFFSET :offset
    """)
    rows = db.execute(sql, {"limit": limit, "offset": offset}).mappings().all()
    return [LeadRecord(**dict(r)) for r in rows]
