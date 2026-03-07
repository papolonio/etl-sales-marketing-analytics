"""
schemas_dashboard.py
====================
Modelos Pydantic para os endpoints do Dashboard Executivo.
Separados de schemas.py para nao poluir o contrato original da rota /roi.
"""

from datetime import date
from typing import List, Optional

from pydantic import BaseModel, ConfigDict


# ── Visao Executiva ────────────────────────────────────────────────────────────

class KpisResponse(BaseModel):
    """KPIs globais do periodo selecionado."""
    total_vendas:       int
    faturamento_total:  float
    ticket_medio:       Optional[float]
    total_leads:        int
    total_spend:        float
    cac:                Optional[float]   # spend / vendas
    cpl:                Optional[float]   # spend / leads


class VendedorRanking(BaseModel):
    """Uma linha por vendedor no ranking de performance."""
    model_config = ConfigDict(from_attributes=True)

    responsible_user_id: int
    salesperson_name:    Optional[str]
    total_vendas:        int
    receita_total:       float
    ticket_medio:        Optional[float]
    avg_days_to_close:   Optional[float]


# ── Run Rate & Evolucao Temporal ───────────────────────────────────────────────

class RunRateItem(BaseModel):
    """Faturamento realizado vs. meta por vendedor × mes."""
    model_config = ConfigDict(from_attributes=True)

    responsible_user_id: int
    user_name:           str
    mes:                 date
    meta_receita:        float
    receita_realizada:   float
    pct_atingimento:     Optional[float]   # (realizada / meta) * 100


class EvolucaoItem(BaseModel):
    """Gasto diario de marketing vs. receita de vendas."""
    model_config = ConfigDict(from_attributes=True)

    data:        date
    total_spend: float
    receita:     float


# ── Funil & Marketing ─────────────────────────────────────────────────────────

class BucketFunil(BaseModel):
    """Distribuicao de leads por faixa de tempo de fechamento."""
    model_config = ConfigDict(from_attributes=True)

    ciclo_bucket:        str
    total_leads:         int
    ganhos:              int
    perdidos:            int
    taxa_conversao_pct:  Optional[float]


class FunilResponse(BaseModel):
    """Tempo medio de fechamento + distribuicao por bucket."""
    avg_days_to_close:   Optional[float]   # media das vendas ganhas
    total_vendas:        int
    distribuicao:        List[BucketFunil]


class CampanhaPerformance(BaseModel):
    """Performance por campanha cruzando spend (Meta) com leads/vendas (Kommo)."""
    model_config = ConfigDict(from_attributes=True)

    campaign_id:   str
    campaign_name: str
    total_spend:   float
    total_leads:   int
    total_ganhos:  int
    receita:       float
    roas:          Optional[float]   # receita / spend
    cac:           Optional[float]   # spend / ganhos
    cpl:           Optional[float]   # spend / leads


# ── Metas (Write-back) ────────────────────────────────────────────────────────

class MetaItem(BaseModel):
    """Uma linha de meta mensal por vendedor."""
    model_config = ConfigDict(from_attributes=True)

    responsible_user_id: int
    user_name:           str
    mes:                 date
    meta_receita:        float


class MetaUpdateRequest(BaseModel):
    """
    Payload do POST /metas.
    Envia a lista completa de metas a serem aplicadas (substituicao total).
    """
    metas: List[MetaItem]
