"""
Schemas Pydantic para os endpoints internos do Dashboard e DaaS publico.
"""

from datetime import date
from typing import List, Optional

from pydantic import BaseModel, ConfigDict


class KpisResponse(BaseModel):
    faturamento_total: Optional[float] = None
    total_vendas:      Optional[int]   = None
    ticket_medio:      Optional[float] = None
    total_leads:       Optional[int]   = None
    total_spend:       Optional[float] = None
    cac:               Optional[float] = None
    cpl:               Optional[float] = None


class EvolucaoItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    data:        Optional[date]  = None
    total_spend: Optional[float] = None
    receita:     Optional[float] = None


class RankingVendedorItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    salesperson_name: Optional[str]   = None
    receita_total:    Optional[float] = None
    total_vendas:     Optional[int]   = None


class SaleRecord(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    lead_id:              Optional[str]   = None
    responsible_user_id:  Optional[int]   = None
    campaign_id:          Optional[str]   = None
    ad_id:                Optional[str]   = None
    data_venda:           Optional[date]  = None
    receita:              Optional[float] = None
    campaign_name:        Optional[str]   = None
    utm_source:           Optional[str]   = None


class LeadRecord(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    lead_id:             Optional[str]   = None
    responsible_user_id: Optional[int]   = None
    campaign_id:         Optional[str]   = None
    data_lead:           Optional[date]  = None
    status_id:           Optional[int]   = None
    is_ganho:            Optional[bool]  = None
    is_perdido:          Optional[bool]  = None
    preco:               Optional[float] = None
    utm_source:          Optional[str]   = None
