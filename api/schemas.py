"""
schemas.py
==========
Modelos Pydantic para validacao e documentacao automatica dos endpoints.

RoiRecord    : representa uma linha da tabela Gold public.fato_roi_marketing
RoiResponse  : envelope paginado retornado por GET /api/v1/roi
"""

from datetime import date
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class RoiRecord(BaseModel):
    """
    Espelha todas as colunas de public.fato_roi_marketing.

    Todos os campos sao Optional porque:
      - Ads sem conversoes terao cac / roas / custo_por_lead = NULL
      - Ads sem leads no CRM terao leads_gerados / vendas_ganhas / receita_total = 0
    """

    model_config = ConfigDict(
        from_attributes=True,    # permite construir a partir de ORM rows
        populate_by_name=True,   # aceita tanto alias quanto nome do campo
    )

    # Dimensao temporal
    data: Optional[date] = Field(None, description="Data de referencia (date_start do Meta Ads)")

    # Hierarquia de campanha
    campaign_id:    Optional[str] = Field(None, description="ID da campanha no Meta Ads")
    campaign_name:  Optional[str] = Field(None, description="Nome da campanha")
    adset_id:       Optional[str] = Field(None, description="ID do grupo de anuncios")
    adset_name:     Optional[str] = Field(None, description="Nome do grupo de anuncios")
    ad_id:          Optional[str] = Field(None, description="ID do anuncio (chave granular)")
    ad_name:        Optional[str] = Field(None, description="Nome do anuncio")

    # Metricas de midia (Meta Ads)
    total_spend:         Optional[float] = Field(None, description="Investimento total no periodo (R$)")
    total_impressions:   Optional[int]   = Field(None, description="Total de impressoes")
    total_clicks:        Optional[int]   = Field(None, description="Total de cliques")
    total_inline_clicks: Optional[int]   = Field(None, description="Cliques no link do anuncio")
    ctr_pct:             Optional[float] = Field(None, description="Click-Through Rate (%)")

    # Metricas de CRM (Kommo)
    leads_gerados:  Optional[int]   = Field(None, description="Leads atribuidos ao anuncio via UTM")
    vendas_ganhas:  Optional[int]   = Field(None, description="Leads com status Ganho (status_id=142)")
    receita_total:  Optional[float] = Field(None, description="Soma do price dos leads ganhos (R$)")

    # KPIs de negocio
    cac:            Optional[float] = Field(None, description="Custo de Aquisicao de Cliente = spend / vendas_ganhas")
    roas:           Optional[float] = Field(None, description="Return on Ad Spend = receita / spend")
    custo_por_lead: Optional[float] = Field(None, description="Custo Por Lead = spend / leads_gerados")


class RoiResponse(BaseModel):
    """Envelope paginado para GET /api/v1/roi"""
    data:   List[RoiRecord]
    total:  int   = Field(..., description="Registros retornados nesta pagina")
    limit:  int   = Field(..., description="Tamanho maximo da pagina solicitado")
    offset: int   = Field(..., description="Posicao de inicio da pagina")
