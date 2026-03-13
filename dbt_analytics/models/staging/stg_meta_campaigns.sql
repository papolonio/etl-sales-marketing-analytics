/*
  stg_meta_campaigns — Camada Silver: Meta Ads Campaigns
  =======================================================
  Limpeza e tipagem do catalogo de campanhas.

  - campaign_id / campaign_name: renomeados de id/name para consistencia
    com a nomenclatura usada em stg_meta_insights e nos modelos Gold.
  - data_inicio: extrai os primeiros 10 caracteres do start_time
    (formato "2025-03-15T00:00:00+0000") antes do cast para DATE,
    evitando falhas de parsing com sufixo de timezone.

  Consome: raw.meta_campaigns
  Alimenta: dim_campaign
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'meta_campaigns') }}
)

SELECT
    id                              AS campaign_id,
    name                            AS campaign_name,
    status,
    objective,
    LEFT(start_time, 10)::DATE      AS data_inicio

FROM source
