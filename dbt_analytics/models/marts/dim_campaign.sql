/*
  dim_campaign — Dimensao de Campanhas Meta Ads
  ===============================================
  Uma linha por campanha. Fonte de verdade para nomes e atributos
  de campanhas nos modelos fact_*.

  Consome: stg_meta_campaigns
*/

SELECT
    campaign_id,
    campaign_name,
    status,
    objective,
    data_inicio

FROM {{ ref('stg_meta_campaigns') }}
