/*
  fact_leads — Camada Gold: Fato de Leads
  =========================================
  Granularidade: 1 linha por lead criado (todos os status).

  Diferenca de fact_sales:
    - fact_sales  = apenas leads ganhos (para metricas de receita)
    - fact_leads  = todos os leads (para metricas de volume e conversao)

  Casos de uso no dashboard:
    - Taxa de Conversao: COUNT(is_ganho) / COUNT(*) por campanha
    - Custo por Lead (CPL): cruzar com fact_marketing_spend via campaign_id + mes
    - Funil de Status: COUNT(*) GROUP BY status_id
    - Tempo medio de fechamento: AVG(days_to_close) WHERE is_ganho OR is_perdido

  Consome: int_marketing_attribution, int_sales_funnel
*/

WITH atribuicao AS (
    SELECT * FROM {{ ref('int_marketing_attribution') }}
),

funil AS (
    SELECT
        lead_id,
        days_to_close,
        ciclo_bucket
    FROM {{ ref('int_sales_funnel') }}
)

SELECT
    -- Chaves de negocio
    a.lead_id,
    a.responsible_user_id,
    a.meta_campaign_id      AS campaign_id,
    a.meta_adset_id         AS adset_id,
    a.meta_ad_id            AS ad_id,

    -- Datas
    a.data_criacao          AS data_lead,
    DATE_TRUNC('month', a.data_criacao)::DATE AS mes_lead,
    a.data_fechamento,

    -- Status e conversao
    a.status_id,
    a.is_ganho,
    a.is_perdido,

    -- Receita (0 para leads nao ganhos)
    a.preco,

    -- Ciclo de vida
    f.days_to_close,
    f.ciclo_bucket,

    -- Contexto de campanha para display direto (evita JOINs no BI)
    a.meta_campaign_name    AS campaign_name,
    a.utm_source,
    a.utm_medium,
    a.tem_atribuicao_meta

FROM atribuicao a
LEFT JOIN funil f
    ON a.lead_id = f.lead_id

ORDER BY a.data_criacao DESC
