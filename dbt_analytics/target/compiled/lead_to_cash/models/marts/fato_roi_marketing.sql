/*
  fato_roi_marketing — Camada Gold: Tabela Fato de ROI de Marketing
  =================================================================
  Esta tabela e o produto final do pipeline Lead-to-Cash.
  Cruza investimento em midia (Meta Ads) com resultado comercial (Kommo CRM)
  no nivel mais granular possivel: por Ad.

  Regras de negocio:
    - JOIN key: stg_meta_insights.ad_id = stg_kommo_leads.utm_ad_id
      (rastreamento via UTMs — simulado pelo mock server)
    - Leads "ganhos" = status_id 142 no Kommo (is_ganho = true)
    - LEFT JOIN preserva todos os ads do Meta, mesmo sem leads no CRM
      (ads com zero conversao sao igualmente relevantes para analise)

  Metricas calculadas:
    total_spend        : investimento total por ad no periodo
    total_impressions  : alcance total
    total_clicks       : cliques totais
    ctr_pct            : Click-Through Rate = (clicks / impressions) * 100
    leads_gerados      : total de leads atribuidos ao ad via UTM
    vendas_ganhas      : leads com status "Ganho" (status_id = 142)
    receita_total      : soma do price dos leads ganhos
    cac                : Custo de Aquisicao de Cliente = spend / vendas_ganhas
    roas               : Return on Ad Spend = receita / spend
    custo_por_lead     : spend / leads_gerados (CPL)

  Protecao contra divisao por zero: NULLIF em todos os denominadores.
*/

WITH meta_aggregated AS (
    -- Agrega metricas de midia por ad (somando todos os dias do periodo)
    SELECT
        campaign_id,
        campaign_name,
        adset_id,
        adset_name,
        ad_id,
        ad_name,
        SUM(spend)              AS total_spend,
        SUM(impressions)        AS total_impressions,
        SUM(clicks)             AS total_clicks,
        SUM(inline_link_clicks) AS total_inline_clicks
    FROM "data_warehouse"."staging"."stg_meta_insights"
    GROUP BY
        campaign_id, campaign_name,
        adset_id, adset_name,
        ad_id, ad_name
),

kommo_aggregated AS (
    -- Agrega resultados de CRM por ad (via UTM tracking)
    -- Apenas leads com utm_ad_id preenchido (descarta leads organicos/sem rastreamento)
    SELECT
        utm_ad_id,
        COUNT(*)                                         AS leads_gerados,
        COUNT(*) FILTER (WHERE is_ganho)                AS vendas_ganhas,
        COALESCE(SUM(preco) FILTER (WHERE is_ganho), 0) AS receita_total
    FROM "data_warehouse"."staging"."stg_kommo_leads"
    WHERE utm_ad_id IS NOT NULL
    GROUP BY utm_ad_id
)

SELECT
    -- Hierarquia de campanha
    m.campaign_id,
    m.campaign_name,
    m.adset_id,
    m.adset_name,
    m.ad_id,
    m.ad_name,

    -- Metricas de midia (Meta Ads)
    ROUND(m.total_spend,         2)  AS total_spend,
    m.total_impressions,
    m.total_clicks,
    m.total_inline_clicks,
    ROUND(
        m.total_clicks::NUMERIC / NULLIF(m.total_impressions, 0) * 100,
        4
    )                                AS ctr_pct,

    -- Metricas de CRM (Kommo)
    COALESCE(k.leads_gerados,  0)    AS leads_gerados,
    COALESCE(k.vendas_ganhas,  0)    AS vendas_ganhas,
    COALESCE(k.receita_total,  0)    AS receita_total,

    -- KPIs de negocio
    -- CAC: quanto custou adquirir cada cliente
    ROUND(
        m.total_spend::NUMERIC / NULLIF(k.vendas_ganhas, 0),
        2
    )                                AS cac,

    -- ROAS: quantos reais retornaram para cada real investido
    ROUND(
        k.receita_total::NUMERIC / NULLIF(m.total_spend, 0),
        4
    )                                AS roas,

    -- CPL: custo por lead gerado
    ROUND(
        m.total_spend::NUMERIC / NULLIF(k.leads_gerados, 0),
        2
    )                                AS custo_por_lead

FROM meta_aggregated m
LEFT JOIN kommo_aggregated k
    ON m.ad_id = k.utm_ad_id

ORDER BY m.total_spend DESC