/*
  fact_marketing_spend — Camada Gold: Gasto de Marketing por Campanha/Dia
  =========================================================================
  Granularidade: 1 linha por campanha por dia.

  Agrega stg_meta_insights (granularidade: ad × dia) para o nivel de
  campanha × dia — nivel adequado para o grafico de "Evolucao Temporal
  de Investimento" e para calculo de CPL por campanha no periodo.

  Casos de uso no dashboard:
    - Evolucao Temporal: SUM(total_spend) GROUP BY data -> linha de investimento
    - CPL por campanha: cruzar com fact_leads via campaign_id + mes
    - Ranking de campanhas por investimento no periodo selecionado

  Coluna mes: primeiro dia do mes (ex: 2026-03-01) — chave de JOIN com
  fact_targets para comparar investimento vs. meta mensal.

  Consome: stg_meta_insights
*/

SELECT
    data_ref                                    AS data,
    DATE_TRUNC('month', data_ref)::DATE         AS mes,
    campaign_id,
    campaign_name,

    -- Metricas agregadas de midia
    SUM(spend)                                  AS total_spend,
    SUM(impressions)                            AS total_impressions,
    SUM(clicks)                                 AS total_clicks,
    SUM(inline_link_clicks)                     AS total_inline_clicks,
    ROUND(
        SUM(clicks)::NUMERIC / NULLIF(SUM(impressions), 0) * 100,
        4
    )                                           AS ctr_pct

FROM {{ ref('stg_meta_insights') }}

GROUP BY
    data_ref,
    campaign_id,
    campaign_name

ORDER BY data_ref DESC, total_spend DESC
