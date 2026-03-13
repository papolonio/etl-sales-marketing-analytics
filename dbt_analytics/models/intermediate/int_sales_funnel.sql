/*
  int_sales_funnel — Camada Intermediate
  ========================================
  Calcula metricas de ciclo de vida para cada lead.

  Decisoes de design:
    - DATE - DATE no PostgreSQL retorna INTEGER (dias) diretamente.
      Nao ha necessidade de EXTRACT(EPOCH...) ou DATEDIFF.

    - days_to_close = NULL para leads em aberto: medias de fechamento
      devem considerar apenas deals concluidos. Incluir leads abertos
      distorceria o tempo medio para baixo (dias parciais).

    - ciclo_bucket: segmentacao em faixas para graficos de distribuicao
      no dashboard (histograma de velocidade de fechamento).
      Faixas calibradas para o mock: closed_at = created + 1-7 dias.

  Consome: stg_kommo_leads
  Alimenta: fact_sales, fact_leads
*/

WITH leads AS (
    SELECT * FROM {{ ref('stg_kommo_leads') }}
)

SELECT
    lead_id,
    status_id,
    pipeline_id,
    responsible_user_id,
    data_criacao,
    data_fechamento,
    is_ganho,
    is_perdido,

    -- Ciclo de vida em dias inteiros (NULL = lead ainda em aberto no funil)
    CASE
        WHEN data_fechamento IS NOT NULL
        THEN (data_fechamento - data_criacao)
    END                                         AS days_to_close,

    -- Bucket de velocidade para analise de distribuicao
    CASE
        WHEN data_fechamento IS NULL                    THEN 'Em Aberto'
        WHEN (data_fechamento - data_criacao) <= 3      THEN 'Rapido (0-3d)'
        WHEN (data_fechamento - data_criacao) <= 14     THEN 'Normal (4-14d)'
        WHEN (data_fechamento - data_criacao) <= 30     THEN 'Longo (15-30d)'
        ELSE                                                 'Muito Longo (>30d)'
    END                                         AS ciclo_bucket

FROM leads
