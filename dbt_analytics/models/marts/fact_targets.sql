/*
  fact_targets — Camada Gold: Metas Mensais por Vendedor
  ========================================================
  Lê o seed metas_vendedores.csv e expoe as metas com tipos corretos.

  Casos de uso no dashboard:
    - Run Rate: JOIN com fact_sales ON responsible_user_id AND mes_venda = mes
      para comparar receita_acumulada vs. meta_receita no mes corrente
    - Write-back: as metas podem ser editadas via st.data_editor no Streamlit
      e o CSV recarregado com `dbt seed --select metas_vendedores`

  Coluna mes: DATE no formato YYYY-MM-01 (primeiro dia do mes) —
  mesma convencao de mes_venda em fact_sales para facilitar o JOIN.

  Consome: seeds/metas_vendedores.csv
*/

SELECT
    responsible_user_id::BIGINT         AS responsible_user_id,
    user_name,
    mes::DATE                           AS mes,
    meta_receita::NUMERIC(12, 2)        AS meta_receita

FROM {{ ref('metas_vendedores') }}
