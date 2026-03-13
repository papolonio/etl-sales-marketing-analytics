{#
  Sobrescreve o comportamento padrao do dbt que concatena o target.schema
  com o custom_schema (ex: public_staging).

  Com este macro:
    - models/staging  (+schema: staging) -> schema 'staging'
    - models/marts    (sem +schema)      -> schema 'public'  (target.schema)

  Ref: https://docs.getdbt.com/docs/build/custom-schemas#an-alternative-pattern-for-generating-schema-names
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
