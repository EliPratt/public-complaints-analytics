{#
  Use the custom schema name as-is ("staging", "marts") instead of dbt's
  default of prefixing it with the target schema ("main_staging").
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
