{% macro check_index_exists(schema_name,table_name,index_name) %}
    {% set index_check_query -%}
    SELECT COUNT(*) AS index_count
    FROM all_indexes
    WHERE owner = '{{ schema_name }}'
    AND table_name = '{{ table_name }}'
    AND index_name = '{{ index_name }}'
    {%- endset %}

    {% set result = run_query(index_check_query) %}

    {% if result and result.columns[0] == "INDEX_COUNT" and result.data[0][0] > 0 -%}
        {{ return(true) }}
    {%- else -%}
        {{ return(false) }}
    {%- endif %}
{% endmacro %}
