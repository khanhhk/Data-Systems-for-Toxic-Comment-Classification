{% macro debug_sql(sql) %}
    {% set results = run_query(sql) %}
    {% for row in results %}
        {{ log(row, info=True) }}
    {% endfor %}
{% endmacro %}