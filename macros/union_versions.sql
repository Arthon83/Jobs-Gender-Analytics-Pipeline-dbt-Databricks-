{% macro union_versions(prefix, parts) %}
    {% for part in parts %}
        select *
        from {{ source('raw_data', prefix ~ '_' ~ part)}}
        {% if not loop.last %}
        union all
        {% endif %}
    {% endfor %}
{% endmacro %}