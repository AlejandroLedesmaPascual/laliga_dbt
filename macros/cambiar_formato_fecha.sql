{% macro cambiar_formato_fecha(column_name) %}
    CONVERT_TIMEZONE('UTC', {{ column_name }})
{% endmacro %}