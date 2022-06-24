{% macro cent_to_dollars(column_name, decimal_places=2) %}
    round( 0.1 * {{column_name}}/100, {{decimal_places}})
{%- endmacro %}