{% macro grant_select(schema = target.schema, role=target.role) %}
 
    {% set name %}
        grant usage on schema {{schema}} to role {{role}};
        grant select on all tables in schema {{schema}} to role {{role}};
        grant select on all views in schema {{schema}} to role {{role}};
    {% endset %}

    {{ log('Granitng select on all tables and views in schema ' ~ schema ~ 'to role' ~ role, info=True) }}
    {% do run_query(query) %}
    {{ log('Priviledges granted!', info= True) }}

{% endmacro %}