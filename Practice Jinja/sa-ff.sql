{% set brands = ['SA','FF'] %}


{%- for brand in brands -%}
   
    select * from exerp_{{brand}}_subscription_sale
    {%- if not loop.last %}
          union all 
      {% endif %}

{%-endfor%}