


{# --To comment a block of code
--Define a variable in Jinja and how to use it:
{% set my_string = 'This is a test!' %}

{{ my_string}} I want to use my variable here in the text.

#}


-- Create a list
{% set my_letters = ['a','b','c','d'] %}

{{ my_letters[0] }}
{{ my_letters[1] }}
{{ my_letters[2] }}


-- For loop
{% for letter in my_letters %}
    my favorite letter is {{letter}}
{% endfor %}



{#

-- Whitespace Control
-- We can control for whitespace by adding a single dash on either side of the Jinja delimiter. 
-- This will trim the whitespace between the Jinja delimiter on that side of the expression.

-- Usage of for loop and if together
{% set foods= ['carrot','hotdog','cucumber','bell pepper'] %}

{% for food in foods -%}
    {%-if food =='hotdog'-%}
        {%- set food_type='snack' -%}
    {%else%}
        {%- set food_type='vegatable' -%}
    {%-endif-%}
    
    The {{food}} is my favorite {{food_type}}

{%endfor%}
#}

--Other variables such as dictionary
{#
{%
    set my_dic = {
        'word1': 'definition1',
        'word2': 'definition2',
        'word3': 'definition3'
    }
%}

{{my_dic.word2}}

{%-for word in my_dic%}
      
      {{my_dic[word]}} 
      
{%-endfor%}
#}








