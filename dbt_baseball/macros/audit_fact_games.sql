{% macro audit_fact_games() %}

{% set dim_games_query %}
select
    game_pk
from {{ ref('dim_games') }}
{% endset %}

{% set fact_games_query %}
select
    game_pk
from {{ ref('fact_games') }}
{% endset %}

{{ log_query_audit(
    model_name='fact_games',
    baseline_description='vs. dim_games',
    a_query=dim_games_query,
    b_query=fact_games_query
) }}

{% endmacro %}
