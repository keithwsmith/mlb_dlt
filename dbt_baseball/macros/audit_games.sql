{% macro audit_games() %}

{% set dw_games_query %}
select
    game_pk
from dw.games
{% endset %}

{% set dim_games_query %}
select
    game_pk
from {{ ref('dim_games') }}
{% endset %}

{{ log_query_audit(
    model_name='dim_games',
    baseline_description='vs. dw.games (raw source)',
    a_query=dw_games_query,
    b_query=dim_games_query
) }}

{% endmacro %}
