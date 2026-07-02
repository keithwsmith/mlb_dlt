{#
    Self-consistency audit: recomputes is_final and run_diff from dim_games's
    own base columns (detailed_state, home_score, away_score) and compares
    against the stored values. This needs zero knowledge of the raw
    dw.games source schema — everything here is a documented dim_games
    column (see dimensions_schema.yml):
        is_final:  "Derived: 1 if detailed_state is Final."
        run_diff:  "Derived: home_score minus away_score."

    Using compare_queries (via log_query_audit) rather than a hand-rolled
    WHERE a <> b check matters here specifically for NULL handling: run_diff
    is legitimately NULL when scores are unavailable, and a raw inequality
    check silently ignores NULL vs NULL mismatches due to three-valued
    logic. compare_queries' underlying INTERSECT/EXCEPT treats NULLs as
    equal to each other, which is the correct behavior for this check.

    NOTE: this only catches drift in is_final/run_diff. It does NOT
    validate winning_team_id/losing_team_id or game_type_desc — their exact
    tie-breaking/null-handling and code-to-description mapping isn't fully
    documented, so recomputing them here risked encoding the WRONG formula
    and generating false failures. Extend this once you can point at the
    actual CASE logic in dim_games.sql.
#}
{% macro audit_dim_games_derived_logic() %}

{% set expected_query %}
select
    game_pk,
    case when detailed_state = 'Final' then 1 else 0 end as is_final,
    home_score - away_score as run_diff
from {{ ref('dim_games') }}
{% endset %}

{% set stored_query %}
select
    game_pk,
    is_final,
    run_diff
from {{ ref('dim_games') }}
{% endset %}

{{ log_query_audit(
    model_name='dim_games',
    baseline_description='self-consistency: is_final/run_diff recomputed vs stored',
    a_query=expected_query,
    b_query=stored_query,
    columns_compared=2
) }}

{% endmacro %}
