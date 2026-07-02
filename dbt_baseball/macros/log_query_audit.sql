{#
    Shared logic for compare_queries()-based audits. Runs the comparison,
    prints the summary table to the console, and logs a row into
    dbo.transformation_audit_log so ETLMonitor's Model Audit tab picks it up.

    Usage: call this from a thin per-model macro (see audit_games.sql /
    audit_fact_games.sql) so `dbt run-operation audit_<model>` still works
    per model, but the actual comparison/logging logic lives in one place.
#}
{% macro log_query_audit(model_name, baseline_description, a_query, b_query, columns_compared=1) %}

{% set audit_query = audit_helper.compare_queries(
    a_query=a_query,
    b_query=b_query
) %}

{% set results = run_query(audit_query) %}

{% if execute %}
    {% do results.print_table() %}

    {#- compare_queries (summarize=true) returns rows shaped like:
        in_a | in_b | count | percent_of_total
        NOTE: {% set %} inside a {% for %} loop in Jinja2 is scoped to that
        single iteration — it does NOT persist across iterations. namespace()
        is required, or these accumulators silently stay at 0. -#}
    {% set ns = namespace(matched_rows=0, missing_from_a=0, missing_from_b=0) %}
    {% for row in results.rows %}
        {% if row['in_a'] and row['in_b'] %}
            {% set ns.matched_rows = ns.matched_rows + row['count'] %}
        {% elif row['in_b'] and not row['in_a'] %}
            {% set ns.missing_from_a = ns.missing_from_a + row['count'] %}
        {% elif row['in_a'] and not row['in_b'] %}
            {% set ns.missing_from_b = ns.missing_from_b + row['count'] %}
        {% endif %}
    {% endfor %}
    {% set total_rows = ns.matched_rows + ns.missing_from_a + ns.missing_from_b %}
    {% set pct = (ns.matched_rows / total_rows * 100) if total_rows > 0 else none %}

    {% set insert_sql %}
        INSERT INTO dbo.transformation_audit_log
            (model_name, baseline_description, rows_compared, columns_compared,
             perfect_match_pct, conflicting_values, missing_from_a, missing_from_b)
        VALUES
            ('{{ model_name }}', '{{ baseline_description }}', {{ total_rows }}, {{ columns_compared }},
             {{ pct if pct is not none else 'NULL' }},
             0, {{ ns.missing_from_a }}, {{ ns.missing_from_b }})
    {% endset %}

    {% do run_query(insert_sql) %}
    {{ log("Logged transformation audit for " ~ model_name ~ ": " ~ pct ~ "% match", info=True) }}
{% endif %}

{% endmacro %}
