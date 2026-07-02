{#
    Logs each model built during THIS dbt invocation into dbo.pipeline
    individually (unchanged from before), AND aggregates test results by
    source table into ONE summary row per source — matching your existing
    "source_test_<table>" naming convention so manual test runs are directly
    comparable to that history in Failure Rates, instead of scattering into
    one row per individual test.

    Deliberately opt-in (guarded by the log_manual_run var in the on-run-end
    hook below) — see prior notes, unchanged.

    HOW GROUPING WORKS: for each test, dbt's depends_on.nodes lists the
    source(s)/model(s) it references. not_null/unique/accepted_values tests
    have exactly ONE dependency — unambiguous. relationships tests have TWO
    (the table being tested, and the table it references) — this macro uses
    depends_on.nodes[0] as the "primary" table for grouping, which is
    correct in practice but isn't a documented dbt guarantee for ordering.
    Spot-check a few relationship-test groupings once this is running.

    Tests with no source dependency (e.g. testing a dbt model, not a raw
    source) are skipped by this grouping — extend _source_group_name()
    if you also want those aggregated, e.g. into "model_test_<model>".
#}
{% macro _guess_layer(node_path) %}
    {%- set path_lower = node_path | lower -%}
    {%- if 'stg' in path_lower or 'staging' in path_lower -%}Stage
    {%- elif 'dim' in path_lower -%}Dimension
    {%- elif 'fact' in path_lower -%}Fact
    {%- elif 'mart' in path_lower -%}Mart
    {%- elif 'int' in path_lower or 'intermediate' in path_lower -%}Intermediate
    {%- else -%}{{ none }}
    {%- endif -%}
{% endmacro %}

{% macro _source_group_name(res) %}
    {%- set source_deps = [] -%}
    {%- for dep_id in res.node.depends_on.nodes -%}
        {%- if dep_id.startswith('source.') -%}
            {%- do source_deps.append(dep_id) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- if source_deps | length > 0 -%}
        {%- set parts = source_deps[0].split('.') -%}
        {{- parts[-1] -}}
    {%- else -%}
        {{- none -}}
    {%- endif -%}
{% endmacro %}

{% macro log_manual_dbt_run(results) %}
{% if execute %}
    {% set test_entries = [] %}

    {% for res in results %}
        {% if res.node.resource_type == 'model' %}
            {% set model_name = res.node.name %}
            {% set status = res.status %}
            {% set elapsed = res.execution_time %}
            {% set is_failure = status in ('error', 'fail', 'failed', 'failure', 'skipped') %}
            {% set error_msg = res.message if is_failure and res.message else none %}
            {% set layer = _guess_layer(res.node.original_file_path) | trim %}

            {% set end_count = none %}
            {% if status == 'success' %}
                {% set count_sql %}
                    select count(*) as cnt from {{ res.node.relation_name }}
                {% endset %}
                {% set count_result = run_query(count_sql) %}
                {% if count_result and count_result.rows | length > 0 %}
                    {% set end_count = count_result.rows[0]['cnt'] %}
                {% endif %}
            {% endif %}

            {% set insert_sql %}
                INSERT INTO dbo.pipeline
                    (run_id, model_name, model_layer, status, start_record_count,
                     end_record_count, records_added, elapsed_seconds,
                     start_time, end_time, error_message, created_at)
                VALUES
                    ('{{ invocation_id }}', '{{ model_name }}',
                     {{ "'" ~ layer ~ "'" if layer and layer != 'None' else 'NULL' }},
                     '{{ status }}', NULL,
                     {{ end_count if end_count is not none else 'NULL' }}, NULL,
                     {{ elapsed }},
                     SYSUTCDATETIME(), SYSUTCDATETIME(),
                     {{ "'" ~ (error_msg | replace("'", "''")) ~ "'" if error_msg else 'NULL' }},
                     SYSUTCDATETIME())
            {% endset %}

            {% do run_query(insert_sql) %}
            {{ log("Logged manual run of " ~ model_name ~ " (" ~ status ~ ") to dbo.pipeline", info=True) }}

        {% elif res.node.resource_type == 'test' %}
            {% set group = _source_group_name(res) | trim %}
            {% if group and group != 'None' %}
                {% do test_entries.append({
                    'group': group,
                    'status': res.status,
                    'elapsed': res.execution_time or 0,
                    'message': res.message,
                    'test_name': res.node.name
                }) %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {#- Aggregate collected test results into one summary row per source table -#}
    {% set group_names = test_entries | map(attribute='group') | unique | list %}
    {% for group_name in group_names %}
        {% set group_entries = test_entries | selectattr('group', 'equalto', group_name) | list %}
        {% set ns = namespace(has_failure=false, total_elapsed=0, fail_notes=[]) %}
        {% for e in group_entries %}
            {% set ns.total_elapsed = ns.total_elapsed + e.elapsed %}
            {% if e.status not in ('pass', 'success') %}
                {% set ns.has_failure = true %}
                {% do ns.fail_notes.append(e.test_name ~ ': ' ~ (e.message or e.status)) %}
            {% endif %}
        {% endfor %}
        {% set final_status = 'FAILURE' if ns.has_failure else 'SUCCESS' %}
        {% set combined_msg = (ns.fail_notes | join(' | '))[:1000] %}
        {% set agg_model_name = 'source_test_' ~ group_name %}

        {% set insert_sql %}
            INSERT INTO dbo.pipeline
                (run_id, model_name, model_layer, status, start_record_count,
                 end_record_count, records_added, elapsed_seconds,
                 start_time, end_time, error_message, created_at)
            VALUES
                ('{{ invocation_id }}', '{{ agg_model_name }}', 'source_test',
                 '{{ final_status }}', NULL, NULL, NULL,
                 {{ ns.total_elapsed }},
                 SYSUTCDATETIME(), SYSUTCDATETIME(),
                 {{ "'" ~ (combined_msg | replace("'", "''")) ~ "'" if combined_msg else 'NULL' }},
                 SYSUTCDATETIME())
        {% endset %}

        {% do run_query(insert_sql) %}
        {{ log("Logged aggregated test group '" ~ agg_model_name ~ "': " ~ final_status ~ " (" ~ (group_entries | length) ~ " tests)", info=True) }}
    {% endfor %}
{% endif %}
{% endmacro %}
