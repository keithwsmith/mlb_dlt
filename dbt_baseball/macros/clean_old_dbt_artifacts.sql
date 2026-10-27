{% macro clean_old_dbt_artifacts(retention_days=60) %}

    {% set tables_to_clean = [
        'fct_dbt__invocations',
        'fct_dbt__model_executions',
        'fct_dbt__seed_executions',
        'fct_dbt__snapshot_executions',
        'fct_dbt__test_executions'
    ] %}

    {# Get target database and schema configured for the package #}
    {% set target_database = var('dbt_artifacts_database', target.database) %}
    {% set target_schema = var('dbt_artifacts_schema', target.schema) %}
	 {{ log("Starting cleanup of dbt_artifacts tables in " ~ target_database ~ "." ~ target_schema ~ " for records older than " ~ retention_days ~ " days.", info=True) }}

    {% for table_name in tables_to_clean %}
        {% set relation = adapter.get_relation(database=target_database, schema=target_schema, identifier=table_name) %}
        
        {% if relation is not none %}
            {% set purge_query %}
                -- fct_dbt__invocations filters on run_started_at; execution tables filter on compile_started_at
                DELETE FROM {{ relation }}
                WHERE {% if table_name == 'fct_dbt__invocations' %} run_started_at {% else %} compile_started_at {% endif %} < CURRENT_TIMESTAMP() - INTERVAL '{{ retention_days }} days';
            {% endset %}
            
            {{ log("Purging old data from " ~ table_name ~ "...", info=True) }}
            {% do run_query(purge_query) %}
			 {% else %}
            {{ log("Table " ~ table_name ~ " does not exist. Skipping.", info=True) }}
        {% endif %}
    {% endfor %}

    {{ log("dbt_artifacts metadata cleanup complete.", info=True) }}

{% endmacro %}