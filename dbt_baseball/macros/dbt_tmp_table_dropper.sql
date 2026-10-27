{% macro dbt_tmp_table_dropper() %}

    {% set tmp_tables %}
        WITH dbt_tmp_tables AS (
            SELECT
            CONCAT('`', table_catalog,'.',table_schema,'.', table_name, '`') AS object
            FROM
            `region-us`.INFORMATION_SCHEMA.TABLES
            WHERE
            table_name LIKE '%dbt_tmp'
        )
        SELECT
            'DROP TABLE IF EXISTS ' || object as object
        FROM
            dbt_tmp_tables;
    {% endset %}

{% set drop_commands = run_query(tmp_tables).columns[0].values() %}

    {% if drop_commands %}
        {% for drop_command in drop_commands %}
            {% do log("Running... " + drop_command, True) %}
            {% do run_query(drop_command) %}
        {% endfor %}
    {% endif %}

{% endmacro %}