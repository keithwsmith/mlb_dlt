{% macro insert_into_table(table_name, column_names, values) %}

    INSERT INTO {{ table_name }} ({{ column_names }})

    VALUES ({{ values }});

{% endmacro %}