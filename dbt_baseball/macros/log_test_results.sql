{% macro log_test_results(results) %}
  
  -- Filter results to only look at test nodes
  {% set test_results = [] %}
  {% for res in results %}
    {% if res.node.resource_type == 'test' %}
      {% do test_results.append(res) %}
    {% endif %}
  {% endfor %}

  -- If no tests ran, exit early
  {% if test_results | length > 0 %}
    
    {% set insert_query %}
    INSERT INTO dbo.dbt_test_log (
        command_invocation_id, 
        test_name, 
        model_name, 
        column_name, 
        status, 
        failures, 
        execution_time_seconds, 
        compiled_sql
    )
    VALUES 
    {% for res in test_results %}
      -- Safe extraction of column name if it exists, otherwise NULL
      {% set col_name = res.node.column_name if res.node.column_name else 'NULL' %}
      -- Clean up compiled SQL strings for T-SQL compatibility
      {% set cleaned_sql = res.node.compiled_code | replace("'", "''") if res.node.compiled_code else 'NULL' %}
      
      (
        '{{ invocation_id }}',
        '{{ res.node.name }}',
        '{{ res.node.attached_node | replace("model.", "") }}',
        {% if col_name == 'NULL' %} NULL {% else %} '{{ col_name }}' {% endif %},
        '{{ res.status }}',
        {{ res.failures if res.failures is not none else 'NULL' }},
        {{ res.execution_time }},
        {% if cleaned_sql == 'NULL' %} NULL {% else %} N'{{ cleaned_sql }}' {% endif %}
      )
      {% if not loop.last %}, {% endif %}
    {% endfor %};
    {% endset %}

    -- Execute the insertion into SQL Server
    {% do run_query(insert_query) %}
    {{ log("Successfully logged " ~ test_results | length ~ " test results to SQL Server.", info=True) }}

  {% endif %}

{% endmacro %}