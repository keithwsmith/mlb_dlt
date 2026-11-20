{% macro log_test_results(results) %}
  {% if execute %}
    {% set tests = results | selectattr('node.resource_type', 'equalto', 'test') | list %}

    {% for chunk in tests | batch(500) %}
      {% set insert_sql %}
        insert into dbo.dbt_test_log (
            command_invocation_id, test_name, model_name, column_name,
            status, failures, execution_time_seconds, compiled_sql, run_started_at
        )
        values
        {% for r in chunk %}
            {# First model this test depends on (standard Jinja, no 'match' test) #}
            {% set ns = namespace(parent=none) %}
            {% for n in r.node.depends_on.nodes %}
              {% if ns.parent is none and n.startswith('model.') %}
                {% set ns.parent = n %}
              {% endif %}
            {% endfor %}
            (
              '{{ invocation_id }}',
              '{{ r.node.name }}',
              {% if ns.parent %}'{{ ns.parent.split(".")[1] }}.{{ ns.parent.split(".")[2] }}'{% else %}null{% endif %},
              {% if r.node.column_name %}'{{ r.node.column_name | replace("'", "''") }}'{% else %}null{% endif %},
              '{{ r.status }}',
              {{ r.failures if r.failures is not none else 'null' }},
              {{ r.execution_time }},
              '{{ (r.node.compiled_code or "") | replace("'", "''") }}',
              '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}'
            ){{ "," if not loop.last }}
        {% endfor %}
      {% endset %}
      {% do run_query(insert_sql) %}
    {% endfor %}
  {% endif %}
{% endmacro %}