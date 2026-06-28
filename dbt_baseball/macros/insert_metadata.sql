-- Description: This macro counts the number of rows in a table and inserts the count into a metadata table
--use this macro at the end of every model you want to be included in the metadata table (using post-hook)

{% macro insert_metadata(tableN,run_type) %}
	{% set my_quote = "'" %}
    {{log("Starting insert_metadata", info=true)}}
	{% set tableName = tableN %}
	{% set hook_type_quoted = "'" ~ run_type ~ "'"%}
	{{log("insert_metadata tableName:", info=true)}}
	{{log(tableName, info=true)}}
	{{log("insert_metadata hook_type_quoted:", info=true)}}
	{{log(hook_type_quoted, info=true)}}

	{% set tableNameQuoted = "'" ~ tableName ~ "'"%}
	{{log("tableNameQuoted:", info=true)}}
	{{log(tableNameQuoted, info=true)}}
	
	{{log("Executing count_query macro", info=true)}}
    {% set count_query %}
       
        SELECT COUNT(*) AS count FROM {{ this }}
    {% endset %}
	{{log("count_query:", info=true)}}
	{{log(count_query, info=true)}}

	{% set count_result = run_query(count_query) %}
    
    {% if execute %}
        {{log("count_result:", info=true)}}
		{{log(count_result, info=true)}}
        {# 
			if we got results from the count query, insert the count into the metadata table
        #}
		
		{% if count_result|length > 0 %}
            {% set count_result_value = count_result.columns[0].values()[0] %}
            {{log("count_result_value:", info=true)}}
			{#
				query definition: insert the metadata into the metadata table
            #}
			{% set insert_query %}
                INSERT INTO dbo.audit (RECORDS_COUNT, HAPPENED_AT, DESCRIPTION,RUN_TYPE)
                VALUES
                (
                    {{count_result_value}} , getdate(), {{ tableNameQuoted }}, {{ hook_type_quoted }}
					                                               
                )
            {% endset %}
			
			{{log("insert_query:", info=true)}}
			{{log(insert_query, info=true)}}
            {{log("now we run", info=true)}}
            {% set insert_result = run_query(insert_query) %}
			{{log("insert_result:", info=true)}}
            {{log(insert_result, info=true)}}

        {% endif %}
	{% else %}
		{% set count_result_value = 0 %}
    {% endif %}
	
{% endmacro %}