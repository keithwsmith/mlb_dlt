{% macro createpk() %}
{% set sql %}
	IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dbo].[dim_game]') AND name = N'PK_dim_game')
   ALTER TABLE dbo.dim_game ADD CONSTRAINT PK_dim_game PRIMARY KEY clustered(gamePk);
{% endset %}

{% do run_query(sql) %}
{% do log("Primary Key Created", info=True) %}
{% endmacro %}