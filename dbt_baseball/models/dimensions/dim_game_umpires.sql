{{
    config(
     
         materialized='incremental',
   unique_key='game_umpire_key')
}}

with final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'game_pk',
			'_dlt_load_id',
			'official_id',
			'official_type'
        ]) }} as game_umpire_key,
        game_pk,
        official_type,
        official_id,
       COALESCE(NULLIF(LTRIM(RTRIM(full_name)), ''), 'Unknown') AS full_name,
		_dlt_load_id as load_id

      FROM {{ source('dw', 'umpires') }} u
	 {% if is_incremental() %}
        where _dlt_load_id > (select max(load_id) from {{ this }})
    {% endif %}

)

select *
from final