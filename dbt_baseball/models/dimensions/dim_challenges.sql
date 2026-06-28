{{
    config(
        materialized='incremental',
        unique_key=['game_pk', 'start_time','event_id']
    )
}}

with final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'game_pk',
			'start_time',
			'event_id'
        ]) }} as challenge_key,
        e.game_pk,
        e.review_player_link,
        e.has_review,
        e.review_challenge_team_id,
        e.review_is_overturned,
        e.review_player_name,
        e.review_player_id,
        e.review_type,
		e.start_time,
		e.event_id,
		e.load_id

    from {{ ref('stg_play_events') }} e
    where e.has_review =1
	 {% if is_incremental() %}
        and load_id > (select max(load_id) from {{ this }})
    {% endif %}

)

select *
from final