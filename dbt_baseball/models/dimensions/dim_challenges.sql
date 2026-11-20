{{
    config(
        materialized='incremental',
        unique_key=['game_pk', 'at_bat_index', 'event_index']
    )
}}

{#
    One row per reviewed play event (manager challenge / umpire review).

    KEY CHANGE: the key used to include event_id, which is dlt's _dlt_id.
    dlt assigns a new _dlt_id every time a game is loaded, so reloading a
    game (play_events --reload) gave the same review a new key and the
    incremental merge ADDED a second row instead of updating the first.

    The key is now MLB's own identifiers for the event:
        game_pk + at_bat_index + event_index
    These stay the same when a game is reloaded, so a reload now updates the
    existing row. event_id is still kept as a column (it is updated to the
    latest load's value on each merge).

    If the same event appears more than once in the rows being processed
    (e.g. a game reloaded twice between runs), only the latest load is kept,
    so the merge never sees two source rows for one key.

    NOTE: rows whose game is later deleted from dw.play_events are NOT
    removed by an incremental run. Run with --full-refresh after deleting
    or narrowing raw play_events data.
#}

with source as (

    select
        e.game_pk,
        e.at_bat_index,
        e.event_index,
        e.review_player_link,
        e.has_review,
        e.review_challenge_team_id,
        e.review_is_overturned,
        e.review_player_name,
        e.review_player_id,
        e.review_type,
        e.start_time,
        e.event_id,
        e.load_id,
        row_number() over (
            partition by e.game_pk, e.at_bat_index, e.event_index
            order by e.load_id desc, e.event_id desc
        ) as rn

    from {{ ref('stg_play_events') }} e
    where e.has_review = 1
    {% if is_incremental() %}
        and e.load_id > (select max(load_id) from {{ this }})
    {% endif %}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'game_pk',
            'at_bat_index',
            'event_index'
        ]) }} as challenge_key,
        game_pk,
        at_bat_index,
        event_index,
        review_player_link,
        has_review,
        review_challenge_team_id,
        review_is_overturned,
        review_player_name,
        review_player_id,
        review_type,
        start_time,
        event_id,
        load_id

    from source
    where rn = 1

)

select *
from final
