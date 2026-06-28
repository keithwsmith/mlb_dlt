with pitch_types as (

    select distinct
        pitch_type_code,
        pitch_type_description,
        pitch_trail_color

    from {{ ref('stg_play_events') }}
    where pitch_type_code is not null

)

select
    pitch_type_code,
    pitch_type_description,
    pitch_trail_color,
    case pitch_type_code
        when 'FF' then 'Fastball'
        when 'SI' then 'Fastball'
        when 'FC' then 'Fastball'
        when 'SL' then 'Breaking'
        when 'CU' then 'Breaking'
        when 'KC' then 'Breaking'
        when 'SV' then 'Breaking'
        when 'ST' then 'Breaking'
        when 'CH' then 'Offspeed'
        when 'FS' then 'Offspeed'
        when 'FO' then 'Offspeed'
        when 'SC' then 'Offspeed'
        when 'KN' then 'Offspeed'
        when 'EP' then 'Offspeed'
        when 'CS' then 'Breaking'
        else 'Other'
    end as pitch_category

from pitch_types
