-- No two versions for the same player should have overlapping
-- date ranges. A row here means effective/end dates are broken.

with versions as (
    select
        player_id,
        version_number,
        effective_date,
        end_date,
        lead(effective_date) over (
            partition by player_id
            order by version_number
        ) as next_effective_date
    from {{ ref('dim_players') }}
)

select *
from versions
where next_effective_date is not null
  and end_date >= next_effective_date
