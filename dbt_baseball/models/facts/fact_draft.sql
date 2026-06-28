{{
    config(
        materialized = 'table',
    )
}}

/*
    fact_draft
    ----------
    One row per draft pick event.
    Contains ONLY meaningfully aggregatable measures at the pick grain.

    Additive:      pick_value, signing_bonus
    Semi-additive: pick_number, round_pick_number, rank, current_age_at_draft

    Everything else (player names, team, school, position, flags, text)
    lives in dim_draft and is accessed via draft_key.

    Source: dw.draft  →  dim_draft (surrogate key)
*/

with source as (

    select * from {{ source('dw', 'draft') }}

),
deduped as (

    select *
    from (

        select
            *,
            row_number() over (
                partition by
                    person__id,
                    person__draft_year,
                    pick_number,
                    round_pick_number
                order by _dlt_load_id desc
            ) as rn
        from source

    ) x
    where rn = 1

),


dim as (

    select * from {{ ref('dim_draft') }}

),

fact_draft as (

    select

        -- ── Foreign key ─────────────────────────────────────────────────
        d.draft_key,
		s.person__draft_year											as person__draft_year,
		d.person_id														as person_id,

        -- ── Pick position (semi-additive: avg / min / max) ──────────────
        cast(s.pick_number as int)                          as pick_number,
        cast(s.round_pick_number as int)                    as round_pick_number,

        -- ── Rank (semi-additive: avg / min / max) ───────────────────────
        cast(s.rank as int)                                 as rank,

        -- ── Financials (fully additive: sum / avg) ──────────────────────
        try_cast(
            replace(replace(s.pick_value, '$', ''), ',', '')
            as decimal(18,2)
        )                                                   as pick_value,

        try_cast(
            replace(replace(s.signing_bonus, '$', ''), ',', '')
            as decimal(18,2)
        )                                                   as signing_bonus,

        -- ── Age (semi-additive: avg / distribution) ─────────────────────
        cast(s.person__current_age as int)                  as current_age

     from deduped s

    inner join dim d
        on s.person__id         = d.person_id
       and s.person__draft_year = d.person__draft_year
       and s.pick_number        = d.pick_number
       and s.round_pick_number  = d.round_pick_number

)

select * from fact_draft