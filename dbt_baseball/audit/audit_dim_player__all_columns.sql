-- =============================================================
-- Audit test: dim_player — compare_all_columns
-- Compares current (is_current = 1) player records against
-- the source to verify key attributes are carried through.
-- =============================================================

{%- set source_query -%}
    SELECT
        id                                  AS player_id,
        full_name,
        first_name,
        last_name,
        TRY_CAST(birth_date AS DATE)        AS birth_date,
        birth_city,
        birth_country,
        height,
        weight,
        primary_position__code              AS primary_position_code,
        primary_position__name              AS primary_position_name,
        bat_side__code                      AS bat_side_code,
        pitch_hand__code                    AS pitch_hand_code,
        TRY_CAST(mlb_debut_date AS DATE)    AS mlb_debut_date,
        active                              AS is_active
    FROM {{ source('dw', 'mlbplayers') }}
{%- endset -%}

{%- set model_query -%}
    SELECT
        player_id,
        full_name,
        first_name,
        last_name,
        birth_date,
        birth_city,
        birth_country,
        height,
        weight,
        primary_position_code,
        primary_position_name,
        bat_side_code,
        pitch_hand_code,
        mlb_debut_date,
        is_active
    FROM {{ ref('dim_player') }}
    WHERE is_current = 1
{%- endset -%}

{{ audit_helper.compare_all_columns(
    a_query     = model_query,
    b_query     = source_query,
    primary_key = 'player_id'
) }}
