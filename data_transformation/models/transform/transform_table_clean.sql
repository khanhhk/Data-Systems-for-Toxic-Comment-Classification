{{ config(
    materialized='table',
    alias='table_clean',
    post_hook=[
      "CREATE INDEX IF NOT EXISTS idx_table_clean_labels ON {{ this }} (labels)",
      "ANALYZE {{ this }}"
    ]
) }}

WITH src AS (
  SELECT
    labels,
    -- Chuyển đổi '{101,102}' → '[101,102]'
    replace(replace(input_ids, '{', '['), '}', ']') AS input_ids_json,
    replace(replace(attention_mask, '{', '['), '}', ']') AS attention_mask_json
  FROM {{ source('staging_source', 'test_1') }}
),
valid AS (
  SELECT
    labels,
    input_ids_json::jsonb AS input_ids,
    attention_mask_json::jsonb AS attention_mask
  FROM src
  WHERE
    input_ids_json LIKE concat('[', '%') AND input_ids_json LIKE concat('%', ']')
    AND attention_mask_json LIKE concat('[', '%') AND attention_mask_json LIKE concat('%', ']')
    AND length(regexp_replace(input_ids_json, '[^,]', '', 'g')) + 1 >= 1
    AND length(regexp_replace(attention_mask_json, '[^,]', '', 'g')) + 1 >= 1
)

SELECT
  labels,
  input_ids,
  attention_mask,
  now() AS dbt_loaded_at
FROM valid
