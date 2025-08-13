{{ config(
    materialized='table',
    alias='test',
    post_hook=[
      "CREATE INDEX IF NOT EXISTS idx_test_labels ON {{ this }} (labels)",
      "ANALYZE {{ this }}"
    ]
) }}

with src as (
  select *
  from {{ source('staging_source', 'test_1') }}
)

select
  labels::int as labels,
  to_jsonb(input_ids) as input_ids,
  to_jsonb(attention_mask) as attention_mask,
  now() as dbt_loaded_at
from src