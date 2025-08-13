WITH cleaned AS (
  SELECT *,
    CASE
      WHEN input_ids LIKE concat('{', '%') AND input_ids LIKE concat('%', '}') THEN
        replace(replace(input_ids, '{', '['), '}', ']')
      ELSE input_ids
    END AS input_ids_json,

    CASE
      WHEN attention_mask LIKE concat('{', '%') AND attention_mask LIKE concat('%', '}') THEN
        replace(replace(attention_mask, '{', '['), '}', ']')
      ELSE attention_mask
    END AS attention_mask_json
  FROM {{ source('staging_source', 'test_1') }}
),
filtered AS (
  SELECT *
  FROM cleaned
  WHERE
    jsonb_typeof(input_ids_json::jsonb) IS DISTINCT FROM 'array'
    OR jsonb_array_length(input_ids_json::jsonb) < 1
    OR jsonb_typeof(attention_mask_json::jsonb) IS DISTINCT FROM 'array'
    OR jsonb_array_length(attention_mask_json::jsonb) < 1
)

SELECT *
FROM filtered
