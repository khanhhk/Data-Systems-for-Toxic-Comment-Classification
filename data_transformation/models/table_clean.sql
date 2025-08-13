SELECT *
FROM production.test
WHERE jsonb_typeof(input_ids) = 'array'
  AND jsonb_array_length(input_ids) >= 1
  AND jsonb_typeof(attention_mask) = 'array'
  AND jsonb_array_length(attention_mask) >= 1