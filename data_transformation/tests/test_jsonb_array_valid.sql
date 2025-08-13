SELECT *
FROM production.test
WHERE jsonb_typeof(input_ids) IS DISTINCT FROM 'array'
   OR jsonb_array_length(input_ids) < 1
   OR jsonb_typeof(attention_mask) IS DISTINCT FROM 'array'
   OR jsonb_array_length(attention_mask) < 1