
-- SELECT statement for Condition_code_coding
SELECT 
    condition_id,
    {{ protected_columns('SYSTEM') }},
    code,
    display,
    filename,
    processed_date 
FROM {{source('raw', 'condition_code_coding') }} x
QUALIFY rank() over(partition by filename order by processed_date desc) = 1
