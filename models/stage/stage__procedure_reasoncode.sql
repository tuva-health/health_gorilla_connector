
-- SELECT statement for Procedure_reasonCode
SELECT 
    procedure_id,
    coding_0_system,
    coding_0_code,
    text,
    filename,
    processed_date 
FROM {{source('raw', 'procedure_reasoncode') }} x
QUALIFY rank() over(partition by filename order by processed_date desc) = 1
