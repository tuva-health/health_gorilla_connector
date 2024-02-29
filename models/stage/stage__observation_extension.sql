
-- SELECT statement for Observation_extension
SELECT 
    observation_id,
    url,
    valuereference_reference,
    filename,
    processed_date 
FROM {{source('raw', 'observation_extension') }} x
QUALIFY rank() over(partition by filename order by processed_date desc) = 1
