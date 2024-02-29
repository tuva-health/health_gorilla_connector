
-- SELECT statement for Observation_hasMember
SELECT 
    observation_id,
    reference,
    display,
    filename,
    processed_date 
FROM {{source('raw', 'observation_hasmember') }} x
QUALIFY rank() over(partition by filename order by processed_date desc) = 1
