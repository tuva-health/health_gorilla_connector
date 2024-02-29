
-- SELECT statement for Immunization_contained
SELECT 
    immunization_id,
    resourcetype,
    id,
    name_0_text,
    name,
    name_0_family,
    name_0_given_0,
    name_0_given_1,
    filename,
    processed_date 
FROM {{source('raw', 'immunization_contained') }} x
QUALIFY rank() over(partition by filename order by processed_date desc) = 1
