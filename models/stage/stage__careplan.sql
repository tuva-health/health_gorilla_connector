
-- SELECT statement for CarePlan
SELECT 
    resourcetype,
    id,
    meta_versionid,
    meta_lastupdated,
    intent,
    subject_reference,
    subject_display,
    extension,
    filename,
    processed_date 
FROM {{source('raw', 'careplan') }} x
QUALIFY rank() over(partition by filename order by processed_date desc) = 1
