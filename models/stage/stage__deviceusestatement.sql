
-- SELECT statement for DeviceUseStatement
SELECT 
    resourcetype,
    id,
    meta_versionid,
    meta_lastupdated,
    subject_reference,
    subject_display,
    derivedfrom_0_reference,
    source_reference,
    filename,
    processed_date 
FROM {{source('raw', 'deviceusestatement') }} x
QUALIFY rank() over(partition by filename order by processed_date desc) = 1
