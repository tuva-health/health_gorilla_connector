
-- SELECT statement for Organization
SELECT 
    resourcetype,
    id,
    meta_versionid,
    meta_lastupdated,
    text_status,
    text_div,
    identifier_0_system,
    identifier_0_value,
    active,
    type_0_coding_0_system,
    type_0_coding_0_code,
    type_0_coding_0_display,
    type_0_text,
    type_1_coding_0_system,
    type_1_coding_0_code,
    type_1_coding_0_display,
    type_1_text,
    type,
    name,
    filename,
    processed_date 
FROM {{source('raw', 'organization') }} x
QUALIFY rank() over(partition by filename order by processed_date desc) = 1
