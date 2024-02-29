
-- SELECT statement for FamilyMemberHistory
SELECT 
    resourcetype,
    id,
    meta_versionid,
    meta_lastupdated,
    extension_0_url,
    extension_0_valuereference_reference,
    status,
    patient_reference,
    patient_display,
    relationship_coding_0_system,
    relationship_coding_0_code,
    relationship_coding_0_display,
    relationship_text,
    sex_coding_0_system,
    sex_coding_0_code,
    sex_coding_0_display,
    sex_text,
    deceasedboolean,
    filename,
    processed_date 
FROM {{source('raw', 'familymemberhistory') }} x
QUALIFY rank() over(partition by filename order by processed_date desc) = 1
