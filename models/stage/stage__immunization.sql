
-- SELECT statement for Immunization
SELECT 
    resourcetype,
    id,
    meta_versionid,
    meta_lastupdated,
    extension,
    status,
    vaccinecode_coding_0_system,
    vaccinecode_coding_0_code,
    vaccinecode_coding_0_display,
    vaccinecode_text,
    patient_reference,
    patient_display,
    occurrencedatetime,
    dosequantity_value,
    dosequantity_unit,
    performer_0_actor_reference,
    manufacturer_reference,
    lotnumber,
    route_coding_0_system,
    route_coding_0_code,
    performer_1_actor_reference,
    location_reference,
    filename,
    processed_date 
FROM {{source('raw', 'immunization') }} x
QUALIFY rank() over(partition by filename order by processed_date desc) = 1
