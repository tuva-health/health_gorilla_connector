
-- SELECT statement for Coverage
SELECT 
    resourcetype,
    id,
    meta_versionid,
    meta_lastupdated,
    contained_0_resourcetype,
    contained_0_id,
    contained_0_patient_reference,
    contained_0_patient_display,
    contained_0_name_0_text,
    contained_0_name_0_family,
    contained_0_name_0_given_0,
    contained_0_address_0_use,
    contained_0_address_0_text,
    contained_0_address_0_line_0,
    contained_0_address_0_city,
    contained_0_address_0_state,
    contained_0_address_0_postalcode,
    contained_0_address_0_country,
    contained_0_address_0_period_start,
    contained_0_address_0_period_end,
    contained,
    identifier_0_value,
    status,
    subscriber_reference,
    beneficiary_reference,
    beneficiary_display,
    payor_0_reference,
    payor_0_display,
    class_0_type_coding_0_system,
    class_0_type_coding_0_code,
    class_0_type_coding_0_display,
    class_0_type_text,
    class_0_value,
    class_0_name,
    class_1_type_coding_0_system,
    class_1_type_coding_0_code,
    class_1_type_coding_0_display,
    class_1_type_text,
    class_1_value,
    order,
    filename,
    processed_date 
FROM {{source('raw', 'coverage') }} x
QUALIFY rank() over(partition by filename order by processed_date desc) = 1
