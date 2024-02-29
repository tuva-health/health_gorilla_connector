
-- SELECT statement for CarePlan_contained
SELECT 
    careplan_id,
    resourcetype,
    id,
    name_0_text,
    identifier_0_system,
    identifier_0_value,
    identifier_0_assigner_display,
    class_system,
    class_code,
    class_display,
    type_0_coding_0_system,
    type_0_coding_0_code,
    type_0_coding_1_system,
    type_0_coding_1_code,
    type_0_coding_2_system,
    type_0_coding_2_code,
    type_0_coding_3_system,
    type_0_coding_3_code,
    type_0_text,
    subject_reference,
    participant_0_type_0_coding_0_system,
    participant_0_type_0_coding_0_code,
    participant_0_type_0_coding_0_display,
    participant_0_type_0_text,
    participant_0_individual_reference,
    period_start,
    period_end,
    location_0_location_reference,
    location_0_location_display,
    identifier_0_type_coding_0_system,
    identifier_0_type_coding_0_code,
    identifier_0_type_coding_0_display,
    identifier_0_type_text,
    identifier_1_system,
    identifier_1_value,
    name_0_family,
    name_0_given_0,
    name_0_given_1,
    name,
    telecom_0_system,
    telecom_0_value,
    telecom_0_use,
    telecom_1_system,
    telecom_1_value,
    telecom_1_use,
    address_0_use,
    address_0_text,
    address_0_line_0,
    address_0_line_1,
    address_0_city,
    address_0_state,
    address_0_postalcode,
    address_use,
    address_text,
    address_line_0,
    address_line_1,
    address_city,
    address_state,
    address_postalcode,
    address_country,
    meta_profile_0,
    status,
    code_coding_0_system,
    code_coding_0_code,
    code_coding_0_display,
    code_text,
    performedperiod_end,
    performer_0_actor_reference,
    reasoncode_0_coding_0_system,
    reasoncode_0_coding_0_code,
    reasoncode_0_text,
    followup_0_coding_0_system,
    followup_0_coding_0_code,
    followup_0_coding_0_display,
    followup_0_text,
    address_0_country,
    performedperiod_start,
    filename,
    processed_date 
FROM {{source('raw', 'careplan_contained') }} x
QUALIFY rank() over(partition by filename order by processed_date desc) = 1