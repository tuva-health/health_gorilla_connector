
-- SELECT statement for Observation
SELECT 
    resourcetype,
    id,
    meta_versionid,
    meta_lastupdated,
    meta_profile_0,
    category_0_text,
    category_0_coding_0_code,
    category_0_coding_0_display,
    category_0_coding_0_system,
    code_coding_0_system,
    code_coding_0_code,
    code_coding_0_display,
    subject_reference,
    subject_display,
    performer_0_reference,
    performer_0_display,
    valuecodeableconcept_coding_0_display,
    valuecodeableconcept_coding_0_system,
    valuecodeableconcept_coding_0_code,
    note_0_text,
    issued,
    effectivedatetime,
    effectiveperiod_start,
    effectiveperiod_end,
    identifier_0_value,
    identifier_0_assigner_display,
    status,
    specimen_reference,
    referencerange_0_text,
    code_text,
    valuestring,
    valuequantity_value,
    valuequantity_unit,
    referencerange_0_low_value,
    referencerange_0_low_unit,
    referencerange_0_high_value,
    referencerange_0_high_unit,
    interpretation_0_coding_0_system,
    interpretation_0_coding_0_code,
    interpretation_0_coding_0_display,
    interpretation_0_coding_1_system,
    interpretation_0_coding_1_code,
    interpretation_0_coding_1_display,
    interpretation_0_coding_2_system,
    interpretation_0_coding_2_code,
    interpretation_0_coding_2_display,
    interpretation_0_coding,
    interpretation_0_text,
    filename,
    processed_date 
FROM {{source('raw', 'observation') }} x
QUALIFY rank() over(partition by filename order by processed_date desc) = 1
