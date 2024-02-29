
-- SELECT statement for CarePlan_activity
SELECT 
    careplan_id,
    reference_reference,
    detail_extension_0_url,
    detail_extension_0_valuecodeableconcept_coding_0_system,
    detail_extension_0_valuecodeableconcept_coding_0_code,
    detail_extension_0_valuecodeableconcept_coding_0_display,
    detail_extension_0_valuecodeableconcept_text,
    detail_code_coding_0_system,
    detail_code_coding_0_code,
    detail_code_coding_0_display,
    detail_code_text,
    detail_description,
    detail_scheduledperiod_start,
    filename,
    processed_date 
FROM {{source('raw', 'careplan_activity') }} x
QUALIFY rank() over(partition by filename order by processed_date desc) = 1
