
-- SELECT statement for DiagnosticReport_extension
SELECT 
    diagnosticreport_id,
    url,
    valuecodeableconcept_coding_0_system,
    valuecodeableconcept_coding_0_code,
    valuecodeableconcept_coding_0_display,
    valuecodeableconcept_text,
    valuereference_reference,
    filename,
    processed_date 
FROM {{source('raw', 'diagnosticreport_extension') }} x
QUALIFY rank() over(partition by filename order by processed_date desc) = 1
