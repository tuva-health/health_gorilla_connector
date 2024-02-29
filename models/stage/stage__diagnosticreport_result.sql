
-- SELECT statement for DiagnosticReport_result
SELECT 
    diagnosticreport_id,
    reference,
    display,
    filename,
    processed_date 
FROM {{source('raw', 'diagnosticreport_result') }} x
QUALIFY rank() over(partition by filename order by processed_date desc) = 1
