
-- SELECT statement for DeviceUseStatement_extension
SELECT 
    device_use_statement_id,
    url,
    valuereference_reference,
    filename,
    processed_date 
FROM {{source('raw', 'deviceusestatement_extension') }} x
QUALIFY rank() over(partition by filename order by processed_date desc) = 1
