
-- SELECT statement for Patient_address
SELECT 
    patient_id,
    use,
    text,
    line_0,
    line_1,
    city,
    state,
    postalcode,
    country,
    period_start,
    period_end,
    filename,
    processed_date 
FROM {{source('raw', 'patient_address') }} x
QUALIFY rank() over(partition by filename order by processed_date desc) = 1
