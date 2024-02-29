
-- SELECT statement for MedicationStatement_MedicationCodeableConcept_coding
SELECT 
    medicationstatement_id,
    "SYSTEM",
    code,
    display,
    filename,
    processed_date 
FROM {{source('raw', 'medicationstatement_medicationcodeableconcept_coding') }} x
QUALIFY rank() over(partition by filename order by processed_date desc) = 1
