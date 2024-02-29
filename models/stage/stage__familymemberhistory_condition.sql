
-- SELECT statement for FamilyMemberHistory_condition
SELECT 
    familymemberhistory_id,
    code_coding_0_system,
    code_coding_0_code,
    code_text,
    filename,
    processed_date 
FROM {{source('raw', 'familymemberhistory_condition') }} x
QUALIFY rank() over(partition by filename order by processed_date desc) = 1
