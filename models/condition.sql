with coding as (
    select
          ccc.condition_id
        , ccc.code as code
        , case when ccc.{{ protected_columns('SYSTEM') }}  in (
            'http://hl7.org/fhir/sid/icd-10-cm'
            ,'http://hl7.org/fhir/sid/icd-10'
            ,'urn:oid:2.16.840.1.113883.3.623.1' -- this is weird, oid is for "us oncology", maintained by mckesson, but all codes are icd10cm
            ) then 'icd-10-cm'
        when ccc.{{ protected_columns('SYSTEM') }} = 'http://hl7.org/fhir/sid/icd-9'
            then 'icd-9-cm'
        when ccc.{{ protected_columns('SYSTEM') }} = 'http://snomed.info/sct'
            then 'snomed-ct'
        when ccc.{{ protected_columns('SYSTEM') }} = 'http://loinc.org'
            then 'loinc'
        else ccc.{{ protected_columns('SYSTEM') }}
        end as {{ protected_columns('SYSTEM') }}
        , ccc.display as display
    from {{ref('stage__condition_code_coding')}} ccc

)



,condition_code as (
    select
          cc.condition_id
        , cc.code
        , cc.{{ protected_columns('SYSTEM') }}
        , cc.display
    from coding cc
    qualify row_number() over(partition by condition_id order by
        case {{ protected_columns('SYSTEM') }}
            when 'icd-10-cm' then 0
            when 'snomed-ct' then 1
            when 'icd-9-cm' then 2
            when 'loinc' then 3
            else 4 end
        , code
        ) = 1
)

select
      cast(c.id as {{ dbt.type_string() }} ) as condition_id
    , cast(p.IDENTIFIER_1_VALUE as {{ dbt.type_string() }} ) as patient_id
    , cast(null as {{ dbt.type_string() }} ) as encounter_id
    , cast(null as {{ dbt.type_string() }} ) as claim_id
    , {{ try_to_cast_date('c.recordeddate', 'YYYY-MM-DD') }} as recorded_date
    , {{ try_to_cast_date('c.onsetdatetime', 'YYYY-MM-DD') }} as onset_date
    , cast(null as date) as resolved_date
    , cast(c.CLINICALSTATUS_CODING_0_DISPLAY as {{ dbt.type_string() }} ) as status
    , cast(
        case when CATEGORY_0_CODING_0_CODE in ('75326-9','55607006')
            then 'problem'
        when CATEGORY_0_CODING_0_CODE in ('282291009','29308-4')
            then 'diagnosis'
        when CATEGORY_0_CODING_0_CODE = '64572001'
            then 'disease'
        end as {{ dbt.type_string() }} ) as condition_type
    , cast(cc.{{ protected_columns('SYSTEM') }} as {{ dbt.type_string() }} ) as source_code_type
    , cast(cc.code as {{ dbt.type_string() }} ) as source_code
    , cast(cc.display as {{ dbt.type_string() }} ) as source_description
    , cast(case
            when icd10.icd_10_cm is not null then 'icd-10-cm'
            when icd9.icd_9_cm is not null then 'icd-9-cm'
            when loinc.loinc is not null then 'loinc'
--             when snomed.conceptid is not null then 'snomed-ct'
            end
        as {{ dbt.type_string() }} ) as normalized_code_type
    , cast(coalesce(replace(icd10.icd_10_cm,'.',''), icd9.icd_9_cm, loinc.loinc)  as {{ dbt.type_string() }} ) as normalized_code
    , cast(coalesce(icd10.description, icd9.long_description, loinc.long_common_name) as {{ dbt.type_string() }} ) as normalized_description
    , cast(null as {{ dbt.type_int() }} ) as condition_rank
    , cast(null as {{ dbt.type_string() }} ) as present_on_admit_code
    , cast(null as {{ dbt.type_string() }} ) as present_on_admit_description
    , cast('healthgorilla' as {{ dbt.type_string() }} ) as data_source
from {{ref('stage__condition')}} c
left join {{ref('stage__patient')}} p
    on right(c.SUBJECT_REFERENCE,24) = p.id
left join condition_code cc
    on c.id = cc.condition_id
left join {{ref('terminology__icd_10_cm')}} icd10
    on cc.{{ protected_columns('SYSTEM') }}  = 'icd-10-cm' and replace(cc.code,'.','') = icd10.icd_10_cm
left join {{ref('terminology__icd_9_cm')}} icd9
    on cc.{{ protected_columns('SYSTEM') }}  = 'icd-9-cm' and cc.code = icd9.icd_9_cm
left join {{ref('terminology__loinc')}} loinc
    on cc.{{ protected_columns('SYSTEM') }} = 'loinc' and cc.code = loinc.loinc
-- left join { { source('term','snomed') } } snomed
--     on cc.{{ protected_columns('SYSTEM') }} = 'snomed-ct' and cc.code = snomed.conceptid

