
select
      cast(id as varchar) as lab_result_id
    , cast(patient_id as varchar ) as patient_id
    , cast(null as varchar ) as encounter_id
    , cast(null as varchar ) as accession_number
    , cast(code_type as varchar ) as source_code_type
    , cast(code as varchar ) as source_code
    , cast(description as varchar ) as source_description
    , cast(null as varchar ) as source_component
    , cast(case
            when loinc_code is not null then 'loinc'
            when snomed_code is not null then 'snomed'
            end as varchar ) as normalized_code_type
    , cast(coalesce(loinc_code, snomed_code) as varchar ) as normalized_code
    , cast(coalesce(loinc_description, snomed_description) as varchar ) as normalized_description
    , cast(null as varchar ) as normalized_component
    , cast(status as varchar ) as status
    , cast(result as varchar ) as result
    , cast(result_date as date) as result_date
    , cast(collection_date as date) as collection_date
    , cast(source_units as varchar ) as source_units
    , cast(null as varchar ) as normalized_units
    , cast(source_reference_range_low as varchar ) as source_reference_range_low
    , cast(source_reference_range_high as varchar ) as source_reference_range_high
    , cast(null as varchar ) as normalized_reference_range_low
    , cast(null as varchar ) as normalized_reference_range_high
--     , cast(source_abnormal_flag as varchar ) as source_abnormal_flag
    , cast(null as varchar ) as source_abnormal_flag
    , cast(null as varchar ) as normalized_abnormal_flag
    , cast(null as varchar ) as specimen
    , cast(null as varchar ) as ordering_practitioner_id
    , cast(data_source as varchar ) as data_source
from {{ref('int__all_observations')}} ao

