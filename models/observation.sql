   select
      cast(ao.id as varchar ) as observation_id
    , cast(ao.patient_id as varchar ) as patient_id
    , cast(null as varchar ) as encounter_id
    , cast(null as varchar ) as panel_id
    , cast(ao.result_date as date) as observation_date
    , cast(ao.category as varchar ) as observation_type
    , cast(ao.code_type as varchar ) as source_code_type
    , cast(ao.code as varchar ) as source_code
    , cast(ao.description as varchar ) as source_description
    , cast(case
        when ao.loinc_code is not null then 'loinc'
        when ao.snomed_code is not null then 'snomed'
        end as varchar ) as normalized_code_type
    , cast(coalesce(ao.loinc_code, ao.snomed_code) as varchar ) as normalized_code
    , cast(coalesce(ao.loinc_description, ao.snomed_description) as varchar ) as normalized_description
    , cast(ao.result as varchar ) as result
    , cast(ao.source_units as varchar ) as source_units
    , cast(null as varchar ) as normalized_units
    , cast(ao.source_reference_range_low as varchar ) as source_reference_range_low
    , cast(ao.source_reference_range_high as varchar ) as source_reference_range_high
    , cast(null as varchar ) as normalized_reference_range_low
    , cast(null as varchar ) as normalized_reference_range_high
    , cast('healthgorilla' as varchar ) as data_source
from {{ref('int__all_observations')}} ao
where category <> 'laboratory'
