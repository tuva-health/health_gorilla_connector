with contained as (
     select *
     from {{ ref('stage__encounter_contained') }} x
     qualify row_number() over(partition by encounter_id order by id) = 1
)

select
      enc.id as encounter_id
    , pat.identifier_1_value as person_id
    , pat.identifier_1_value as patient_id
    , coalesce(etm.tuva_type,'other') as encounter_type
    , {{ try_to_cast_date('enc.period_start', 'YYYY-MM-DD') }} as encounter_start_date
    , {{ try_to_cast_date('enc.period_end', 'YYYY-MM-DD') }} as encounter_end_date
    , {{ dbt.datediff(try_to_cast_date('enc.period_start', 'YYYY-MM-DD'),try_to_cast_date('enc.period_end', 'YYYY-MM-DD'),'day') }} as length_of_stay
    , null as admit_source_code
    , null as admit_source_description
    , null as admit_type_code
    , null as admit_type_description
    , null as discharge_disposition_code
    , null as discharge_disposition_description
    , con.name_0_text as attending_provider_id
    , null as attending_provider_name
    , null as facility_id
    , null as facility_name
    , null as primary_diagnosis_code_type
    , null as primary_diagnosis_code
    , null as primary_diagnosis_description
    , null as ms_drg_code
    , null as ms_drg_description
    , null as apr_drg_code
    , null as apr_drg_description
    , null as paid_amount
    , null as allowed_amount
    , null as charge_amount
    , 'healthgorilla' as data_source
from {{ ref('stage__encounter') }} as enc
left join {{ ref('stage__patient' ) }} as pat
    on right(enc.subject_reference,24) = pat.id
left join contained con
    on enc.id = con.encounter_id
left join {{ref('encounter_type_map')}} etm
    on enc.type_0_text = etm.hg_type
