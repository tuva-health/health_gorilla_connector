/*
oids:
 urn:oid:1.2.840.114350 is the epic oid Epic aasigns[sic] OIDs under this node 1.2.840.114350 for each customer production deployment.
 2.16.840.1.113883.5.4 = Act code (action code), type of action represented
 urn:oid:2.16.840.1.113883.4.391 = ecw prefix
 urn:oid:2.16.840.1.113883.12.4 = patient class
 2.16.840.1.113883.3.42 = dod (MHS)

 */
--
-- select * from encounter_contained
-- where RESOURCETYPE = 'Location'
--
-- select * From ENCOUNTER
 with contained as (
     select *
     from {{ ref('stage__encounter_contained') }} x
     qualify row_number() over(partition by encounter_id order by id) = 1
 )

select
      enc.id as encounter_id -- should this be the source specific id?
    , pat.identifier_1_value as patient_id
--     , case type_0_text
--         when
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
    , null as facility_npi
    , null as primary_diagnosis_code_type -- we have a diagnosis reference, but not sure yet what its pointing to
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
