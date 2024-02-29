with sources as (

    select * ,
       case when "SYSTEM" = 'http://www.nlm.nih.gov/research/umls/rxnorm' then 'rxnorm'
               when "SYSTEM" = 'http://hl7.org/fhir/sid/ndc' then 'ndc'
               when "SYSTEM" = 'http://snomed.info/sct' then 'snomed-ct'
                when "SYSTEM" = 'http://loinc.org' then 'loinc'
                when "SYSTEM" = 'urn:oid:2.16.840.1.113883.6.208' then 'fdb' --2205 fdb nddf
                when "SYSTEM" = 'urn:oid:2.16.840.1.113883.6.68' then 'medispan gpi' -- 949 medispan gpi
                when "SYSTEM" = 'urn:oid:2.16.840.1.113883.4.64' then 'clinical formulation id' -- 294 Clinical Formulation ID
                when "SYSTEM" = 'urn:oid:2.16.840.1.113883.6.253' then 'medispan mddi' -- 295 medispan drug descriptor mddid
                when "SYSTEM" = 'urn:oid:2.16.840.1.113883.6.314' then 'multum drug id' -- 81 multum-drug-id
                when "SYSTEM" = 'urn:oid:2.16.840.1.113883.6.55' then 'upc' -- 8 upc
                when "SYSTEM" = 'urn:oid:2.16.840.1.113883.6.27' then 'multum' -- 1 multum
                else 'unknown' end as source_code_type,
       row_number() over (partition by MEDICATIONSTATEMENT_ID
           order by case
               when "SYSTEM" = 'http://www.nlm.nih.gov/research/umls/rxnorm' then 0
               when "SYSTEM" = 'http://hl7.org/fhir/sid/ndc' then 1
               when "SYSTEM" = 'http://snomed.info/sct' then 2
                when "SYSTEM" = 'http://loinc.org' then 3
                when "SYSTEM" = 'urn:oid:2.16.840.1.113883.6.208' then 4 --2205 fdb nddf
                when "SYSTEM" = 'urn:oid:2.16.840.1.113883.6.68' then 5 -- 949 medispan gpi
                when "SYSTEM" = 'urn:oid:2.16.840.1.113883.4.64' then 6 -- 294 Clinical Formulation ID
                when "SYSTEM" = 'urn:oid:2.16.840.1.113883.6.253' then 7 -- 295 medispan drug descriptor mddid
                when "SYSTEM" = 'urn:oid:2.16.840.1.113883.6.314' then 8 -- 81 multum-drug-id
                when "SYSTEM" = 'urn:oid:2.16.840.1.113883.6.55' then 9 -- 8 upc
                when "SYSTEM" = 'urn:oid:2.16.840.1.113883.6.27' then 10 -- 1 multum
                else 999 end,
            case when display is not null then 0 else 1 end) as src_ord,
         case when "SYSTEM" = 'http://www.nlm.nih.gov/research/umls/rxnorm' then
             row_number() over (partition by MEDICATIONSTATEMENT_ID
               order by case when display is not null then 0 else 1 end) end as rxn_rw,
         case when "SYSTEM" = 'http://hl7.org/fhir/sid/ndc' then
             row_number() over (partition by MEDICATIONSTATEMENT_ID
               order by case when display is not null then 0 else 1 end) end as ndc_rw
    from {{ref('stage__medicationstatement_medicationcodeableconcept_coding')}}
)
select
      cast(med.id as {{ dbt.type_string() }} ) as medication_id
    , cast(pat.identifier_1_value as {{ dbt.type_string() }} ) as patient_id
    , cast(null as {{ dbt.type_string() }} ) as encounter_id
    , {{ try_to_cast_date('coalesce(cast( effectiveperiod_Start as varchar),cast(effectivedatetime as varchar))', 'YYYY-MM-DD') }} as dispensing_date
    , cast(null as date ) as prescribing_date
    , cast(src.source_code_type as {{ dbt.type_string() }} ) as source_code_type
    , cast(src.code as {{ dbt.type_string() }} ) as source_code
    , cast(coalesce(src.display,med.medicationcodeableconcept_text) as {{ dbt.type_string() }} ) as source_description
    , cast(ndc.code as {{ dbt.type_string() }} ) as ndc_code
    , cast(ndc.display as {{ dbt.type_string() }} ) as ndc_description
    , cast(rxn.code as {{ dbt.type_string() }} ) as rxnorm_code
    , cast(rxn.display as {{ dbt.type_string() }} ) as rxnorm_description
    , cast(null as {{ dbt.type_string() }} ) as atc_code
    , cast(null as {{ dbt.type_string() }} ) as atc_description
    , cast(med.dosage_0_route_coding_0_display as {{ dbt.type_string() }} ) as route
    , cast(null as {{ dbt.type_string() }} ) as strength
    , cast(med.dosage_0_doseandrate_0_dosequantity_value as {{ dbt.type_int() }} ) as quantity
    , cast(med.dosage_0_doseandrate_0_dosequantity_unit as {{ dbt.type_string() }} ) as quantity_unit
    , cast(null as {{ dbt.type_int() }} ) as days_supply
    , cast(null as {{ dbt.type_string() }} ) as practitioner_id
    , cast('healthgorilla' as {{ dbt.type_string() }} ) as data_source
from {{ref('stage__medicationstatement')}} med
inner join {{ref('stage__patient') }} pat
    on right(med.subject_reference, 24) = pat.id
left join sources src
    on med.id = src.medicationstatement_id and src.src_ord = 1
left join sources rxn
    on med.id = rxn.medicationstatement_id and rxn.rxn_rw = 1
left join sources ndc
    on med.id = ndc.medicationstatement_id and ndc.ndc_rw = 1
