with coding_system_raw as (
    select cast(obvs.id  as {{ dbt.type_string() }} ) || coalesce(cast( '__' || obvscon.id as {{ dbt.type_string() }}),'') as id
         ,0 as table_priority
         ,0 as array_priority
         ,obvscon.CODE_CODING_0_SYSTEM as code_type_raw
         ,obvscon.CODE_CODING_0_CODE as code
         ,obvscon.CODE_CODING_0_DISPLAY as description
    from {{ref('stage__observation')}} obvs
    left join {{ref('stage__observation_contained')}}  obvscon
        on obvs.id = obvscon.observation_id and obvscon.RESOURCETYPE = 'Observation'
    where  obvscon.CODE_CODING_0_CODE is not null
        or obvscon.CODE_CODING_0_DISPLAY is not null

    union all

    select cast(obvs.id  as {{ dbt.type_string() }} ) || coalesce(cast( '__' || obvscon.id as {{ dbt.type_string() }}),'')
         ,0 as table_priority
         ,1 as array_priority
         ,obvscon.CODE_CODING_1_SYSTEM as code_type_raw
         ,obvscon.CODE_CODING_1_CODE as code
         ,obvscon.CODE_CODING_1_DISPLAY as description
    from {{ref('stage__observation')}} obvs
    left join {{ref('stage__observation_contained')}}  obvscon
        on obvs.id = obvscon.observation_id and obvscon.RESOURCETYPE = 'Observation'
    where  obvscon.CODE_CODING_1_CODE is not null
        or obvscon.CODE_CODING_1_DISPLAY is not null

    union all

    select cast(obvs.id  as {{ dbt.type_string() }} ) ||  coalesce(cast( '__' || obvscon.id as {{ dbt.type_string() }}),'')
         ,1 as table_priority
         ,0 as array_priority
         ,obvs.CODE_CODING_0_SYSTEM as code_type_raw
         ,obvs.CODE_CODING_0_CODE as code
         ,coalesce(obvs.CODE_CODING_0_DISPLAY,obvs.code_text) as description
    from {{ref('stage__observation')}} obvs
    left join {{ref('stage__observation_contained')}}  obvscon
        on obvs.id = obvscon.observation_id and obvscon.RESOURCETYPE = 'Observation'
    where obvs.CODE_CODING_0_CODE is not null
        or obvs.CODE_CODING_0_DISPLAY is not null
        or obvs.code_text is not null
)
,coding_system as (
    select id,
           table_priority,
           array_priority,
           code_type_raw,
           case when code_type_raw = 'http://loinc.org' then 'loinc'
                when code_type_raw = 'http://snomed.info/sct' then 'snomed_ct'
                when code_type_raw = 'http://www.ama-assn.org/go/cpt' then 'hcpcs level 1'
                when code_type_raw like 'urn:oid:2.16.840.1.113883.3.623%' then 'usoncology'
                when code_type_raw like 'urn:oid:2.16.840.1.113883.5.4%' then  'actcode'
                when code_type_raw like 'urn:oid:2.16.840.1.113883.6.233%' then 'usva'
                when code_type_raw = 'http://hl7.org/fhir/sid/icf-nl' then 'icf'
                when code_type_raw like 'urn:oid:1.2.840.113619.21%' then 'centricity'
                when code_type_raw like 'urn:oid:1.2.840.114350%' then 'epic'
                else code_type_raw end as code_type,
           case when code_type_raw = 'http://loinc.org' then 0
                when code_type_raw = 'http://snomed.info/sct' then 1
                when code_type_raw = 'http://www.ama-assn.org/go/cpt' then 2
                when code_type_raw like 'urn:oid:2.16.840.1.113883.3.623%' then 3
                when code_type_raw like 'urn:oid:2.16.840.1.113883.5.4%' then  4
                when code_type_raw like 'urn:oid:2.16.840.1.113883.6.233%' then 5
                when code_type_raw = 'http://hl7.org/fhir/sid/icf-nl' then 6
                when code_type_raw like 'urn:oid:1.2.840.113619.21%' then 7
                when code_type_raw like 'urn:oid:1.2.840.114350%' then 8
                else 9 end as code_type_priority,
           code,
           description
    From coding_system_raw x
    qualify row_number() over(partition by id order by table_priority,code_type_priority,array_priority) = 1
)
select
      cast(obvs.id  as {{ dbt.type_string() }} ) || coalesce(cast( '__' || obvscon.id as {{ dbt.type_string() }}),'') as id
    , cast(pat.identifier_1_value as {{ dbt.type_string() }} ) as patient_id
    , coding_system.code_type
    , coding_system.code
    , coding_system.description
    , cast(obvs.status as {{ dbt.type_string() }} ) as status
    , cast(coalesce(obvscon.valuequantity_value, obvs.valuequantity_value, obvs.valuestring) as {{ dbt.type_string() }} ) as result
    , cast(obvs.effectivedatetime as date) as result_date
    , cast(obvscon.collection_collecteddatetime as date) as collection_date
    , cast(coalesce(obvscon.valuequantity_unit,  obvs.valuequantity_unit) as {{ dbt.type_string() }} ) as source_units
    , cast(obvs.referencerange_0_low_value as {{ dbt.type_string() }} ) as source_reference_range_low
    , cast(obvs.referencerange_0_high_value as {{ dbt.type_string() }} ) as source_reference_range_high
    , cast(obvs.interpretation_0_coding_0_display as {{ dbt.type_string() }}) as source_abnormal_flag
    , coalesce(obvscon.category_0_coding_0_code,obvs.category_0_coding_0_code) as category
    , loinc.loinc as loinc_code
    , loinc.long_common_name as loinc_description
    , snomed.snomed_ct as snomed_code
    , snomed.description  as snomed_description
    , 'healthgorilla' as data_source
from {{ref('stage__observation')}} obvs
left join {{ref('stage__patient')}} pat
    on replace(obvs.subject_reference,'Patient/','') = pat.id
left join {{ref('stage__observation_contained')}} obvscon
    on obvs.id = obvscon.observation_id and obvscon.RESOURCETYPE = 'Observation'
left join coding_system
    on cast(obvs.id  as {{ dbt.type_string() }} ) || coalesce(cast( '__' || obvscon.id as {{ dbt.type_string() }}),'') = coding_system.id
left join {{ref('terminology__loinc')}} loinc
    on coding_system.code_type = 'loinc' and coding_system.code = loinc.loinc
left join {{ref('terminology__snomed_ct')}} snomed
    on coding_system.code_type = 'snomed_ct' and coding_system.code = snomed.snomed_ct
where not (obvs.code_text = 'n/a' and obvs.code_coding_0_system is null)
