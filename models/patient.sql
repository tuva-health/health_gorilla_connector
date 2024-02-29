with address as (select *
                  from {{ ref('stage__patient_address') }} x
                      qualify row_number() over (partition by PATIENT_ID order by case when use = 'home' then 0 else 1 end,case when period_end is null then 0 else 1 end, period_start ) =
                              1)

--  select * From cte where IDENTIFIER_1_SYSTEM <> 'https://app.elationemr.com' or IDENTIFIER_1_SYSTEM is null

 select IDENTIFIER_1_VALUE                  as patient_id
      , pat.name_0_family                   as first_name
      , pat.name_0_given_0                  as last_name
      , pat.gender                          as sex
      , null                                as race
      , pat.birthdate                       as birth_date
      , null                                as death_date
      , null                                as death_flag
      , LINE_0 || coalesce(' ' || LINE_1, '') as address
      , address.CITY                        as city
      , address.STATE                       as state
      , address.POSTALCODE                     zip_code
      , null                                as county
      , null                                as latitude
      , null                                as longitude
      , 'healthgorilla'                     as data_source
 from {{ ref('stage__patient') }} as pat
    left join address on pat.id = address.PATIENT_ID