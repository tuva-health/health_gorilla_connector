with address as (
    select *
    from {{ ref('stage__patient_address') }} x
    qualify row_number() over (
        partition by patient_id
        order by
              case when use = 'home' then 0 else 1 end
            , case when period_end is null then 0 else 1 end
            , period_start
    ) = 1
)

 select identifier_1_value as person_id
      , identifier_1_value as patient_id
      , pat.name_0_family as first_name
      , pat.name_0_given_0 as last_name
      , pat.gender as sex
      , null as race
      , pat.birthdate as birth_date
      , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as death_date
      , null as death_flag
      , null as subscriber_id
      , null as social_security_number
      , line_0 || coalesce(' ' || line_1, '') as address
      , address.city as city
      , address.state as state
      , address.postalcode as zip_code
      , null as county
      , null as latitude
      , null as longitude
      , null as phone
      , 'healthgorilla' as data_source
from {{ ref('stage__patient') }} as pat
    left join address on pat.id = address.patient_id