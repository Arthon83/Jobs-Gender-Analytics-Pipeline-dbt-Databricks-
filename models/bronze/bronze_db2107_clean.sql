{{ config(
    materialized = 'table'
) }}
---------------------------------------
--           

with source as (
        select *
    from {{ ref('bronze_db2107') }}

),

parsed as (
        select
        *,
        split(VIMA_ID, '_') as vima_parts
    from source

),

renamed as (
        select
        vima_parts[0]                      as VIMA_ID,
        cast(vima_parts[1] as string)      as CIM_ID,
        cast(vima_parts[2] as int)         as NEM,
        cast(vima_parts[3] as int)         as CSALALL,
        cast(vima_parts[4] as int)         as AGE,
        cast(vima_parts[5] as int)         as LWP,
        cast(vima_parts[6] as int)         as LWS,
        cast(vima_parts[7] as int)         as LWM,
        cast(vima_parts[8] as int)         as LWF,
        cast(vima_parts[9] as int)         as LWC
    from parsed

),

joined as (

    -- CIM_ID -> Település megfeleltetés 
    select
        r.*,
        c.KSHIR
    from renamed r
    left join {{ source('reference', 'cim_telep') }} c
        on r.CIM_ID = c.CIM_ID

),

derived_bins as (

    -- Háztartási viszonyok bináris indikátorok
    select
        *,
        case when (LWM > 0 or LWF > 0) then 1 else 0 end as LWSZBIN,
        case when LWC > 0 then 1 else 0 end as LWCBIN,
        case when LWS > 0 then 1 else 0 end as LWSBIN
    from joined

),

ht_type as (

    -- Háztartás típus kód
    select
        *,
        concat(
            'H',
            cast(LWCBIN as string),
            cast(LWP as string),
            cast(LWSZBIN as string)
        ) as HT_TYPE
    from derived_bins

),

final as (

    -- Korcsoport képzés
    select
        *,
        case
            when AGE >= 85 then '85+'
            when AGE >= 80 then '80-84'
            when AGE >= 75 then '75-79'
            when AGE >= 70 then '70-74'
            when AGE >= 65 then '65-69'
            when AGE >= 60 then '60-64'
            when AGE >= 55 then '55-59'
            when AGE >= 50 then '50-54'
            when AGE >= 45 then '45-49'
            when AGE >= 40 then '40-44'
            when AGE >= 35 then '35-39'
            when AGE >= 30 then '30-34'
            when AGE >= 25 then '25-29'
            when AGE >= 20 then '20-24'
            when AGE >= 15 then '15-19'
            when AGE >= 10 then '10-14'
            when AGE >= 5  then '05-09'
            else '00-04'
        end as AGE_GROUP
    from ht_type

),

dedup as (
    -- Deduplikálás VIMA_ID alapján
    select
        *,
        row_number() over (
            partition by VIMA_ID
            order by VIMA_ID
        ) as rn
    from final

)

select *
from dedup
where rn = 1;