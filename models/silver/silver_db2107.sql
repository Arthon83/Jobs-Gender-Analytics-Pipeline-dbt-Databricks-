{{ config(
    materialized = 'table',
    persist_docs = {
      "relation": true,
      "columns": true
    }
) }}

with base as (

    select
        VIMA_ID,
        CIM_ID,
        cast(NEM as string) as NEM_CODE,
        CSALALL,
        AGE,
        AGE_GROUP,
        KSHIR
    from {{ ref('bronze_db2107_clean') }}

),

lgaa as (
    select *
    from {{ source('reference', 'lgaa2407') }}
),

feaor as (
    select
        NEKOD2,
        NEKOD4,
        MEGNEV2,
        MEGNEV4
    from {{ source('reference', 'feaor') }}
),

cims as (
    select *
    from {{ source('reference', 'cim_telep') }}
),

telep as (
    select distinct
        TELEPULESKOD,
        TELEPULES_NEVE,
        MEGYE_NEVE 
    from {{ source('reference', 'telep') }}
),

gender as (
    select
        KOD,
        NEM
    from {{ source('reference', 'gender') }}
),

joined as (

    select
        b.VIMA_ID,
        b.AGE,
        b.AGE_GROUP,
        b.CSALALL,
        g.NEM as NEM_MEGNEV,
        b.KSHIR,
        
        t.TELEPULES_NEVE,
        t.MEGYE_NEVE,

        
        l.LGAA518,

        f.MEGNEV2,
        f.MEGNEV4

    from base b

    left join lgaa l
        on b.VIMA_ID = l.VIMA_ID

    left join cims c
        on b.CIM_ID = c.CIM_ID

    left join telep t
        on b.KSHIR = t.TELEPULESKOD

    left join gender g
        on b.NEM_CODE = g.KOD

    left join feaor f
        on l.LGAA029 = f.NEKOD4
)

select *
from joined
where LGAA518 is not null;