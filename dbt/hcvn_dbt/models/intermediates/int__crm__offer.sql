with src__crm__offer as (
    select
        *
    from
        {{ ref('stg__f_crm_offer') }}
),
final as (
    SELECT
        ID_CA as CA_ID,
        ID_CA_OFF as CA_OFF_ID, --keep
        DECODE(CODE_CA_TYPE, 'XNA', null, CODE_CA_TYPE) as CA_TYPE, --keep
        decode(FLAG_CA_F_ORBP,'X',null,FLAG_CA_F_ORBP) as CA_F_ORBP, --keep
        DECODE(CODE_CA_OFFER_TYPE, 'XNA', null, CODE_CA_OFFER_TYPE) as CA_OFFER_TYPE_CODE, --keep
        DATE_CA_START as CA_START_DT,
        ID_CA_LIM as CA_LIM_ID
FROM src__crm__offer t
)

select * from final