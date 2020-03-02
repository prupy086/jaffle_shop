{{ config(schema='BSL_RT') }}
with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from WARP_SAL_RT.SS1_SCHEMA1.RAW_ORDERS

),

renamed as (

    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status

    from source

)

select * from renamed
