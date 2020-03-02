{{ config(schema='BSL_RT') }}
with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from WARP_SAL_RT.SS1_SCHEMA1.RAW_CUSTOMERS

),

renamed as (

    select
        id as customer_id,
        first_name,
        last_name,
        email

    from source

)

select * from renamed
