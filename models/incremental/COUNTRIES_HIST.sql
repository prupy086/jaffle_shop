{{
    config(
        materialized ='incremental',
        transient = 'false',
        unique_key = 'uqid' 
    )
}}

WITH cte as (
  Select *,lead("header__timestamp") over (partition by country_id order by "header__change_seq","header__timestamp" ,"header__change_oper" ) as end_time_raw from "SNOWFL_RT01"."ADMIN"."COUNTRIES__ct"
),cte2 as 
--MERGE INTO Countries_hist ch USING
(Select c.country_id ||'_'||CAST("header__timestamp" AS TEXT) as uqid  , c."header__operation",c.country_id,c.country_name, c.region_id,c.country_abr_name, c.Country_president_name, c."header__timestamp",c.end_time_raw
from cte c 
where 
  c.country_id in ( 'AE','SR')
 and c."header__operation" <> 'BEFOREIMAGE' 
 order by c.country_id, c."header__timestamp")
Select * from cte2
-- {% if is_incremental() %}

--   -- this filter will only be applied on an incremental run
--   where date_day >= (select max(date_day) from {{ this }})

-- {% endif %}
