{{
    config(
        materialized ='incremental',
        transient = 'false',
        unique_key = 'uqid' 
    )
}}

with base as (
Select country_id ||'|'||CAST("header__change_seq" AS TEXT) as uqid 
  ,*
  ,ROW_NUMBER() over(partition by country_id order by "header__change_seq","header__timestamp","header__change_oper" )  as rnk
 from "SNOWFL_RT01"."ADMIN"."COUNTRIES__ct"
 where country_id in ( 'AE','SR')
 AND  "header__operation" <> 'BEFOREIMAGE'
   {% if is_incremental() %}
    and "header__timestamp" > (Select max("header__timestamp") from {{this}})
   {% endif %}
  
),
delete_keys As
(
Select DISTINCT country_id from base where "header__change_oper" = 'D'
),
non_delete_keys as (
Select DISTINCT country_id from base 
  except 
  Select DISTINCT country_id from delete_keys
),
upsert_records as (
Select b.UQID,
b."header__change_seq",
b."header__change_oper",
b."header__change_mask",
b."header__stream_position",
b."header__operation",
b."header__transaction_id",
b."header__timestamp",
b.COUNTRY_ID,
b.COUNTRY_NAME,
b.REGION_ID,
b.COUNTRY_ABR_NAME,
b.COUNTRY_PRESIDENT_NAME
,lead(b."header__timestamp") over (partition by b.country_id order by b."header__change_seq",b."header__timestamp" ,b."header__change_oper" ) as end_time
,NULL AS DELETE_MARKER
,NULL AS DELETED_BY_UQID
from base b
inner join non_delete_keys ndk
  on b.country_id = ndk.country_id
--WHERE   (b."header__operation" <> 'BEFOREIMAGE' ) 
),
delete_set_delta as (
Select b.*
  from base b
  inner join delete_keys dk
  on b.country_id = dk.country_id
--WHERE   (b."header__operation" <> 'BEFOREIMAGE') 
-- OR ( b."header__operation" = 'BEFOREIMAGE' AND RNK  = 1 )
)
,delete_set_delta2 as 
(
  Select *
  ,IFNULL(lag(rnk) over (partition by  country_id order by "header__change_seq","header__timestamp" ,"header__change_oper"  ),1)  as rnk2
  ,'Y' AS DELETE_MARKER
  from delete_set_delta where "header__operation" = 'DELETE'
),
upsert_delete_delta as 
(Select dsd.UQID,
dsd."header__change_seq",
dsd."header__change_oper",
dsd."header__change_mask",
dsd."header__stream_position",
dsd."header__operation",
dsd."header__transaction_id",
dsd."header__timestamp",
dsd.COUNTRY_ID,
dsd.COUNTRY_NAME,
dsd.REGION_ID,
dsd.COUNTRY_ABR_NAME,
dsd.COUNTRY_PRESIDENT_NAME,
dsd2."header__timestamp" as end_time,
dsd2.DELETE_MARKER AS DELETE_MARKER
,dsd2.UQID AS DELETED_BY_UQID
 from delete_set_delta dsd
left Join delete_set_delta2  dsd2
on dsd.rnk <=dsd2.rnk
and  dsd.rnk  between dsd2.rnk2 and dsd2.rnk)
,
{% if is_incremental() %}
delete_set_target
 as (
   Select ch.UQID,
        ch."header__change_seq",
        ch."header__change_oper",
        ch."header__change_mask",
        ch."header__stream_position",
        ch."header__operation",
        ch."header__transaction_id",
        ch."header__timestamp",
        ch.COUNTRY_ID,
        ch.COUNTRY_NAME,
        ch.REGION_ID,
        ch.COUNTRY_ABR_NAME,
        ch.COUNTRY_PRESIDENT_NAME
        ,CASE WHEN ch.END_TIME is null then dsd."header__timestamp" end  as end_time
        ,'Y' AS DELETE_MARKER
        ,dsd.UQID AS DELETED_BY_UQID
    from  {{ this}} ch
    inner join delete_set_delta dsd
    on ch.country_id = dsd.country_id
   and (dsd."header__operation" = 'DELETE' AND dsd.RNK  = 1 )
 ),
 update_set_target
 as (
   Select ch.UQID,
        ch."header__change_seq",
        ch."header__change_oper",
        ch."header__change_mask",
        ch."header__stream_position",
        ch."header__operation",
        ch."header__transaction_id",
        ch."header__timestamp",
        ch.COUNTRY_ID,
        ch.COUNTRY_NAME,
        ch.REGION_ID,
        ch.COUNTRY_ABR_NAME,
        ch.COUNTRY_PRESIDENT_NAME
        ,CASE WHEN ch.END_TIME is null then b."header__timestamp" end  as end_time
        ,NULL AS DELETE_MARKER
        ,b.UQID AS DELETED_BY_UQID
    from  {{ this}} ch
    inner join base b
    on ch.country_id = b.country_id
	and ch.end_date = NULL
	and (b."header__operation" <> 'DELETE' AND b.RNK  = 1 )
 ),
 {% endif %}
 final as 
( Select * from upsert_delete_delta
 union all
 Select * from upsert_records
  {% if is_incremental() %}
 UNION all
 Select * from delete_set_target
 UNION ALL
 select * from update_set_target
  {% endif %}
 )
select * from final