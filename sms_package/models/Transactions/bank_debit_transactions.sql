{{
    config(
        materialized='incremental',
        schema='data_science'
    )
}}


SELECT
user_id,
amount,
paybill,
account,
transaction_code,
transaction_date,
created_at
FROM {{ref ('dbt_financial_debits')}}
WHERE lower(split_part(paybill,' ',2)) = 'bank'
OR lower(split_part(paybill,' ',3)) = 'bank'
OR lower(split_part(paybill,' ',3)) = 'equity'
OR lower(split_part(paybill,' ',2)) = 'stanbic'
OR lower(split_part(paybill,' ',2)) = 'stanchart'
OR lower(split_part(paybill,' ',1)) = 'citi'
OR lower(split_part(paybill,' ',1)) = 'dtb'
OR lower(split_part(paybill,' ',1)) = 'gulf'
OR lower(split_part(paybill,' ',2)) = 'chase'
OR lower(split_part(recipient,' ',2)) = 'cfc'
OR UPPER(split_part(recipient,' ',1)) = 'CBA'
OR lower(split_part(recipient,' ',1)) = 'ecobank'
OR lower(split_part(sender,' ',1)) = 'gulf'),

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  AND created_at > (select max(created_at) from {{ this }})

{% endif %}