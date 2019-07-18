{{
    config(
        materialized='incremental',
        schema='data_science'
    )
}}


SELECT
user_id,
amount,
sender,
transaction_code,
transaction_date,
created_at
FROM {{ref('dbt_financial_credits')}}
WHERE lower(split_part(sender,' ',2)) = 'bank'
OR lower(split_part(sender,' ',3)) = 'bank'
OR lower(split_part(sender,' ',3)) = 'equity'
OR lower(split_part(sender,' ',4)) = 'bank'
OR lower(split_part(sender,' ',5)) = 'bank'
OR split_part(sender,' ',1) =  '329137'
OR split_part(sender,' ',1) =  '329330'
OR split_part(sender,' ',1) =  '517819'
OR lower(split_part(sender,' ',2)) = 'stanbic'
OR lower(split_part(sender,' ',1)) = 'citibank'
OR lower(split_part(sender,' ',1)) = 'dtb'
OR lower(split_part(sender,' ',1)) = 'gulf'

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  AND created_at > (select max(created_at) from {{ this }})

{% endif %}

