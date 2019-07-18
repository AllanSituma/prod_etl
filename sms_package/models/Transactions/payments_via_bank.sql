{{
    config(
        materialized='incremental',
        schema='data_science'
    )
}}

WITH all_transactions as(

SELECT *
FROM {{ref('dbt_financial_debits')}}
)

SELECT
user_id,
split_part(paybill,' ',3) Bank,
amount,
transaction_code,
created_at,
transaction_date
FROM all_transactions
WHERE lower(split_part(paybill,' ',2)) = 'via'
OR lower(split_part(recipient,' ',3)) = 'via'
OR lower(split_part(recipient,' ',4)) = 'via'
OR lower(split_part(recipient,' ',5)) = 'via'
OR lower(split_part(recipient,' ',6)) = 'via'


{% if is_incremental() %}

AND created_at > (SELECT max(created_at) FROM {{this}})

{% endif %}

