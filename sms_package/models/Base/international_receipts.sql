{{
    config(
        materialized='incremental',
        schema='data_science',
        post_hook='create index if not exists {{ this.name }}__index_on_user_id on {{ this }} ("user_id")'
    )

}}


SELECT
user_id,
amount,
sender,
transaction_code,
created_at,
transaction_date
FROM {{ref('dbt_financial_credits')}}
WHERE lower(split_part(sender,' ',6)) = 'bank'
AND split_part(sender,' ',1) NOT IN ('329137','329330','517819')

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  AND created_at > (select max(created_at) from {{ this }})

{% endif %}