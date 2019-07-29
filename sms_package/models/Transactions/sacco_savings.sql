{{
	config(
		materialized='incremental',
		schema='data_science',
		post_hook='create index if not exists {{ this.name }}__index_on_user_id on {{ this }} ("user_id")'
	)
}}


WITH all_transactions as (
SELECT
    amount,
	paybill,
	user_id,
	split_part(paybill,' ',1) first_part,
	split_part(paybill,' ',2) second_part,
	split_part(paybill,' ',3) third_part,
	split_part(paybill,' ',4) fourth_part,
	split_part(paybill,' ',5) fifth_part,
	split_part(paybill,' ',6) sixth_part,
	created_at,
	transaction_code,
	transaction_date
FROM {{ref('dbt_financial_debits')}})


SELECT user_id,
	      paybill,
	      amount,
	      first_part,
		  transaction_code,
	      transaction_date,
		  created_at
FROM all_transactions
WHERE second_part = 'SACCO'
	OR first_part = 'KBSACCO'
	OR third_part = 'SACCO'
	OR second_part = 'Sacco'
	OR second_part = 'Sacco.'
    OR third_part = 'Sacco'
    OR fourth_part = 'SACCO'
    OR fourth_part = 'Sacco'

{% if is_incremental() %}

AND created_at > (SELECT MAX(created_at) FROM {{this}})

{% endif %}