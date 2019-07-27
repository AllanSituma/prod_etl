{{
	config(
		materialized='table',
		schema='data_science',
		post_hook='create index if not exists {{ this.name }}__index_on_user_id on {{ this }} ("user_id")'
	)
}}

WITH all_transactions as (
SELECT *
FROM public_data_science.mshwari_loans_receipts

),

daily as (
SELECT user_id as client,
	   count(*) num_trans,
	    sum(amount) sum_payments
FROM all_transactions
GROUP BY user_id,date_trunc('day',transaction_date::date)
),

weekly as (
SELECT user_id as client,
	   count(*) num_trans,
	    sum(amount) sum_payments
FROM all_transactions
GROUP BY user_id,date_trunc('week',transaction_date::date)
),

monthly as (
SELECT user_id as client,
	   count(*) num_trans,
	   sum(amount) sum_payments
FROM all_transactions
GROUP BY user_id,date_trunc('month',transaction_date::date)
),


query as (

SELECT 
	client,
    0 as daily_avg_count,
	0 as daily_avg_sum,
    0 as weekly_avg_count,
	0 as weekly_avg_sum,
    round(avg(num_trans)) as monthly_avg_count,
	round(avg(sum_payments::int),2) as monthly_avg_sum
  FROM monthly
  GROUP BY client
  UNION
SELECT 
	client,
    round(avg(num_trans)) as daily_avg_count,
	round(avg(sum_payments::int),2) as daily_avg_sum,
    0 as weekly_avg_count,
	0 as weekly_avg_sum,
    0 as monthly_avg_count,
	0 as monthly_avg_sum
  FROM daily
  GROUP BY client
UNION
SELECT 
	client,
    0 as daily_avg_count,
	0 as daily_avg_sum,
    round(avg(num_trans)) as weekly_avg_count,
	round(avg(sum_payments::int),2) as weekly_avg_sum,
    0 as monthly_avg_count,
	0 as monthly_avg_sum
  FROM weekly
  GROUP BY client

),

agg_trans as (
SELECT client,
       sum(daily_avg_count) avg_daily_trans,
       sum(daily_avg_sum) avg_daily_amount,
	   sum(weekly_avg_count) avg_weekly_trans,
	   sum(weekly_avg_sum) avg_weekly_amount,
	   sum(monthly_avg_count) avg_monthly_trans,
	   sum(monthly_avg_sum) avg_monthly_amount
FROM query
GROUP BY client)

SELECT user_id,
       max(all_transactions.amount) max_mshwari_receipts,
	   min(all_transactions.amount) min_mshwari_receipts,
	   sum(all_transactions.amount) total_mshwari_receipts,
	   count(transaction_code) num_mshwari_transactions,
 	   agg_trans.avg_daily_trans avg_daily_mshwari_transactions,
	   agg_trans.avg_daily_amount avg_daily_mshwari_amount,
	   agg_trans.avg_weekly_trans avg_weekly_mshwari_transactions,
	   agg_trans.avg_weekly_amount avg_weekly_mshwari_amount,
	   agg_trans.avg_monthly_trans avg_monthly_mshwari_transactions,
	   agg_trans.avg_monthly_amount avg_monthly_mshwari_amount
FROM all_transactions
JOIN agg_trans ON all_transactions.user_id = agg_trans.client
GROUP BY user_id,
	    agg_trans.avg_daily_trans,
	    agg_trans.avg_daily_amount,
	    agg_trans.avg_weekly_trans,
	    agg_trans.avg_weekly_amount,
	    agg_trans.avg_monthly_trans,
	    agg_trans.avg_monthly_amount