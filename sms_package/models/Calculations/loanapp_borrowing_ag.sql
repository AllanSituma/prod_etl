{{
	config(
		materialized='table',
		schema='data_science',
		post_hook='create index if not exists {{ this.name }}__index_on_user_id on {{ this }} ("user_id")'
	)
}}

WITH all_transactions AS (
SELECT * FROM public_data_science.borrowed_loans
),

num_loan_apps AS (
SELECT user_id,
       count(distinct first_part)num_apps,
	   array_agg(distinct first_part) apps,
	   array_agg(distinct amount) apps_amount
FROM all_transactions
GROUP BY user_id),

daily AS (
select user_id AS client,
	   count(*) num_trans,
	    sum(amount) sum_payments
FROM all_transactions
GROUP BY user_id,date_trunc('day',transaction_date::date)
),

weekly AS (
select user_id AS client,
	   count(*) num_trans,
	    sum(amount) sum_payments
FROM all_transactions
GROUP BY user_id,date_trunc('week',transaction_date::date)
),

monthly AS (
select user_id AS client,
	   count(*) num_trans,
	   sum(amount) sum_payments
FROM all_transactions
GROUP BY user_id,date_trunc('month',transaction_date::date)
),


query AS (

SELECT 
	client,
    0 AS daily_avg_count,
	0 AS daily_avg_sum,
    0 AS weekly_avg_count,
	0 AS weekly_avg_sum,
    round(avg(num_trans),2) AS monthly_avg_count,
	round(avg(sum_payments::int),2) AS monthly_avg_sum
  FROM monthly
  GROUP BY client
  UNION
SELECT 
	client,
    round(avg(num_trans),2) AS daily_avg_count,
	round(avg(sum_payments::int),2) AS daily_avg_sum,
    0 AS weekly_avg_count,
	0 AS weekly_avg_sum,
    0 AS monthly_avg_count,
	0 AS monthly_avg_sum
  FROM daily
  GROUP BY client
UNION
SELECT 
	client,
    0 AS daily_avg_count,
	0 AS daily_avg_sum,
    round(avg(num_trans),2) AS weekly_avg_count,
	round(avg(sum_payments::int),2) AS weekly_avg_sum,
    0 AS monthly_avg_count,
	0 AS monthly_avg_sum
  FROM weekly
  GROUP BY client
 ),
 
 frequency AS (
select client,
       sum(daily_avg_count) avg_daily_trans,
       sum(daily_avg_sum) avg_daily_amount,
	   sum(weekly_avg_count) avg_weekly_trans,
	   sum(weekly_avg_sum) avg_weekly_amount,
	   sum(monthly_avg_count) avg_monthly_trans,
	   sum(monthly_avg_sum) avg_monthly_sum
FROM query
GROUP BY client
),

min_borrowed AS (
with amounts_rank_min AS (
select distinct on (user_id) user_id,
	   first_part,
	   amount min_amount,
	   rank()over(partition by user_id order by amount ASc)
FROM all_transactions  )

select *
FROM amounts_rank_min
where rank = 1
),

max_borrowed AS (
with amounts_rank_max AS (
select distinct on (user_id) user_id,
	   first_part,
	   amount max_amount,
	   rank()over(partition by user_id order by amount desc)
FROM all_transactions  )

select *
FROM amounts_rank_max
where rank = 1
)


select DISTINCT ON (all_transactions.user_id) all_transactions.user_id,
	   min_borrowed.min_amount min_creditline,
	   min_borrowed.first_part min_credit_lender,
	   max_borrowed.max_amount max_creditline,
	   max_borrowed.first_part max_creditline_lender,
	   num_loan_apps.num_apps loan_apps_used,
	   num_loan_apps.apps loan_apps,
	   apps_amount,
	   count (*) total_loanapp_transactions,
	   sum(amount) total_loanapp_borrowing,
	   frequency.avg_daily_trans avg_daily_loanapps_borrowing_frequency,
       frequency.avg_daily_amount avg_daily_loanapps_borrowing_amount,
	   frequency.avg_weekly_trans avg_weekly_loanapps_borrowing_frequency,
	   frequency.avg_weekly_amount avg_loanapps_borrowing_amount,
	   frequency.avg_monthly_trans avg_monthly_loanapps_borrowings_frequency,
	   frequency.avg_monthly_sum avg_monthly_loanapps_borrowing_amount
FROM all_transactions
join frequency on frequency.client = all_transactions.user_id
join num_loan_apps on num_loan_apps.user_id = all_transactions.user_id
join min_borrowed on min_borrowed.user_id = all_transactions.user_id
join max_borrowed on max_borrowed.user_id = all_transactions.user_id
GROUP BY all_transactions.user_id,
	   min_borrowed.first_part,
	   min_borrowed.min_amount,
	   max_borrowed.max_amount,
	   max_borrowed.first_part,
	   apps_amount,
	   num_loan_apps.num_apps,
	   num_loan_apps.apps,
	   frequency.client,
	   frequency.avg_daily_trans,
       frequency.avg_daily_amount,
	   frequency.avg_weekly_trans,
	   frequency.avg_weekly_amount,
	   frequency.avg_monthly_trans,
	   frequency.avg_monthly_sum 