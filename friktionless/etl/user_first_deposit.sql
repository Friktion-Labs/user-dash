# user_first_deposit_sql

with first_deposit_dt as (
    select user_address, min(date(deposit_initiated_ts)) as first_deposit_dt 
    from transactions.fact_deposits group by user_address
)

select 
    dep.user_address, 
    date(deposit_initiated_ts) as first_deposit_date,
    deposit_initiated_epoch as first_deposit_epoch,
    sum(deposit_initiated_amt) as first_deposit_amount,
    array_agg(deposited_asset)as first_deposit_token
from transactions.fact_deposits dep 
inner join first_deposit_dt fd on fd.user_address = dep.user_address and fd.first_deposit_dt = date(dep.deposit_initiated_ts)
group by 1, 2, 3
