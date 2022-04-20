# user_first_withdrawal_sql

with first_withdrawal_dt as (
    select user_address, min(date(withdrawal_initiated_ts)) as first_withdrawal_dt 
    from transactions.fact_withdrawals group by user_address
)

select 
    dep.user_address, 
    date(withdrawal_initiated_ts) as first_withdrawal_date,
    withdrawal_initiated_epoch as first_withdrawal_epoch,
    sum(withdrawal_initiated_amt) as first_withdrawal_amount,
    array_agg(deposited_asset)as first_withdrawal_token
from transactions.fact_withdrawals dep 
inner join first_withdrawal_dt fd on fd.user_address = dep.user_address and fd.first_withdrawal_dt = date(dep.withdrawal_initiated_ts)
group by 1, 2, 3
