with 
base as (
select
    pc.globalId,
    pc.product_name,
    pc.strategy,
    pc.volt_number,
    case when pc.deposited_asset = 'tsUSDC' then 'SOL [tsUSDC]' else pc.asset end as asset,
    case when pc.is_high_voltage then 'High Voltage' else 'Low Voltage' end as voltage,
    fd.user_address,
    min(fd.deposit_initiated_ts) as first_deposit_ts
from `friktion-dev.transactions.fact_deposits` fd
    left join analytics.product_catalog pc on pc.product_name = fd.product_name
group by 1,2,3,4,5,6,7
),

agg as (
select
    b.product_name,
    b.strategy,
    b.volt_number,
    b.asset,
    e.epoch as first_epoch,
    count(b.user_address) as epoch_user_count
from base b
    left join analytics.epoch e on e.globalId = b.globalId and b.first_deposit_ts between timestamp_seconds(e.start) and timestamp_seconds(e.end)
group by 1,2,3,4,5
)

select
    a.product_name,
    a.strategy,
    a.volt_number,
    a.asset,
    a.first_epoch as epoch,
    sum(a.epoch_user_count) over (partition by a.product_name, a.strategy, a.volt_number, a.asset order by a.first_epoch asc) as cumulative_unique_users
from agg a
where
    ('All' = '{strategy}' or a.strategy = '{strategy}') and
    ('All' = '{volt_number}' or a.volt_number = '{volt_number}') and
    ('All' = '{asset}' or a.asset = '{asset}')
