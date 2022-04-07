select
    case when pc.deposited_asset = 'tsUSDC' then 'SOL [tsUSDC]' else pc.asset end as asset,
    pc.volt_number,
    avg(fd.deposit_initiated_amt) as avg_deposit_initiated_amt
from `friktion-dev.transactions.fact_deposits` fd
    left join `friktion-dev.analytics.product_catalog` pc on pc.product_name = fd.product_name
where
    pc.volt_number = '{0}' and
    asset in ({1})
group by 1,2
order by 2,1