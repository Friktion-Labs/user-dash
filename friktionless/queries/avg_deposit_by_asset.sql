select
    case when pc.deposited_asset = 'tsUSDC' then 'SOL [tsUSDC]' else pc.asset end as asset,
    pc.volt_number,
    case when pc.is_high_voltage then 'High Voltage' else 'Low Voltage' end as voltage
    avg(fd.deposit_initiated_amt) as avg_deposit_initiated_amt
from `transactions.fact_deposits` fd
    left join `analytics.product_catalog` pc on pc.product_name = fd.product_name
where
    cast(pc.volt_number as string) = '{0}' and
    asset in ({1}) and
    voltage in ({2})
group by 1,2
order by 2,1