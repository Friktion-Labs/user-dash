select
    distinct case when pc.deposited_asset = 'tsUSDC' then 'SOL [tsUSDC]' else pc.asset end as asset
from `friktion-dev.analytics.product_catalog` pc