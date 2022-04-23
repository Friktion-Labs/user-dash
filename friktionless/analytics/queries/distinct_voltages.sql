select
    distinct case when pc.is_high_voltage then 'High Voltage' else 'Low Voltage' end as voltage
from `analytics.product_catalog` pc