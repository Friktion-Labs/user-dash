select
    distinct cast(pc.volt_number as string) as volt_number
from `friktion-dev.analytics.product_catalog` pc