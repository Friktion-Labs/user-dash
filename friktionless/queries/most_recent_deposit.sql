-- TODO: Parameterize project-id, dataset, and table

select
    max(d.timestamp) as most_recent_txn_ts
from `solana.deposits` d