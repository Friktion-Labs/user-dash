-- TODO: Parameterize project-id, dataset, and table

select
    max(dcp.timestamp) as most_recent_txn_ts
from `solana.deposits_cancel_pending` dcp