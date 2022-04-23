-- TODO: Parameterize project-id, dataset, and table

select
    max(wcp.timestamp) as most_recent_txn_ts
from `solana.withdrawals_claim_pending` wcp