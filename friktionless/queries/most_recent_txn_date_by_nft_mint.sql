select
    nft.mintAddress as mint_address,
    max(nft.blockTimeIso) as most_recent_txn
from solana.lightning_og_nft nft
group by 1