import pandas as pd
from google.cloud import bigquery
import requests
from datetime import datetime
import time

def create_lightning_og_nft_table(friktion_gcloud_project):
    '''
    Create a table which contains all historical transactions related to the Lightning OG NFTs in the supplied google cloud project.

    Example
    ----------
    import friktionless as fless
    fless.friktion_lightning_og_nfts_table.create_lightning_og_nft_table('some_project_name')

    '''
    # Utility function to query BitQuery
    # TODO: Remove from inside function and create as stand alone utility. Becomes less relevant as we move away from BitQuery.
    def run_query(query, retries=10):
        """
        Query graphQL API.

        If timeerror
        """
        headers = {"X-API-KEY": "BQYCaXaMZlqZrPCSQVsiJrKtxKRVcSe4"}

        retries_counter = 0
        try:
            request = requests.post(
                "https://graphql.bitquery.io/", json={"query": query}, headers=headers
            )
            result = request.json()
            # print(dir(request.content))
            # Make sure that there is no error message
            # assert not request.content.errors
            assert "errors" not in result
        except:
            while (
                (request.status_code != 200
                or "errors" in result)
                and retries_counter < 10
            ):
                print(datetime.now(), f"Retry number {retries_counter}")
                if "errors" in result:
                    print(result["errors"])
                print(datetime.now(), f"Query failed for reason: {request.reason}. sleeping for {150*retries_counter} seconds and retrying...")
                time.sleep(150*retries_counter)
                request = requests.post(
                    "https://graphql.bitquery.io/",
                    json={"query": query},
                    headers=headers,
                )
                retries_counter += 1
            if retries_counter >= retries:
                raise Exception(
                    "Query failed after {} retries and return code is {}.{}".format(
                        retries_counter, request.status_code, query
                    )
                )
        return request.json()
    
    # Setup historical Lightning OG data

    ## Read-in query to identify the most recent transactions timestamp for each NFT mint
    with open('friktionless/queries/most_recent_txn_date_by_nft_mint.sql') as txn_query:
        txn_query_string = txn_query.read()

    ## Construct a BigQuery client object.
    client = bigquery.Client()

    ## Read GBQ into DataFrame using client
    df_og_nft = client.query(txn_query_string).result().to_dataframe()

    
    # Open BitQuery query which pulls all transactions after the most recent from the DB per mint
    with open('friktionless/queries/lightning_og_nft_txns_bitQuery.txt') as bitquery_query:
    
        bitquery_query_string = bitquery_query.read()

    # Loop through each mint, query, and then write output to GBQ
    index = 0

    for index, record in df_og_nft.iterrows():
        print(datetime.now(),'Starting run for address {}...NFT: {}'.format(record['mint_address'], index))
        print(datetime.now(),'Querying BitQuery...')
        result = run_query(bitquery_query_string % (record['mint_address'], record['most_recent_txn']))
        
        if not result['data']['solana']['transfers']:
            print('No data returned')
            index += 1
            pass
        else:
            temp_df = pd.json_normalize(result['data']['solana']['transfers'])
            temp_df.rename(columns={
                'block.timestamp.iso8601' : 'blockTimeIso',
                'block.timestamp.unixtime' : 'blockTimeUnix',
                'block.height' : 'blockHeight',
                'currency.tokenType' : 'tokenType',
                'currency.address' : 'mintAddress',
                'receiver.address' : 'receiverAddress',
                'receiver.type' : 'receiverType',
                'sender.address' : 'senderAddress',
                'sender.type' : 'senderType',
                'transaction.signature' : 'transactionSignature',
                'transaction.signer' : 'transactionSigner'
                },
                        inplace=True
            )

            print(datetime.now(), 'Writing to GBQ...')
            temp_df.to_gbq('solana.lightning_og_nft', if_exists='append')
            index += 1