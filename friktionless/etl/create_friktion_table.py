from google.cloud import bigquery

def create_friktion_table(friktion_gcloud_project, schema_name='solana', table_name='', table_schema=[]):
    '''
    This is a generalized function for creating an analytical table for friktion.
    '''

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Any data that is sourced directly from on-chain will live under the solana schema
    # schema_name = 'solana'


    ''' 
    This is a generalized schema which applies to the five core friktion tables:

    1. deposits
    2. cancel_pending_deposits
    3. withdrawals
    4. cancel_pending_withdrawals
    5. claim_pending_withdrawals

    '''
    # schema = [
    #         bigquery.SchemaField("txSignature", "STRING"),
    #         bigquery.SchemaField("amount", "FLOAT64"),
    #         bigquery.SchemaField("instructionAction", "STRING"),
    #         bigquery.SchemaField("instructionOrder", "STRING"),
    #         bigquery.SchemaField("userAddress", "STRING"),
    #         bigquery.SchemaField("timestamp", "STRING"),
    #         bigquery.SchemaField("currencyName", "STRING"),
    #         bigquery.SchemaField("currencyAddress", "STRING"),
    #         bigquery.SchemaField("senderAddress", "STRING"),
    #         bigquery.SchemaField("senderTokenMint", "STRING"),
    #         bigquery.SchemaField("receiverAddress", "STRING"),
    #         bigquery.SchemaField("globalId", "STRING"),
    #         bigquery.SchemaField("vaultAuthority", "STRING"),
    #         bigquery.SchemaField("shareTokenMint", "STRING"),
    #         bigquery.SchemaField("depositTokenSymbol", "STRING"),
    #         bigquery.SchemaField("depositTokenCoingeckoId", "STRING"),
    #         bigquery.SchemaField("userAction", "STRING")
    #     ]

    try:
        # Check to see if the dataset already exists
        client.get_dataset(".".join([friktion_gcloud_project, schema_name]))
    except:
        # If the dataset is not found, create the new dataset within GCP in order to be able to write new tables
        dataset = bigquery.Dataset(".".join([friktion_gcloud_project, schema_name]))
        dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
        print(
            "Created dataset {}".format(dataset)
        )

    # table_id = ".".join([friktion_gcloud_project, schema_name, table_name])

    # try:
    #     client.get_table(table_id)
    #     print(
    #         "Table {}.{}.{} already exists".format(friktion_gcloud_project, schema_name, table_name)
    #     )
    # except:
    #     table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )
