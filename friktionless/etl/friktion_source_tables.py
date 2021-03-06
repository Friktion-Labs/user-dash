from google.cloud import bigquery

def create_friktion_table(friktion_gcloud_project, table_name):
    '''
    This is a generalized function for creating an analytical table for friktion.
    '''

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Name the schema which will contain the tables
    # Any data that is sourced directly from on-chain will live under the solana schema
    schema_name = 'solana'


    ''' 
    This is a generalized schema which applies to the five core friktion tables:

    1. deposits
    2. cancel_pending_deposits
    3. withdrawals
    4. cancel_pending_withdrawals
    5. claim_pending_withdrawals

    '''
    schema = [
            bigquery.SchemaField("txSignature", "STRING"),
            bigquery.SchemaField("amount", "FLOAT64"),
            bigquery.SchemaField("instructionAction", "STRING"),
            bigquery.SchemaField("instructionOrder", "STRING"),
            bigquery.SchemaField("userAddress", "STRING"),
            bigquery.SchemaField("timestamp", "STRING"),
            bigquery.SchemaField("currencyName", "STRING"),
            bigquery.SchemaField("currencyAddress", "STRING"),
            bigquery.SchemaField("senderAddress", "STRING"),
            bigquery.SchemaField("senderTokenMint", "STRING"),
            bigquery.SchemaField("receiverAddress", "STRING"),
            bigquery.SchemaField("globalId", "STRING"),
            bigquery.SchemaField("vaultAuthority", "STRING"),
            bigquery.SchemaField("shareTokenMint", "STRING"),
            bigquery.SchemaField("depositTokenSymbol", "STRING"),
            bigquery.SchemaField("depositTokenCoingeckoId", "STRING"),
            bigquery.SchemaField("userAction", "STRING")
        ]

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

    table_id = ".".join([friktion_gcloud_project, schema_name, table_name])

    try:
        client.get_table(table_id)
        print(
            "Table {}.{}.{} already exists".format(friktion_gcloud_project, schema_name, table_name)
        )
    except:
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

def create_friktion_deposits_table(friktion_gcloud_project):
    '''
    Create a source table for transactions where a user deposits tokens into friktion in a the supplied google cloud project.
    This source data comes directly from on-chain via the BitQuery API.

    Example
    ----------
    import friktionless as fless
    fless.friktion_source_tables.create_friktion_deposits_table('some_project_name')

    '''

    # Name the table we're creating
    table_name = 'deposits'
    create_friktion_table(friktion_gcloud_project, table_name)
    # table_maker = fless.tables.TableMaker()
    # table_maker.create('deposits', schema=transactional_schema)

def create_friktion_cancel_pending_deposits_table(friktion_gcloud_project):
    '''
    Create a source table for transactions where a user cancels their token deposit into friktion in a the supplied google cloud project.
    This source data comes directly from on-chain via the BitQuery API.

    Example
    ----------
    import friktionless as fless
    fless.friktion_source_tables.create_friktion_cancel_pending_deposits_table('some_project_name')

    '''

    # Name the table we're creating
    table_name = 'deposits_cancel_pending'
    create_friktion_table(friktion_gcloud_project, table_name)

def create_friktion_withdrawals_table(friktion_gcloud_project):
    '''
    Create a source table for transactions where a user withdraws tokens from friktion in a the supplied google cloud project.
    This source data comes directly from on-chain via the BitQuery API.

    Example
    ----------
    import friktionless as fless
    fless.friktion_source_tables.create_friktion_withdrawals_table('some_project_name')

    '''

    # Name the table we're creating
    table_name = 'withdrawals'
    create_friktion_table(friktion_gcloud_project, table_name)

def create_friktion_cancel_pending_withdrawals_table(friktion_gcloud_project):
    '''
    Create a source table for transactions where a user cancels their token withdrawal from friktion in a the supplied google cloud project.
    This source data comes directly from on-chain via the BitQuery API.

    Example
    ----------
    import friktionless as fless
    fless.friktion_source_tables.create_friktion_cancel_pending_withdrawals_table('some_project_name')

    '''

    # Name the table we're creating
    table_name = 'withdrawals_cancel_pending'
    create_friktion_table(friktion_gcloud_project, table_name)

def create_friktion_claim_pending_withdrawals_table(friktion_gcloud_project):
    '''
    Create a source table for transactions where a user claims their token withdrawal from friktion in a the supplied google cloud project.
    This source data comes directly from on-chain via the BitQuery API.

    Example
    ----------
    import friktionless as fless
    fless.friktion_source_tables.create_friktion_claim_pending_withdrawals_table('some_project_name')

    '''

    # Name the table we're creating
    table_name = 'withdrawals_claim_pending'
    create_friktion_table(friktion_gcloud_project, table_name)