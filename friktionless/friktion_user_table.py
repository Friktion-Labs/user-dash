from google.cloud import bigquery

def create_friktion_user_table(friktion_gcloud_project):
    '''
    Create a user table for friktion users in a the supplied google cloud project.

    Example
    ----------
    import friktionless as fless
    fless.create_friktion_user_table('my-gcloud-project')
    
    '''
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Name the table we're creating
    user_schema_name = 'users'
    table_name = 'friktion_users'
    table_id = ".".join([friktion_gcloud_project, user_schema_name, table_name])

    schema = [
        bigquery.SchemaField("depositor_address", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("sol_balance", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("is_lightning_og_holder", "BOOL"),
        bigquery.SchemaField("nummber_lightning_ogs_held", "INT64"),
        bigquery.SchemaField("assets_under_management_USD", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("aum_delta_30_days", "FLOAT64"),
        bigquery.SchemaField("aum_delta_60_days", "FLOAT64"),
        bigquery.SchemaField("aum_delta_90_days", "FLOAT64"),
        bigquery.SchemaField("aum_delta_1_epoch", "FLOAT64"),
        bigquery.SchemaField("aum_delta_2_epoch", "FLOAT64"),
        bigquery.SchemaField("aum_delta_3_epoch", "FLOAT64"),
        bigquery.SchemaField("aum_delta_4_epoch", "FLOAT64"),
        bigquery.SchemaField("aum_delta_5_epoch", "FLOAT64"),
        bigquery.SchemaField("total_deposited", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("total_withdrawn", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("first_deposit_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("first_deposit_epoch", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("first_deposit_amount", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("last_deposit_date", "DATE"),
        bigquery.SchemaField("last_deposit_epoch", "INT64"),
        bigquery.SchemaField("last_deposit_amount", "FLOAT64"),
        bigquery.SchemaField("first_withdrawal_date", "DATE"),
        bigquery.SchemaField("first_withdrawal_epoch", "INT64"),
        bigquery.SchemaField("first_withdrawal_amount", "FLOAT64"),
        bigquery.SchemaField("last_withdrawal_date", "DATE"),
        bigquery.SchemaField("last_withdrawal_epoch", "INT64"),
        bigquery.SchemaField("last_withdrawal_amount", "FLOAT64"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )