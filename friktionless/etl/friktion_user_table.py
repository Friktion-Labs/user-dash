from .create_friktion_table import create_friktion_table
from google.cloud import bigquery

def create_friktion_user_table(friktion_gcloud_project):
    '''
    Create a user table for friktion users in a the supplied google cloud project.

    Example
    ----------
    import friktionless as fless
    fless.friktion_user_table.create_friktion_user_table('some_project_name')

    '''
    user_schema_name = 'users'
    
    user_table_name = 'friktion_users'
    
    user_table_schema = [
        bigquery.SchemaField("user_address", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("as_of_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("sol_balance", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("ever_lightning_og_holder", "BOOL"),
        bigquery.SchemaField("is_lightning_og_holder", "BOOL"),
        bigquery.SchemaField("nummber_lightning_ogs_held", "INT64"),
        bigquery.SchemaField("assets_under_management_USD", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("total_deposited_USD", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("total_withdrawn_USD", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("tvl_delta_30_days", "FLOAT64"),
        bigquery.SchemaField("tvl_delta_60_days", "FLOAT64"),
        bigquery.SchemaField("tvl_delta_90_days", "FLOAT64"),
        bigquery.SchemaField("tvl_delta_1_epoch", "FLOAT64"),
        bigquery.SchemaField("tvl_delta_2_epoch", "FLOAT64"),
        bigquery.SchemaField("tvl_delta_3_epoch", "FLOAT64"),
        bigquery.SchemaField("tvl_delta_4_epoch", "FLOAT64"),
        bigquery.SchemaField("tvl_delta_5_epoch", "FLOAT64"),
        bigquery.SchemaField("first_deposit_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("first_deposit_epoch", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("first_deposit_amount", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("first_deposit_token", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("last_deposit_date", "DATE"),
        bigquery.SchemaField("last_deposit_epoch", "INT64"),
        bigquery.SchemaField("last_deposit_amount", "FLOAT64"),
        bigquery.SchemaField("last_deposit_token", "STRING"),
        bigquery.SchemaField("days_since_last_deposit", "INT64"),
        bigquery.SchemaField("epochs_since_last_deposit", "INT64"),
        bigquery.SchemaField("first_withdrawal_date", "DATE"),
        bigquery.SchemaField("first_withdrawal_epoch", "INT64"),
        bigquery.SchemaField("first_withdrawal_amount", "FLOAT64"),
        bigquery.SchemaField("last_withdrawal_date", "DATE"),
        bigquery.SchemaField("last_withdrawal_epoch", "INT64"),
        bigquery.SchemaField("last_withdrawal_amount", "FLOAT64"),
        bigquery.SchemaField("has_churned", "BOOL"),
        bigquery.SchemaField("churn_date", "DATE"),
        bigquery.SchemaField("churn_epoch", "INT64"),
    ]

    create_friktion_table(friktion_gcloud_project, schema_name=user_schema_name, table_name=user_table_name, table_schema=user_table_schema)
