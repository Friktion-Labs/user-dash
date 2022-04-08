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
        bigquery.SchemaField("userAddress", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("asOfDate", "DATE", mode="REQUIRED"),
        # bigquery.SchemaField("sol_balance", "FLOAT64", mode="REQUIRED"),
        # bigquery.SchemaField("ever_lightning_og_holder", "BOOL"),
        # bigquery.SchemaField("is_lightning_og_holder", "BOOL"),
        # bigquery.SchemaField("nummber_lightning_ogs_held", "INT64"),
        bigquery.SchemaField("totalValueLockedUSD", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("totalDepositedUSD", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("totalWithdrawnUSD", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("tvlDelta30Days", "FLOAT64"),
        bigquery.SchemaField("tvlDelta60Days", "FLOAT64"),
        bigquery.SchemaField("tvlDelta90Days", "FLOAT64"),
        bigquery.SchemaField("tvlDelta1Epoch", "FLOAT64"),
        bigquery.SchemaField("tvlDelta2Epoch", "FLOAT64"),
        bigquery.SchemaField("tvlDelta3Epoch", "FLOAT64"),
        bigquery.SchemaField("tvlDelta4Epoch", "FLOAT64"),
        bigquery.SchemaField("tvlDelta5Epoch", "FLOAT64"),
        bigquery.SchemaField("firstDepositDate", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("firstDepositEpoch", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("firstDepositAmount", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("firstDepositToken", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("lastDepositDate", "DATE"),
        bigquery.SchemaField("lastDepositEpoch", "INT64"),
        bigquery.SchemaField("lastDepositAmount", "FLOAT64"),
        bigquery.SchemaField("lastDepositToken", "STRING"),
        bigquery.SchemaField("daysSinceLastDeposit", "INT64"),
        bigquery.SchemaField("epochsSinceLastDeposit", "INT64"),
        bigquery.SchemaField("firstWithdrawalDate", "DATE"),
        bigquery.SchemaField("firstWithdrawalEpoch", "INT64"),
        bigquery.SchemaField("firstWithdrawalAmount", "FLOAT64"),
        bigquery.SchemaField("lastWithdrawalDate", "DATE"),
        bigquery.SchemaField("lastWithdrawalEpoch", "INT64"),
        bigquery.SchemaField("lastWithdrawalAmount", "FLOAT64"),
        bigquery.SchemaField("hasChurned", "BOOL"),
        bigquery.SchemaField("churnDate", "DATE"),
        bigquery.SchemaField("churnEpoch", "INT64"),
    ]

    create_friktion_table(friktion_gcloud_project, schema_name=user_schema_name, table_name=user_table_name, table_schema=user_table_schema)
