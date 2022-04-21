'''
The ETL API provides hardened data warehouse hydration in code.

The api is designed for easy use in python scripts and command line scripting
'''

from .create_friktion_table import create_friktion_table
from .user_etl import write_user_first_deposit_table