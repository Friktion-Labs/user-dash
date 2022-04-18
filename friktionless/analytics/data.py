import pandas as pd
from datetime import datetime, date

def deposits(
    friktion_gcloud_project='lyrical-amulet-337502', start_date=date(2021, 12, 16), end_date=date.today(), start_deposit_initiated_epoch='null', end_deposit_initiated_epoch='null', start_custody_epoch='null', end_custody_epoch='null', deposited_asset='null', strategy='null', volt_number='null', is_high_voltage='null'):
    '''
    Queries the transactions.fact_deposits table in the Friktion data warehouse and returns a dataframe that matches the entered parameters.

    The default start and end dates range from the beginning of the Friktion deposit dataset to the current date.

    Parameters
    ----------
    friktion_gcloud_project : str, reference to the Google Cloud project ID which the fact_deposits table resides in
        For now, as this library is being used by the Friktion core development and analytics team, it will point to Friktion's Google Cloud project ID, lyrical-amulet-337502.
    start_date: str, default '2021-12-16', the earliest deposit date for the Friktion Volts program
        The starting date for the date range from which to pull deposit-related data. The current implementation only accepts 'YYYY-MM-DD' style date strings, but will be expanded in the future to include full datetimes.
    end_date: str, default 'datetime.date.today()'
        The ending date for the date range from for which to pull deposit-related data. The current implementation only accepts 'YYYY-MM-DD' style date strings, but will be expanded in the future to include full datetimes.
    start_deposit_initiated_epoch: int, default 'null', optional
        The starting epoch for the epoch range from which to pull deposit-related data. This, and other "deposit initiated epoch" parameters specifically refer to the epoch in which the user *initiates* the deposit, not when Friktion takes *custody* of the deposit. If start_deposit_initiatiated_epoch is not set, the underlying query will default to using all the relavant deposit_initiated epochs within a given date range.
    end_deposit_initiated_epoch: int, default 'null', optional
        The ending epoch for  the epoch range from which to pull deposit-related data. This, and other "deposit initiated epoch" parameters specifically refer to the epoch in which the user *initiates* the deposit, not when Friktion takes *custody* of the deposit. If end_deposit_initiatiated_epoch is not set, the underlying query will default to using all the relavant deposit_initiated epochs within a given date range.
    start_custody_epoch: int, default 'null', optional
        The starting epoch for the epoch range from which to pull deposit-related data. This, and other "custody epoch" parameters specifically refer to the epoch in which Friktion *takes custody* of the deposit, not when the user *initiates* the deposit. If start_custody_epoch is not set, the underlying query will default to using all the relavant custody epochs within a given date range.
    end_custody_epoch: int, default 'null', optional
        The ending epoch for the epoch range from which to pull deposit-related data. This, and other "custody epoch" parameters specifically refer to the epoch in which Friktion *takes custody* of the deposit, not when the user *initiates* the deposit. If end_custody_epoch is not set, the underlying query will default to using all the relavant custody epochs within a given date range.
    deposited_asset: str, default 'null', optional
        The token which a user deposits into a Friktion volt. Value can be one of:

        ``'BTC'``
            Sollet-wrapepd BTC
        ``'SOL'``
            Solana
        ``'mSOL'``
            Marinade-staked SOL
        ``'AVAX'``
        ``'stSOL'``
            Lido-staked SOL
        ``'STEP'``
            Step Finance
        ``'SBR'``
            Saber
        ``'FTT'``
            FTX token
        ``'LUNA'``
        ``'scnSOL'``
            Socean-staked SOL
        ``'SRM'``
            Serum
        ``'MNGO'``
            Mango
        ``'RAY'``
            Raydium
        ``'ETH'``
            Sollet-wrapped ETH
        ``'USDC'``
            USD Coin, Solana SPL. USDC is currently the deposited asset for all but one Volt 02 vaults.
        ``'tsUSDC'``
            Tulip-wrapped USD Coin. Users who deposit this token are earning yield on their USDC deposited in Tulip as well as the tsUSDC deposited in Friktion.
    strategy: str, default 'null', optional
        The strategy employed by the Volt for which a deposit is made. Value can be one of:

        ``'Covered Call'``
            Synonymous with Volt 01, this strategy involves selling an out-of-the-money (OTM) call option and collecting the premium as return.
        ``'Cash Secured Put'``
            Synonymous with Volt 02, this strategy involves selling an out-of-the-money (OTM) put option and collecting the premium as return.
    volt_number: int, default 'null', optional
        The volt number in which users are depositing their funds. Value can be one of:

        ``1``
            Synonymous with the Covered Call strategy.
        ``2``
            Synonymous with the Cash Secured Put strategy.
    is_high_voltage: bool, default 'null', optional
        The voltage level of the volt a user is depositing their funds into. 
        
        Per Friktion's own documentation: "Higher Voltage means increased risk, defined by a higher probability of the option being exercised. In return for taking on increased risk, expected option premiums are higher, resulting in higher APYs. In flat markets, tactical traders can use Higher Voltage to gain yield."

        Currently, there are only two "High Voltage" volts, Volt 01 where SOL is the deposited and underlying asset and Volt 02 where USDC is the deposited asset and SOL is the underlying asset.

    '''

    # Open fact_deposits_all SQL query
    with open ('friktionless/queries/fact_deposits_all.sql') as query:
        query_string = query.read()

    # Read in data from Google BigQuery
    df = pd.read_gbq(
        query=query_string.format(
            start_date,
            end_date,
            start_deposit_initiated_epoch,
            end_deposit_initiated_epoch,
            start_custody_epoch,
            end_custody_epoch,
            deposited_asset,
            strategy,
            volt_number,
            is_high_voltage
            ), 
        project_id=friktion_gcloud_project
        )
    
    return df


def withdrawals(
    friktion_gcloud_project='lyrical-amulet-337502', start_date=date(2021, 12, 16), end_date=date.today(), start_withdrawal_initiated_epoch='null', end_withdrawal_initiated_epoch='null', start_custody_epoch='null', end_custody_epoch='null', deposited_asset='null', strategy='null', volt_number='null', is_high_voltage='null'):
    '''
    Queries the transactions.fact_withdrawals table in the Friktion data warehouse and returns a dataframe that matches the entered parameters.

    The default start and end dates range from the beginning of the Friktion withdrawal dataset to the current date.

    Parameters
    ----------
    friktion_gcloud_project : str, reference to the Google Cloud project ID which the fact_withdrawals table resides in
        For now, as this library is being used by the Friktion core development and analytics team, it will point to Friktion's Google Cloud project ID, lyrical-amulet-337502.
    start_date: str, default '2021-12-16', the earliest withdrawal date for the Friktion Volts program
        The starting date for the date range from which to pull withdrawal-related data. The current implementation only accepts 'YYYY-MM-DD' style date strings, but will be expanded in the future to include full datetimes.
    end_date: str, default 'datetime.date.today()'
        The ending date for the date range from for which to pull withdrawal-related data. The current implementation only accepts 'YYYY-MM-DD' style date strings, but will be expanded in the future to include full datetimes.
    start_withdrawal_initiated_epoch: int, default 'null', optional
        The starting epoch for the epoch range from which to pull withdrawal-related data. This, and other "withdrawal initiated epoch" parameters specifically refer to the epoch in which the user *initiates* the withdrawal, not when Friktion loses *custody* of the deposit. If start_withdrawal_initiatiated_epoch is not set, the underlying query will default to using all the relavant withdrawal_initiated epochs within a given date range.
    end_withdrawal_initiated_epoch: int, default 'null', optional
        The ending epoch for  the epoch range from which to pull withdrawal-related data. This, and other "withdrawal initiated epoch" parameters specifically refer to the epoch in which the user *initiates* the withdrawal, not when Friktion loses *custody* of the deposit. If end_withdrawal_initiatiated_epoch is not set, the underlying query will default to using all the relavant withdrawal_initiated epochs within a given date range.
    start_custody_epoch: int, default 'null', optional
        The starting epoch for the epoch range from which to pull withdrawal-related data. This, and other "custody epoch" parameters specifically refer to the epoch in which Friktion *loses custody* of the deposit, not when the user *initiates* the withdrawal. If start_custody_epoch is not set, the underlying query will default to using all the relavant custody epochs within a given date range.
    end_custody_epoch: int, default 'null', optional
        The ending epoch for the epoch range from which to pull withdrawal-related data. This, and other "custody epoch" parameters specifically refer to the epoch in which Friktion *loses custody* of the deposit, not when the user *initiates* the withdrawal. If end_custody_epoch is not set, the underlying query will default to using all the relavant custody epochs within a given date range.
    deposited_asset: str, default 'null', optional
        The token which a user has deposited into a Friktion volt. Value can be one of:

        ``'BTC'``
            Sollet-wrapepd BTC
        ``'SOL'``
            Solana
        ``'mSOL'``
            Marinade-staked SOL
        ``'AVAX'``
        ``'stSOL'``
            Lido-staked SOL
        ``'STEP'``
            Step Finance
        ``'SBR'``
            Saber
        ``'FTT'``
            FTX token
        ``'LUNA'``
        ``'scnSOL'``
            Socean-staked SOL
        ``'SRM'``
            Serum
        ``'MNGO'``
            Mango
        ``'RAY'``
            Raydium
        ``'ETH'``
            Sollet-wrapped ETH
        ``'USDC'``
            USD Coin, Solana SPL. USDC is currently the deposited asset for all but one Volt 02 vaults.
        ``'tsUSDC'``
            Tulip-wrapped USD Coin. Users who deposit this token are earning yield on their USDC deposited in Tulip as well as the tsUSDC deposited in Friktion.
    strategy: str, default 'null', optional
        The strategy employed by the Volt from which a withdrawal is being made. Value can be one of:

        ``'Covered Call'``
            Synonymous with Volt 01, this strategy involves selling an out-of-the-money (OTM) call option and collecting the premium as return.
        ``'Cash Secured Put'``
            Synonymous with Volt 02, this strategy involves selling an out-of-the-money (OTM) put option and collecting the premium as return.
    volt_number: int, default 'null', optional
        The volt number from which users are withdrawing their funds. Value can be one of:

        ``1``
            Synonymous with the Covered Call strategy.
        ``2``
            Synonymous with the Cash Secured Put strategy.
    is_high_voltage: bool, default 'null', optional
        The voltage level of the volt a user is withdrawing their funds from. 
        
        Per Friktion's own documentation: "Higher Voltage means increased risk, defined by a higher probability of the option being exercised. In return for taking on increased risk, expected option premiums are higher, resulting in higher APYs. In flat markets, tactical traders can use Higher Voltage to gain yield."

        Currently, there are only two "High Voltage" volts, Volt 01 where SOL is the deposited and underlying asset and Volt 02 where USDC is the deposited asset and SOL is the underlying asset.

    '''

    # Open fact_withdrawals_all SQL query
    with open ('friktionless/queries/fact_withdrawals_all.sql') as query:
        query_string = query.read()

    # Read in data from Google BigQuery
    df = pd.read_gbq(
        query=query_string.format(
            start_date,
            end_date,
            start_withdrawal_initiated_epoch,
            end_withdrawal_initiated_epoch,
            start_custody_epoch,
            end_custody_epoch,
            deposited_asset,
            strategy,
            volt_number,
            is_high_voltage
            ), 
        project_id=friktion_gcloud_project
        )
    
    return df


def cancelled_deposits(
    friktion_gcloud_project='lyrical-amulet-337502', start_date=date(2021, 12, 16), end_date=date.today(), start_deposit_cancelled_epoch='null', end_deposit_cancelled_epoch='null', start_custody_epoch='null', end_custody_epoch='null', deposited_asset='null', strategy='null', volt_number='null', is_high_voltage='null'):
    '''
    Queries the transactions.fact_deposits_cancel_pending table in the Friktion data warehouse and returns a dataframe that matches the entered parameters.

    The default start and end dates range from the beginning of the Friktion deposit dataset to the current date.

    Parameters
    ----------
    friktion_gcloud_project : str, reference to the Google Cloud project ID which the fact_deposits_cancel_pending table resides in
        For now, as this library is being used by the Friktion core development and analytics team, it will point to Friktion's Google Cloud project ID, lyrical-amulet-337502.
    start_date: str, default '2021-12-16', the earliest cancelled deposit date for the Friktion Volts program
        The starting date for the date range from which to pull deposit cancellation-related data. The current implementation only accepts 'YYYY-MM-DD' style date strings, but will be expanded in the future to include full datetimes.
    end_date: str, default 'datetime.date.today()'
        The ending date for the date range from for which to pull deposit cancellation-related data. The current implementation only accepts 'YYYY-MM-DD' style date strings, but will be expanded in the future to include full datetimes.
    start_deposit_cancelled_epoch: int, default 'null', optional
        The starting epoch for the epoch range from which to pull deposit cancellation-related data. This, and other "deposit cancelled epoch" parameters specifically refer to the epoch in which the user *initiates* the deposit cancellation, not when Friktion would have taken *custody* of the deposit. If start_deposit_cancelled_epoch is not set, the underlying query will default to using all the relavant deposit_cancelled epochs within a given date range.
    end_deposit_cancelled_epoch: int, default 'null', optional
        The ending epoch for the epoch range from which to pull deposit cancellation-related data. This, and other "deposit cancelled epoch" parameters specifically refer to the epoch in which the user *initiates* the deposit cancellation, not when Friktion would have taken *custody* of the deposit. If end_deposit_cancellation_epoch is not set, the underlying query will default to using all the relavant deposit_cancelled epochs within a given date range.
    start_custody_epoch: int, default 'null', optional
        The starting epoch for the epoch range from which to pull deposit cancellation-related data. This, and other "custody epoch" parameters specifically refer to the epoch in which Friktion would have *taken custody* of the deposit, not when the user *initiates* the deposit cancellation. If start_custody_epoch is not set, the underlying query will default to using all the relavant custody epochs within a given date range.
    end_custody_epoch: int, default 'null', optional
        The ending epoch for the epoch range from which to pull deposit cancellation-related data. This, and other "custody epoch" parameters specifically refer to the epoch in which Friktion would have *taken custody* of the deposit, not when the user *initiates* the deposit cancellation. If end_custody_epoch is not set, the underlying query will default to using all the relavant custody epochs within a given date range.
    deposited_asset: str, default 'null', optional
        The token which a user would have deposited into a Friktion volt. Value can be one of:

        ``'BTC'``
            Sollet-wrapepd BTC
        ``'SOL'``
            Solana
        ``'mSOL'``
            Marinade-staked SOL
        ``'AVAX'``
        ``'stSOL'``
            Lido-staked SOL
        ``'STEP'``
            Step Finance
        ``'SBR'``
            Saber
        ``'FTT'``
            FTX token
        ``'LUNA'``
        ``'scnSOL'``
            Socean-staked SOL
        ``'SRM'``
            Serum
        ``'MNGO'``
            Mango
        ``'RAY'``
            Raydium
        ``'ETH'``
            Sollet-wrapped ETH
        ``'USDC'``
            USD Coin, Solana SPL. USDC is currently the deposited asset for all but one Volt 02 vaults.
        ``'tsUSDC'``
            Tulip-wrapped USD Coin. Users who deposit this token are earning yield on their USDC deposited in Tulip as well as the tsUSDC deposited in Friktion.
    strategy: str, default 'null', optional
        The strategy employed by the Volt for which a cancelled deposit is made. Value can be one of:

        ``'Covered Call'``
            Synonymous with Volt 01, this strategy involves selling an out-of-the-money (OTM) call option and collecting the premium as return.
        ``'Cash Secured Put'``
            Synonymous with Volt 02, this strategy involves selling an out-of-the-money (OTM) put option and collecting the premium as return.
    volt_number: int, default 'null', optional
        The volt number in which users would have deposited their funds. Value can be one of:

        ``1``
            Synonymous with the Covered Call strategy.
        ``2``
            Synonymous with the Cash Secured Put strategy.
    is_high_voltage: bool, default 'null', optional
        The voltage level of the volt a user would have deposited their funds into. 
        
        Per Friktion's own documentation: "Higher Voltage means increased risk, defined by a higher probability of the option being exercised. In return for taking on increased risk, expected option premiums are higher, resulting in higher APYs. In flat markets, tactical traders can use Higher Voltage to gain yield."

        Currently, there are only two "High Voltage" volts, Volt 01 where SOL is the deposited and underlying asset and Volt 02 where USDC is the deposited asset and SOL is the underlying asset.

    '''

    # Open fact_deposits_cancel_pending_all SQL query
    with open ('friktionless/queries/fact_deposits_cancel_pending_all.sql') as query:
        query_string = query.read()

    # Read in data from Google BigQuery
    df = pd.read_gbq(
        query=query_string.format(
            start_date,
            end_date,
            start_deposit_cancelled_epoch,
            end_deposit_cancelled_epoch,
            start_custody_epoch,
            end_custody_epoch,
            deposited_asset,
            strategy,
            volt_number,
            is_high_voltage
            ), 
        project_id=friktion_gcloud_project
        )
    
    return df


def cancelled_withdrawals(
    friktion_gcloud_project='lyrical-amulet-337502', start_date=date(2021, 12, 16), end_date=date.today(), start_withdrawal_cancelled_epoch='null', end_withdrawal_cancelled_epoch='null', start_custody_epoch='null', end_custody_epoch='null', deposited_asset='null', strategy='null', volt_number='null', is_high_voltage='null'):
    '''
    Queries the transactions.fact_withdrawals_cancel_pending table in the Friktion data warehouse and returns a dataframe that matches the entered parameters.

    The default start and end dates range from the beginning of the Friktion withdrawal dataset to the current date.

    Parameters
    ----------
    friktion_gcloud_project : str, reference to the Google Cloud project ID which the fact_withdrawals_cancel_pending table resides in
        For now, as this library is being used by the Friktion core development and analytics team, it will point to Friktion's Google Cloud project ID, lyrical-amulet-337502.
    start_date: str, default '2021-12-16', the earliest cancelled withdrawal date for the Friktion Volts program
        The starting date for the date range from which to pull deposit cancellation-related data. The current implementation only accepts 'YYYY-MM-DD' style date strings, but will be expanded in the future to include full datetimes.
    end_date: str, default 'datetime.date.today()'
        The ending date for the date range from for which to pull withdrawal cancellation-related data. The current implementation only accepts 'YYYY-MM-DD' style date strings, but will be expanded in the future to include full datetimes.
    start_withdrawal_cancelled_epoch: int, default 'null', optional
        The starting epoch for the epoch range from which to pull withdrawal cancellation-related data. This, and other "withdrawal cancelled epoch" parameters specifically refer to the epoch in which the user *initiates* the withdrawal cancellation, not when Friktion would have lost *custody* of the withdrawal. If start_withdrawal_cancelled_epoch is not set, the underlying query will default to using all the relavant withdrawal_cancelled epochs within a given date range.
    end_withdrawal_cancelled_epoch: int, default 'null', optional
        The ending epoch for the epoch range from which to pull withdrawal cancellation-related data. This, and other "withdrawal cancelled epoch" parameters specifically refer to the epoch in which the user *initiates* the withdrawal cancellation, not when Friktion would have lost *custody* of the withdrawal. If end_withdrawal_cancellation_epoch is not set, the underlying query will default to using all the relavant withdrawal_cancelled epochs within a given date range.
    start_custody_epoch: int, default 'null', optional
        The starting epoch for the epoch range from which to pull withdrawal cancellation-related data. This, and other "custody epoch" parameters specifically refer to the epoch in which Friktion would have *lost custody* of the deposit, not when the user *initiates* the withdrawal cancellation. If start_custody_epoch is not set, the underlying query will default to using all the relavant custody epochs within a given date range.
    end_custody_epoch: int, default 'null', optional
        The ending epoch for the epoch range from which to pull withdrawal cancellation-related data. This, and other "custody epoch" parameters specifically refer to the epoch in which Friktion would have *lost custody* of the withdrawal, not when the user *initiates* the withdrawal cancellation. If end_custody_epoch is not set, the underlying query will default to using all the relavant custody epochs within a given date range.
    deposited_asset: str, default 'null', optional
        The token which a user would have withdrawn from a Friktion volt. Value can be one of:

        ``'BTC'``
            Sollet-wrapepd BTC
        ``'SOL'``
            Solana
        ``'mSOL'``
            Marinade-staked SOL
        ``'AVAX'``
        ``'stSOL'``
            Lido-staked SOL
        ``'STEP'``
            Step Finance
        ``'SBR'``
            Saber
        ``'FTT'``
            FTX token
        ``'LUNA'``
        ``'scnSOL'``
            Socean-staked SOL
        ``'SRM'``
            Serum
        ``'MNGO'``
            Mango
        ``'RAY'``
            Raydium
        ``'ETH'``
            Sollet-wrapped ETH
        ``'USDC'``
            USD Coin, Solana SPL. USDC is currently the deposited asset for all but one Volt 02 vaults.
        ``'tsUSDC'``
            Tulip-wrapped USD Coin. Users who deposit this token are earning yield on their USDC deposited in Tulip as well as the tsUSDC deposited in Friktion.
    strategy: str, default 'null', optional
        The strategy employed by the Volt for which a cancelled withdrawal is made. Value can be one of:

        ``'Covered Call'``
            Synonymous with Volt 01, this strategy involves selling an out-of-the-money (OTM) call option and collecting the premium as return.
        ``'Cash Secured Put'``
            Synonymous with Volt 02, this strategy involves selling an out-of-the-money (OTM) put option and collecting the premium as return.
    volt_number: int, default 'null', optional
        The volt number in which users would have withdrawn their funds from. Value can be one of:

        ``1``
            Synonymous with the Covered Call strategy.
        ``2``
            Synonymous with the Cash Secured Put strategy.
    is_high_voltage: bool, default 'null', optional
        The voltage level of the volt a user would have withdrawn their funds from. 
        
        Per Friktion's own documentation: "Higher Voltage means increased risk, defined by a higher probability of the option being exercised. In return for taking on increased risk, expected option premiums are higher, resulting in higher APYs. In flat markets, tactical traders can use Higher Voltage to gain yield."

        Currently, there are only two "High Voltage" volts, Volt 01 where SOL is the deposited and underlying asset and Volt 02 where USDC is the deposited asset and SOL is the underlying asset.

    '''

    # Open fact_withdrawals_cancel_pending_all SQL query
    with open ('friktionless/queries/fact_withdrawals_cancel_pending_all.sql') as query:
        query_string = query.read()

    # Read in data from Google BigQuery
    df = pd.read_gbq(
        query=query_string.format(
            start_date,
            end_date,
            start_withdrawal_cancelled_epoch,
            end_withdrawal_cancelled_epoch,
            start_custody_epoch,
            end_custody_epoch,
            deposited_asset,
            strategy,
            volt_number,
            is_high_voltage
            ), 
        project_id=friktion_gcloud_project
        )
    
    return df