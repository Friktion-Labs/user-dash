import pandas as pd
import altair as alt


def create_net_funds_flow_chart(friktion_gcloud_project, product_name):
    '''
    Create a bar chart which shows the deposits, withdrawls, and net funds flow by epoch for a supplied google cloud project and product name.

    Example
    ----------
    import friktionless as fless
    fless.analytics.create_net_funds_flow_chart('some_project_name','Covered Call - SOL - Low Voltage')

    '''
    # Open net_funds_flow SQL query
    with open ('friktionless/queries/net_funds_flow.sql') as query:
        query_string = query.read()

    # Read in data from Google BigQuery
    df = pd.read_gbq(query=query_string.format(product_name), project_id=friktion_gcloud_project)

    # Create Altair charts
    deposits = alt.Chart(df).mark_bar().encode(
        x = alt.X(
            'epoch:O',
            axis=alt.Axis(
                labelAngle=0, 
                title='Epoch'
                )
        ),
        y = alt.Y(
            'net_deposit_amt',
            axis=alt.Axis(
                title='Gross Funds Flow',
                format='$,.0f'
                )
        ),
        color = alt.value('green')
    ).properties(
        height=200,
        width=1000,
        title=product_name
    )

    withdrawals = alt.Chart(df).mark_bar().encode(
        x = alt.X(
            'epoch:O',
            axis=alt.Axis(
                labelAngle=0, 
                title='Epoch'
                )
        ),
        y = alt.Y(
            'net_withdrawal_amt_neg',
            axis=alt.Axis(
                title='Gross Funds Flow',
                format='$,.0f'
                )
        ),
        color = alt.value('red')
    ).properties(
        height=200,
        width=1000,
        title=product_name
    )

    net = alt.Chart(df).mark_bar().encode(
        x = alt.X(
            'epoch:O',
            axis=alt.Axis(
                labelAngle=0, 
                title='Epoch'
                )
        ),
        y = alt.Y(
            'net_funds_flow',
            axis=alt.Axis(
                title='Net Funds Flow',
                format='$,.0f'
                )
        ),
        color = alt.value('black')
    ).properties(
        height=200,
        width=1000
    )

    # Combine charts together into a single view
    final_chart = (deposits + withdrawals) & net

    return final_chart