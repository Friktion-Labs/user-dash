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
                title='',
                labelFontSize=12,
                labelPadding=10
                )
        ),
        y = alt.Y(
            'net_deposit_amt',
            axis=alt.Axis(
                title='Gross Funds Flow',
                format=',.0f',
                labelFontSize=12,
                labelPadding=10,
                titleFontSize=16,
                titlePadding=20,
                grid=True,
                ticks=False
                )
        ),
        color = alt.value('black'),
        tooltip=[
            alt.Tooltip(
            'epoch:O',
            title='Epoch'
            ),
            alt.Tooltip(
            'net_deposit_amt',
            title='Net Deposit Amount',
            format=',.0f'
            )
        ]
    ).properties(
        height=150,
        width=1180
    )

    withdrawals = alt.Chart(df).mark_bar().encode(
        x = alt.X(
            'epoch:O',
            axis=alt.Axis(
                labelAngle=0, 
                title='',
                labelFontSize=12,
                labelPadding=10
                )
        ),
        y = alt.Y(
            'net_withdrawal_amt_neg',
            axis=alt.Axis(
                title='Gross Funds Flow',
                format=',.0f',
                labelFontSize=12,
                labelPadding=10,
                titleFontSize=16,
                titlePadding=20,
                grid=True,
                ticks=False
                )
        ),
        tooltip=[
            alt.Tooltip(
            'epoch:O',
            title='Epoch'
            ),
            alt.Tooltip(
            'net_withdrawal_amt',
            title='Net Withdrawal Amount',
            format=',.0f'
            )
        ]
    ).properties(
        height=150,
        width=1180
    )

    net = alt.Chart(df).mark_bar().encode(
        x = alt.X(
            'epoch:O',
            axis=alt.Axis(
                labelAngle=0, 
                title='Epoch',
                labelFontSize=12,
                labelPadding=10,
                titleFontSize=16,
                titlePadding=20
                )
        ),
        y = alt.Y(
            'net_funds_flow',
            axis=alt.Axis(
                title='Net Funds Flow',
                format=',.0f',
                labelFontSize=12,
                labelPadding=10,
                titleFontSize=16,
                titlePadding=20,
                grid=True,
                ticks=False
                )
        ),
        color = alt.condition(
            alt.datum.net_funds_flow >= 0,
            alt.value('green'),
            alt.value('red')
        ),
        tooltip=[
            alt.Tooltip(
            'epoch:O',
            title='Epoch'
            ),
            alt.Tooltip(
            'net_deposit_amt',
            title='Net Deposit Amount',
            format=',.0f'
            ),
            alt.Tooltip(
            'net_withdrawal_amt',
            title='Net Withdrawal Amount',
            format=',.0f'
            ),
            alt.Tooltip(
            'net_funds_flow',
            title='Net Funds Flow',
            format=',.0f'
            )
        ]
    ).properties(
        height=150,
        width=1180
    )

    # Combine charts together into a single view
    final_chart = (deposits + withdrawals) & net
    

    return final_chart.configure_concat(spacing=50).configure_view(strokeOpacity=0).configure_axisY(domainOpacity=0)


def create_cumulative_users_chart(friktion_gcloud_project, strategy, volt_number, asset, voltage):
    '''
    Create a line chart which shows the cumulative number of unique users by epoch, segmented by product name, for a supplied google cloud project, strategy, volt number, and asset.

    Example
    ----------
    import friktionless as fless
    fless.analytics.create_cumulative_users_chart('some_project_name','Covered Call',1,'SOL')

    '''
    # Open net_funds_flow SQL query
    with open ('friktionless/queries/cumulative_users_by_product_strategy_volt.sql') as query:
        query_string = query.read()

    # Read in data from Google BigQuery
    df = pd.read_gbq(query=query_string.format(strategy=strategy, volt_number=volt_number, asset=asset, voltage=voltage), project_id=friktion_gcloud_project)

    # Create Altair charts
    cumulative_users = alt.Chart(df).mark_line(point=True).encode(
        x=alt.X(
            'epoch:O',
            axis=alt.Axis(
                title='Epoch',
                labelAngle=0,
                labelFontSize=12,
                labelPadding=10,
                titleFontSize=16,
                titlePadding=20
            )
        ),
        y=alt.Y(
            'cumulative_unique_users',
            axis=alt.Axis(
                title='Cumulative Users',
                labelFontSize=12,
                labelPadding=10,
                titleFontSize=16,
                titlePadding=20,
                ticks=False
            )
        ),
        color=alt.Color(
            'product_name',
            legend=alt.Legend(
                orient='bottom',
                columns=7,
                title='Product Name'
            )
        ),
        tooltip=[
            alt.Tooltip(
                'product_name',
                title='Product Name'
            ),
            alt.Tooltip(
                'cumulative_unique_users',
                title='Cumulative Unique Users',
                format=','
            )
        ]
    ).properties(
        height=400,
        width=560
    )

    new_users = alt.Chart(df).mark_bar().encode(
        x=alt.X(
            'epoch:O',
            axis=alt.Axis(
                title='Epoch',
                labelAngle=0,
                labelFontSize=12,
                labelPadding=10,
                titleFontSize=16,
                titlePadding=20
            )
        ),
        y=alt.Y(
            'sum(epoch_new_users)',
            axis=alt.Axis(
                title='New Users',
                labelFontSize=12,
                labelPadding=10,
                titleFontSize=16,
                titlePadding=20,
                ticks=False
            )
        ),
        color=alt.Color(
            'product_name',
            legend=alt.Legend(
                orient='bottom',
                columns=3,
                title='Product Name'
            )
        ),
        order=alt.Order(
        'sum(epoch_new_users)',
        sort='descending'
        ),
        tooltip=[
            alt.Tooltip(
                'product_name',
                title='Product Name'
            ),
            alt.Tooltip(
                'epoch_new_users',
                title='New Users',
                format=','
            )
        ]
    ).properties(
        height=400,
        width=560
    )
    
    # Combine charts together into a single view
    final_chart = cumulative_users | new_users

    return final_chart.configure_concat(spacing=50)