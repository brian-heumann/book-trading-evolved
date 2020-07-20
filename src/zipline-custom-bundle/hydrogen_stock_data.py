import pandas as pd
from os import listdir


universe = [
    {'isin': 'CA0585861085', 'symbol': 'PO0.F', 'name': "Ballard Power"},
    {'isin': 'GB00B0130H42', 'symbol': 'IJ8.F', 'name': "ITM Power"},
    {'isin': 'NO0010081235', 'symbol': 'D7G.F', 'name': "Nel"},
    {'isin': 'SE0006425815', 'symbol': '27W.F', 'name': "Powercell Sweden"},
    {'isin': 'US72919P2020', 'symbol': 'PLUN.F', 'name': "Plug Power"},
    {'isin': 'NO0003067902', 'symbol': '2HX.F', 'name': "Hexagon Composites"},
    {'isin': 'FR0000120073', 'symbol': 'AIL.DE', 'name': "Air Liquide"},
    {'isin': 'IE00BZ12WP82', 'symbol': 'LIN.F', 'name': "Linde"},
    {'isin': 'US2310211063', 'symbol': 'CUM.F', 'name': 'Cummins'},
    {'isin': 'FR0011742329', 'symbol': 'M6P.F', 'name': 'McPhy Energy S.A.'},
    # { 'isin': 'US6541101050', 'name': 'Nikola Corporation','symbol': '8NI.F' },
    {'isin': 'DE000A0HL8N9', 'name': '2G Energy',  'symbol': '2GB.DE'}
]


"""
The ingest function needs to have this exact signature,
meaning these arguments passed, as shown below.
"""


def hydrogen_stock_data(environ,
                        asset_db_writer,
                        minute_bar_writer,
                        daily_bar_writer,
                        adjustment_writer,
                        calendar,
                        start_session,
                        end_session,
                        cache,
                        show_progress,
                        output_dir):

    # Get list of files from path
    # Slicing off the last part
    # 'example.csv'[:-4] = 'example'
    symbols = [asset['symbol'] for asset in universe]

    if not symbols:
        raise ValueError("No symbols found in universe.")

    # Prepare an empty DataFrame for dividends
    divs = pd.DataFrame(columns=['sid',
                                 'amount',
                                 'ex_date',
                                 'record_date',
                                 'declared_date',
                                 'pay_date']
                        )

    # Prepare an empty DataFrame for splits
    splits = pd.DataFrame(columns=['sid',
                                   'ratio',
                                   'effective_date']
                          )

    # Prepare an empty DataFrame for metadata
    metadata = pd.DataFrame(columns=('start_date',
                                     'end_date',
                                     'auto_close_date',
                                     'symbol',
                                     'exchange'
                                     )
                            )

    # Check valid trading dates, according to the selected exchange calendar
    sessions = calendar.sessions_in_range(start_session, end_session)

    # Get data for all stocks and write to Zipline
    daily_bar_writer.write(
        process_stocks(symbols, sessions, metadata, divs)
    )

    # Write the metadata
    asset_db_writer.write(equities=metadata)

    # Write splits and dividends
    adjustment_writer.write(splits=splits,
                            dividends=divs)


"""
Generator function to iterate stocks,
build historical data, metadata 
and dividend data
"""


def process_stocks(symbols, sessions, metadata, divs):
    # Loop the stocks, setting a unique Security ID (SID)
    for sid, asset in enumerate(universe):

        symbol = asset['symbol']
        isin = asset['isin']

        print('Loading {}...'.format(symbol))
        # Read the stock data from the database (with isin as the table name).
        df = pd.read_sql(isin, engine, index_col='Date',
                         parse_dates={'Date': '%Y-%m-%d'})

        # Check first and last date.
        start_date = df.index[0]
        end_date = df.index[-1]

        # Synch to the official exchange calendar
        df = df.reindex(sessions.tz_localize(None))[start_date:end_date]

        # Forward fill missing data
        df.fillna(method='ffill', inplace=True)

        # Drop remaining NaN
        df.dropna(inplace=True)

        # The auto_close date is the day after the last trade.
        ac_date = end_date + pd.Timedelta(days=1)

        # Add a row to the metadata DataFrame. Don't forget to add an exchange field.
        metadata.loc[sid] = start_date, end_date, ac_date, symbol, "FRA"

        # If there's dividend data, add that to the dividend DataFrame
        if 'dividend' in df.columns:

            # Slice off the days with dividends
            tmp = df[df['dividend'] != 0.0]['dividend']
            div = pd.DataFrame(data=tmp.index.tolist(), columns=['ex_date'])

            # Provide empty columns as we don't have this data for now
            div['record_date'] = pd.NaT
            div['declared_date'] = pd.NaT
            div['pay_date'] = pd.NaT

            # Store the dividends and set the Security ID
            div['amount'] = tmp.tolist()
            div['sid'] = sid

            # Start numbering at where we left off last time
            ind = pd.Index(range(divs.shape[0], divs.shape[0] + div.shape[0]))
            div.set_index(ind, inplace=True)

            # Append this stock's dividends to the list of all dividends
            divs = divs.append(div)

        yield sid, df
