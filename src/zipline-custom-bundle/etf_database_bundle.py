import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm  # Used for progress bar


engine = create_engine(
    "postgresql+pg8000://quotes:clue0QS-train@raspberrypi/quotes")

etfs = [
    {'isin': 'IE00BKM4GZ66',
        'symbol': 'IS3N.F',
        'name': 'iShares Core MSCI Emerging Markets IMI UCITS',
        'class': 'etf'},

    {'isin': 'IE00BP3QZB59',
        'symbol': 'IS3S.DE',
        'name': 'iShares MSCI World Value Factor UCITS',
        'class': 'etf'},

    {'isin': 'IE00BF4RFH31',
        'symbol': 'IUSN.F',
        'name': 'iShares MSCI World Small Cap UCITS USD Acc',
        'class': 'etf'},

    {'isin': 'IE00BL25JP72',
        'symbol': 'XDEM.DE',
        'name': 'db x-trackers MSCI World Mom Factor DR 1C',
        'class': 'etf'},

    {'isin': 'IE00BL25JL35',
        'symbol': 'XDEQ.DE',
        'name': 'db x-trackers MSCI World Quality Factor DR 1C',
        'class': 'etf'}
]

universe = etfs  # TBD: Analyze why etfs don't work

"""
The ingest function needs to have this exact signature,
meaning these arguments passed, as shown below.
"""


def etf_database_bundle(environ,
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
        process_stocks(universe, sessions, metadata, divs)
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


def process_stocks(universe, sessions, metadata, divs):
    # Loop the stocks, setting a unique Security ID (SID)

    sid = 0
    for asset in tqdm(universe):
        sid += 1
        isin = asset['isin']
        symbol = asset['symbol']

        # Ask the database for the data
        df = pd.read_sql(isin, engine, index_col='Date', parse_dates=[
            {'Date': '%Y-%m-%d'}])
        df.rename(columns={
            'Date': 'date',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume',
            'Dividends': 'dividends',
            'Stock Splits': 'split_ratio'
        }, inplace=True, copy=False)
        df.index.rename('date', inplace=True)

        # Check first and last date.
        start_date = df.index[0]
        end_date = df.index[-1]

        # Synch to the official exchange calendar
        df = df.reindex(sessions.tz_localize(None))[start_date:end_date]

        # Data on volume, dividends and split_ratio is not available for ETFs
        df['volume'] = df['dividends'] = df['split_ratio'] = 0

        # Forward fill missing data
        df.fillna(method='ffill', inplace=True)

        # Drop remaining NaN
        df.dropna(inplace=True)

        # The auto_close date is the day after the last trade.
        ac_date = end_date + pd.Timedelta(days=1)

        # Add a row to the metadata DataFrame.
        metadata.loc[sid] = start_date, end_date, ac_date, symbol, 'XFRA'

        tqdm.write(symbol)
        yield sid, df
