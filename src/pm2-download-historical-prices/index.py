#!/usr/local/bin/python3.7

import pg8000
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, String, MetaData
from datetime import datetime, date, timedelta
import yfinance as yf
import investpy


print("Starting download job at", datetime.now())


connection_def = "postgresql+pg8000://quotes:clue0QS-train@raspberrypi/quotes"
engine = create_engine(connection_def)

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

hydrogens = [
    {'isin': 'CA0585861085',
     'symbol': 'PO0.F',
     'name': "Ballard Power",
     'class': 'stock'},

    {'isin': 'GB00B0130H42',
        'symbol': 'IJ8.F',
        'name': "ITM Power",
        'class': 'stock'},

    {'isin': 'NO0010081235',
        'symbol': 'D7G.F',
        'name': "Nel",
        'class': 'stock'},

    {'isin': 'SE0006425815',
        'symbol': '27W.F',
        'name': "Powercell Sweden",
        'class': 'stock'},

    {'isin': 'US72919P2020',
        'symbol': 'PLUN.F',
        'name': "Plug Power",
        'class': 'stock'},

    {'isin': 'NO0003067902',
        'symbol': '2HX.F',
        'name': "Hexagon Composites",
        'class': 'stock'},

    {'isin': 'FR0000120073',
        'symbol': 'AIL.DE',
        'name': "Air Liquide",
        'class': 'stock'},

    {'isin': 'IE00BZ12WP82',
        'symbol': 'LIN.F',
        'name': "Linde",
        'class': 'stock'},

    {'isin': 'US2310211063',
        'symbol': 'CUM.F',
        'name': 'Cummins',
        'class': 'stock'},

    {'isin': 'FR0011742329',
        'symbol': 'M6P.F',
        'name': 'McPhy Energy S.A.',
        'class': 'stock'},

    {'isin': 'US6541101050',
        'name': 'Nikola Corporation',
        'symbol': '8NI.F',
        'class': 'stock'},

    {'isin': 'DE000A0HL8N9',
        'name': '2G Energy',
        'symbol': '2GB.DE',
        'class': 'stock'}
]

universe = etfs + hydrogens


for asset in universe:
    table_name = asset['isin']
    sql = """
        CREATE TABLE IF NOT EXISTS public."{table_name}" (
            "Date" timestamp NULL,
            "Close" float8 NULL,
            "High" float8 NULL,
            "Low" float8 NULL,
            "Open" float8 NULL,
            "Volume" integer NULL,
            "Dividends" float8 NULL,
            "Stock Splits" float8 NULL
        );
        """.format(table_name=table_name)
    engine.execute(sql)


def retrieve_latest_date(engine, isin):
    latest_date_sql = 'select MAX("Date") from "{isin}";'.format(isin=isin)
    latest_date_result = engine.execute(latest_date_sql)
    return latest_date_result.first()[0]


def tommorow():
    return (datetime.now() + timedelta(days=1)).date()


def next_day(d):
    return (date(2000, 1, 1) if d == None else (d + timedelta(days=1)).date())


def get_iso_date(d: date):
    return d.strftime('%Y-%m-%d')


def download_stock_data(symbol: str, from_date: datetime.date, to_date: datetime.date):
    """Download the stock data (using yfinance)"""

    stock = yf.Ticker(symbol)
    data = stock.history(
        start=get_iso_date(from_date),
        end=get_iso_date(to_date),
        auto_adjust=True
    )
    return data


def download_etf_data(name: str, from_date: datetime.date, end_date: datetime.date):
    """Download the ETF data (using investpy)"""

    data = investpy.etfs.get_etf_historical_data(
        name, 'germany', from_date.strftime(
            '%d/%m/%Y'), to_date.strftime('%d/%m/%Y')
    )
    data.drop(axis=1, labels=['Currency', 'Exchange'], inplace=True)
    return data


def download_data(asset, from_date, to_date):
    """Download data for an asset

    Parameters
    ----------
    asset : obj
        an object describing the asset to download
    from_date : date
        download the data from this date
    to_date: date
        download the until this date

    Returns
    -------
    pd.Dataframe:
        A pandas Dataframe which contains the asset data in the specified date frame.
    """

    if asset['class'] == 'stock':
        # Use stock symbols
        return download_stock_data(asset['symbol'], from_date, to_date)
    else:
        # Use ETF names
        return download_etf_data(asset['name'], from_date, to_date)


for asset in etfs + hydrogens:
    isin = asset['isin']
    name = asset['name']
    symbol = asset['symbol']

    latest_date = retrieve_latest_date(engine, isin)
    from_date = next_day(latest_date)
    to_date = datetime.now().date()

    print("Downloading data for", name, "in the range from",
          str(from_date), "till", str(to_date))

    if(from_date < to_date):
        df = download_data(asset, from_date, to_date)

        print("Retrieved", df['Close'].count(), "entries.")
        print(df.head())
        df.to_sql(isin, engine, if_exists='append')

    else:
        print("The latest data is from " +
              str(latest_date.date()) + ",", "no fresh data.")

    print("\n")

print("Download job completed at", datetime.now())
