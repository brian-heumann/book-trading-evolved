import pg8000
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, String, MetaData
from datetime import datetime, date, timedelta
import yfinance as yf

connection_def = "postgresql+pg8000://quotes:clue0QS-train@raspberrypi/quotes"
engine = create_engine(connection_def)

etfs = [
    {'isin': 'IE00BKM4GZ66',
        'symbol': 'IS3N.F',
        'name': 'iShares Core MSCI Emerging Markets IMI UCITS'},
    {'isin': 'IE00BP3QZB59',
        'symbol': 'IS3S.DE',
        'name': 'iShares MSCI World Value Factor UCITS'},
    {'isin': 'IE00BF4RFH31',
        'symbol': 'IUSN.DE',
        'name': 'iShares MSCI World Small Cap UCITS USD Acc'},
    {'isin': 'IE00BL25JP72',
        'symbol': 'XDEM.DE',
        'name': 'db x-trackers MSCI World Mom Factor DR 1C'},
    {'isin': 'IE00BL25JL35',
        'symbol': 'XDEQ.DE',
        'name': 'db x-trackers MSCI World Quality Factor DR 1C'}
]

hydrogens = [
    {'isin': 'CA0585861085',
     'symbol': 'PO0.F',
     'name': "Ballard Power"},

    {'isin': 'GB00B0130H42',
        'symbol': 'IJ8.F',
        'name': "ITM Power"},

    {'isin': 'NO0010081235',
        'symbol': 'D7G.F',
        'name': "Nel"},

    {'isin': 'SE0006425815',
        'symbol': '27W.F',
        'name': "Powercell Sweden"},

    {'isin': 'US72919P2020',
        'symbol': 'PLUN.F',
        'name': "Plug Power"},

    {'isin': 'NO0003067902',
        'symbol': '2HX.F',
        'name': "Hexagon Composites"},

    {'isin': 'FR0000120073',
        'symbol': 'AIL.DE',
        'name': "Air Liquide"},

    {'isin': 'IE00BZ12WP82',
        'symbol': 'LIN.F',
        'name': "Linde"},

    {'isin': 'US2310211063',
        'symbol': 'CUM.F',
        'name': 'Cummins'},

    {'isin': 'FR0011742329',
        'symbol': 'M6P.F',
        'name': 'McPhy Energy S.A.'},

    {'isin': 'US6541101050',
        'name': 'Nikola Corporation',
        'symbol': '8NI.F'},

    {'isin': 'DE000A0HL8N9',
        'name': '2G Energy',
        'symbol': '2GB.DE'}
]

universe = etfs + hydrogens
universe


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


for asset in universe:
    isin = asset['isin']
    name = asset['name']
    symbol = asset['symbol']

    latest_date = retrieve_latest_date(engine, isin)
    from_date = next_day(latest_date)
    to_date = datetime.now().date()

    if(from_date < to_date):
        stock = yf.Ticker(symbol)
        df = stock.history(
            start=get_iso_date(from_date),
            end=get_iso_date(to_date),
            auto_adjust=True
        )

        print("Retrieved", df['Close'].count(), "entries for",
              name, "from", from_date, "till", to_date, ".")
        print(df.head())
        print("\n\n")
        df.to_sql(isin, engine, if_exists='append')

    else:
        print("The latest data for", isin, "is from " +
              str(latest_date.date()) + ",", "no need to download new data.")
