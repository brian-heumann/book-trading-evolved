
import investpy
import pg8000
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, String, MetaData
from datetime import datetime, date, timedelta


connection_def = "postgresql+pg8000://quotes:clue0QS-train@raspberrypi/quotes"
engine = create_engine(connection_def)

universe = [
    {'isin': 'IE00BKM4GZ66',
     'name': 'iShares Core MSCI Emerging Markets IMI UCITS'}, 
    {'isin': 'IE00BP3QZB59',
     'name': 'iShares MSCI World Value Factor UCITS'}, 
    {'isin': 'IE00BF4RFH31',
     'name': 'iShares MSCI World Small Cap UCITS USD Acc'}, 
    {'isin': 'IE00BL25JP72',
     'name': 'db x-trackers MSCI World Mom Factor DR 1C'}, 
    {'isin': 'IE00BL25JL35',
     'name': 'db x-trackers MSCI World Quality Factor DR 1C'}
]

for asset in universe:
    table_name = asset['isin']
    sql = """
        CREATE TABLE IF NOT EXISTS public."{table_name}" (
            "Date" timestamp NULL,
            "Close" float8 NULL,
            "Currency" text NULL,
            "Exchange" text NULL,
            "High" float8 NULL,
            "Low" float8 NULL,
            "Open" float8 NULL
        );
        """.format(table_name=table_name)
    engine.execute(sql)


def tommorow():
    return (datetime.now() + timedelta(days=1)).date()


def next_day(d):
    if d is None:
        return date(2000, 1, 1)
    else: 
        return (d + timedelta(days=1)).date()


for asset in universe:
    isin = asset['isin']
    etf = asset['name']

    latest_date_sql = 'select MAX("Date") from "{isin}";'.format(isin=isin)
    latest_date_result = engine.execute(latest_date_sql).first()[0]

    from_date = next_day(latest_date_result)
    to_date = datetime.now().date()

    if(from_date < to_date):
        df = investpy.etfs.get_etf_historical_data(
            etf,
            'germany',
            from_date.strftime("%d/%m/%Y"),
            to_date.strftime("%d/%m/%Y")
        )
        print("Retrieved", df['Close'].count(), 
              "entries for", isin, "from", from_date, "till", to_date, ".")
        df.to_sql(isin, engine, if_exists='append')

    else:
        print("Latest data for", isin, "is from", str(latest_date_result),
              "hence no download.")




