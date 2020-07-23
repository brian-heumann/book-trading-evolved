import pandas as pd
import pg8000
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, String, MetaData
from datetime import datetime, date, timedelta
import math


engine = create_engine(
    "postgresql+pg8000://quotes:clue0QS-train@raspberrypi/quotes")


def read_individual_returns(isin: str, engine) -> pd.DataFrame:
    """Retrieves the returns series for an equity in the database.

    Args:
    -----
    isin: 
        The name of the equity (also the name of the underlying database table).

    engine:
        The database connection

    Returns:
    --------
    A panda Dataframe object.
    """

    df = pd.read_sql(
        isin,
        engine,
        index_col='Date',
        columns=['Date', 'Close'],
        parse_dates={'Dates': '%Y-%m-%d'}
    )
    df[isin] = df['Close'].pct_change()
    df.drop(['Close'], axis='columns', inplace=True)
    return df


def merge_returns(left, right):
    """Merge dataframes into one.

    Args:
    -----   
        left, right:
            The dataframes to merge
    Returns:
    --------
        A merged dataframe with data aligned across the index
    """

    if left is None:
        return right
    else:
        return pd.merge(left, right, how='outer', left_index=True, right_index=True)


def read_returns(names: list, engine) -> pd.DataFrame:
    """Reads the data from a list of tables and merges the closing prices into a single dataframe.
    """
    merged_df = None
    for table_name in names:
        df = read_individual_returns(table_name, engine)
        merged_df = merge_returns(merged_df, df)

    return merged_df


def isNan(num):
    """Checks is num is nan"""
    return num != num
