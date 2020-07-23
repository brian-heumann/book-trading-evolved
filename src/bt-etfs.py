# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %% [markdown]
# # My GMV ETF Portfolio

# %%
from zipline import run_algorithm
from zipline.api import order_target_percent, symbol, schedule_function, date_rules, time_rules

from datetime import datetime
import pytz

import matplotlib.pyplot as plt

import pyfolio as pf


# %%
def initialize(context):
    context.universe = [
        {'isin': 'IE00BKM4GZ66',
         'symbol': 'IS3N.F',
         'weight': 0.04,
         'name': 'iShares Core MSCI Emerging Markets IMI UCITS'},

        {'isin': 'IE00BP3QZB59',
         'symbol': 'IS3S.DE',
         'weight': 0.055,
         'name': 'iShares MSCI World Value Factor UCITS'},

        {'isin': 'IE00BF4RFH31',
         'symbol': 'IUSN.F',
         'weight': 0.08,
         'name': 'iShares MSCI World Small Cap UCITS USD Acc'},

        {'isin': 'IE00BL25JP72',
         'symbol': 'XDEM.DE',
         'weight': 0.21,
         'name': 'db x-trackers MSCI World Mom Factor DR 1C'},

        {'isin': 'IE00BL25JL35',
         'symbol': 'XDEQ.DE',
         'weight': 0.17,
         'name': 'db x-trackers MSCI World Quality Factor DR 1C',
         'class': 'etf'}
    ]
    # Schedule the daily trading routine for once per month
    schedule_function(handle_data, date_rules.month_start(0),
                      time_rules.market_close())


# %%
def handle_data(context, data):

    for asset in context.universe:
        # For all positions in the universe allocate the weight
        #
        position = symbol(asset['symbol'])
        weight = asset['weight']

        if data.can_trade(position):
            order_target_percent(position, weight)


# %%
def analyze2(context, perf):
    returns, positions, transactions = pf.utils.extract_rets_pos_txn_from_zipline(
        perf)
    pf.create_returns_tear_sheet(returns, benchmark_rets=None)


# %%
def analyze(context, perf):
    fig = plt.figure(figsize=(12, 8))

    # First Chart
    ax = fig.add_subplot(311)
    ax.semilogy(perf['portfolio_value'], linestyle='-',
                label="Equity Curve", linewidth=3.0)
    ax.legend()
    ax.grid(False)

    # Second Chart
    ax = fig.add_subplot(312)
    ax.plot(perf['gross_leverage'], linestyle='-',
            label="Exposure", linewidth=1.0)
    ax.legend()
    ax.grid(True)

    # Third Chart
    # Second Chart
    ax = fig.add_subplot(313)
    ax.plot(perf['returns'], linestyle='-.', label="Returns", linewidth=1.0)
    ax.legend()
    ax.grid(True)


# %%
# Run the backtest
#
start = datetime(2018, 1, 1, tzinfo=pytz.UTC)
end = datetime(2020, 7, 21, tzinfo=pytz.UTC)

result = run_algorithm(
    start=start,
    end=end,
    initialize=initialize,
    analyze=analyze,
    handle_data=handle_data,
    capital_base=10000,
    data_frequency='daily',
    bundle='etf_database_bundle'
)


# %%
