# Download historical prices
Downloads historical prices using yfinance and investpy libraries and stores them in a Postgres database.


## Downloading the historical prices
For downloading I use yfinance for stocks, and investypy for ETF data. Both have very similar download methods which return pandas dataframes. 

With pd.Dataframe.to_sql() the data is written into the database.


## Database configuration
I use a Postgres database on my raspberrypi.

| Element       | Value                                                         |
| ------------- | ------------------------------------------------------------- |
| Host          | raspberrypi                                                   |
| Port          | default (5321)                                                |
| Database      | quotes                                                        |
| User          | quotes (same as database)                                     |
| Password      | clue0QS-train'                                                |
| Driver string | "postgresql+pg8000://quotes:clue0QS-train@raspberrypi/quotes" |

For each asset I use the ISIN code as a table name in the database.


## Target environment

I usually use Jupyter notebooks to develop my Python scripts. When I am done, I copy the python script from the development machine onto my Raspberry Pi:

```sh
(local) > scp -rp src/pm2-download-historical-prices/ pi@raspberrypi:/home/pi/Projects
```



Log into the Raspberry Pi and change into the directory with the script:
```sh
> cd Projects/pm2-download-historical-prices
```

Install the development packages for libxml2 and libxslt:
```sh
> sudo apt-get install libxml2-dev libxslt-dev python-dev
```

Make sure that the prerequisite Python libraries are installed (consider using the `--user` option if you get user rights warnings during the normal installation):
```sh
> pip3.7 install pg8000 pandas sqlalchemy yfinance investpy
```

Start the pm2 manager to run the script, specifies the python 3.7 interpreter, a cron expression to restart this script every day on midnight, and restarts whenever the script changes:

```sh
> pm2 start index.py --name "download-historical-data-job" --interpreter=/usr/local/bin/python3.7 --cron "59 23 * * 1-6" --watch 
``` 