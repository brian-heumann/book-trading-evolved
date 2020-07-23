#!/bin/bash
echo "Ingest fresh data for etf_database_bundle."

echo "Cleaning zipline bundle..."
zipline clean -b etf_database_bundle --before 2099-12-31

echo "Copying bundle..."
cp etf_database_bundle.py /home/heumann/anaconda3/envs/py35/lib/python3.5/site-packages/zipline/data/bundles/

echo "Ingesting new bundle data..."
zipline ingest -b etf_database_bundle

echo "Done."