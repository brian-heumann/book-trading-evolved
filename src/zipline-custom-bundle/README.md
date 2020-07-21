# Create custom Zipline data bundle

## Prequisites
* Make sure that you are in the same Python environment, if not create and activate it before the next steps, e.g. with `conda activate <env_name>`
* Make sure you have installed zipline.


## Adding the bundle to Zipline      
1. To ensure zipline can find and import the bundle's module, copy the *.py files to the zipline directory. If you used conda, the zipline directory is `~/anaconda3/envs/py35/lib/python3.5/site-packages/zipline/data/bundles`. (This has costed me a lot of time to figure out!)

2. BUT the extension.py file is located in `~/.zipline`. (And this has costed me a lot of time to figure out, too!)
   
3. Run the command
   ```
   > zipline ingest -b <bundle_name>
