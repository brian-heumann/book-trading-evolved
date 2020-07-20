# Create custom Zipline data bundle

## Prequisites
* Make sure that you are in the same Python environment, if not create and activate it before the next steps, e.g. with `conda activate <env_name>`
* Make sure you have installed zipline.


## Adding the bundle to Zipline      
1. Copy the *.py files to the zipline directory, on Linux this is in `~/.zipline`. The name of the file is also the bundle's name.
   
2. Run the command
   ```
   > zipline ingest -b <bundle_name>
