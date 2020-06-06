# PullERA5
Convenience scripts for downloading ERA5-Reanalysis data from different sources.

Pulling from the S3 database is faster, but has more limited data selection.

Making requests directly to the CDS server allows more fine tuning of the process (although the script needs a bit more fiddling to make all of these options available) but can take quite a bit of time as the CDS server sorts through the large quantities of raw data. 

## Environment Setup
These instructions are for `conda` but any library installer _should_ work.

### Install libraries needed for Open S3 downloads

`conda install -c conda-forge xarray`
`conda install -c conda-forge botocore`
`conda install -c conda-forge boto3`
`conda install -c conda-forge netcdf4`

### Install (additional) libraries needed for CDS downloads

`conda install -c conda-forge cdsapi`

### Set CDS credentials on machine

Go to home directory (`cd $HOME`) and create a file called `.cdsapirc` stating: 

    url: https://cds.climate.copernicus.eu/api/v2
    key: {UID}:{API key}
    verify:0

The values of {UID} and {API} can be found by first registering at the CDS web interface site and then viewing the "API key" section of your profile information.