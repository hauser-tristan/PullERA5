# PullERA5
Convenience scripts for downloading ERA5-Reanalysis data from different sources.

Pulling from the S3 database is faster, but has more limited data selection.

Making requests directly to the CDS server allows more fine tuning of the process (although the script needs a bit more fiddling to make all of these options avalible) but can take quite a bit of time as the CDS server sorts through the large quantities of raw data. 
