""" ERA5 values from S3
    ===================
    A sketch of extracting ERA5 fields from their S3 storage location.
    There's a limited amount of fields, time ranges, etc, available. 
    However, the setup is minimal and if running the scrip on an Amazon 
    instance then the download speeds between S3 nodes is much better than 
    would typically see when accessing the ECMWF servers directly. 

    Requires libraries for interacting with AWS (botocore, boto3).

    TODO: Currently assumes that user wants to download full years of data,
          should edit to work on a monthly basis to save time/space if needed.

    Avalible meteorological parameters (as of 05-2020) : 
    air_pressure_at_mean_sea_level
    air_temperature_at_2_metres
    air_temperature_at_2_metres_1hour_Maximum
    air_temperature_at_2_metres_1hour_Minimum
    dew_point_temperature_at_2_metres
    eastward_wind_at_100_metres
    eastward_wind_at_10_metres
    lwe_thickness_of_surface_snow_amount
    northward_wind_at_100_metres
    northward_wind_at_10_metres
    precipitation_amount_1hour_Accumulation
    sea_surface_temperature
    sea_surface_wave_from_direction
    sea_surface_wave_mean_period
    significant_height_of_wind_and_swell_waves
    snow_density
    surface_air_pressure

    Example call : 
    python pull-era5-s3.py \
        --region_lab NorthAtlantic \
        --metprm sea_surface_temperature \
        --storage_path <<LOCAL DIRECTORY PATH>> \ 
        --min_year 2000 \ 
        --max_year 2000

    python pull-era5-s3.py \
        --region_lab NorthAtlantic \
        --metprm air_temperature_at_2_metres \
        --storage_path /data/ERA5 \ 
        --min_year 2000 \ 
        --max_year 2000


    """

import xarray as xr
import itertools
import botocore
import argparse
import datetime
import boto3
import os

# ::<>::x::<>::x::<>::x::<>::x::<>::x::<>::x::<>::x::<>::x::<>::x::<>::x::<>:: #
parser = argparse.ArgumentParser()
parser.add_argument(
    '--region_lab',
    type=str,
    help="shorthand for sub-region to keep"
)
parser.add_argument(
    '--metprm',
    type=str,
    default='eastward_wind_at_10_metres',
    help="name of desired meteorological parmater"
)
parser.add_argument(
    '--storage_path',
    type=str,
    default='~/Vault/ERA5',
    help="name of storage directory"
)
parser.add_argument(
    '--min_year',
    type=int,
    default=1979,
    help="earliest year to download"
)
parser.add_argument(
    '--max_year',
    type=int,
    default=2020,
    help="latest year to download"
)
args = parser.parse_args()
args = vars(args)

metprm = args['metprm']

""" Define region boxes
    -------------------
    Define a region as a list of [min_lon,max_lon,min_lat,max_lat].
    Keep a running dictionary of various regions from different projects
    for convience, but have to choose which one to use for a particular
    run of the script.
    Can't download a particular subregion, rather have to download the 
    full file, then chop a region out of it. 
    """

region_boxes = {'NorthSea'      : [ 10, 12,49,62],
                'BVI'           : [-70,-60,13,25],
                'NorthAtlantic' : [-60, 25,40,70],
                'Ukraine'       : [ 20, 42,43,53]
}
region_lab = args['region_lab']
region_box = {
	'min_lat':region_boxes[args['region_lab']][2],
	'max_lat':region_boxes[args['region_lab']][3],
	'min_lon':region_boxes[args['region_lab']][0],
	'max_lon':region_boxes[args['region_lab']][1]
}


""" Move to local storage directory
    ------------------------------- """
os.chdir(args['storage_path'])

""" Identify location of files on aws-s3 servers
    -------------------------------------------- """
era5_bucket = 'era5-pds'
#/No AWS keys required/#
client = boto3.client(
    's3',config=botocore.client.Config(
        signature_version=botocore.UNSIGNED)
)

""" Select the years and months to download 
    ---------------------------------------
    Current S3 repository has years from 1979 to 2007 
    """
years = range(args['min_year'],(args['max_year']+1))
months = range(1,13) 

# ::<>::x::<>::x::<>::x::<>::x::<>::x::<>::x::<>::x::<>::x::<>::x::<>::x::<>:: #

for y,m in itertools.product(years,months) :
    date = datetime.date(y,m,1)
    # ... file path patterns ...
    s3_data_ptrn = 'cds/{year}/{month}/data/{metprm}.nc'
    data_file_ptrn = '{year}{month}_{metprm}.nc'
    year = date.strftime('%Y')
    month = date.strftime('%m')
    s3_data_key = s3_data_ptrn.format(
        year=year, month=month, metprm=metprm)
    data_file = data_file_ptrn.format(
        year=year, month=month, metprm=metprm)

    # ... check if file already exists ...
    if not os.path.isfile(data_file): 
        print("Downloading %s from S3..." % s3_data_key)
        client.download_file(era5_bucket, s3_data_key, data_file)

    ds = xr.open_dataset(data_file)
    #ds = ds.load()

    """ The simulation grid is reported with longitude units in the 0:360
        range, which makes it hard to define a continuous box from
        west to east near the prime meridian. So switch
        things. The difficult part is that once relabel the
        longitude values then need to resort the values. This
        requires loading the whole field into memory, which can
        exceed memory restrictions on some machines. To work
        around that first subset the region, write out the file,
        then reload the smaller file in and sort along the indexes
        from there.
        """

    lat = ds.lat.values
    lon = ds.lon.values
    ds = ds.loc[dict(
        lon=lon[
            (lon>=(360+region_box['min_lon']))
            & (lon<=(360+region_box['max_lon']))
        ],
        lat=lat[(lat>=region_box['min_lat'])
            & (lat<=region_box['max_lat'])
        ]
    )]
    ds = ds.assign_coords(lon=(((ds.lon + 180) % 360) - 180))
    ## In older xarray versions would manually overwrite coordinate
    #| lon = ds.lon.values
    #| lon[lon>180.0] = lon[lon>180.0]-360.0
    #| ds.lon.values = lon
    os.remove(data_file)
    ds.to_netcdf(
        'tmp_'+metprm+'_'+str(y)+'-'+str(m).zfill(2)+'.nc')
    ds = xr.open_dataset(
        'tmp_'+metprm+'_'+str(y)+'-'+str(m).zfill(2)+'.nc')
    ds = ds.sortby('lon')
    ds.to_netcdf(
        metprm+'_'+str(y)+'-'+str(m).zfill(2)+'.nc')
    os.remove(
        'tmp_'+metprm+'_'+str(y)+'-'+str(m).zfill(2)+'.nc')


# ::<>::x::<>::x::<>::x::<>::x::<>::x::<>::x::<>::x::<>::x::<>::x::<>::x::<>:: #
