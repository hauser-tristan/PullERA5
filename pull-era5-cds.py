""" ERA5 values from CDS
    ====================
    A sketch of extracting ERA5 fields from their Climate Data Store
    storage location. Data processing happens on the server, so large
    requests can take some time before they are ready for downloading.

    Done using library provided by service, can download through conda-forge

    TODO: Currently assumes that user wants to download full years of data,
    TODO: Currently assumes that user wants to download single layer data.

    Example call : 
    python pull-era5-s3.py \
        --region_lab NorthAtlantic \
        --metprm sea_surface_temperature \
        --storage_path <<LOCAL DIRECTORY PATH>> \ 
        --min_year 2000 \ 
        --max_year 2000

    """

import numpy as np
import argparse
import cdsapi
import os

# ::<>::x::<>::x::<>::x::<>::x::<>::x::<>::x::<>::x::<>::x::<>::x::<>::x::<>:: #
parser = argparse.ArgumentParser()
parser.add_argument(
    '--storage_path',
    type=str,
    default='~/Vault/ERA5',
    help="name of storage directory"
)
parser.add_argument(
    '--region_lab',
    type=str,
    default='NorthAtlantic',
    help="shorthand for sub-region to keep"
)
parser.add_argument(
    '--metprm',
    type=str,
    default='sea_surface_temperature',
    help="name of desired meteorological parmater"
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
#print(args)
metprm = args['metprm']

""" Link to CDS
    ----------- 
    Do this using library provided by service
    """
client = cdsapi.Client()

""" Define region boxes
    -------------------
    Define a region as a list of [min_lon,max_lon,min_lat,max_lat].
    Keep a running dictionary of various regions from different projects
    for convience, but have to choose which one to use for a particular
    run of the script.
    """
region_boxes = {'NorthSea' : [10,12,49,62],
                'BVI'      : [-70,-60,13,25],
                'NorthAtlantic' : [-60,25,40,70]
}
region_lab = args['region_lab']
region_box = {
	'min_lat':region_boxes[args['region_lab']][2],
	'max_lat':region_boxes[args['region_lab']][3],
	'min_lon':region_boxes[args['region_lab']][0],
	'max_lon':region_boxes[args['region_lab']][1]
}

""" Select the years and months to download 
    --------------------------------------- """
years = [str(x) for x in range(args['min_year'],(args['max_year']+1))]
months = [str(x) for x in range(1,13)]


""" Move to local storage directory
    ------------------------------- """
os.chdir(args['storage_path'])

# ::<>::x::<>::x::<>::x::<>::x::<>::x::<>::x::<>::x::<>::x::<>::x::<>::x::<>:: #
for year in years :
    if not os.path.isfile(year+'_'+metprm+'.nc'): 
        print("Downloading %s for %s " %(metprm, year))
    else :
        print("%s data for %s already stored" %(metprm, year))
        break

    request = {
        'product_type':'reanalysis',
        'format':'netcdf',
        'variable':metprm,
        'year':year,
        'month':months,
        'day':[
            '01','02','03',        
            '04','05','06',        
            '07','08','09',        
            '10','11','12',        
            '13','14','15',
            '16','17','18',
            '19','20','21',
            '22','23','24',
            '25','26','27',   
            '28','29','30',
            '31'
        ],
        'area': [
            region_box['min_lat'],
            region_box['min_lon'],
            region_box['max_lat'],
            region_box['max_lon']
        ],
        'grid': [0.25, 0.25], 
        'time':[
            '00:00','01:00','02:00',
            '03:00','04:00','05:00',
            '06:00','07:00','08:00',
            '09:00','10:00','11:00',
            '12:00','13:00','14:00',
            '15:00','16:00','17:00',   
            '18:00','19:00','20:00',
            '21:00','22:00','23:00',
        ]
    }
    client.retrieve(
        'reanalysis-era5-single-levels',
        request,
        metprm+'_'+year+'.nc'
    )

