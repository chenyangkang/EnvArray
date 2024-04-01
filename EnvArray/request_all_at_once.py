import argparse
import cdsapi
import pandas as pd
import datetime
import os
from joblib import Parallel, delayed
import numpy as np
import xarray as xr

# Create the parser
parser = argparse.ArgumentParser(description='ERA downloader')

# Add arguments
parser.add_argument('from_year', type=int, help='year from')
parser.add_argument('to_year', type=int, help='year to')
parser.add_argument('njobs', type=int, help='njobs')
parser.add_argument('output_folder', type=str, help='output_folder')

# Parse the command line arguments
args = parser.parse_args()



def request_one_year(year):
    c=cdsapi.Client()
    c.retrieve(
            'reanalysis-era5-single-levels',
            {   
                'product_type': 'reanalysis',
                'format': 'netcdf',
                'variable':[
                    '10m_u_component_of_wind', '10m_v_component_of_wind', '100m_u_component_of_wind','100m_v_component_of_wind',
                    '2m_temperature','2m_dewpoint_temperature','sea_surface_temperature',
                    'surface_pressure', 'mean_sea_level_pressure',
                    'total_cloud_cover', 'low_cloud_cover','medium_cloud_cover','high_cloud_cover','cloud_base_height',
                    'total_precipitation','large_scale_precipitation',
                    'lake_cover', 'lake_mix_layer_temperature',
                    'snowmelt','snow_depth',
                    'surface_latent_heat_flux', 'surface_net_solar_radiation','downward_uv_radiation_at_the_surface',
                    'high_vegetation_cover','low_vegetation_cover','leaf_area_index_high_vegetation','leaf_area_index_low_vegetation',
                    'significant_height_of_combined_wind_waves_and_swell','mean_wave_direction'
                ],
                'year':year,
                'month':['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'],
                'day':['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31'],
                'time':['00:00','01:00','02:00','03:00','04:00','05:00','06:00','07:00','08:00',
                        '09:00','10:00','11:00','12:00','13:00','14:00','15:00','16:00',
                        '17:00','18:00','19:00','20:00','21:00','22:00','23:00'],
            },
            os.path.join(args.output_folder, f'download_ERA5_{year}.nc')
        )


Parallel(n_jobs=int(args.njobs))(delayed(request_one_year)(year) for year in list(range(args.from_year, args.to_year+1)))

