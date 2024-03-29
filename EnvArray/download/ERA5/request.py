import cdsapi
import pandas as pd
import datetime
import os
from .process import process_ERA5


def get_all_dates(start_date, end_date):
    date_list = [datetime.datetime.strptime('1970-01-01','%Y-%m-%d')+datetime.timedelta(days=i) for i in range(99999)]
    # date_list = [i for i in date_list if (i.year<=2020 and i.year>=2010)]
    request_df = pd.DataFrame(pd.Series(date_list,name='date'))
    request_df['date'] = pd.to_datetime(request_df['date'])
    request_df['year'] = request_df.date.dt.year
    request_df['month'] = [str(i).split('-')[-2] for i in request_df.date.dt.date]
    request_df['day'] = [str(i).split('-')[-1] for i in request_df.date.dt.date]
    request_df = request_df[
        (request_df['date']>=pd.to_datetime(start_date)) & (request_df['date']<=pd.to_datetime(end_date))
    ]
    return request_df


def get_data(year,month,day,output_folder):
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
            'month':month,
            'day':day,
            'time':['00:00','01:00','02:00','03:00','04:00','05:00','06:00','07:00','08:00',
                    '09:00','10:00','11:00','12:00','13:00','14:00','15:00','16:00',
                    '17:00','18:00','19:00','20:00','21:00','22:00','23:00'],
        },
        os.path.join(output_folder, f'download_ERA5_{year}_{month}_{day}.nc')
    )


def get_ERA5_data_and_process(start_date, end_date, time_interval, spatial_resolution, output_folder):
    date_df = get_all_dates(start_date, end_date)
    for year in date_df.year.unique():
        sub = date_df[date_df.year==year]
        for month in sub.month.unique():
            subsub = sub[sub.month==month]
            for day in subsub.day.unique():

                get_data(str(year),
                         str(month).zfill(2),
                         str(day).zfill(2),
                         output_folder)
                
                process_ERA5(str(year), 
                             str(month).zfill(2), 
                             str(day).zfill(2), 
                             time_interval, 
                             spatial_resolution, 
                             output_folder) # aggregate to 1 day and 30 km


# def get_ERA5_data(start_date, end_date, time_interval, spatial_resolution, output_folder):
#     date_df = get_all_dates(start_date, end_date)
#     for year in date_df.year.unique():
#         sub = date_df[date_df.year==year]
#         for month in sub.month.unique():
#             subsub = sub[sub.month==month]
#             get_data(year,month,subsub,output_folder)

# def ERA5_process(start_date, end_date, time_interval, spatial_resolution, output_folder):
#     date_df = get_all_dates(start_date, end_date)
#     for year in date_df.year.unique():
#         sub = date_df[date_df.year==year]
#         for month in sub.month.unique():
#             subsub = sub[sub.month==month]
#             process_ERA5(year, month, time_interval, spatial_resolution, output_folder) # aggregate to 1 day and 50 km
