import cdsapi
import pandas as pd
import datetime
import os
from joblib import Parallel, delayed
import numpy as np
import xarray as xr



class ERA5DataMonthlyRequestProcessor:
    def __init__(self, start_date, end_date, output_folder, time_interval, spatial_coarsen, n_jobs=1, 
                 delete_raw=False, download_skip_exist=True, process_skip_exist=True):
        self.start_date = start_date
        self.end_date = end_date
        self.output_folder = output_folder
        self.time_interval = time_interval
        self.spatial_coarsen = spatial_coarsen
        self.n_jobs = n_jobs
        self.date_df = self.get_all_dates()
        self.date_df['year'] = self.date_df['year'].astype('str')
        self.date_df['month'] = [i.zfill(2) for i in self.date_df['month'].astype('str')]
        self.date_df['day'] = [i.zfill(2) for i in self.date_df['day'].astype('str')]
        self.delete_raw = delete_raw
        self.download_skip_exist = download_skip_exist
        self.process_skip_exist = process_skip_exist

        # check output folder
        os.makedirs(self.output_folder, exist_ok=True)
        

    def get_all_dates(self):
        date_list = [datetime.datetime.strptime('1970-01-01','%Y-%m-%d')+datetime.timedelta(days=i) for i in range(99999)]
        request_df = pd.DataFrame(pd.Series(date_list,name='date'))
        request_df['date'] = pd.to_datetime(request_df['date'])
        request_df['year'] = request_df.date.dt.year
        request_df['month'] = [str(i).split('-')[-2] for i in request_df.date.dt.date]
        request_df['day'] = [str(i).split('-')[-1] for i in request_df.date.dt.date]
        request_df = request_df[
            (request_df['date']>=pd.to_datetime(self.start_date)) & (request_df['date']<=pd.to_datetime(self.end_date))
        ]
        return request_df

    def get_ERA5_data_and_process(self):
        unique_month_df = self.date_df[['year','month']].drop_duplicates()
        dates = [(str(year), str(month).zfill(2)) for year, month in zip(unique_month_df.year, unique_month_df.month)]

        # Using Joblib to parallelize the downloading and processing
        Parallel(n_jobs=self.n_jobs)(delayed(self.get_data_and_process)(date) for date in dates)

    def get_data_and_process(self, date_tuple):
        year, month = date_tuple
        try:
            print(year, month)
            all_days = self.date_df[(self.date_df['year']==year) & (self.date_df['month']==month)]['day'].values
            all_days = [str(i).zfill(2) for i in all_days]

            # Decide whether download
            download_flag = True
            download_reason = None
            if self.download_skip_exist:
                if os.path.exists(os.path.join(self.output_folder, f'download_ERA5_{year}_{month}.nc')):
                    download_flag = False
                    download_reason = f'Download exists: download_ERA5_{year}_{month}.nc'

                all_days_exists_flag = True
                for day in all_days:
                    if not os.path.exists(os.path.join(self.output_folder, f'download_ERA5_{year}_{month}_{day}_processed.nc')):
                        all_days_exists_flag = False
                        break

                if self.process_skip_exist:
                    if all_days_exists_flag:
                        download_flag = False
                        download_reason = 'All processed day detected for this month. Not downloading.'

                # print(all_days_exists_flag, download_flag)

            print(f'Download: {download_flag}. Reason: {download_reason}')

            # download
            if download_flag:
                self.get_data(year, month)
            
            # process
            for day in all_days:
                # Decide whether process
                if os.path.exists(os.path.join(self.output_folder, f'download_ERA5_{year}_{month}_{day}_processed.nc')):
                    if self.process_skip_exist:
                        print(f'SKIP - Processed exists: download_ERA5_{year}_{month}_{day}_processed.nc')
                        continue

                # process
                print('Start Processing')
                self.process_ERA5(year, month, day)

            # Finally delete raws
            if self.delete_raw:
                try:
                    os.remove(os.path.join(self.output_folder, f'download_ERA5_{year}_{month}.nc'))
                except Exception as e:
                    print(f'Delete raw download_ERA5_{year}_{month}.nc fail: {e}')

        except Exception as e:
            print(f"Error processing {year}-{month}: {e}")

    def get_data(self,year,month):
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
                    'day':['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31'],
                    'time':['00:00','01:00','02:00','03:00','04:00','05:00','06:00','07:00','08:00',
                            '09:00','10:00','11:00','12:00','13:00','14:00','15:00','16:00',
                            '17:00','18:00','19:00','20:00','21:00','22:00','23:00'],
                },
                os.path.join(self.output_folder, f'download_ERA5_{year}_{month}.nc')
            )
        
    def process_ERA5(self, year, month, day):
        data = xr.open_dataset(os.path.join(self.output_folder, f'download_ERA5_{year}_{month}.nc'))
        data = data.sel(time=f'{year}-{month}-{day}')
        comb = data.resample(time=self.time_interval).mean()

        ## transforming the longitude to -180, 180
        aa = comb.sel(longitude=slice(180+1e-8, 360))
        aa['longitude'] = aa['longitude']-360
        bb = comb.sel(longitude=slice(0, 180))
        comb = xr.concat([aa,bb], dim='longitude')

        ## Spatial resample 3 to 1
        coarsen_weight = self.spatial_coarsen # 30km/9km = ~ 3
        res = comb.coarsen(
            longitude = coarsen_weight, # 3 into 1
            latitude = coarsen_weight,
            boundary='trim'
        ).mean()

        ## Saving
        res.to_netcdf(os.path.join(self.output_folder, f'download_ERA5_{year}_{month}_{day}_processed.nc'))




## Aggregate to 1D
# a = data[['u10','v10','t2m','sp','tcc','tp']].resample(time=time_interval).mean().rename({
#     'u10':'mean_u10',
#     'v10':'mean_v10',
#     't2m':'mean_t2m',
#     'sp':'mean_sp',
#     'tcc':'mean_tcc',
#     'tp':'mean_tp',
# })

# b = data[['t2m']].resample(time=time_interval).max().rename({
#     't2m':'max_t2m',
# })

# c = data[['t2m']].resample(time=time_interval).min().rename({
#     't2m':'min_t2m',
# })

# comb = xr.combine_by_coords([
#     a,b,c
# ])
        

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
